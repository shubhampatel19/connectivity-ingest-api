package ingestion

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/auth"
)

// memStore is an in-memory ObjectStore.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    map[string]int
	down    bool
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}, puts: map[string]int{}} }

var errStoreDown = errors.New("connection refused")

func (m *memStore) Put(_ context.Context, key string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errStoreDown
	}
	if int64(len(data)) != size {
		return fmt.Errorf("put %s: got %d bytes, size said %d", key, len(data), size)
	}
	m.objects[key] = data
	m.puts[key]++
	return nil
}

func (m *memStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, errStoreDown
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return false, errStoreDown
	}
	_, ok := m.objects[key]
	return ok, nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, errStoreDown
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errStoreDown
	}
	delete(m.objects, key)
	return nil
}

func (m *memStore) keysWithPrefix(prefix string) []string {
	keys, _ := m.List(context.Background(), prefix)
	return keys
}

// ---- harness ----

type bundleHarness struct {
	t      *testing.T
	h      http.Handler
	signer *auth.Signer
	store  *memStore
	kafka  *fakeProducer
}

func newBundleHarness(t *testing.T, chunkSize int64) *bundleHarness {
	t.Helper()
	signer, _ := auth.NewSigner("test-secret-0123456789")
	store, kafka := newMemStore(), &fakeProducer{}
	s := NewServer(Config{
		EnrollmentKey: "enroll-me", MaxBodyBytes: 1 << 20, ProduceTimeout: time.Second,
		Bundles: BundleConfig{ChunkSize: chunkSize, MaxBundleBytes: 1 << 20},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), signer, kafka, store, prometheus.NewRegistry())
	s.now = func() time.Time { return testNow }
	return &bundleHarness{t: t, h: s.Handler(nil), signer: signer, store: store, kafka: kafka}
}

func (b *bundleHarness) do(method, path, device string, body io.Reader, extra map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range deviceHeaders(b.signer, device) {
		req.Header.Set(k, v)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	b.h.ServeHTTP(rr, req)
	return rr
}

func (b *bundleHarness) init(device string, req wire.BundleInitRequest) (*httptest.ResponseRecorder, wire.BundleStatus) {
	body, _ := json.Marshal(req)
	rr := b.do(http.MethodPost, "/v1/bundles", device, bytes.NewReader(body), nil)
	var st wire.BundleStatus
	_ = json.Unmarshal(rr.Body.Bytes(), &st)
	return rr, st
}

func (b *bundleHarness) chunk(device, id string, i int, data []byte) *httptest.ResponseRecorder {
	sum := sha256.Sum256(data)
	return b.do(http.MethodPut, fmt.Sprintf("/v1/bundles/%s/chunks/%d", id, i), device, bytes.NewReader(data),
		map[string]string{wire.HeaderChunkSHA256: hex.EncodeToString(sum[:])})
}

func (b *bundleHarness) complete(device, id string) (*httptest.ResponseRecorder, wire.BundleStatus) {
	rr := b.do(http.MethodPost, "/v1/bundles/"+id+"/complete", device, strings.NewReader("{}"), nil)
	var st wire.BundleStatus
	_ = json.Unmarshal(rr.Body.Bytes(), &st)
	return rr, st
}

func newBundle(t *testing.T, device string, size int) (wire.BundleInitRequest, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	sum := sha256.Sum256(data)
	return wire.BundleInitRequest{BundleID: "bundle-1", DeviceID: device, SizeBytes: int64(size),
		SHA256: hex.EncodeToString(sum[:]), Reason: wire.ReasonAlert, CollectedAt: testNow}, data
}

func uploadAll(b *bundleHarness, device string, req wire.BundleInitRequest, data []byte, chunkSize int) {
	for i := 0; i*chunkSize < len(data); i++ {
		end := min((i+1)*chunkSize, len(data))
		if rr := b.chunk(device, req.BundleID, i, data[i*chunkSize:end]); rr.Code != http.StatusNoContent {
			b.t.Fatalf("chunk %d: got %d: %s", i, rr.Code, rr.Body)
		}
	}
}

// ---- tests ----

func TestBundleUploadEndToEnd(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)

	rr, st := b.init("dev-1", req)
	if rr.Code != http.StatusOK || st.State != wire.BundleUploading || st.ChunkCount != 3 || len(st.ReceivedChunks) != 0 {
		t.Fatalf("init: %d %+v", rr.Code, st)
	}
	uploadAll(b, "dev-1", req, data, 100)

	rr, st = b.complete("dev-1", req.BundleID)
	if rr.Code != http.StatusOK || st.State != wire.BundleComplete {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body)
	}
	if got := b.store.objects[bundleKey("dev-1", req.BundleID)]; !bytes.Equal(got, data) {
		t.Fatal("stored bundle does not match uploaded bytes")
	}
	if left := b.store.keysWithPrefix("uploads/"); len(left) != 0 {
		t.Fatalf("upload session not cleaned up: %v", left)
	}
	if len(b.kafka.records) != 1 || b.kafka.records[0].topic != wire.TopicBundles || b.kafka.records[0].key != "dev-1" {
		t.Fatalf("expected one bundles.uploaded event keyed by device, got %+v", b.kafka.records)
	}
	var ev wire.BundleUploadedEvent
	_ = json.Unmarshal(b.kafka.records[0].value, &ev)
	if ev.ObjectKey != bundleKey("dev-1", req.BundleID) || ev.SHA256 != req.SHA256 || ev.Reason != wire.ReasonAlert {
		t.Fatalf("unexpected event: %+v", ev)
	}

	// Completing again (lost response, device retries) is harmless.
	if rr, st := b.complete("dev-1", req.BundleID); rr.Code != http.StatusOK || st.State != wire.BundleComplete {
		t.Fatalf("repeat complete: %d %s", rr.Code, rr.Body)
	}
	if rr, st := b.init("dev-1", req); rr.Code != http.StatusOK || st.State != wire.BundleComplete {
		t.Fatalf("init after completion should report complete: %d %+v", rr.Code, st)
	}
	if len(b.kafka.records) != 1 {
		t.Fatalf("repeat calls must not republish, got %d events", len(b.kafka.records))
	}
}

func TestBundleInitReportsReceivedChunks(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)
	b.init("dev-1", req)
	b.chunk("dev-1", req.BundleID, 0, data[:100])
	b.chunk("dev-1", req.BundleID, 2, data[200:])

	rr, st := b.init("dev-1", req)
	if rr.Code != http.StatusOK || fmt.Sprint(st.ReceivedChunks) != "[0 2]" || fmt.Sprint(st.Missing()) != "[1]" {
		t.Fatalf("resume status wrong: %d %+v", rr.Code, st)
	}
	status := b.do(http.MethodGet, "/v1/bundles/"+req.BundleID, "dev-1", nil, nil)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"received_chunks":[0,2]`) {
		t.Fatalf("GET status: %d %s", status.Code, status.Body)
	}
}

func TestBundleInitRejectsReusedIDWithDifferentContent(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, _ := newBundle(t, "dev-1", 250)
	b.init("dev-1", req)
	other, _ := newBundle(t, "dev-1", 250)
	if rr, _ := b.init("dev-1", other); rr.Code != http.StatusConflict {
		t.Fatalf("got %d, want 409", rr.Code)
	}
}

func TestBundleChunkValidation(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)

	if rr := b.chunk("dev-1", req.BundleID, 0, data[:100]); rr.Code != http.StatusNotFound {
		t.Fatalf("chunk before init: got %d, want 404", rr.Code)
	}
	b.init("dev-1", req)

	corrupt := b.do(http.MethodPut, "/v1/bundles/"+req.BundleID+"/chunks/0", "dev-1", bytes.NewReader(data[:100]),
		map[string]string{wire.HeaderChunkSHA256: strings.Repeat("0", 64)})
	if corrupt.Code != http.StatusConflict {
		t.Fatalf("checksum mismatch: got %d, want 409 so the device resends", corrupt.Code)
	}
	if rr := b.chunk("dev-1", req.BundleID, 0, data[:90]); rr.Code != http.StatusBadRequest {
		t.Fatalf("short chunk: got %d, want 400", rr.Code)
	}
	if rr := b.chunk("dev-1", req.BundleID, 3, data[:50]); rr.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range chunk: got %d, want 400", rr.Code)
	}
	if rr := b.chunk("dev-2", req.BundleID, 0, data[:100]); rr.Code != http.StatusForbidden {
		t.Fatalf("another device's bundle: got %d, want 403", rr.Code)
	}
	if left := b.store.keysWithPrefix(sessionPrefix(req.BundleID) + "chunk-"); len(left) != 0 {
		t.Fatalf("rejected chunks must not be stored: %v", left)
	}
}

func TestBundleCompleteWithMissingChunks(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)
	b.init("dev-1", req)
	b.chunk("dev-1", req.BundleID, 0, data[:100])

	rr, st := b.complete("dev-1", req.BundleID)
	if rr.Code != http.StatusConflict || fmt.Sprint(st.Missing()) != "[1 2]" {
		t.Fatalf("got %d %+v, want 409 listing chunks 1 and 2", rr.Code, st)
	}
	if len(b.kafka.records) != 0 {
		t.Fatal("nothing should be published for an incomplete bundle")
	}
}

func TestBundleCompleteChecksumMismatch(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)
	req.SHA256 = strings.Repeat("ab", 32) // well-formed, but not this content
	b.init("dev-1", req)
	uploadAll(b, "dev-1", req, data, 100)

	if rr, _ := b.complete("dev-1", req.BundleID); rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422", rr.Code)
	}
	if _, ok := b.store.objects[bundleKey("dev-1", req.BundleID)]; ok {
		t.Fatal("a bundle that fails verification must not be kept")
	}
	if len(b.kafka.records) != 0 {
		t.Fatal("a bundle that fails verification must not be published")
	}
}

func TestBundleCompleteRetriesPublishWithoutReassembling(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, data := newBundle(t, "dev-1", 250)
	b.init("dev-1", req)
	uploadAll(b, "dev-1", req, data, 100)

	b.kafka.err = errors.New("broker unreachable")
	if rr, _ := b.complete("dev-1", req.BundleID); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("Kafka down: got %d, want 503", rr.Code)
	}
	if left := b.store.keysWithPrefix(sessionPrefix(req.BundleID)); len(left) == 0 {
		t.Fatal("the session must survive a failed publish so the device can retry")
	}

	b.kafka.err = nil
	if rr, _ := b.complete("dev-1", req.BundleID); rr.Code != http.StatusOK {
		t.Fatalf("retry after Kafka recovers: got %d: %s", rr.Code, rr.Body)
	}
	if n := b.store.puts[bundleKey("dev-1", req.BundleID)]; n != 1 {
		t.Fatalf("bundle assembled %d times, want 1", n)
	}
	if len(b.kafka.records) != 1 {
		t.Fatalf("published %d events, want 1", len(b.kafka.records))
	}
}

func TestBundleStoreDownIsRetryable(t *testing.T) {
	b := newBundleHarness(t, 100)
	req, _ := newBundle(t, "dev-1", 250)
	b.store.down = true
	rr, _ := b.init("dev-1", req)
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d, want 503 with Retry-After", rr.Code)
	}
}

func TestBundleRoutesOffWithoutStore(t *testing.T) {
	h, signer := newTestServer(t, &fakeProducer{}, 1<<20)
	req, _ := newBundle(t, "dev-1", 10)
	rr := post(h, "/v1/bundles", deviceHeaders(signer, "dev-1"), req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404 so devices keep bundles spooled", rr.Code)
	}
}
