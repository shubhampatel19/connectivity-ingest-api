package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/auth"
)

// A record Postgres would reject must be refused here, with a 400 to one
// device, not accepted and left to stall a consumer.
func TestTextThatWouldPoisonAConsumerIsRefusedAtTheDoor(t *testing.T) {
	kafka := &fakeProducer{}
	h, signer := newTestServer(t, kafka, 1<<20)

	b := batchFor("dev-1")
	b.Samples[0].Metric = "cpu\x00temp"
	if rr := post(h, "/v1/telemetry", deviceHeaders(signer, "dev-1"), b); rr.Code != http.StatusBadRequest {
		t.Fatalf("telemetry with a NUL metric: %d, want 400", rr.Code)
	}
	a := wire.DeviceAlert{AlertID: "a1", DeviceID: "dev-1", Code: wire.AlertThermalCritical,
		Severity: wire.SeverityCritical, Message: "cpu\x00", ObservedAt: testNow}
	if rr := post(h, "/v1/alerts", deviceHeaders(signer, "dev-1"), a); rr.Code != http.StatusBadRequest {
		t.Fatalf("alert with a NUL message: %d, want 400", rr.Code)
	}
	reg := wire.RegisterRequest{DeviceID: "dev-1", Model: "edge\x00server"}
	if rr := post(h, "/v1/register", map[string]string{"X-Enrollment-Key": "enroll-me"}, reg); rr.Code != http.StatusBadRequest {
		t.Fatalf("registration with a NUL model: %d, want 400", rr.Code)
	}
	if len(kafka.records) != 0 {
		t.Fatalf("%d records reached Kafka", len(kafka.records))
	}
}

// blockingProducer holds every produce until released, like a Kafka that has
// stopped acknowledging.
type blockingProducer struct {
	fakeProducer
	entered chan struct{}
	release chan struct{}
}

func (b *blockingProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	b.entered <- struct{}{}
	<-b.release
	return nil
}

func TestRequestsBeyondMaxInFlightAreShedAtOnce(t *testing.T) {
	signer, _ := auth.NewSigner("test-secret-0123456789")
	p := &blockingProducer{entered: make(chan struct{}, 8), release: make(chan struct{})}
	s := NewServer(Config{EnrollmentKey: "enroll-me", MaxBodyBytes: 1 << 20, ProduceTimeout: 5 * time.Second, MaxInFlight: 2},
		slog.New(slog.NewTextHandler(io.Discard, nil)), signer, p, nil, prometheus.NewRegistry())
	s.now = func() time.Time { return testNow }
	h := s.Handler(nil)

	results := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- post(h, "/v1/telemetry", deviceHeaders(signer, "dev-1"), batchFor("dev-1")).Code }()
	}
	<-p.entered
	<-p.entered // both slots are now held

	start := time.Now()
	rr := post(h, "/v1/telemetry", deviceHeaders(signer, "dev-1"), batchFor("dev-1"))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("third request: %d (Retry-After %q), want 503 with Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("shedding took %v; it must not wait", d)
	}
	if got := testutil.ToFloat64(s.m.shed.WithLabelValues("telemetry")); got != 1 {
		t.Fatalf("shed counter = %v", got)
	}
	// Health and readiness are never shed: a busy server is not a dead one.
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	hr := httptest.NewRecorder()
	h.ServeHTTP(hr, req)
	if hr.Code != http.StatusOK {
		t.Fatalf("/healthz while saturated: %d", hr.Code)
	}

	close(p.release)
	for i := 0; i < 2; i++ {
		if code := <-results; code != http.StatusAccepted {
			t.Fatalf("admitted request finished with %d", code)
		}
	}
	// The slots are free again.
	p2 := post(h, "/v1/telemetry", deviceHeaders(signer, "dev-1"), batchFor("dev-1"))
	if p2.Code != http.StatusAccepted {
		t.Fatalf("after release: %d", p2.Code)
	}
}

// slowReader hands out its data in small pieces with a pause between them,
// like a chunk coming up a thin link.
type slowReader struct {
	data  []byte
	piece int
	pause time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, io.EOF
	}
	time.Sleep(s.pause)
	n := copy(p, s.data[:min(s.piece, len(s.data))])
	s.data = s.data[n:]
	return n, nil
}

// The server's read timeout is sized for small JSON bodies. A chunk on a thin
// link takes longer than that, and must still be accepted.
func TestSlowChunkOutlivesTheServerReadTimeout(t *testing.T) {
	signer, _ := auth.NewSigner("test-secret-0123456789")
	store := newMemStore()
	const chunk = 4096
	s := NewServer(Config{
		EnrollmentKey: "enroll-me", MaxBodyBytes: 1 << 20, ProduceTimeout: time.Second,
		Bundles: BundleConfig{ChunkSize: chunk, MaxBundleBytes: 1 << 20, ChunkTimeout: 10 * time.Second},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), signer, &fakeProducer{}, store, prometheus.NewRegistry())
	s.now = func() time.Time { return testNow }

	srv := httptest.NewUnstartedServer(s.Handler(nil))
	srv.Config.ReadTimeout = 300 * time.Millisecond // far shorter than the upload below
	srv.Start()
	defer srv.Close()

	data := bytes.Repeat([]byte("x"), chunk)
	sum := sha256.Sum256(data)
	initBody, _ := json.Marshal(wire.BundleInitRequest{BundleID: "b1", DeviceID: "dev-1", SizeBytes: chunk,
		SHA256: hex.EncodeToString(sum[:]), Reason: wire.ReasonAlert, CollectedAt: testNow})
	do := func(method, path string, body io.Reader, length int64, extra map[string]string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, body)
		req.ContentLength = length
		for k, v := range deviceHeaders(signer, "dev-1") {
			req.Header.Set(k, v)
		}
		for k, v := range extra {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		return resp
	}
	if resp := do(http.MethodPost, "/v1/bundles", bytes.NewReader(initBody), int64(len(initBody)), nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("init: %d", resp.StatusCode)
	}
	// About 1.2 seconds for the body, four times the server's read timeout.
	body := &slowReader{data: data, piece: 512, pause: 150 * time.Millisecond}
	resp := do(http.MethodPut, "/v1/bundles/b1/chunks/0", body, chunk, map[string]string{wire.HeaderChunkSHA256: hex.EncodeToString(sum[:])})
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("slow chunk: %d %s", resp.StatusCode, msg)
	}
}
