package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/spr732/connectivity-contracts/wire"
	"github.com/spr732/connectivity-ingest-api/internal/auth"
)

type produced struct {
	topic, key string
	value      []byte
}

type fakeProducer struct {
	mu      sync.Mutex
	err     error
	records []produced
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.records = append(f.records, produced{topic, string(key), value})
	return nil
}
func (f *fakeProducer) Ping(context.Context) error { return f.err }
func (f *fakeProducer) Close()                     {}

var testNow = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func newTestServer(t *testing.T, p Producer, maxBody int64) (http.Handler, *auth.Signer) {
	t.Helper()
	signer, err := auth.NewSigner("test-secret-0123456789")
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{EnrollmentKey: "enroll-me", MaxBodyBytes: maxBody, ProduceTimeout: time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)), signer, p, prometheus.NewRegistry())
	s.now = func() time.Time { return testNow }
	return s.Handler(nil), signer
}

func post(h http.Handler, path string, headers map[string]string, body any) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func deviceHeaders(s *auth.Signer, id string) map[string]string {
	return map[string]string{"X-Device-ID": id, "Authorization": "Bearer " + s.Issue(id)}
}

func batchFor(id string) wire.TelemetryBatch {
	return wire.TelemetryBatch{BatchID: "batch-1", DeviceID: id,
		Samples: []wire.Sample{{Metric: "cpu_temp_c", Value: 51.2, TS: testNow}}}
}

func TestRegister(t *testing.T) {
	p := &fakeProducer{}
	h, signer := newTestServer(t, p, 1<<20)
	req := wire.RegisterRequest{DeviceID: "dev-1", Model: "edge-server-e1", Firmware: "1.0.0", SiteID: "site-a"}

	if rr := post(h, "/v1/register", nil, req); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing enrollment key: got %d, want 401", rr.Code)
	}

	rr := post(h, "/v1/register", map[string]string{"X-Enrollment-Key": "enroll-me"}, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("register: got %d, want 200: %s", rr.Code, rr.Body)
	}
	var resp wire.RegisterResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !signer.Verify("dev-1", resp.Token) {
		t.Fatal("issued token does not verify")
	}
	if len(p.records) != 1 || p.records[0].topic != wire.TopicDeviceEvents || p.records[0].key != "dev-1" {
		t.Fatalf("expected one device event keyed by device, got %+v", p.records)
	}
}

func TestTelemetry(t *testing.T) {
	tests := []struct {
		name     string
		maxBody  int64
		prodErr  error
		headers  func(*auth.Signer) map[string]string
		body     func() any
		wantCode int
		wantMsgs int
	}{
		{
			name:     "accepted",
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return batchFor("dev-1") },
			wantCode: http.StatusAccepted, wantMsgs: 1,
		},
		{
			name:     "no credentials",
			headers:  func(*auth.Signer) map[string]string { return nil },
			body:     func() any { return batchFor("dev-1") },
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "batch for another device",
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return batchFor("dev-2") },
			wantCode: http.StatusForbidden,
		},
		{
			name:    "timestamp from the future",
			headers: func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body: func() any {
				b := batchFor("dev-1")
				b.Samples[0].TS = testNow.Add(time.Hour)
				return b
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "body too large",
			maxBody:  64,
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return batchFor("dev-1") },
			wantCode: http.StatusRequestEntityTooLarge,
		},
		{
			name:     "kafka down",
			prodErr:  errors.New("broker unreachable"),
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return batchFor("dev-1") },
			wantCode: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			maxBody := tc.maxBody
			if maxBody == 0 {
				maxBody = 1 << 20
			}
			p := &fakeProducer{err: tc.prodErr}
			h, signer := newTestServer(t, p, maxBody)

			rr := post(h, "/v1/telemetry", tc.headers(signer), tc.body())
			if rr.Code != tc.wantCode {
				t.Fatalf("got %d, want %d: %s", rr.Code, tc.wantCode, rr.Body)
			}
			if len(p.records) != tc.wantMsgs {
				t.Fatalf("produced %d records, want %d", len(p.records), tc.wantMsgs)
			}
			if tc.wantCode == http.StatusServiceUnavailable && rr.Header().Get("Retry-After") == "" {
				t.Fatal("503 should carry Retry-After")
			}
			if tc.wantMsgs == 1 {
				rec := p.records[0]
				if rec.topic != wire.TopicTelemetry || rec.key != "dev-1" {
					t.Fatalf("unexpected record routing: %+v", rec)
				}
				var env wire.TelemetryEnvelope
				if err := json.Unmarshal(rec.value, &env); err != nil {
					t.Fatal(err)
				}
				if env.Batch.BatchID != "batch-1" || !env.ReceivedAt.Equal(testNow) {
					t.Fatalf("unexpected envelope: %+v", env)
				}
			}
		})
	}
}
