package ingestion

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/auth"
)

func alertFor(id string) wire.DeviceAlert {
	return wire.DeviceAlert{
		AlertID: "alert-1", DeviceID: id, SiteID: "site-a",
		Code: wire.AlertThermalCritical, Severity: wire.SeverityCritical, Value: 93.1,
		Message: "cpu_temp=93.1C over fault threshold 90C", ObservedAt: testNow, BundleID: "bundle-1",
	}
}

func TestAlerts(t *testing.T) {
	tests := []struct {
		name     string
		prodErr  error
		headers  func(*auth.Signer) map[string]string
		body     func() any
		wantCode int
		wantMsgs int
	}{
		{
			name:     "accepted",
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return alertFor("dev-1") },
			wantCode: http.StatusAccepted, wantMsgs: 1,
		},
		{
			name:    "code this cloud doesn't know yet is still accepted",
			headers: func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body: func() any {
				a := alertFor("dev-1")
				a.Code = "fan.stalled"
				return a
			},
			wantCode: http.StatusAccepted, wantMsgs: 1,
		},
		{
			name:     "no credentials",
			headers:  func(*auth.Signer) map[string]string { return nil },
			body:     func() any { return alertFor("dev-1") },
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "alert for another device",
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return alertFor("dev-2") },
			wantCode: http.StatusForbidden,
		},
		{
			name:    "invalid alert",
			headers: func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body: func() any {
				a := alertFor("dev-1")
				a.ObservedAt = testNow.Add(time.Hour)
				return a
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "malformed JSON",
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return "not an alert" },
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "kafka down",
			prodErr:  errors.New("broker unreachable"),
			headers:  func(s *auth.Signer) map[string]string { return deviceHeaders(s, "dev-1") },
			body:     func() any { return alertFor("dev-1") },
			wantCode: http.StatusServiceUnavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakeProducer{err: tc.prodErr}
			h, signer := newTestServer(t, p, 1<<20)
			rr := post(h, "/v1/alerts", tc.headers(signer), tc.body())
			if rr.Code != tc.wantCode {
				t.Fatalf("got %d, want %d: %s", rr.Code, tc.wantCode, rr.Body)
			}
			if len(p.records) != tc.wantMsgs {
				t.Fatalf("produced %d records, want %d", len(p.records), tc.wantMsgs)
			}
			if tc.wantCode == http.StatusServiceUnavailable && rr.Header().Get("Retry-After") == "" {
				t.Fatal("503 should carry Retry-After")
			}
			if tc.wantMsgs != 1 {
				return
			}
			rec := p.records[0]
			if rec.topic != wire.TopicAlerts || rec.key != "dev-1" {
				t.Fatalf("alerts must go to %s keyed by device, got %+v", wire.TopicAlerts, rec)
			}
			var env wire.AlertEnvelope
			if err := json.Unmarshal(rec.value, &env); err != nil {
				t.Fatal(err)
			}
			if env.Alert.AlertID != "alert-1" || !env.ReceivedAt.Equal(testNow) {
				t.Fatalf("unexpected envelope: %+v", env)
			}
			var resp wire.AlertAccepted
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil || resp.AlertID != "alert-1" {
				t.Fatalf("response should echo the alert id, got %s", rr.Body)
			}
		})
	}
}
