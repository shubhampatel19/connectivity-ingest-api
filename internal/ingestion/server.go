// Package ingestion is the cloud's front door for devices: it authenticates
// them, validates payloads and hands everything to Kafka. It keeps no state of
// its own, so it can be scaled horizontally and restarted freely.
package ingestion

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/auth"
)

type Config struct {
	EnrollmentKey  string
	MaxBodyBytes   int64
	ProduceTimeout time.Duration
	Bundles        BundleConfig
}

type Server struct {
	cfg      Config
	log      *slog.Logger
	signer   *auth.Signer
	producer Producer
	store    ObjectStore // nil disables log bundle uploads
	m        *metrics
	now      func() time.Time
}

// NewServer builds the API. A nil store turns the bundle routes off; devices
// see 404 and keep their bundles spooled until the cloud supports them.
func NewServer(cfg Config, log *slog.Logger, signer *auth.Signer, p Producer, store ObjectStore, reg prometheus.Registerer) *Server {
	return &Server{cfg: cfg, log: log, signer: signer, producer: p, store: store, m: newMetrics(reg), now: time.Now}
}

// Handler wires the routes. metricsHandler may be nil (tests).
func (s *Server) Handler(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	// Each device route gets a server span named after its pattern, which
	// continues the device's trace when it sent a traceparent header.
	route := func(pattern, name string, h http.Handler) {
		mux.Handle(pattern, platform.TraceHTTP(pattern, s.instrument(name, h)))
	}
	route("POST /v1/register", "register", http.HandlerFunc(s.handleRegister))
	route("POST /v1/heartbeat", "heartbeat", s.authenticate(http.HandlerFunc(s.handleHeartbeat)))
	route("POST /v1/telemetry", "telemetry", s.authenticate(http.HandlerFunc(s.handleTelemetry)))
	route("POST /v1/alerts", "alerts", s.authenticate(http.HandlerFunc(s.handleAlert)))
	if s.store != nil {
		route("POST /v1/bundles", "bundle_init", s.authenticate(http.HandlerFunc(s.handleBundleInit)))
		route("GET /v1/bundles/{bundle_id}", "bundle_status", s.authenticate(http.HandlerFunc(s.handleBundleStatus)))
		route("PUT /v1/bundles/{bundle_id}/chunks/{index}", "bundle_chunk", s.authenticate(http.HandlerFunc(s.handleBundleChunk)))
		route("POST /v1/bundles/{bundle_id}/complete", "bundle_complete", s.authenticate(http.HandlerFunc(s.handleBundleComplete)))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}
	return s.recoverer(requestID(mux))
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-Enrollment-Key")
	if subtle.ConstantTimeCompare([]byte(key), []byte(s.cfg.EnrollmentKey)) != 1 {
		s.m.authFailures.Inc()
		writeError(w, http.StatusUnauthorized, "invalid enrollment key")
		return
	}

	var req wire.RegisterRequest
	if !s.decode(w, r, 64<<10, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "device_id is required")
		return
	}

	ev := wire.DeviceEvent{
		Type: wire.EventRegistered, DeviceID: req.DeviceID,
		Model: req.Model, Firmware: req.Firmware, SiteID: req.SiteID,
		At: s.now().UTC(),
	}
	if err := s.publish(r.Context(), wire.TopicDeviceEvents, req.DeviceID, ev); err != nil {
		writeUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, wire.RegisterResponse{DeviceID: req.DeviceID, Token: s.signer.Issue(req.DeviceID)})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	ev := wire.DeviceEvent{Type: wire.EventHeartbeat, DeviceID: deviceID, At: s.now().UTC()}
	if err := s.publish(r.Context(), wire.TopicDeviceEvents, deviceID, ev); err != nil {
		writeUnavailable(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())

	var batch wire.TelemetryBatch
	if !s.decode(w, r, s.cfg.MaxBodyBytes, &batch) {
		return
	}
	if batch.DeviceID != deviceID {
		writeError(w, http.StatusForbidden, "batch device_id does not match credentials")
		return
	}
	if err := batch.Validate(s.now()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	trace.SpanFromContext(r.Context()).SetAttributes(
		attribute.String("batch.id", batch.BatchID),
		attribute.Int("batch.samples", len(batch.Samples)),
	)
	env := wire.TelemetryEnvelope{ReceivedAt: s.now().UTC(), Batch: batch}
	if err := s.publish(r.Context(), wire.TopicTelemetry, deviceID, env); err != nil {
		writeUnavailable(w)
		return
	}
	s.m.samplesAccepted.Add(float64(len(batch.Samples)))
	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": len(batch.Samples)})
}

// handleAlert accepts one device alert. 202 means the alert is in Kafka for
// the case service, not that a case exists: whether it becomes one is the
// case service's decision.
func (s *Server) handleAlert(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())

	var alert wire.DeviceAlert
	if !s.decode(w, r, 64<<10, &alert) {
		return
	}
	if alert.DeviceID != deviceID {
		writeError(w, http.StatusForbidden, "alert device_id does not match credentials")
		return
	}
	if err := alert.Validate(s.now()); err != nil {
		s.m.alertsRejected.Inc()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	trace.SpanFromContext(r.Context()).SetAttributes(
		attribute.String("alert.id", alert.AlertID),
		attribute.String("alert.code", string(alert.Code)),
	)
	env := wire.AlertEnvelope{ReceivedAt: s.now().UTC(), Alert: alert}
	if err := s.publish(r.Context(), wire.TopicAlerts, deviceID, env); err != nil {
		writeUnavailable(w)
		return
	}
	s.m.alertsAccepted.WithLabelValues(string(alert.Severity)).Inc()
	writeJSON(w, http.StatusAccepted, wire.AlertAccepted{AlertID: alert.AlertID})
}

// handleReady checks only Kafka, the dependency every request needs. The
// object store is deliberately left out: if it went down and readiness
// followed it, the orchestrator would pull every replica out of rotation and
// a log-upload outage would take telemetry down with it.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.producer.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "kafka unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// publish writes v to Kafka keyed by device, so a device's records stay in
// order on one partition.
func (s *Server) publish(ctx context.Context, topic, key string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.ProduceTimeout)
	defer cancel()
	if err := s.producer.Produce(ctx, topic, []byte(key), payload); err != nil {
		s.m.produceErrors.WithLabelValues(topic).Inc()
		s.log.ErrorContext(ctx, "kafka produce failed", "topic", topic, "key", key, "err", err)
		return err
	}
	return nil
}

// decode reads a JSON body capped at limit bytes, writing the error response
// itself when it fails.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "body exceeds limit")
			return false
		}
		writeError(w, http.StatusBadRequest, "malformed JSON: "+err.Error())
		return false
	}
	return true
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deviceID := r.Header.Get("X-Device-ID")
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !s.signer.Verify(deviceID, token) {
			s.m.authFailures.Inc()
			writeError(w, http.StatusUnauthorized, "invalid device credentials")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), deviceIDKey{}, deviceID)))
	})
}

type deviceIDKey struct{}

func deviceIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(deviceIDKey{}).(string)
	return id
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeUnavailable tells devices to back off and retry. Retry-After is a hint;
// the agent's own backoff decides the actual wait.
func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	writeError(w, http.StatusServiceUnavailable, "temporarily unable to accept data")
}
