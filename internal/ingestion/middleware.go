package ingestion

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// instrument records metrics and an access log line for one named route.
// Successful requests log at debug so steady-state traffic doesn't drown the
// lines you need during an incident.
func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		annotateSpan(r)
		if id := platform.TraceID(r.Context()); id != "" {
			w.Header().Set("X-Trace-ID", id)
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		s.m.requests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
		s.m.duration.WithLabelValues(route).Observe(elapsed.Seconds())

		level := slog.LevelDebug
		switch {
		case rec.status >= 500:
			level = slog.LevelWarn
		case rec.status >= 400:
			level = slog.LevelInfo
		}
		s.log.Log(r.Context(), level, "request",
			"route", route,
			"status", rec.status,
			"duration_ms", elapsed.Milliseconds(),
			"device_id", r.Header.Get("X-Device-ID"),
			"request_id", r.Header.Get("X-Request-ID"),
		)
	})
}

// annotateSpan adds what every device request says about itself to its
// server span: which device, and for site B, which gateway sent it and how
// long the gateway held it on disk first.
func annotateSpan(r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	if !span.IsRecording() {
		return
	}
	if id := r.Header.Get("X-Device-ID"); id != "" {
		span.SetAttributes(attribute.String("device.id", id))
	}
	if gw := r.Header.Get("X-Gateway-ID"); gw != "" {
		span.SetAttributes(attribute.String("gateway.id", gw))
	}
	// A duration measured on the gateway's own clock, not a timestamp, so
	// clock skew between the site and the cloud can't distort it.
	if ms, err := strconv.ParseInt(r.Header.Get("X-Gateway-Queued-Ms"), 10, 64); err == nil && ms >= 0 {
		span.SetAttributes(attribute.Float64("gateway.queued_seconds", float64(ms)/1000))
	}
}

// requestID makes sure every request carries an ID that shows up in logs and
// in the response, so one failing call can be traced end to end.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
			r.Header.Set("X-Request-ID", id)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.log.ErrorContext(r.Context(), "panic in handler", "panic", v, "stack", string(debug.Stack()),
					"request_id", r.Header.Get("X-Request-ID"))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
