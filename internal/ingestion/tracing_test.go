package ingestion

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

// traceCapturingProducer records the trace context each record would carry.
type traceCapturingProducer struct {
	fakeProducer
	traceparents []string
}

func (p *traceCapturingProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	p.traceparents = append(p.traceparents, platform.TraceHeaders(ctx)["traceparent"])
	return p.fakeProducer.Produce(ctx, topic, key, value)
}

func TestDeviceTraceReachesKafkaAndTheResponse(t *testing.T) {
	// Even with span export off, a device's trace must survive the hop:
	// the processor joins it from the record headers.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	const deviceTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	p := &traceCapturingProducer{}
	h, signer := newTestServer(t, p, 1<<20)
	headers := deviceHeaders(signer, "dev-1")
	headers["traceparent"] = "00-" + deviceTrace + "-00f067aa0ba902b7-01"

	rr := post(h, "/v1/telemetry", headers, batchFor("dev-1"))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("telemetry: got %d: %s", rr.Code, rr.Body)
	}
	if got := rr.Header().Get("X-Trace-ID"); got != deviceTrace {
		t.Errorf("X-Trace-ID %q, want the device's trace %s", got, deviceTrace)
	}
	if len(p.records) != 1 || p.records[0].topic != wire.TopicTelemetry {
		t.Fatalf("records: %+v", p.records)
	}
	if tp := p.traceparents[0]; len(tp) < 35 || tp[3:35] != deviceTrace {
		t.Errorf("the Kafka record would carry traceparent %q, not the device's trace", tp)
	}
}

func TestNoTraceNoHeaders(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	p := &traceCapturingProducer{}
	h, signer := newTestServer(t, p, 1<<20)
	rr := post(h, "/v1/telemetry", deviceHeaders(signer, "dev-1"), batchFor("dev-1"))
	if rr.Code != http.StatusAccepted || rr.Header().Get("X-Trace-ID") != "" || p.traceparents[0] != "" {
		t.Errorf("an untraced request got X-Trace-ID %q and record traceparent %q", rr.Header().Get("X-Trace-ID"), p.traceparents[0])
	}
}
