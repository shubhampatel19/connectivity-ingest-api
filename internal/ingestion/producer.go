package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

// Producer is the narrow slice of Kafka the HTTP layer needs. Keeping it an
// interface lets handler tests run without a broker.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
	Ping(ctx context.Context) error
	Close()
}

type KafkaProducer struct{ cl *kgo.Client }

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Wait for every in-sync replica before acknowledging. With a single
		// broker this changes nothing, but it is the setting you want in
		// production and the one the failure drills assume.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &KafkaProducer{cl: cl}, nil
}

// Produce blocks until the broker acknowledges the record. The HTTP handler
// only returns 202 after this succeeds, so an accepted batch is a durable one.
//
// The record carries the trace context in its headers, so the consumer's
// span joins the device's trace. The producer span covers the wait for
// acks=all, which is where a slow or degraded broker shows up.
func (p *KafkaProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	ctx, span := platform.Tracer().Start(ctx, "publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.kafka.message.key", string(key)),
		))
	defer span.End()

	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	for k, v := range platform.TraceHeaders(ctx) {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		platform.RecordError(ctx, err)
		return err
	}
	span.SetAttributes(
		attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
		attribute.Int64("messaging.kafka.offset", rec.Offset),
	)
	return nil
}

func (p *KafkaProducer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

func (p *KafkaProducer) Close() { p.cl.Close() }
