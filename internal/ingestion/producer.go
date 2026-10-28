package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
func (p *KafkaProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

func (p *KafkaProducer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

func (p *KafkaProducer) Close() { p.cl.Close() }
