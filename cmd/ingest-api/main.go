// Command ingest-api is the HTTP API devices and gateways send data to.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spr732/connectivity-go-kit/platform"
	"github.com/spr732/connectivity-ingest-api/internal/auth"
	"github.com/spr732/connectivity-ingest-api/internal/ingestion"
)

func main() {
	log := platform.NewLogger("ingest-api")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var env platform.Env
	addr := env.String("HTTP_ADDR", ":8080")
	brokers := env.List("KAFKA_BROKERS", "localhost:29092")
	secret := env.Required("TOKEN_SECRET")
	enrollmentKey := env.Required("ENROLLMENT_KEY")
	maxBody := env.Int("MAX_BODY_BYTES", 1<<20)
	produceTimeout := env.Duration("PRODUCE_TIMEOUT", 5*time.Second)
	if err := env.Err(); err != nil {
		return err
	}

	signer, err := auth.NewSigner(secret)
	if err != nil {
		return err
	}
	producer, err := ingestion.NewKafkaProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	reg := platform.NewRegistry()
	srv := ingestion.NewServer(ingestion.Config{
		EnrollmentKey:  enrollmentKey,
		MaxBodyBytes:   int64(maxBody),
		ProduceTimeout: produceTimeout,
	}, log, signer, producer, reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting", "brokers", brokers)
	return platform.Serve(ctx, log, addr, srv.Handler(platform.MetricsHandler(reg)))
}
