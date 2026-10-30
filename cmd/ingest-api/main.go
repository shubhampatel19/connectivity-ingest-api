// Command ingest-api is the HTTP API devices and gateways send data to.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhampatel19/connectivity-go-kit/platform"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/auth"
	"github.com/shubhampatel19/connectivity-ingest-api/internal/ingestion"
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

	// Log bundles. Leaving S3_ENDPOINT empty turns bundle uploads off.
	s3Endpoint := env.String("S3_ENDPOINT", "")
	s3AccessKey := env.String("S3_ACCESS_KEY", "")
	s3SecretKey := env.String("S3_SECRET_KEY", "")
	s3Bucket := env.String("S3_BUCKET", "log-bundles")
	s3Region := env.String("S3_REGION", "us-east-1")
	s3TLS := env.String("S3_USE_TLS", "false") == "true"
	chunkSize := env.Int("BUNDLE_CHUNK_SIZE", 1<<20)
	maxBundle := env.Int("MAX_BUNDLE_BYTES", 50<<20)
	env.Check(s3Endpoint == "" || (s3AccessKey != "" && s3SecretKey != ""), "S3_ACCESS_KEY and S3_SECRET_KEY are required when S3_ENDPOINT is set")
	env.Check(chunkSize > 0, "BUNDLE_CHUNK_SIZE must be positive")
	env.Check(maxBundle > 0, "MAX_BUNDLE_BYTES must be positive")
	if err := env.Err(); err != nil {
		return err
	}

	tracingCtx, cancelTracing := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownTracing, err := platform.InitTracing(tracingCtx, "ingest-api", log)
	cancelTracing()
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	signer, err := auth.NewSigner(secret)
	if err != nil {
		return err
	}
	producer, err := ingestion.NewKafkaProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var store ingestion.ObjectStore
	if s3Endpoint != "" {
		s3, err := ingestion.NewS3Store(s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, s3Region, s3TLS)
		if err != nil {
			return err
		}
		store = s3
		// Telemetry must not wait on the object store, so the bucket is
		// created in the background. Bundle requests get 503 until it exists.
		go ensureBucket(ctx, log, s3, s3Bucket)
	} else {
		log.Warn("S3_ENDPOINT not set; log bundle uploads are disabled")
	}

	reg := platform.NewRegistry()
	srv := ingestion.NewServer(ingestion.Config{
		EnrollmentKey:  enrollmentKey,
		MaxBodyBytes:   int64(maxBody),
		ProduceTimeout: produceTimeout,
		Bundles:        ingestion.BundleConfig{ChunkSize: int64(chunkSize), MaxBundleBytes: int64(maxBundle)},
	}, log, signer, producer, store, reg)
	log.Info("starting", "brokers", brokers)
	return platform.Serve(ctx, log, addr, srv.Handler(platform.MetricsHandler(reg)))
}

func ensureBucket(ctx context.Context, log *slog.Logger, s3 *ingestion.S3Store, bucket string) {
	for attempt := 1; ; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s3.EnsureBucket(reqCtx)
		cancel()
		if err == nil {
			log.Info("object store ready", "bucket", bucket)
			return
		}
		log.Warn("object store not ready, retrying", "bucket", bucket, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
