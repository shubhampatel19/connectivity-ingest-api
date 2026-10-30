package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

var ErrObjectNotFound = errors.New("object not found")

// ObjectStore is the slice of S3 the bundle endpoints need. Upload sessions
// and finished bundles both live here, which keeps the ingest API stateless.
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Exists(ctx context.Context, key string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// S3Store implements ObjectStore on any S3-compatible service (SeaweedFS
// locally, AWS S3 or similar in production).
type S3Store struct {
	cl     *minio.Client
	bucket string
}

// NewS3Store connects to an S3 endpoint. Setting the region explicitly stops
// the client from issuing a GetBucketLocation call before its first request,
// which some S3-compatible servers answer inconsistently.
func NewS3Store(endpoint, accessKey, secretKey, bucket, region string, useTLS bool) (*S3Store, error) {
	cl, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useTLS,
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	return &S3Store{cl: cl, bucket: bucket}, nil
}

// EnsureBucket creates the bucket if it doesn't exist yet.
func (s *S3Store) EnsureBucket(ctx context.Context) error {
	exists, err := s.cl.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = s.cl.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	if minio.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
		return nil
	}
	return err
}

// span starts a client span for one S3 call. end records the error, if any.
func (s *S3Store) span(ctx context.Context, op, key string) (context.Context, func(error)) {
	ctx, span := platform.Tracer().Start(ctx, "s3 "+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("aws.s3.bucket", s.bucket), attribute.String("aws.s3.key", key)))
	return ctx, func(err error) {
		platform.RecordError(ctx, err)
		span.End()
	}
}

func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) (err error) {
	ctx, end := s.span(ctx, "PutObject", key)
	defer func() { end(err) }()
	_, err = s.cl.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *S3Store) Get(ctx context.Context, key string) (_ io.ReadCloser, err error) {
	ctx, end := s.span(ctx, "GetObject", key)
	defer func() {
		if errors.Is(err, ErrObjectNotFound) {
			end(nil) // an expected answer, not a failure
			return
		}
		end(err)
	}()
	obj, err := s.cl.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; Stat surfaces a missing key before the caller reads.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		if isNotFound(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return obj, nil
}

func (s *S3Store) Exists(ctx context.Context, key string) (_ bool, err error) {
	ctx, end := s.span(ctx, "StatObject", key)
	defer func() { end(err) }()
	_, err = s.cl.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (s *S3Store) List(ctx context.Context, prefix string) (_ []string, err error) {
	ctx, end := s.span(ctx, "ListObjects", prefix)
	defer func() { end(err) }()
	var keys []string
	for obj := range s.cl.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) (err error) {
	ctx, end := s.span(ctx, "DeleteObject", key)
	defer func() { end(err) }()
	return s.cl.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

func isNotFound(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject"
}
