package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.cl.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
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

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.cl.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.cl.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.cl.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

func isNotFound(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject"
}
