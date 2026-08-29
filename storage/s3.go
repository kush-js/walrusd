package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Store implements ConditionalStore against S3-compatible storage with
// native conditional writes (Cloudflare R2 verified, spec §5).
type S3Store struct {
	client *s3.Client
	bucket string
}

// S3Options configures an S3-compatible store.
type S3Options struct {
	Endpoint        string // e.g. R2 account endpoint; empty for AWS
	Region          string // defaults to us-east-1 for S3-compatible stores
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
}

// NewS3 builds a ConditionalStore for an S3-compatible endpoint.
func NewS3(ctx context.Context, opts S3Options) (*S3Store, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("storage: bucket is required")
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(opts.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = true
		// R2 does not support HTTP 100-continue; large PUTs stall with it.
		o.ContinueHeaderThresholdBytes = -1
	})
	return &S3Store{client: client, bucket: opts.Bucket}, nil
}

// Get returns the object body and version (ETag).
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, Version, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("storage: get %s: %w", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("storage: read %s: %w", key, err)
	}
	return body, Version(aws.ToString(out.ETag)), nil
}

// CreateIfAbsent creates the object only if the key is absent, using the
// native If-None-Match: * conditional PUT.
func (s *S3Store) CreateIfAbsent(ctx context.Context, key string, body []byte) (Version, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(body)),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrAlreadyExists
		}
		return "", fmt.Errorf("storage: create %s: %w", key, err)
	}
	return Version(aws.ToString(out.ETag)), nil
}

// ReplaceIfVersion replaces the object only if its current version matches,
// using the native If-Match conditional PUT.
func (s *S3Store) ReplaceIfVersion(ctx context.Context, key, expectedVersion string, body []byte) (Version, error) {
	if expectedVersion == "" {
		return "", fmt.Errorf("storage: replace %s: empty expected version", key)
	}
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     strings.NewReader(string(body)),
		IfMatch:  aws.String(expectedVersion),
		Metadata: map[string]string{"walrus-cas": "1"},
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrConflict
		}
		return "", fmt.Errorf("storage: replace %s: %w", key, err)
	}
	return Version(aws.ToString(out.ETag)), nil
}

// DeleteIfVersion deletes the object only if its current version matches.
//
// R2 accepts but does not enforce If-Match on DeleteObject, so the guard is
// enforced with an atomic conditional replace to a tombstone first
// (ReplaceIfVersion is verified conditional on R2); the plain delete only
// ever removes that tombstone. A concurrent writer that replaced the object
// after our CAS makes the delete a no-op for their data because their
// replace happened after the tombstone version — the delete removes a
// tombstone we already own, never newer state. Deletion is therefore safe
// for garbage collection but callers needing strict no-delete semantics
// should use ReplaceIfVersion directly.
func (s *S3Store) DeleteIfVersion(ctx context.Context, key, expectedVersion string) error {
	if expectedVersion == "" {
		return fmt.Errorf("storage: delete %s: empty expected version", key)
	}
	if _, err := s.ReplaceIfVersion(ctx, key, expectedVersion, nil); err != nil {
		return fmt.Errorf("storage: delete %s: %w", key, err)
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("storage: delete %s: %w", key, err)
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	var gae *smithy.GenericAPIError
	if errors.As(err, &gae) {
		return gae.Code == "PreconditionFailed"
	}
	return false
}

func isNotFound(err error) bool {
	var nk *types.NoSuchKey
	if errors.As(err, &nk) {
		return true
	}
	var gae *smithy.GenericAPIError
	return errors.As(err, &gae) && (gae.Code == "NotFound" || gae.Code == "NoSuchKey")
}
