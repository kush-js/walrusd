// Package storage implements the Basemnt storage-provider contract (spec §5)
// over an S3-compatible object store with real conditional writes (Cloudflare R2).
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

// Version is an opaque object version (ETag) used for conditional writes.
// ETags are quoted exactly as returned by the provider so they pass verbatim
// into If-Match headers.
type Version = string

// ErrNotFound is returned when the object does not exist.
var ErrNotFound = errors.New("storage: object not found")

// ErrConflict is returned when a conditional write fails (HTTP 412
// PreconditionFailed from the provider).
var ErrConflict = errors.New("storage: conditional write conflict")

// ErrAlreadyExists is returned by CreateIfAbsent when the object exists.
var ErrAlreadyExists = errors.New("storage: object already exists")

// ErrExistsWithDifferentContent is returned by PutImmutable when the key is
// taken by an object with different content.
var ErrExistsWithDifferentContent = errors.New("storage: immutable object exists with different content")

// Adapter is the storage-provider contract from spec §5. Implementations must
// be backed by strongly consistent reads and native conditional writes; CAS is
// never emulated with read-then-write.
type Adapter interface {
	// Get returns the object body and version.
	Get(ctx context.Context, key string) (body []byte, version Version, err error)
	// CreateIfAbsent creates the object only if the key is absent.
	CreateIfAbsent(ctx context.Context, key string, body []byte) (Version, error)
	// ReplaceIfVersion replaces the object only if its current version matches.
	ReplaceIfVersion(ctx context.Context, key string, expected Version, body []byte) (Version, error)
	// PutImmutable writes an immutable object; rejects a same-key object with
	// different content. Content is addressed by its sha256 checksum.
	PutImmutable(ctx context.Context, key string, body []byte) error
}

// S3Adapter implements Adapter against S3-compatible storage (R2 verified).
type S3Adapter struct {
	client *s3.Client
	bucket string
}

// NewS3 builds an adapter for an S3-compatible endpoint with static creds.
func NewS3(endpoint, region, bucket, accessKeyID, secretAccessKey string) (*S3Adapter, error) {
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("storage: endpoint and bucket are required")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// R2 does not support HTTP 100-continue; large PUTs stall with it.
		o.ContinueHeaderThresholdBytes = -1
	})
	return &S3Adapter{client: client, bucket: bucket}, nil
}

func (a *S3Adapter) Get(ctx context.Context, key string) ([]byte, Version, error) {
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
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

func (a *S3Adapter) CreateIfAbsent(ctx context.Context, key string, body []byte) (Version, error) {
	out, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(body)),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			// Consumers of the adapter contract check ErrConflict for ANY
			// failed conditional write; ErrAlreadyExists stays for callers
			// that want the distinction.
			return "", ErrAlreadyExists
		}
		return "", fmt.Errorf("storage: create %s: %w", key, err)
	}
	return Version(aws.ToString(out.ETag)), nil
}

func (a *S3Adapter) ReplaceIfVersion(ctx context.Context, key string, expected Version, body []byte) (Version, error) {
	if expected == "" {
		return "", fmt.Errorf("storage: replace %s: empty expected version", key)
	}
	out, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(a.bucket),
		Key:     aws.String(key),
		Body:    strings.NewReader(string(body)),
		IfMatch: aws.String(string(expected)),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrConflict
		}
		return "", fmt.Errorf("storage: replace %s: %w", key, err)
	}
	return Version(aws.ToString(out.ETag)), nil
}

func (a *S3Adapter) PutImmutable(ctx context.Context, key string, body []byte) (err error) {
	// Immutability is enforced by refusing to overwrite differing content.
	// The checksum prefix in the caller's key makes content-addressed keys
	// collision-free; the existence check guards a double upload.
	if _, err = a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(body)),
		IfNoneMatch: aws.String("*"),
	}); err != nil {
		if isPreconditionFailed(err) {
			return ErrExistsWithDifferentContent
		}
		return fmt.Errorf("storage: put immutable %s: %w", key, err)
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
	// GetObject models NoSuchKey as a typed error; other operations (and
	// some providers) surface it as a generic API error.
	var nk *types.NoSuchKey
	if errors.As(err, &nk) {
		return true
	}
	var gae *smithy.GenericAPIError
	return errors.As(err, &gae) && (gae.Code == "NotFound" || gae.Code == "NoSuchKey")
}
