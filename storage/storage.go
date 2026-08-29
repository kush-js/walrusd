// Package storage defines the conditional object-store contract (spec §5)
// and provides the S3-compatible adapter. Conditional operations must be
// atomic at the provider; CAS is never emulated with GET+PUT.
package storage

import (
	"context"
	"errors"
)

// Version is an opaque object version (ETag) used as the CAS token.
type Version = string

var (
	// ErrNotFound is returned when the object does not exist.
	ErrNotFound = errors.New("storage: object not found")
	// ErrConflict is returned when a conditional write loses the race
	// (HTTP 412 PreconditionFailed at the provider).
	ErrConflict = errors.New("storage: conditional write conflict")
	// ErrAlreadyExists is returned by CreateIfAbsent when the key exists.
	ErrAlreadyExists = errors.New("storage: object already exists")
)

// ConditionalStore is the minimum interface the runtime requires from an
// object store (spec §5). Implementations MUST provide:
//   - strong read-after-write consistency,
//   - conditional create-if-absent,
//   - conditional replace/delete with an opaque version,
//   - atomic evaluation of the condition for one mutation.
type ConditionalStore interface {
	Get(ctx context.Context, key string) (body []byte, version Version, err error)
	CreateIfAbsent(ctx context.Context, key string, body []byte) (version Version, err error)
	ReplaceIfVersion(ctx context.Context, key, expectedVersion string, body []byte) (version Version, err error)
	DeleteIfVersion(ctx context.Context, key, expectedVersion string) error
}
