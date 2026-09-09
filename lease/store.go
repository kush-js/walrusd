// Lease record storage contract.
//
// Leases live in Redis/Valkey (or the in-process memory backend for
// dev/tests), never in object storage. Object-storage providers for the
// replica therefore need no conditional-write support: plain GET/PUT/DELETE
// plus listing for the LTX chain is sufficient. Mutual exclusion comes from
// atomic compare-and-swap on the lease store; the opaque token returned by
// a successful write is the CAS authority for the next one.
package lease

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when the lease key does not exist.
	ErrNotFound = errors.New("lease: not found")
	// ErrConflict is returned when a conditional write loses the race.
	ErrConflict = errors.New("lease: conditional write conflict")
	// ErrAlreadyExists is returned by CreateIfAbsent when the key exists.
	ErrAlreadyExists = errors.New("lease: already exists")
)

// Store persists lease records with atomic single-key CAS. Implementations
// MUST evaluate the condition atomically (Lua script, transaction, or mutex);
// CAS is never emulated with read-then-write.
type Store interface {
	// Get returns the record body and its CAS token.
	Get(ctx context.Context, key string) (body []byte, token string, err error)
	// CreateIfAbsent writes body only if key is absent.
	CreateIfAbsent(ctx context.Context, key string, body []byte) (token string, err error)
	// ReplaceIfToken writes body only if the current token matches.
	ReplaceIfToken(ctx context.Context, key, expectedToken string, body []byte) (token string, err error)
}
