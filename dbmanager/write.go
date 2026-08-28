//go:build vfs
// +build vfs

// Durable write path and idempotency (spec §7.3, §10): a write is
// acknowledged only after the epoch-aware manifest CAS succeeds; retries with
// the same idempotency key never duplicate a mutation.
package dbmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/superfly/ltx"
	"sync"
)

// WriteResult is the durable outcome of a write.
type WriteResult struct {
	CommitSeq uint64
}

// ErrNotOwner mirrors ownership.ErrNotOwner for retryable routing away.
var ErrNotOwner = errors.New("dbmanager: not the durable owner")

// idempotency records durable results per (database_id, operation, key).
type idempotency struct {
	mu   sync.Mutex
	seen map[string]WriteResult
}

func newIdempotency() *idempotency { return &idempotency{seen: map[string]WriteResult{}} }

func idemKey(databaseID, operation, key string) string {
	sum := sha256.Sum256([]byte(databaseID + "\x00" + operation + "\x00" + key))
	return hex.EncodeToString(sum[:8])
}

func (i *idempotency) lookup(databaseID, operation, key string) (WriteResult, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	r, ok := i.seen[idemKey(databaseID, operation, key)]
	return r, ok
}

func (i *idempotency) store(databaseID, operation, key string, r WriteResult) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.seen[idemKey(databaseID, operation, key)] = r
}

// ExecuteWrite runs one mutation end-to-end:
//
//  1. idempotency dedupe (retry after ambiguous failure returns original result)
//  2. open/retain the writable hot instance (acquires ownership via Fencer)
//  3. execute the transaction against SQLite through the VFS
//  4. sync dirty pages as an immutable LTX object
//  5. advance the commit manifest with epoch-aware CAS — the durable commit
//     point; no acknowledgement precedes it (spec invariant 6)
//
// exec is the caller's transaction function. It receives the DSN and a sync
// callback: after its writes are committed locally, exec MUST invoke sync()
// (while its connection is still open — the connection's VFS file is what
// sync flushes) and only then close its connection. This ordering is what
// makes the LTX object exist before the manifest CAS runs.
func (m *Manager) ExecuteWrite(ctx context.Context, dbID, prefix, operation, idemKeyStr string, exec func(dsn string, sync func() error) error) (WriteResult, error) {
	// Dedupe: a retry after ambiguous transport failure must not re-execute.
	if r, ok := m.idem.lookup(dbID, operation, idemKeyStr); ok {
		return r, nil
	}

	if err := m.OpenWritable(ctx, dbID, prefix); err != nil {
		return WriteResult{}, err
	}
	dsn, err := m.DSN(dbID)
	if err != nil {
		return WriteResult{}, err
	}

	// Capture authority before executing; it is revalidated at the manifest
	// CAS step, which is the sole durable commit gate.
	epoch, leaseID, err := m.fencer.Authorize(ctx, dbID)
	if err != nil {
		return WriteResult{}, fmt.Errorf("dbmanager: write %s: %w", dbID, err)
	}

	syncFn := func() error {
		m.AttachDB(dbID)
		h := m.get(dbID)
		if h == nil {
			return ErrNotWritable
		}
		// Flush dirty pages to an immutable LTX object now (not on the lazy
		// periodic ticker) so the manifest can name it deterministically.
		if err := h.syncAll(ctx); err != nil {
			// Ambiguous: the LTX may exist remotely. The idempotency key
			// makes a retry safe either way (spec §11).
			return fmt.Errorf("dbmanager: sync %s: %w", dbID, err)
		}
		return nil
	}

	if err := exec(dsn, syncFn); err != nil {
		return WriteResult{}, fmt.Errorf("dbmanager: execute %s: %w", dbID, err)
	}

	h := m.get(dbID)
	if h == nil {
		return WriteResult{}, ErrNotWritable
	}
	objs := []Object{{
		MinTXID:  h.lastMinTXID(),
		MaxTXID:  h.lastMaxTXID(),
		Checksum: Checksum([]byte(ltx.FormatFilename(h.lastMinTXID(), h.lastMaxTXID()))),
	}}
	seq, err := m.committer.Commit(ctx, dbID, epoch, leaseID, m.workerID, objs)
	if errors.Is(err, ErrFencingLost) || errors.Is(err, ErrStaleEpoch) {
		// Loss of authority (spec §7.4): demote, never acknowledge success.
		m.demote(dbID)
		return WriteResult{}, fmt.Errorf("%w: manifest CAS lost for %s", ErrNotOwner, dbID)
	}
	if err != nil {
		return WriteResult{}, fmt.Errorf("dbmanager: commit %s: %w", dbID, err)
	}

	result := WriteResult{CommitSeq: seq}
	m.idem.store(dbID, operation, idemKeyStr, result)
	return result, nil
}

// ErrFencingLost means the manifest CAS failed: another epoch advanced the
// manifest. Mirrors the commit package error without an import cycle.
var ErrFencingLost = errors.New("dbmanager: fencing lost")

// ErrStaleEpoch means the caller's epoch is behind the current manifest.
var ErrStaleEpoch = errors.New("dbmanager: stale epoch")

// Checksum returns the sha256 hex checksum for manifest object pointers.
func Checksum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
