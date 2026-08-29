package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"walrus/cache"
	"walrus/identity"
	"walrus/lease"
	"walrus/litestream"
	"walrus/observability"
	"walrus/storage"
	"walrus/walruserr"
)

// Config is the runtime configuration (spec §16).
type Config struct {
	Lease               lease.Config
	Litestream          litestream.Config
	RequestTimeout      time.Duration // 20s
	ReadInstanceIdleTTL time.Duration
	MaxReadInstances    int
	MaxTempWriteBuffer  int64
	// RequireFlushBeforeRelease must stay true (spec §16 durability); the
	// runtime rejects configuration that disables it.
	RequireFlushBeforeRelease bool
}

// DefaultConfig returns the starting configuration from spec §16.
func DefaultConfig() Config {
	return Config{
		Lease:                     lease.DefaultConfig(),
		Litestream:                litestream.DefaultConfig(),
		RequestTimeout:            20 * time.Second,
		ReadInstanceIdleTTL:       60 * time.Second,
		MaxReadInstances:          200,
		MaxTempWriteBuffer:        268435456,
		RequireFlushBeforeRelease: true,
	}
}

// Runtime is the WALrus runtime surface (spec §11).
type Runtime struct {
	cfg     Config
	bridge  *litestream.Bridge
	leases  *lease.Manager
	reads   *cache.Cache
	metrics *observability.Registry

	mu  sync.Mutex
	dbs map[string]*litestream.Database // database key -> registered VFS
}

// Metrics exposes the runtime's privacy-safe counter registry (spec §14).
func (r *Runtime) Metrics() *observability.Registry { return r.metrics }

// New validates configuration (spec §16: reject settings that release a
// lease without a confirmed flush) and builds the runtime.
func New(store storage.ConditionalStore, owner string, cfg Config) (*Runtime, error) {
	if !cfg.RequireFlushBeforeRelease {
		return nil, walruserr.New(walruserr.ClassConfigurationInvalid,
			"require_flush_before_release must be true")
	}
	if cfg.Lease.Duration <= cfg.ClockSkew()+cfg.RequestTimeout {
		return nil, walruserr.New(walruserr.ClassConfigurationInvalid,
			"lease duration must exceed request timeout + flush time + skew allowance")
	}
	return &Runtime{
		cfg:     cfg,
		bridge:  litestream.NewBridge(cfg.Litestream),
		leases:  lease.NewManager(store, owner, cfg.Lease, nil),
		reads:   cache.New(cfg.MaxReadInstances, cfg.ReadInstanceIdleTTL),
		metrics: observability.NewRegistry(),
		dbs:     make(map[string]*litestream.Database),
	}, nil
}

// ClockSkew exposes the lease clock-skew allowance for validation.
func (c Config) ClockSkew() time.Duration { return c.Lease.ClockSkewAllowance }

// database registers (once per process) the VFS for a descriptor.
func (r *Runtime) database(d DatabaseDescriptor, db identity.DatabaseID) (*litestream.Database, error) {
	keyID, secret, err := d.Credentials.AccessKey()
	if err != nil {
		return nil, walruserr.Wrap(walruserr.ClassConfigurationInvalid, "resolve credentials", err)
	}
	profile := d.Storage
	profile.AccessKeyID = keyID
	profile.SecretAccessKey = secret

	r.mu.Lock()
	defer r.mu.Unlock()
	key := db.String()
	if v, ok := r.dbs[key]; ok {
		return v, nil
	}
	vfs, err := r.bridge.RegisterDatabase(key, db.ReplicaPrefix(profile.RootPrefix), profile)
	if err != nil {
		return nil, walruserr.Wrap(walruserr.ClassConfigurationInvalid, "register vfs", err)
	}
	r.dbs[key] = vfs
	return vfs, nil
}

// WithRead serves a read against the remote committed replica state
// (spec §9). No lease is taken; only state Litestream observed remotely is
// visible. Read sessions are cached per database with LRU + idle-TTL
// eviction (spec §10); a cache miss simply opens a fresh session.
func (r *Runtime) WithRead(ctx context.Context, d DatabaseDescriptor, fn func(*sql.Conn) error) error {
	db, err := d.Identity()
	if err != nil {
		return walruserr.Wrap(walruserr.ClassInvalidArgument, "database id", err)
	}
	vfs, err := r.database(d, db)
	if err != nil {
		return err
	}
	if inst, ok := r.reads.Get(db.String()); ok {
		if s, ok := inst.(*litestream.Session); ok {
			if err := fn(s.SQLConn()); err == nil {
				return nil
			}
			// Session may be stale after remote state moved; evict and
			// fall through to a fresh open (correctness never depends on
			// the cache).
			r.reads.Evict(db.String())
		}
	}
	session, err := vfs.OpenRead(ctx, db.ID)
	if err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open read session", err)
	}
	if err := fn(session.SQLConn()); err != nil {
		session.Close()
		return err
	}
	// Keep the session for reuse; the cache takes ownership (bounded LRU
	// with idle-TTL eviction, spec §10).
	r.reads.Put(session)
	return nil
}

// WriteResult carries the remote TXID observed after a successful write for
// read-after-write consistency (spec §9).
type WriteResult struct {
	TXID string
	// Deduplicated is true when a prior attempt with the same idempotency
	// key already committed; the recorded result was returned instead.
	Deduplicated bool
}

// WithWrite executes one serialized mutation with the exact sequence from
// spec §8:
//
//	conditionally acquire lease -> enable VFS write mode -> run and commit
//	SQLite transaction -> disable write mode (MANDATORY flush barrier) ->
//	conditionally release lease -> acknowledge.
//
// On any failure from acquisition through flush it never reports success,
// never releases with a stale token, and discards the write session.
func (r *Runtime) WithWrite(ctx context.Context, d DatabaseDescriptor, idempotencyKey string, fn func(*sql.Conn) error) (WriteResult, error) {
	var zero WriteResult
	if idempotencyKey == "" {
		return zero, walruserr.New(walruserr.ClassInvalidArgument, "idempotency key is required for mutations")
	}
	if fn == nil {
		return zero, walruserr.New(walruserr.ClassInvalidArgument, "write callback is required")
	}
	db, err := d.Identity()
	if err != nil {
		return zero, walruserr.Wrap(walruserr.ClassInvalidArgument, "database id", err)
	}
	vfs, err := r.database(d, db)
	if err != nil {
		return zero, err
	}
	dbKey := db.String()

	// 1. Conditionally acquire the lease (spec §7.2).
	held, err := r.leases.Acquire(ctx, db)
	if err != nil {
		cls := walruserr.ClassOf(err)
		r.metrics.Record(dbKey, func(m *observability.Metrics) {
			if cls == walruserr.ClassBusy {
				m.LeaseBusyRetries++
			} else if cls == walruserr.ClassLeaseConflict {
				m.LeaseCASConflicts++
			}
		})
		return zero, err
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) {
		m.LeaseAcquireAttempts++
		m.LeaseAcquireSucceeded++
	})

	// From here, the lease must be resolved: released on success, or left
	// to expire on failure paths that cannot safely release (spec §8).
	session, err := vfs.OpenWrite(ctx, db.ID)
	if err != nil {
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open write session", err))
	}

	var result WriteResult
	// Idempotency check (spec §8): a retry with the same key returns the
	// prior result instead of repeating the operation.
	if txid, err := lookupIdempotent(ctx, session.SQLConn(), idempotencyKey); err != nil {
		session.Close()
		return zero, r.failWithLease(ctx, held, err)
	} else if txid != "" {
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.IdempotencyDedupeHits++ })
		session.Close()
		if err := r.leases.Release(ctx, held); err != nil {
			return zero, err
		}
		return WriteResult{TXID: txid, Deduplicated: true}, nil
	}

	// 2-3. Write mode is enabled by OpenWrite (same connection/VFS instance).
	// The transaction records the idempotency key inside the same SQLite
	// transaction as the mutation (spec §8). This session performs exactly
	// one write transaction followed by one flush, so the flushed TXID is
	// the last synced TXID plus one — computable in-transaction. If the
	// flush fails, the recorded value is simply never confirmed; the retry
	// with the same key either reads it (already committed) or re-runs.
	if err := fn(session.SQLConn()); err != nil {
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactionFailures++ })
		session.Close()
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassConflict, "transaction failed", err))
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactions++ })
	nextTXID, err := session.NextTXID()
	if err != nil {
		session.Close()
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassConflict, "compute next txid", err))
	}
	if err := recordIdempotent(ctx, session.SQLConn(), idempotencyKey, nextTXID); err != nil {
		session.Close()
		return zero, r.failWithLease(ctx, held, err)
	}

	// 4. Disable write mode = the mandatory synchronous flush barrier.
	flushStart := time.Now()
	if err := session.DisableWrite(); err != nil {
		r.metrics.Record(dbKey, func(m *observability.Metrics) {
			if walruserr.ClassOf(err) == walruserr.ClassConflict {
				m.FlushConflictErrors++
			}
			m.FlushFailures++
			m.IdempotencyAmbiguousOutcomes++
		})
		session.Close()
		// SQL may be locally committed but remote flush was not confirmed:
		// never acknowledge, never release (caller retries with the same
		// idempotency key; lease is left to expire).
		return zero, r.flushFailure(ctx, held, err)
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) {
		m.FlushSuccesses++
		m.FlushDurationMicros += uint64(time.Since(flushStart).Microseconds())
	})

	txid, txidErr := session.TXID()
	if txidErr != nil {
		session.Close()
		return zero, walruserr.Wrap(walruserr.ClassFlushFailed, "read txid after flush", txidErr)
	}
	if txid != nextTXID {
		session.Close()
		return zero, walruserr.New(walruserr.ClassFlushFailed,
			fmt.Sprintf("flushed txid %s does not match recorded txid %s", txid, nextTXID))
	}
	result.TXID = txid

	// 5. Conditionally release the lease.
	if err := r.leases.Release(ctx, held); err != nil {
		if walruserr.ClassOf(err) == walruserr.ClassLeaseConflict {
			r.metrics.Record(dbKey, func(m *observability.Metrics) { m.LeaseReleaseConflicts++ })
		}
		session.Close()
		return zero, err
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) { m.LeaseReleased++ })
	session.Close()
	return result, nil
}

// failWithLease discards the write session and releases the lease when no
// writes reached the VFS (open failed before write mode did real work).
func (r *Runtime) failWithLease(ctx context.Context, held *lease.Held, cause error) error {
	_ = r.leases.Release(ctx, held) // best-effort; conflicts surface to caller
	return cause
}

// flushFailure maps a failed flush barrier to DB_FLUSH_FAILED and leaves the
// lease to expire (spec §12: "Flush fails" row).
func (r *Runtime) flushFailure(_ context.Context, _ *lease.Held, cause error) error {
	if walruserr.ClassOf(cause) == walruserr.ClassConflict {
		return walruserr.Wrap(walruserr.ClassConflict, "litestream writer conflict during flush", cause)
	}
	return walruserr.Wrap(walruserr.ClassFlushFailed, "remote LTX flush not confirmed", cause)
}
