package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"walrus/identity"
	"walrus/lease"
	"walrus/litestream"
	"walrus/observability"
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
	metrics *observability.Registry

	mu  sync.Mutex
	dbs map[string]*litestream.Database // database key -> registered VFS
}

// New validates configuration (spec §16: reject settings that release a
// lease without a confirmed flush) and builds the runtime. Leases live in
// store (Redis/Valkey for shared deployments, memory for dev/tests);
// object storage needs no conditional-write support.
func New(store lease.Store, owner string, cfg Config) (*Runtime, error) {
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
		metrics: observability.NewRegistry(),
		dbs:     make(map[string]*litestream.Database),
	}, nil
}

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
	// Key by identity AND storage profile: same database_id against a
	// different bucket/prefix/endpoint/file-root is a different replica.
	// Credentials are excluded so rotation reuses the VFS; a profile
	// change registers a new VFS instead of writing through a stale
	// replica client.
	key := db.String() + "|" + profile.Provider + "|" + profile.Endpoint + "|" + profile.Region + "|" + profile.Bucket + "|" + profile.RootPrefix + "|" + profile.FileRoot
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

// ReadDSN registers (once per process) the read VFS for the descriptor's
// database and returns a SQLite DSN the HOST's own SQLite (e.g. Bun's
// bun:sqlite) can open natively in litestream read mode: reads stream LTX
// pages straight from object storage through the shared VFS. The DSN is
// read-only; writes must go through WithWrite (spec §8).
func (r *Runtime) ReadDSN(ctx context.Context, d DatabaseDescriptor) (string, error) {
	db, err := d.Identity()
	if err != nil {
		return "", walruserr.Wrap(walruserr.ClassInvalidArgument, "database id", err)
	}
	vfs, err := r.database(d, db)
	if err != nil {
		return "", err
	}
	return vfs.ReadDSN(ctx, db.ID), nil
}

func (r *Runtime) WithRead(ctx context.Context, d DatabaseDescriptor, fn func(*sql.Conn) error) error {
	db, err := d.Identity()
	if err != nil {
		return walruserr.Wrap(walruserr.ClassInvalidArgument, "database id", err)
	}
	vfs, err := r.database(d, db)
	if err != nil {
		return err
	}
	// Bound reads even for callers without a deadline. Parent cancellation
	// still wins: the effective deadline is min(parent, timeout).
	if r.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.RequestTimeout)
		defer cancel()
	}
	// Fresh session per call. The VFS registration is shared via r.dbs,
	// but the *sql.Conn is never shared: the old LRU of Sessions handed
	// one Conn to concurrent callers and closed it under them on
	// evict/replace. Correctness never depends on caching a Conn.
	// Empty-DB fast path: a read-mode VFS open on a zero-LTX replica
	// blocks forever in Litestream's waitForRestorePlan (uncancellable
	// CGO). Probe first; with no flushed state the database is empty by
	// definition, so serve fn an empty query-only connection instead.
	has, err := vfs.HasLTX(ctx)
	if err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "probe replica", err)
	}
	if !has {
		return withEmptyRead(ctx, fn)
	}
	session, err := vfs.OpenRead(ctx, db.ID)
	if err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open read session", err)
	}
	defer session.Close()
	return fn(session.SQLConn())
}

// withEmptyRead serves one read against an empty database: no LTX exists,
// so every table is absent. An in-memory query-only connection gives exact
// empty-DB SQLite semantics without touching the VFS.
func withEmptyRead(ctx context.Context, fn func(*sql.Conn) error) error {
	mem, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open empty read session", err)
	}
	defer mem.Close()
	mem.SetMaxOpenConns(1)
	conn, err := mem.Conn(ctx)
	if err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open empty read session", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
		return walruserr.Wrap(walruserr.ClassRemoteUnavailable, "open empty read session", err)
	}
	return fn(conn)
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
	// Bound the whole write (acquire+tx+flush+release) even for callers
	// without a deadline. Parent cancellation still wins.
	if r.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.RequestTimeout)
		defer cancel()
	}
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
	// Cleanup uses a non-cancelled context so ROLLBACK runs even when the
	// request deadline fired mid-transaction.
	cleanupCtx := context.Background()
	rollback := func() { _, _ = session.Exec(cleanupCtx, `ROLLBACK`) }
	// 2. Single SQLite transaction for mutation + idempotency record.
	// Without this, autocommit commits each statement separately and a
	// crash between the last statement and the idempotency INSERT leaves
	// the mutation durable-but-unrecorded (retry duplicates). Callbacks
	// must not manage transactions themselves.
	if _, err := session.Exec(ctx, `BEGIN IMMEDIATE`); err != nil {
		session.Close()
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassConflict, "begin write transaction", err))
	}
	var result WriteResult
	// Idempotency check (spec §8): a retry with the same key returns the
	// prior result instead of repeating the operation.
	if txid, err := lookupIdempotent(ctx, session.SQLConn(), idempotencyKey); err != nil {
		rollback()
		session.Close()
		return zero, r.failWithLease(ctx, held, err)
	} else if txid != "" {
		rollback()
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.IdempotencyDedupeHits++ })
		session.Close()
		if err := r.leases.Release(ctx, held); err != nil {
			return zero, err
		}
		return WriteResult{TXID: txid, Deduplicated: true}, nil
	}
	// 3. Run the mutation inside the open transaction. Write mode is
	// enabled by OpenWrite on the same connection/VFS instance. This
	// session performs exactly one write transaction followed by one
	// flush, so the flushed TXID is the last synced TXID plus one.
	if err := fn(session.SQLConn()); err != nil {
		rollback()
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactionFailures++ })
		session.Close()
		if isNestedTxError(err) {
			return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassInvalidArgument, "write callback must not manage transactions (no BEGIN/COMMIT inside fn)", err))
		}
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassConflict, "transaction failed", err))
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactions++ })
	nextTXID, err := session.NextTXID()
	if err != nil {
		rollback()
		session.Close()
		return zero, r.failWithLease(ctx, held, walruserr.Wrap(walruserr.ClassConflict, "compute next txid", err))
	}
	if err := recordIdempotent(ctx, session.SQLConn(), idempotencyKey, nextTXID); err != nil {
		rollback()
		session.Close()
		return zero, r.failWithLease(ctx, held, err)
	}
	// Lease-expiry guard: if we already ran past expires_at (minus skew),
	// a successor may own the lease. Flushing now would fork the LTX
	// chain. Roll back, discard, leave the lease to expire — never flush
	// or release on a stale token.
	if r.leaseStale(held) {
		rollback()
		session.Close()
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.LeaseReleaseConflicts++ })
		return zero, walruserr.New(walruserr.ClassLeaseConflict, "lease expired before flush; transaction rolled back, retry with the same idempotency key")
	}
	if _, err := session.Exec(ctx, `COMMIT`); err != nil {
		rollback()
		session.Close()
		// COMMIT may have partially reached storage: ambiguous like a
		// flush failure — never ack, leave lease to expire.
		return zero, r.flushFailure(ctx, held, walruserr.Wrap(walruserr.ClassFlushFailed, "commit not confirmed", err))
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

// leaseStale reports whether the held lease already ran past its expiry
// (minus clock-skew allowance). Flushing or releasing now would risk
// forking the LTX chain under a successor owner.
func (r *Runtime) leaseStale(held *lease.Held) bool {
	if held == nil {
		return true
	}
	return !time.Now().Before(held.Record.ExpiresAt.Add(-r.cfg.Lease.ClockSkewAllowance))
}

// isNestedTxError detects callbacks that issued their own BEGIN/SAVEPOINT.
func isNestedTxError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "cannot start a transaction within a transaction") ||
		strings.Contains(s, "already in a transaction") ||
		strings.Contains(s, "cannot commit - no transaction is active")
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
