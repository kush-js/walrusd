package runtime

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"walrusd/cache"
	"walrusd/identity"
	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/observability"
	"walrusd/walrusderr"
)

// Config is the runtime configuration (spec §16).
type Config struct {
	Lease               lease.Config
	Litestream          litestream.Config
	RequestTimeout      time.Duration // 20s
	RetryPolicy         RetryPolicy   // outer WithWrite retry policy
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
		RetryPolicy:               DefaultRetryPolicy(),
		ReadInstanceIdleTTL:       60 * time.Second,
		MaxReadInstances:          200,
		MaxTempWriteBuffer:        268435456,
		RequireFlushBeforeRelease: true,
	}
}

// Runtime is the walrusd runtime surface (spec §11).
type Runtime struct {
	cfg     Config
	bridge  *litestream.Bridge
	leases  *lease.Manager
	reads   *cache.Cache
	metrics *observability.Registry

	mu      sync.Mutex
	dbs     map[string]*list.Element // database key -> registered VFS
	dbOrder *list.List               // front = most recently used
	dbLimit int
	closed  bool
}

type databaseEntry struct {
	key     string
	vfs     *litestream.Database
	refs    int
	evicted bool
}

var errReadSessionClosed = errors.New("runtime: cached read session closed")

type cachedReadSession struct {
	session   *litestream.Session
	metricsID string

	mu     sync.Mutex
	closed bool
}

func newCachedReadSession(s *litestream.Session, metricsID string) *cachedReadSession {
	return &cachedReadSession{session: s, metricsID: metricsID}
}

func (s *cachedReadSession) Key() string { return s.session.Key() }

func (s *cachedReadSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.session.Close()
}

func (s *cachedReadSession) run(ctx context.Context, fn func(*sql.Conn) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errReadSessionClosed
	}
	return s.session.Run(ctx, fn)
}

// New validates configuration (spec §16: reject settings that release a
// lease without a confirmed flush) and builds the runtime. Leases live in
// store (Redis/Valkey for shared deployments, memory for dev/tests);
// object storage needs no conditional-write support.
func New(store lease.Store, owner string, cfg Config) (*Runtime, error) {
	if !cfg.RequireFlushBeforeRelease {
		return nil, walrusderr.New(walrusderr.ClassConfigurationInvalid,
			"require_flush_before_release must be true")
	}
	if cfg.Lease.Duration <= cfg.ClockSkew()+cfg.RequestTimeout {
		return nil, walrusderr.New(walrusderr.ClassConfigurationInvalid,
			"lease duration must exceed request timeout + flush time + skew allowance")
	}
	lsCfg := cfg.Litestream
	lsCfg.MaxTempWriteBuffer = cfg.MaxTempWriteBuffer
	dbLimit := cfg.MaxReadInstances
	if dbLimit <= 0 {
		dbLimit = 64
	}
	rt := &Runtime{
		cfg:     cfg,
		bridge:  litestream.NewBridge(lsCfg),
		leases:  lease.NewManager(store, owner, cfg.Lease, nil),
		reads:   cache.New(cfg.MaxReadInstances, cfg.ReadInstanceIdleTTL),
		metrics: observability.NewRegistry(),
		dbs:     make(map[string]*list.Element),
		dbOrder: list.New(),
		dbLimit: dbLimit,
	}
	rt.reads.SetObserver(rt.readInstanceAdded, rt.readInstanceRemoved)
	return rt, nil
}

func (c Config) ClockSkew() time.Duration { return c.Lease.ClockSkewAllowance }

// MetricsSnapshot returns a copy of the current per-database metrics keyed
// by the privacy-safe database hash (spec §14).
func (r *Runtime) MetricsSnapshot() map[uint64]observability.Metrics {
	return r.metrics.Snapshot()
}

// MetricsTotal returns the process-wide sum of all current metrics.
func (r *Runtime) MetricsTotal() observability.Metrics {
	return r.metrics.Total()
}

func (r *Runtime) readInstanceAdded(inst cache.ReadInstance) {
	session, ok := inst.(*cachedReadSession)
	if !ok {
		return
	}
	r.metrics.Record(session.metricsID, func(m *observability.Metrics) {
		m.ActiveReadInstances++
	})
}

func (r *Runtime) readInstanceRemoved(inst cache.ReadInstance) {
	session, ok := inst.(*cachedReadSession)
	if !ok {
		return
	}
	r.metrics.Record(session.metricsID, func(m *observability.Metrics) {
		if m.ActiveReadInstances > 0 {
			m.ActiveReadInstances--
		}
		m.ReadCacheEvictions++
	})
}

// database registers (once per process) the VFS for a descriptor.
func (r *Runtime) database(d DatabaseDescriptor, db identity.DatabaseID) (*databaseEntry, error) {
	keyID, secret, err := d.Credentials.AccessKey()
	if err != nil {
		return nil, walrusderr.Wrap(walrusderr.ClassConfigurationInvalid, "resolve credentials", err)
	}
	profile := d.Storage
	profile.AccessKeyID = keyID
	profile.SecretAccessKey = secret

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, walrusderr.New(walrusderr.ClassRemoteUnavailable, "runtime is closed")
	}
	// Key by identity AND storage profile: same database_id against a
	// different bucket/prefix/endpoint/file-root is a different replica.
	// Credentials are excluded so rotation reuses the VFS; a profile
	// change registers a new VFS instead of writing through a stale
	// replica client.
	key := db.String() + "|" + profile.Provider + "|" + profile.Endpoint + "|" + profile.Region + "|" + profile.Bucket + "|" + profile.RootPrefix + "|" + profile.FileRoot
	if el, ok := r.dbs[key]; ok {
		r.dbOrder.MoveToFront(el)
		entry := el.Value.(*databaseEntry)
		entry.refs++
		r.mu.Unlock()
		return entry, nil
	}
	vfs, err := r.bridge.RegisterDatabase(key, db.ReplicaPrefix(profile.RootPrefix), profile)
	if err != nil {
		r.mu.Unlock()
		return nil, walrusderr.Wrap(walrusderr.ClassConfigurationInvalid, "register vfs", err)
	}
	entry := &databaseEntry{key: key, vfs: vfs, refs: 1}
	el := r.dbOrder.PushFront(entry)
	r.dbs[key] = el
	var evicted []*databaseEntry
	for len(r.dbs) > r.dbLimit {
		oldest := r.dbOrder.Back()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(*databaseEntry)
		delete(r.dbs, entry.key)
		r.dbOrder.Remove(oldest)
		entry.evicted = true
		if entry.refs == 0 {
			evicted = append(evicted, entry)
		}
	}
	r.mu.Unlock()
	for _, entry := range evicted {
		_ = r.teardownDatabase(entry)
	}
	return entry, nil
}

// releaseDatabase drops a caller's reference and tears the entry down if it
// was evicted while the caller was using it.
func (r *Runtime) releaseDatabase(entry *databaseEntry) {
	r.mu.Lock()
	if entry.refs <= 0 {
		r.mu.Unlock()
		panic("runtime: database entry released without a reference")
	}
	entry.refs--
	shouldClose := entry.evicted && entry.refs == 0
	r.mu.Unlock()
	if shouldClose {
		_ = r.teardownDatabase(entry)
	}
}

// teardownDatabase drains the cached read session before unregistering the
// VFS names. The session close waits for an in-flight run to finish, which
// guarantees no connection still owns either VFS.
func (r *Runtime) teardownDatabase(entry *databaseEntry) error {
	if r.reads != nil {
		r.reads.Evict(entry.key)
	}
	return entry.vfs.Close()
}

// Close releases cached read sessions and database-owned temporary files.
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	var dbs []*databaseEntry
	for el := r.dbOrder.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*databaseEntry)
		entry.evicted = true
		if entry.refs == 0 {
			dbs = append(dbs, entry)
		}
	}
	r.dbs = make(map[string]*list.Element)
	r.dbOrder.Init()
	r.mu.Unlock()

	var errs []error
	if r.reads != nil {
		if err := r.reads.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, entry := range dbs {
		if err := r.teardownDatabase(entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ReadDSN registers (once per process) the read VFS for the descriptor's
// database and returns a SQLite DSN the HOST's own SQLite (e.g. node:sqlite,
// which enables SQLite's URI filenames) can open natively in litestream read
// mode: reads stream LTX pages straight from object storage through the shared
// VFS. The DSN is read-only; writes must go through WithWrite (spec §8).
func (r *Runtime) ReadDSN(ctx context.Context, d DatabaseDescriptor) (string, error) {
	db, err := d.Identity()
	if err != nil {
		return "", walrusderr.Wrap(walrusderr.ClassInvalidArgument, "database id", err)
	}
	entry, err := r.database(d, db)
	if err != nil {
		return "", err
	}
	defer r.releaseDatabase(entry)
	return entry.vfs.ReadDSN(ctx, db.ID), nil
}

func (r *Runtime) WithRead(ctx context.Context, d DatabaseDescriptor, fn func(*sql.Conn) error) (err error) {
	db, err := d.Identity()
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassInvalidArgument, "database id", err)
	}
	metricsID := db.String()
	defer func() {
		if err != nil {
			r.metrics.Record(metricsID, func(m *observability.Metrics) { m.ReadFailures++ })
		}
	}()
	entry, err := r.database(d, db)
	if err != nil {
		return err
	}
	defer r.releaseDatabase(entry)
	vfs := entry.vfs
	// Bound reads even for callers without a deadline. Parent cancellation
	// still wins: the effective deadline is min(parent, timeout).
	if r.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.RequestTimeout)
		defer cancel()
	}
	key := vfs.Key
	if inst, ok := r.reads.Get(key); ok {
		r.metrics.Record(metricsID, func(m *observability.Metrics) { m.ReadCacheHits++ })
		session, ok := inst.(*cachedReadSession)
		if !ok {
			r.reads.Evict(key)
		} else {
			err := session.run(ctx, fn)
			if err == nil {
				return nil
			}
			r.reads.Evict(key)
			if !errors.Is(err, errReadSessionClosed) {
				return err
			}
		}
	}
	// Empty-DB fast path: a read-mode VFS open on a zero-LTX replica
	// blocks forever in Litestream's waitForRestorePlan (uncancellable
	// CGO). Probe first; with no flushed state the database is empty by
	// definition, so serve fn an empty query-only connection instead.
	has, err := vfs.HasLTX(ctx)
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "probe replica", err)
	}
	if !has {
		return withEmptyRead(ctx, fn)
	}
	session, err := vfs.OpenRead(ctx, db.ID)
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "open read session", err)
	}
	r.metrics.Record(metricsID, func(m *observability.Metrics) { m.ReadOpens++ })
	cached := newCachedReadSession(session, metricsID)
	if err := cached.run(ctx, fn); err != nil {
		_ = cached.Close()
		return err
	}
	r.reads.Put(cached)
	return nil
}

// withEmptyRead serves one read against an empty database: no LTX exists,
// so every table is absent. An in-memory query-only connection gives exact
// empty-DB SQLite semantics without touching the VFS.
func withEmptyRead(ctx context.Context, fn func(*sql.Conn) error) error {
	mem, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "open empty read session", err)
	}
	defer mem.Close()
	mem.SetMaxOpenConns(1)
	conn, err := mem.Conn(ctx)
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "open empty read session", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "open empty read session", err)
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
// Retryable failures repeat the whole operation with the same idempotency
// key. On any failure from acquisition through flush it never reports
// success, never releases with a stale token, and discards the write session.
func (r *Runtime) WithWrite(ctx context.Context, d DatabaseDescriptor, idempotencyKey string, fn func(*sql.Conn) error) (WriteResult, error) {
	var zero WriteResult
	if idempotencyKey == "" {
		return zero, walrusderr.New(walrusderr.ClassInvalidArgument, "idempotency key is required for mutations")
	}
	if fn == nil {
		return zero, walrusderr.New(walrusderr.ClassInvalidArgument, "write callback is required")
	}
	db, err := d.Identity()
	if err != nil {
		return zero, walrusderr.Wrap(walrusderr.ClassInvalidArgument, "database id", err)
	}
	dbKey := db.String()
	policy := r.cfg.RetryPolicy.withDefaults()
	start := time.Now()

	// The retry budget bounds the whole operation; RequestTimeout bounds each
	// individual attempt so a blocked acquire/transaction/flush cannot pin a
	// caller for the entire retry window.
	budgetCtx := ctx
	var budgetDeadline time.Time
	if policy.MaxTotal > 0 {
		budgetDeadline = start.Add(policy.MaxTotal)
		if deadline, ok := budgetCtx.Deadline(); !ok || budgetDeadline.Before(deadline) {
			var cancelBudget context.CancelFunc
			budgetCtx, cancelBudget = context.WithDeadline(budgetCtx, budgetDeadline)
			defer cancelBudget()
		}
	}

	for retry := 0; ; retry++ {
		attemptCtx := budgetCtx
		var cancelAttempt context.CancelFunc
		if r.cfg.RequestTimeout > 0 {
			attemptCtx, cancelAttempt = context.WithTimeout(budgetCtx, r.cfg.RequestTimeout)
		}
		res, err := r.withWriteAttempt(attemptCtx, db, d, idempotencyKey, fn)
		if cancelAttempt != nil {
			cancelAttempt()
		}
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if budgetCtx.Err() != nil {
			if isRetryableWrite(err) {
				return zero, err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return zero, walrusderr.Busy("write retry budget exhausted", 0)
			}
		}
		if policy.MaxTotal <= 0 || !isRetryableWrite(err) {
			return zero, err
		}

		delay := policy.delay(retry+1, err)
		if delay <= 0 {
			return zero, err
		}
		if !budgetDeadline.IsZero() {
			remaining := time.Until(budgetDeadline)
			if remaining <= 0 {
				return zero, err
			}
			if delay > remaining {
				delay = remaining
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-budgetCtx.Done():
			timer.Stop()
			return zero, err
		case <-timer.C:
		}
		if !budgetDeadline.IsZero() && !time.Now().Before(budgetDeadline) {
			return zero, err
		}
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteRetries++ })
	}
}

func (r *Runtime) withWriteAttempt(ctx context.Context, db identity.DatabaseID, d DatabaseDescriptor, idempotencyKey string, fn func(*sql.Conn) error) (WriteResult, error) {
	var zero WriteResult
	entry, err := r.database(d, db)
	if err != nil {
		return zero, err
	}
	defer r.releaseDatabase(entry)
	vfs := entry.vfs
	dbKey := db.String()
	// 1. Conditionally acquire the lease (spec §7.2).
	held, err := r.leases.Acquire(ctx, db, d.Storage.RootPrefix)
	if err != nil {
		cls := walrusderr.ClassOf(err)
		r.metrics.Record(dbKey, func(m *observability.Metrics) {
			if cls == walrusderr.ClassBusy {
				m.LeaseBusyRetries++
			} else if cls == walrusderr.ClassLeaseConflict {
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
		return zero, r.failWithLease(ctx, held, walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "open write session", err))
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
		return zero, r.failWithLease(ctx, held, walrusderr.Wrap(walrusderr.ClassConflict, "begin write transaction", err))
	}
	var result WriteResult
	// Idempotency check (spec §8): a retry with the same key returns the
	// prior result instead of repeating the operation.
	var existingTXID string
	if err := session.Run(ctx, func(conn *sql.Conn) error {
		var err error
		existingTXID, err = lookupIdempotent(ctx, conn, idempotencyKey)
		return err
	}); err != nil {
		rollback()
		session.Close()
		return zero, r.failWithLease(ctx, held, err)
	} else if existingTXID != "" {
		rollback()
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.IdempotencyDedupeHits++ })
		session.Close()
		if err := r.leases.Release(ctx, held); err != nil {
			return zero, err
		}
		return WriteResult{TXID: existingTXID, Deduplicated: true}, nil
	}
	// 3. Run the mutation inside the open transaction. Write mode is
	// enabled by OpenWrite on the same connection/VFS instance. This
	// session performs exactly one write transaction followed by one
	// flush, so the flushed TXID is the last synced TXID plus one.
	if err := session.Run(ctx, fn); err != nil {
		rollback()
		r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactionFailures++ })
		session.Close()
		if isNestedTxError(err) {
			return zero, r.failWithLease(ctx, held, walrusderr.Wrap(walrusderr.ClassInvalidArgument, "write callback must not manage transactions (no BEGIN/COMMIT inside fn)", err))
		}
		return zero, r.failWithLease(ctx, held, walrusderr.Wrap(walrusderr.ClassConflict, "transaction failed", err))
	}
	r.metrics.Record(dbKey, func(m *observability.Metrics) { m.WriteTransactions++ })
	nextTXID, err := session.NextTXID()
	if err != nil {
		rollback()
		session.Close()
		return zero, r.failWithLease(ctx, held, walrusderr.Wrap(walrusderr.ClassConflict, "compute next txid", err))
	}
	if err := session.Run(ctx, func(conn *sql.Conn) error {
		return recordIdempotent(ctx, conn, idempotencyKey, nextTXID)
	}); err != nil {
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
		return zero, walrusderr.New(walrusderr.ClassLeaseConflict, "lease expired before flush; transaction rolled back, retry with the same idempotency key")
	}
	if _, err := session.Exec(ctx, `COMMIT`); err != nil {
		rollback()
		session.Close()
		// COMMIT may have partially reached storage: ambiguous like a
		// flush failure — never ack, leave lease to expire.
		return zero, r.flushFailure(ctx, held, walrusderr.Wrap(walrusderr.ClassFlushFailed, "commit not confirmed", err))
	}
	// 4. Disable write mode = the mandatory synchronous flush barrier.
	flushStart := time.Now()
	if err := session.DisableWrite(); err != nil {
		r.metrics.Record(dbKey, func(m *observability.Metrics) {
			if walrusderr.ClassOf(err) == walrusderr.ClassConflict {
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
		return zero, walrusderr.Wrap(walrusderr.ClassFlushFailed, "read txid after flush", txidErr)
	}
	if txid != nextTXID {
		session.Close()
		return zero, walrusderr.New(walrusderr.ClassFlushFailed,
			fmt.Sprintf("flushed txid %s does not match recorded txid %s", txid, nextTXID))
	}
	result.TXID = txid
	// A cached read session may still hold the pre-write index. Drop it so
	// the next read opens at the flushed remote state.
	r.reads.Evict(vfs.Key)
	// 5. Conditionally release the lease.
	if err := r.leases.Release(ctx, held); err != nil {
		if walrusderr.ClassOf(err) == walrusderr.ClassLeaseConflict {
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
	if walrusderr.ClassOf(cause) == walrusderr.ClassConflict {
		return walrusderr.Wrap(walrusderr.ClassConflict, "litestream writer conflict during flush", cause)
	}
	return walrusderr.Wrap(walrusderr.ClassFlushFailed, "remote LTX flush not confirmed", cause)
}
