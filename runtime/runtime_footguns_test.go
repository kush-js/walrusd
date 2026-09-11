package runtime_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
	"walrusd/walruserr"
)

// footgunRuntime builds a runtime with caller-supplied timing for
// lease-expiry tests. Duration must exceed RequestTimeout+skew (New enforces).
func footgunRuntime(t *testing.T, store lease.Store, owner string, reqTimeout, leaseDur, skew time.Duration) *runtime.Runtime {
	t.Helper()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	cfg.RequestTimeout = reqTimeout
	cfg.Lease.Duration = leaseDur
	cfg.Lease.ClockSkewAllowance = skew
	cfg.Lease.AcquireRetryBudget = 200 * time.Millisecond
	cfg.Lease.RetryBackoffMin = 5 * time.Millisecond
	cfg.Lease.RetryBackoffMax = 20 * time.Millisecond
	rt, err := runtime.New(store, owner, cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return rt
}

func footgunDescriptor(t *testing.T, id string) runtime.DatabaseDescriptor {
	return runtime.DatabaseDescriptor{
		DatabaseID: id,
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: t.TempDir(),
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
}

// The batch must be one SQLite transaction: a failing second statement rolls
// back the first (no durable-but-unrecorded partial state).
func TestFootgunBatchAtomic(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/atomic")
	_, err := rt.WithWrite(context.Background(), d, "bad-batch", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(),
			`CREATE TABLE atomic_t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(), `THIS IS NOT SQL`)
		return err
	})
	if err == nil {
		t.Fatal("expected bad SQL to fail the batch")
	}
	var n int
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='atomic_t'`).Scan(&n)
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 0 {
		t.Fatalf("partial batch persisted: atomic_t exists")
	}
}

// Callbacks must not manage transactions: the runtime owns BEGIN/COMMIT.
func TestFootgunNestedTransactionRejected(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/nested")
	_, err := rt.WithWrite(context.Background(), d, "nested", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `BEGIN`)
		return err
	})
	if err == nil {
		t.Fatal("expected nested BEGIN to be rejected")
	}
	if got := walruserr.ClassOf(err); got != walruserr.ClassInvalidArgument {
		t.Fatalf("class = %q, want DB_INVALID_ARGUMENT", got)
	}
}

// A transaction that ignores ctx and runs past the request budget must fail
// (never ack), roll back, and leave the same key retryable. Valid configs
// guarantee Duration > RequestTimeout+skew, so the request deadline always
// fires before lease expiry; the leaseStale guard behind it is
// defense-in-depth for clock jumps. The safety contract: error + no
// partial state + clean retry — never a success without confirmed flush.
func TestFootgunLeaseExpiryGuard(t *testing.T) {
	store := lease.NewMemoryStore()
	// Request budget 300ms; fn sleeps 900ms ignoring ctx, so every
	// ctx-bound step after the sleep fails. Lease 600ms keeps New's
	// Duration > RequestTimeout+skew validation happy.
	rt := footgunRuntime(t, store, "api-1", 300*time.Millisecond, 600*time.Millisecond, 50*time.Millisecond)
	d := footgunDescriptor(t, "users/expiry")
	_, err := rt.WithWrite(context.Background(), d, "slow", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(),
			`CREATE TABLE slow_t (id INTEGER PRIMARY KEY)`); err != nil {
			return err
		}
		time.Sleep(900 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Fatal("expected slow write past the request budget to fail")
	}
	t.Logf("slow write failed as required: %v", err)
	// Rolled back: table must not exist, and the same key retries clean.
	var n int
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='slow_t'`).Scan(&n)
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 0 {
		t.Fatalf("expired-lease write leaked partial state")
	}
	res, err := rt.WithWrite(context.Background(), d, "slow", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE slow_t (id INTEGER PRIMARY KEY)`)
		return err
	})
	if err != nil {
		t.Fatalf("retry after expiry guard: %v", err)
	}
	if res.TXID == "" || res.Deduplicated {
		t.Fatalf("unexpected retry result: %+v", res)
	}
}

// Concurrent reads on one DB must never share+close a single *sql.Conn.
func TestFootgunConcurrentReads(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/parread")
	if _, err := rt.WithWrite(context.Background(), d, "seed", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(),
			`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(), `INSERT INTO t (v) VALUES ('x')`)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
				var v string
				return conn.QueryRowContext(context.Background(),
					`SELECT v FROM t LIMIT 1`).Scan(&v)
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
}

// Same database_id under different storage profiles must not share a VFS.
func TestFootgunProfileIsolation(t *testing.T) {
	store := lease.NewMemoryStore()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	rt, err := runtime.New(store, "api-1", cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	mk := func(id, root string) runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: id,
			Storage:    litestream.Profile{Provider: "file", FileRoot: root},
			Credentials: runtime.StaticCredentials{
				AccessKeyID: "k", SecretAccessKey: "s",
			},
		}
	}
	root1, root2 := t.TempDir(), t.TempDir()
	d1, d2 := mk("users/same", root1), mk("users/same", root2)
	ctx := context.Background()
	if _, err := rt.WithWrite(ctx, d1, "k1", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE t (v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO t VALUES ('r1')`)
		return err
	}); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := rt.WithWrite(ctx, d2, "k1", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE t (v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO t VALUES ('r2')`)
		return err
	}); err != nil {
		t.Fatalf("write2: %v", err)
	}
	var v1, v2 string
	if err := rt.WithRead(ctx, d1, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT v FROM t LIMIT 1`).Scan(&v1)
	}); err != nil {
		t.Fatalf("read1: %v", err)
	}
	if err := rt.WithRead(ctx, d2, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT v FROM t LIMIT 1`).Scan(&v2)
	}); err != nil {
		t.Fatalf("read2: %v", err)
	}
	if v1 != "r1" || v2 != "r2" {
		t.Fatalf("profile isolation broken: %q vs %q", v1, v2)
	}
}

// Idempotent retry across instances returns the original TXID.
func TestFootgunDedupAcrossInstances(t *testing.T) {
	store := lease.NewMemoryStore()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	newRT := func(owner string) *runtime.Runtime {
		rt, err := runtime.New(store, owner, cfg)
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		return rt
	}
	// Share one file root so both instances see one replica.
	root := t.TempDir()
	mk := func() runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID:  "users/dedup",
			Storage:     litestream.Profile{Provider: "file", FileRoot: root},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
	}
	ctx := context.Background()
	r1, err := newRT("api-1").WithWrite(ctx, mk(), "op", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO t (v) VALUES ('a')`)
		return err
	})
	if err != nil {
		t.Fatalf("write1: %v", err)
	}
	r2, err := newRT("api-2").WithWrite(ctx, mk(), "op", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO t (v) VALUES ('SHOULD NOT RUN')`)
		return err
	})
	if err != nil {
		t.Fatalf("write2: %v", err)
	}
	if !r2.Deduplicated || r2.TXID != r1.TXID {
		t.Fatalf("dedup = %+v, want txid %s", r2, r1.TXID)
	}
}
