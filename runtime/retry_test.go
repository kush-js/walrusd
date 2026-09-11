package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"walrusd/identity"
	"walrusd/lease"
	"walrusd/runtime"
	"walrusd/walrusderr"
)

func retryTestRuntime(t *testing.T, store lease.Store, owner string, policy runtime.RetryPolicy) *runtime.Runtime {
	t.Helper()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	cfg.RequestTimeout = 500 * time.Millisecond
	cfg.Lease.Duration = time.Second
	cfg.Lease.ClockSkewAllowance = 0
	cfg.Lease.AcquireRetryBudget = 5 * time.Millisecond
	cfg.Lease.RetryBackoffMin = time.Millisecond
	cfg.Lease.RetryBackoffMax = 2 * time.Millisecond
	cfg.RetryPolicy = policy
	rt, err := runtime.New(store, owner, cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return rt
}

func holdLease(t *testing.T, store lease.Store, d runtime.DatabaseDescriptor) (*lease.Manager, *lease.Held) {
	t.Helper()
	db, err := identity.NewDatabaseID(d.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	lm := lease.NewManager(store, "retry-test-holder", lease.DefaultConfig(), nil)
	held, err := lm.Acquire(context.Background(), db, d.Storage.RootPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return lm, held
}

// The production defaults are ten 1s retries, then doubling 2s, 4s, 8s,
// 16s, 32s, 64s delays, with a 64s total wall-clock budget.
func TestWithWriteRetriesUntilLeaseReleased(t *testing.T) {
	store := lease.NewMemoryStore()
	d := descriptor(t, "users/retry-release")
	lm, held := holdLease(t, store, d)

	policy := runtime.RetryPolicy{
		FixedDelay:   10 * time.Millisecond,
		FixedRetries: 1,
		Multiplier:   2,
		MaxDelay:     20 * time.Millisecond,
		MaxTotal:     300 * time.Millisecond,
	}
	rt := retryTestRuntime(t, store, "retry-release", policy)

	released := make(chan error, 1)
	go func() {
		time.Sleep(35 * time.Millisecond)
		released <- lm.Release(context.Background(), held)
	}()

	res, err := rt.WithWrite(context.Background(), d, "retry-release", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`)
		return err
	})
	if err != nil {
		t.Fatalf("write should succeed after lease release: %v", err)
	}
	if res.TXID == "" || res.Deduplicated {
		t.Fatalf("unexpected result: %+v", res)
	}
	if err := <-released; err != nil {
		t.Fatalf("release held lease: %v", err)
	}
	if got := rt.MetricsTotal().WriteRetries; got == 0 {
		t.Fatal("WriteRetries = 0, want at least one outer retry")
	}
}

func TestWithWriteRetryBudgetExhaustionReturnsBusy(t *testing.T) {
	store := lease.NewMemoryStore()
	d := descriptor(t, "users/retry-budget")
	lm, held := holdLease(t, store, d)
	defer lm.Release(context.Background(), held)

	policy := runtime.RetryPolicy{
		FixedDelay:   10 * time.Millisecond,
		FixedRetries: 1,
		Multiplier:   2,
		MaxDelay:     20 * time.Millisecond,
		MaxTotal:     80 * time.Millisecond,
	}
	rt := retryTestRuntime(t, store, "retry-budget", policy)

	start := time.Now()
	_, err := rt.WithWrite(context.Background(), d, "retry-budget", func(*sql.Conn) error {
		t.Fatal("callback ran while lease was held")
		return nil
	})
	elapsed := time.Since(start)
	if class(err) != "DB_BUSY" {
		t.Fatalf("class = %q, want DB_BUSY (err=%v)", class(err), err)
	}
	if elapsed < 40*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("retry budget elapsed %s, want roughly 80ms", elapsed)
	}
}

func TestWithWriteTerminalErrorIsNotRetried(t *testing.T) {
	rt := newTestRuntime(t, "terminal")
	d := descriptor(t, "users/terminal")

	start := time.Now()
	_, err := rt.WithWrite(context.Background(), d, "", func(*sql.Conn) error {
		t.Fatal("callback ran for invalid request")
		return nil
	})
	if walrusderr.ClassOf(err) != walrusderr.ClassInvalidArgument {
		t.Fatalf("class = %q, want DB_INVALID_ARGUMENT", walrusderr.ClassOf(err))
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("terminal error took %s, want immediate failure", elapsed)
	}
	if got := rt.MetricsTotal().WriteRetries; got != 0 {
		t.Fatalf("WriteRetries = %d, want 0", got)
	}
}

// releaseErrorStore commits the release but reports an ambiguous transport
// failure, forcing the outer loop to retry after a successfully flushed
// first attempt.
type releaseErrorStore struct {
	*lease.MemoryStore
	mu     sync.Mutex
	failed bool
}

func (s *releaseErrorStore) ReplaceIfToken(ctx context.Context, key, expectedToken string, body []byte) (string, error) {
	token, err := s.MemoryStore.ReplaceIfToken(ctx, key, expectedToken, body)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && !s.failed {
		s.failed = true
		return token, errors.New("injected lost release response")
	}
	return token, err
}

func TestWithWriteRetryDeduplicatesCommittedAttempt(t *testing.T) {
	store := &releaseErrorStore{MemoryStore: lease.NewMemoryStore()}
	d := descriptor(t, "users/retry-dedup")
	policy := runtime.RetryPolicy{
		FixedDelay:   10 * time.Millisecond,
		FixedRetries: 1,
		Multiplier:   2,
		MaxDelay:     20 * time.Millisecond,
		MaxTotal:     300 * time.Millisecond,
	}
	rt := retryTestRuntime(t, store, "retry-dedup", policy)

	calls := 0
	res, err := rt.WithWrite(context.Background(), d, "committed", func(conn *sql.Conn) error {
		calls++
		if _, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('once')`)
		return err
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if !res.Deduplicated {
		t.Fatalf("Deduplicated = false, want true: %+v", res)
	}
	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
	var recorded string
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(),
			`SELECT txid FROM _walrusd_idempotency WHERE idempotency_key = ?`, "committed").Scan(&recorded)
	}); err != nil {
		t.Fatalf("read idempotency record: %v", err)
	}
	if res.TXID != recorded {
		t.Fatalf("retry TXID = %q, want original %q", res.TXID, recorded)
	}
}

func TestWithWriteRetryStopsOnContextCancellation(t *testing.T) {
	store := lease.NewMemoryStore()
	d := descriptor(t, "users/retry-cancel")
	lm, held := holdLease(t, store, d)
	defer lm.Release(context.Background(), held)

	policy := runtime.RetryPolicy{
		FixedDelay:   500 * time.Millisecond,
		FixedRetries: 10,
		Multiplier:   2,
		MaxDelay:     time.Second,
		MaxTotal:     5 * time.Second,
	}
	rt := retryTestRuntime(t, store, "retry-cancel", policy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := rt.WithWrite(ctx, d, "retry-cancel", func(*sql.Conn) error {
		t.Fatal("callback ran while lease was held")
		return nil
	})
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("cancellation took %s, want prompt return", elapsed)
	}
}
