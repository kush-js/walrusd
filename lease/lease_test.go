package lease_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"walrus/identity"
	"walrus/lease"
	"walrus/storage"
	"walrus/walruserr"
)

func newManager(t *testing.T, mutate func(*lease.Config)) (*lease.Manager, *storage.MemoryStore) {
	t.Helper()
	store := storage.NewMemoryStore()
	cfg := lease.DefaultConfig()
	cfg.AcquireRetryBudget = 200 * time.Millisecond
	if mutate != nil {
		mutate(&cfg)
	}
	m := lease.NewManager(store, "api-test", cfg, nil)
	return m, store
}

func db(t *testing.T) identity.DatabaseID {
	t.Helper()
	d, err := identity.NewDatabaseID("users/user_1")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestAcquireCreateAbsent(t *testing.T) {
	m, store := newManager(t, nil)
	d := db(t)
	held, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if held.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", held.Epoch())
	}
	body, _, err := store.Get(context.Background(), d.LeaseKey(""))
	if err != nil {
		t.Fatal(err)
	}
	var rec lease.Record
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.State != lease.StateHeld || rec.LeaseID != held.LeaseID() || rec.Owner != "api-test" {
		t.Fatalf("unexpected record %+v", rec)
	}
}

func TestAcquireBusyWhileHeld(t *testing.T) {
	m, _ := newManager(t, nil)
	d := db(t)
	if _, err := m.Acquire(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	_, err := m.Acquire(context.Background(), d)
	if walruserr.ClassOf(err) != walruserr.ClassBusy {
		t.Fatalf("class = %v, want DB_BUSY", err)
	}
	var hint walruserr.RetryAfterHint
	if ok := asHint(err, &hint); !ok {
		t.Fatal("expected Retry-After hint")
	}
}

func asHint(err error, target *walruserr.RetryAfterHint) bool {
	e, ok := err.(walruserr.RetryAfterHint)
	if ok {
		*target = e
	}
	return ok
}

func TestAcquireAfterRelease(t *testing.T) {
	m, _ := newManager(t, nil)
	d := db(t)
	held, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Release(context.Background(), held); err != nil {
		t.Fatalf("release: %v", err)
	}
	next, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if next.Epoch() != held.Epoch()+1 {
		t.Fatalf("epoch = %d, want %d", next.Epoch(), held.Epoch()+1)
	}
	if next.LeaseID() == held.LeaseID() {
		t.Fatal("lease ID must be new per acquisition")
	}
}

func TestAcquireTakeoverAfterExpiry(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	var clock atomic.Value
	clock.Store(now)
	m, _ := newManager(t, func(c *lease.Config) { c.Now = func() time.Time { return clock.Load().(time.Time) } })
	d := db(t)
	if _, err := m.Acquire(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	// Still held: busy.
	if _, err := m.Acquire(context.Background(), d); walruserr.ClassOf(err) != walruserr.ClassBusy {
		t.Fatalf("expected busy, got %v", err)
	}
	// Advance past expiry + skew: takeover succeeds with epoch bump.
	clock.Store(now.Add(35 * time.Second))
	taken, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if taken.Epoch() != 2 {
		t.Fatalf("epoch = %d, want 2", taken.Epoch())
	}
}

func TestReleaseKeepsObjectAndEpoch(t *testing.T) {
	m, store := newManager(t, nil)
	d := db(t)
	held, _ := m.Acquire(context.Background(), d)
	if err := m.Release(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	body, _, err := store.Get(context.Background(), d.LeaseKey(""))
	if err != nil {
		t.Fatal(err)
	}
	var rec lease.Record
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.State != lease.StateReleased || rec.Epoch != 1 {
		t.Fatalf("unexpected record %+v", rec)
	}
}

func TestStaleReleaseConflicts(t *testing.T) {
	// Fresh manager sharing the same store; short lease so takeover is quick.
	store := storage.NewMemoryStore()
	cfg := lease.DefaultConfig()
	cfg.AcquireRetryBudget = 300 * time.Millisecond
	cfg.Duration = 10 * time.Millisecond
	cfg.ClockSkewAllowance = 0
	m, _ := lease.NewManager(store, "api-a", cfg, nil), store
	d := db(t)
	held, _ := m.Acquire(context.Background(), d)
	time.Sleep(15 * time.Millisecond)
	other, err := m.Acquire(context.Background(), d) // takeover via expiry
	if err != nil {
		t.Fatalf("successor acquire: %v", err)
	}
	if err := m.Release(context.Background(), held); walruserr.ClassOf(err) != walruserr.ClassLeaseConflict {
		t.Fatalf("stale release class = %v, want DB_LEASE_CONFLICT", err)
	}
	if err := m.Release(context.Background(), other); err != nil {
		t.Fatalf("fresh release: %v", err)
	}
}

func TestAcquireConcurrentSingleWinner(t *testing.T) {
	m, _ := newManager(t, nil)
	d := db(t)
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Acquire(context.Background(), d); err == nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("winners = %d, want 1", winners.Load())
	}
}

func TestEpochMonotonic(t *testing.T) {
	m, _ := newManager(t, func(c *lease.Config) {
		c.Duration = 5 * time.Millisecond
		c.ClockSkewAllowance = 0
		c.RetryBackoffMin = 1 * time.Millisecond
		c.RetryBackoffMax = 2 * time.Millisecond
	})
	d := db(t)
	prev := uint64(0)
	for i := 0; i < 5; i++ {
		held, err := m.Acquire(context.Background(), d)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if held.Epoch() <= prev {
			t.Fatalf("epoch not monotonic: %d after %d", held.Epoch(), prev)
		}
		prev = held.Epoch()
		time.Sleep(8 * time.Millisecond)
	}
}
