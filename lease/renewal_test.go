package lease_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"walrusd/identity"
	"walrusd/lease"
)

// countingStore counts conditional replaces and can fail them on demand.
type countingStore struct {
	lease.Store
	replaces     atomic.Int64
	failReplaces atomic.Bool
}

func (s *countingStore) ReplaceIfToken(ctx context.Context, key, expectedToken string, body []byte) (string, error) {
	if s.failReplaces.Load() {
		return "", lease.ErrConflict
	}
	s.replaces.Add(1)
	return s.Store.ReplaceIfToken(ctx, key, expectedToken, body)
}

func renewalManager(t *testing.T, store lease.Store) (*lease.Manager, identity.DatabaseID) {
	t.Helper()
	cfg := lease.DefaultConfig()
	cfg.Duration = 60 * time.Millisecond
	cfg.ClockSkewAllowance = 0
	cfg.AcquireRetryBudget = 200 * time.Millisecond
	m := lease.NewManager(store, "api-test", cfg, nil)
	d, err := identity.NewDatabaseID("users/renewal")
	if err != nil {
		t.Fatal(err)
	}
	return m, d
}

// A live holder must keep its lease beyond the original expiry window —
// renewal is what stops a slow transaction or flush from losing its fence
// to a successor — and renewal must stop at release.
func TestHeldRenewsUntilReleased(t *testing.T) {
	store := &countingStore{Store: lease.NewMemoryStore()}
	m, d := renewalManager(t, store)

	held, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // several renewal ticks, inside the hold window

	if held.Lost() {
		t.Fatal("held lease reported lost while the store was healthy")
	}
	if !held.ExpiresAt().After(time.Now()) {
		t.Fatalf("lease expired at %s despite renewal", held.ExpiresAt())
	}
	if got := store.replaces.Load(); got < 2 {
		t.Fatalf("renewal made %d replaces, want at least 2", got)
	}

	if err := m.Release(context.Background(), held); err != nil {
		t.Fatalf("release: %v", err)
	}
	before := store.replaces.Load()
	time.Sleep(80 * time.Millisecond)
	if got := store.replaces.Load(); got != before {
		t.Fatalf("renewal continued after release: %d replaces, want %d", got, before)
	}
}

// A renewal lost to the store marks the lease unusable: the runtime must
// not flush or release on a fence it can no longer prove it owns.
func TestHeldLostWhenRenewalFails(t *testing.T) {
	store := &countingStore{Store: lease.NewMemoryStore()}
	m, d := renewalManager(t, store)

	held, err := m.Acquire(context.Background(), d)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	store.failReplaces.Store(true)
	time.Sleep(80 * time.Millisecond) // renewal ticks fail

	if !held.Lost() {
		t.Fatal("held lease not marked lost after failed renewals")
	}
}
