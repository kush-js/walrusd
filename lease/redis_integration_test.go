package lease_test

import (
	"context"
	"os"
	"testing"
	"time"

	"walrus/identity"
	"walrus/lease"
)

func redisStore(t *testing.T) *lease.RedisStore {
	t.Helper()
	addr := os.Getenv("WALRUS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("WALRUS_TEST_REDIS_ADDR not set; skipping live Redis test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := lease.NewRedisStore(ctx, lease.RedisOptions{Addr: addr})
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRedisStoreConformance(t *testing.T) {
	RunStoreConformance(t, func(t *testing.T) lease.Store { return redisStore(t) })
}

// Two managers sharing one Redis instance serialize: the second acquires
// only after the first releases, with epoch monotonicity across the handoff.
func TestRedisCrossManagerHandoff(t *testing.T) {
	s := redisStore(t)
	ctx := context.Background()
	d, err := identity.NewDatabaseID("users/redis-handoff")
	if err != nil {
		t.Fatal(err)
	}
	m1 := lease.NewManager(s, "api-1", lease.DefaultConfig(), nil)
	held, err := m1.Acquire(ctx, d)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	m2 := lease.NewManager(s, "api-2", lease.DefaultConfig(), nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h2, err := m2.Acquire(ctx, d)
		if err != nil {
			t.Errorf("acquire 2: %v", err)
			return
		}
		if h2.Epoch() != held.Epoch()+1 {
			t.Errorf("epoch = %d, want %d", h2.Epoch(), held.Epoch()+1)
		}
		if err := m2.Release(ctx, h2); err != nil {
			t.Errorf("release 2: %v", err)
		}
	}()
	select {
	case <-done:
		t.Fatal("second manager acquired while first holds the lease")
	case <-time.After(200 * time.Millisecond):
	}
	if err := m1.Release(ctx, held); err != nil {
		t.Fatalf("release 1: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second manager never acquired after release")
	}
}
