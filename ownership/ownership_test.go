package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// memCAS is a faithful in-memory CAS adapter (mirrors commit's semantics).
type memCAS struct {
	mu   sync.Mutex
	objs map[string]casObj
	n    int
}

type casObj struct {
	body []byte
	ver  string
}

func newMemCAS() *memCAS { return &memCAS{objs: map[string]casObj{}} }

func (a *memCAS) Get(_ context.Context, key string) ([]byte, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.objs[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), o.body...), o.ver, nil
}

func (a *memCAS) put(key string, body []byte) string {
	a.n++
	v := string(rune('a'+a.n%26)) + "-v"
	a.objs[key] = casObj{body: append([]byte(nil), body...), ver: v}
	return v
}

func (a *memCAS) CreateIfAbsent(_ context.Context, key string, body []byte) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.objs[key]; ok {
		return "", ErrConflict
	}
	return a.put(key, body), nil
}

func (a *memCAS) ReplaceIfVersion(_ context.Context, key string, expected string, body []byte) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.objs[key]
	if !ok && expected != "" {
		return "", ErrConflict
	}
	if ok && o.ver != expected {
		return "", ErrConflict
	}
	return a.put(key, body), nil
}

func testCfg() LeaseConfig {
	return LeaseConfig{
		LeaseDuration: 30 * time.Second,
		RenewInterval: 10 * time.Second,
		MaxClockSkew:  2 * time.Second,
	}
}

func TestAcquireCreatesEpoch1(t *testing.T) {
	m, err := New(newMemCAS(), "w1", testCfg())
	if err != nil {
		t.Fatal(err)
	}
	res, err := m.AcquireOrRenew(context.Background(), "org/o/user/u", "k")
	if err != nil {
		t.Fatal(err)
	}
	if res.Record.Epoch != 1 || res.Record.OwnerWorkerID != "w1" || res.Record.LeaseID == "" {
		t.Fatalf("bad record: %+v", res.Record)
	}
}

func TestRenewKeepsEpoch(t *testing.T) {
	m, err := New(newMemCAS(), "w1", testCfg())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r1, err := m.AcquireOrRenew(ctx, "org/o/user/u", "k")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := m.AcquireOrRenew(ctx, "org/o/user/u", "k")
	if err != nil {
		t.Fatal(err)
	}
	if r2.Record.Epoch != r1.Record.Epoch {
		t.Fatalf("renew must not bump epoch: %d -> %d", r1.Record.Epoch, r2.Record.Epoch)
	}
	if r2.Record.LeaseID != r1.Record.LeaseID {
		t.Fatal("renew must keep lease id")
	}
	if !r2.Record.LeaseExpiresAt.After(r1.Record.LeaseExpiresAt) {
		t.Fatal("renew must extend expiry")
	}
}

func TestTakeoverBumpsEpoch(t *testing.T) {
	store := newMemCAS()
	m1, _ := New(store, "w1", testCfg())
	ctx := context.Background()
	if _, err := m1.AcquireOrRenew(ctx, "org/o/user/u", "k"); err != nil {
		t.Fatal(err)
	}
	m2, _ := New(store, "w2", testCfg())
	// Lease still valid: takeover must be refused.
	if _, err := m2.AcquireOrRenew(ctx, "org/o/user/u", "k"); !errors.Is(err, ErrTakeoverPending) {
		t.Fatalf("want ErrTakeoverPending, got %v", err)
	}
	// Expire the lease.
	m2.leases.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	m2.now = m2.leases.Now
	r, err := m2.AcquireOrRenew(ctx, "org/o/user/u", "k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Record.Epoch != 2 || r.Record.OwnerWorkerID != "w2" {
		t.Fatalf("takeover must bump epoch to 2 for w2: %+v", r.Record)
	}
	// Original writer is now fenced: w1's live clock sees a lease owned by
	// w2 that appears valid (issued in the future relative to w1's clock),
	// so its renewal is refused as takeover-pending — either way it may not
	// write. A real deployment also checks owner identity before renewing.
	if _, err := m1.AcquireOrRenew(ctx, "org/o/user/u", "k"); err == nil {
		t.Fatalf("old writer must be fenced, got nil error")
	}
}

func TestStaleWriterCannotOverwrite(t *testing.T) {
	store := newMemCAS()
	m1, _ := New(store, "w1", testCfg())
	ctx := context.Background()
	r1, _ := m1.AcquireOrRenew(ctx, "org/o/user/u", "k")

	// Simulate a stale writer replaying an old CAS: version has moved.
	m2, _ := New(store, "w2", testCfg())
	future := time.Now().Add(2 * time.Hour)
	m2.now = func() time.Time { return future }
	if _, err := m2.AcquireOrRenew(ctx, "org/o/user/u", "k"); err != nil {
		t.Fatal(err)
	}
	// w1's stale replace uses r1.Version; must conflict, never overwrite.
	raw, _ := json.Marshal(r1.Record)
	if _, err := store.ReplaceIfVersion(ctx, "k", r1.Version, raw); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS must conflict, got %v", err)
	}
	// The record still belongs to w2 at a higher epoch.
	body, _, _ := store.Get(ctx, "k")
	var rec Record
	_ = json.Unmarshal(body, &rec)
	if rec.OwnerWorkerID != "w2" {
		t.Fatalf("record overwritten by stale writer: %+v", rec)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := LeaseConfig{LeaseDuration: 10 * time.Second, RenewInterval: 10 * time.Second, MaxClockSkew: 2 * time.Second}
	if _, err := New(newMemCAS(), "w", bad); err == nil {
		t.Fatal("renew+skew >= lease must be rejected")
	}
}
