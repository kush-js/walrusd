package litestream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A registry lock collision must fail within the caller's deadline — the
// semaphore exists so a re-entrant call (or a stuck open) surfaces
// ErrRegistryBusy instead of wedging every database in the process.
func TestRegistryLockBusyFailsFast(t *testing.T) {
	if err := lockRegistry(context.Background()); err != nil {
		t.Fatalf("lock registry: %v", err)
	}
	defer unlockRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := lockRegistry(ctx)
	if !errors.Is(err, ErrRegistryBusy) {
		t.Fatalf("lockRegistry on held semaphore = %v, want ErrRegistryBusy", err)
	}
}
