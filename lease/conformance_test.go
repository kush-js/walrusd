package lease_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"walrus/lease"
)

// RunStoreConformance exercises the lease.Store CAS contract every backend
// must satisfy: read-after-write, create-if-absent exclusivity, token-gated
// replace, and exactly-one-winner under concurrency.
func RunStoreConformance(t *testing.T, newStore func(t *testing.T) lease.Store) {
	t.Helper()
	ctx := context.Background()
	ns := fmt.Sprintf("conformance/%d/", time.Now().UnixNano())

	t.Run("ReadAfterWrite", func(t *testing.T) {
		s := newStore(t)
		tok, err := s.CreateIfAbsent(ctx, ns+"a", []byte(`{"v":1}`))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if tok == "" {
			t.Fatal("empty token")
		}
		body, got, err := s.Get(ctx, ns+"a")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(body) != `{"v":1}` || got != tok {
			t.Fatalf("get = %q %q, want body+token back", body, got)
		}
	})

	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		if _, _, err := s.Get(ctx, ns+"missing"); !errors.Is(err, lease.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateExclusivity", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.CreateIfAbsent(ctx, ns+"b", []byte("one")); err != nil {
			t.Fatalf("first create: %v", err)
		}
		if _, err := s.CreateIfAbsent(ctx, ns+"b", []byte("two")); !errors.Is(err, lease.ErrAlreadyExists) {
			t.Fatalf("err = %v, want ErrAlreadyExists", err)
		}
		body, _, _ := s.Get(ctx, ns+"b")
		if string(body) != "one" {
			t.Fatalf("loser overwrote: %q", body)
		}
	})

	t.Run("ReplaceGatedOnToken", func(t *testing.T) {
		s := newStore(t)
		tok, err := s.CreateIfAbsent(ctx, ns+"c", []byte("one"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := s.ReplaceIfToken(ctx, ns+"c", "stale", []byte("bad")); !errors.Is(err, lease.ErrConflict) {
			t.Fatalf("err = %v, want ErrConflict", err)
		}
		tok2, err := s.ReplaceIfToken(ctx, ns+"c", tok, []byte("two"))
		if err != nil {
			t.Fatalf("replace: %v", err)
		}
		if tok2 == tok {
			t.Fatal("token must rotate on every write")
		}
		if _, err := s.ReplaceIfToken(ctx, ns+"c", tok, []byte("stale")); !errors.Is(err, lease.ErrConflict) {
			t.Fatalf("reused token err = %v, want ErrConflict", err)
		}
		if _, err := s.ReplaceIfToken(ctx, ns+"ghost", tok, []byte("x")); !errors.Is(err, lease.ErrNotFound) {
			t.Fatalf("missing-key err = %v, want ErrNotFound", err)
		}
	})

	t.Run("ConcurrentCreateOneWinner", func(t *testing.T) {
		s := newStore(t)
		const n = 16
		var wg sync.WaitGroup
		wins := make(chan string, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if tok, err := s.CreateIfAbsent(ctx, ns+"d", []byte(fmt.Sprintf("w%d", i))); err == nil {
					wins <- tok
				} else if !errors.Is(err, lease.ErrAlreadyExists) {
					t.Errorf("w%d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		close(wins)
		if len(wins) != 1 {
			t.Fatalf("winners = %d, want exactly 1", len(wins))
		}
	})

	t.Run("ConcurrentReplaceOneWinner", func(t *testing.T) {
		s := newStore(t)
		tok, err := s.CreateIfAbsent(ctx, ns+"e", []byte("base"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		const n = 16
		var wg sync.WaitGroup
		wins := make(chan string, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if nt, err := s.ReplaceIfToken(ctx, ns+"e", tok, []byte(fmt.Sprintf("w%d", i))); err == nil {
					wins <- nt
				} else if !errors.Is(err, lease.ErrConflict) && !errors.Is(err, lease.ErrNotFound) {
					t.Errorf("w%d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		close(wins)
		if len(wins) != 1 {
			t.Fatalf("winners = %d, want exactly 1", len(wins))
		}
	})
}

func TestMemoryStoreConformance(t *testing.T) {
	RunStoreConformance(t, func(t *testing.T) lease.Store {
		return lease.NewMemoryStore()
	})
}
