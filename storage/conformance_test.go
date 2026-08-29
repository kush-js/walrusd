package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"walrus/storage"
)

// RunConformance exercises the ConditionalStore contract. Every
// implementation must pass. It asserts read-after-write consistency,
// conditional create, conditional replace, and conditional delete.
func RunConformance(t *testing.T, newStore func(t *testing.T) storage.ConditionalStore) {
	ctx := context.Background()
	// Unique namespace per run so re-runs never see stale objects.
	ns := fmt.Sprintf("conformance/%d/", time.Now().UnixNano())
	key := func(name string) string { return ns + name }

	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		_, _, err := s.Get(ctx, key("missing"))
		if err == nil {
			t.Fatal("expected error for missing key")
		}
	})

	t.Run("CreateIfAbsent", func(t *testing.T) {
		s := newStore(t)
		v1, err := s.CreateIfAbsent(ctx, key("a"), []byte("one"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if v1 == "" {
			t.Fatal("empty version")
		}
		// Lost race.
		if _, err := s.CreateIfAbsent(ctx, key("a"), []byte("two")); err == nil {
			t.Fatal("expected already-exists error")
		}
		// Read-after-write consistency: exact bytes.
		body, v, err := s.Get(ctx, key("a"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !bytes.Equal(body, []byte("one")) || v != v1 {
			t.Fatalf("inconsistent read: %q / %q", body, v)
		}
	})

	t.Run("ReplaceIfVersion", func(t *testing.T) {
		s := newStore(t)
		v1, _ := s.CreateIfAbsent(ctx, key("b"), []byte("one"))
		// Stale CAS loses.
		if _, err := s.ReplaceIfVersion(ctx, key("b"), "stale", []byte("two")); err == nil {
			t.Fatal("expected conflict")
		}
		v2, err := s.ReplaceIfVersion(ctx, key("b"), v1, []byte("two"))
		if err != nil {
			t.Fatalf("replace: %v", err)
		}
		if v2 == v1 {
			t.Fatal("version must change after replace")
		}
		body, v, _ := s.Get(ctx, key("b"))
		if !bytes.Equal(body, []byte("two")) || v != v2 {
			t.Fatalf("inconsistent read after replace: %q / %q", body, v)
		}
	})

	t.Run("DeleteIfVersion", func(t *testing.T) {
		s := newStore(t)
		v1, _ := s.CreateIfAbsent(ctx, key("c"), []byte("one"))
		if err := s.DeleteIfVersion(ctx, key("c"), "stale"); err == nil {
			t.Fatal("expected conflict on stale delete")
		}
		if err := s.DeleteIfVersion(ctx, key("c"), v1); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, _, err := s.Get(ctx, key("c")); err == nil {
			t.Fatal("expected not-found after delete")
		}
	})
}

func TestMemoryStoreConformance(t *testing.T) {
	RunConformance(t, func(t *testing.T) storage.ConditionalStore {
		return storage.NewMemoryStore()
	})
}
