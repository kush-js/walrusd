package litestream

import (
	"context"
	"testing"
	"time"
)

// HasLTX must reuse the database's own replica client: constructing a fresh
// client per probe pays SDK config load and a new connection on every read.
func TestHasLTXReusesDatabaseClient(t *testing.T) {
	b := NewBridge(DefaultConfig())
	p := Profile{Provider: "memory", Bucket: "b", RootPrefix: "bench"}
	db, err := b.RegisterDatabase("bench-db", "bench-prefix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	has, err := db.HasLTX(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("fresh memory replica should hold no LTX")
	}
	if db.wrapper.client == nil {
		t.Fatal("probe should reuse the wrapper's replica client")
	}
}
