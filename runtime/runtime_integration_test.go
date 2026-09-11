package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

// TestWithWriteRedisEndToEnd runs the full spec §8 write path with leases in
// Redis/Valkey shared by three runtimes (= three API instances): write on
// one, idempotent retry on another, read-back on a third. The replica lives
// on the local file provider to prove the object-storage side needs no
// conditional-write support. Skipped unless WALRUSD_TEST_REDIS_ADDR is set.
func TestWithWriteRedisEndToEnd(t *testing.T) {
	addr := os.Getenv("WALRUSD_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("WALRUSD_TEST_REDIS_ADDR not set; skipping live Redis end-to-end test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	newRuntime := func(owner string) *runtime.Runtime {
		t.Helper()
		store, err := lease.NewRedisStore(ctx, lease.RedisOptions{Addr: addr})
		if err != nil {
			t.Fatalf("new redis store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		cfg := runtime.DefaultConfig()
		cfg.Litestream.WriteBufferRootPath = t.TempDir()
		rt, err := runtime.New(store, owner, cfg)
		if err != nil {
			t.Fatalf("new runtime: %v", err)
		}
		return rt
	}

	root := t.TempDir()
	prefix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	desc := func() runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: prefix + "/users/user_1",
			Storage: litestream.Profile{
				Provider: "file",
				FileRoot: root,
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
	}

	// Write on instance 1.
	w := newRuntime("api-1")
	res, err := w.WithWrite(ctx, desc(), "seed", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx,
			`CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (1, 'hello from walrusd')`)
		return err
	})
	if err != nil {
		t.Fatalf("with write: %v", err)
	}
	if res.TXID == "" || res.Deduplicated {
		t.Fatalf("unexpected result: %+v", res)
	}
	t.Logf("flushed txid: %s", res.TXID)

	// Retry with the same key on another instance must deduplicate.
	r2 := newRuntime("api-2")
	res2, err := r2.WithWrite(ctx, desc(), "seed", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO notes (id, body) VALUES (2, 'SHOULD NOT RUN')`)
		return err
	})
	if err != nil {
		t.Fatalf("deduplicated write: %v", err)
	}
	if !res2.Deduplicated || res2.TXID != res.TXID {
		t.Fatalf("dedup result = %+v, want prior txid %s", res2, res.TXID)
	}

	// Read on a third instance must see the flushed state (spec §9).
	r3 := newRuntime("api-3")
	var body string
	err = r3.WithRead(ctx, desc(), func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, "SELECT body FROM notes LIMIT 1").Scan(&body)
	})
	if err != nil {
		t.Fatalf("with read: %v", err)
	}
	if body != "hello from walrusd" {
		t.Fatalf("read body = %q", body)
	}
}
