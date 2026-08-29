package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"walrus/identity"
	"walrus/lease"
	"walrus/litestream"
	"walrus/runtime"
	"walrus/storage"
	"walrus/walruserr"
)

func class(err error) string { return string(walruserr.ClassOf(err)) }

func newTestRuntime(t *testing.T, owner string) *runtime.Runtime {
	t.Helper()
	store := storage.NewMemoryStore()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	rt, err := runtime.New(store, owner, cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return rt
}

func descriptor(t *testing.T, org, user string) runtime.DatabaseDescriptor {
	t.Helper()
	return runtime.DatabaseDescriptor{
		OrganizationID: org,
		UserID:         user,
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: t.TempDir(),
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
}

func TestWithWriteRequiresIdempotencyKey(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "org_1", "user_1")
	_, err := rt.WithWrite(context.Background(), d, "", func(conn *sql.Conn) error { return nil })
	if class(err) != "DB_INVALID_ARGUMENT" {
		t.Fatalf("class = %q, want DB_INVALID_ARGUMENT", class(err))
	}
}

func TestWithWriteLifecycle(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "org_1", "user_1")

	res, err := rt.WithWrite(context.Background(), d, "op_1", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), `
			CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)
		`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(),
			`INSERT INTO events (id, body) VALUES (1, 'hello')`)
		return err
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.TXID == "" {
		t.Fatal("expected TXID after flush")
	}

	// Read back from a fresh read session: sees only flushed remote state.
	var body string
	err = rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(),
			`SELECT body FROM events WHERE id = 1`).Scan(&body)
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if body != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func TestWithWriteSecondWriterWaits(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "org_1", "user_1")

	// Hold the lease externally to simulate a concurrent writer.
	db, _ := identity.NewDatabaseID(d.OrganizationID, d.UserID)
	_ = rt
	store := storage.NewMemoryStore()
	lm := lease.NewManager(store, "api-other", lease.DefaultConfig(), nil)
	held, err := lm.Acquire(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Release(context.Background(), held)

	// A runtime sharing the same store sees DB_BUSY.
	shared, err := runtime.New(store, "api-2", runtime.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = shared.WithWrite(ctx, d, "op_2", func(conn *sql.Conn) error { return nil })
	if err == nil {
		t.Fatal("expected busy error while lease held")
	}
	if !errors.Is(err, context.DeadlineExceeded) && class(err) != "DB_BUSY" {
		t.Fatalf("want DB_BUSY (or ctx deadline), got %v", err)
	}
}

func TestWriteAfterReleaseSeesFlushedState(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "org_1", "user_1")

	if _, err := rt.WithWrite(context.Background(), d, "op_a", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS t (v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := rt.WithWrite(context.Background(), d, "op_b", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `INSERT INTO t (v) VALUES ('second')`)
		return err
	}); err != nil {
		t.Fatalf("write2: %v", err)
	}
	var count int
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM t`).Scan(&count)
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}
