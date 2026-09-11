package runtime_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

func TestWriteReusesDatabaseVFS(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/reuse")
	before := litestream.RegisteredVFSCount()

	for i := 0; i < 25; i++ {
		_, err := rt.WithWrite(context.Background(), d, "write", func(conn *sql.Conn) error {
			if _, err := conn.ExecContext(context.Background(),
				`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
				return err
			}
			_, err := conn.ExecContext(context.Background(),
				`INSERT INTO t (v) VALUES ('value')`)
			return err
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	after := litestream.RegisteredVFSCount()
	if got := after - before; got != 2 {
		t.Fatalf("registered VFS instances grew by %d, want exactly 2 (read + write)", got)
	}
}

func TestWriteLeavesNoTempDirectories(t *testing.T) {
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)

	rt := newTestRuntime(t, "api-1")
	d := descriptor(t, "users/cleanup")
	for i := 0; i < 25; i++ {
		if _, err := rt.WithWrite(context.Background(), d, "write", func(conn *sql.Conn) error {
			if _, err := conn.ExecContext(context.Background(),
				`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
				return err
			}
			_, err := conn.ExecContext(context.Background(),
				`INSERT INTO t (v) VALUES ('value')`)
			return err
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("temporary directories remain after writes: %s", strings.Join(names, ", "))
	}
}

func TestConcurrentFirstWritesDifferentDatabases(t *testing.T) {
	rt := newTestRuntime(t, "api-1")
	descriptors := []runtime.DatabaseDescriptor{
		descriptor(t, "users/concurrent-a"),
		descriptor(t, "users/concurrent-b"),
	}
	errs := make([]error, len(descriptors))
	var wg sync.WaitGroup
	for i, d := range descriptors {
		wg.Add(1)
		go func(i int, d runtime.DatabaseDescriptor) {
			defer wg.Done()
			_, errs[i] = rt.WithWrite(context.Background(), d, "write", func(conn *sql.Conn) error {
				_, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`)
				return err
			})
		}(i, d)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

func TestMaxTempWriteBufferIsEnforced(t *testing.T) {
	cfg := runtime.DefaultConfig()
	cfg.MaxTempWriteBuffer = 16 * 1024
	cfg.RetryPolicy = runtime.RetryPolicy{}
	rt, err := runtime.New(lease.NewMemoryStore(), "api-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := descriptor(t, "users/buffer-limit")

	_, err = rt.WithWrite(context.Background(), d, "large", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(),
			`CREATE TABLE t (v BLOB)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(),
			`INSERT INTO t (v) VALUES (zeroblob(1048576))`)
		return err
	})
	if err == nil {
		t.Fatal("expected the temporary write-buffer limit to reject the mutation")
	}
}

func TestWriteBufferRootIsLeftClean(t *testing.T) {
	root := t.TempDir()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = root
	rt, err := runtime.New(lease.NewMemoryStore(), "api-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := runtime.DatabaseDescriptor{
		DatabaseID: "users/configured-cleanup",
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: t.TempDir(),
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
	for i := 0; i < 5; i++ {
		if _, err := rt.WithWrite(context.Background(), d, "write", func(conn *sql.Conn) error {
			_, err := conn.ExecContext(context.Background(),
				`CREATE TABLE IF NOT EXISTS t (v TEXT)`)
			return err
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	matches, err := filepath.Glob(filepath.Join(root, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("write buffer root not clean: %v", matches)
	}
}
