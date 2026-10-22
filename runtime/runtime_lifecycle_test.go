package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestConcurrentWritesSameDatabaseQueue proves that same-process writers wait
// for the holder's release instead of paying the jittered cross-process
// backoff. The backoff is set to five seconds so the old polling path cannot
// accidentally pass.
func TestConcurrentWritesSameDatabaseQueue(t *testing.T) {
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = t.TempDir()
	cfg.Compaction.Enabled = false
	cfg.RetryPolicy = runtime.DefaultRetryPolicy()
	cfg.Lease.AcquireRetryBudget = 10 * time.Second
	cfg.Lease.RetryBackoffMin = 5 * time.Second
	cfg.Lease.RetryBackoffMax = 5 * time.Second
	rt, err := runtime.New(lease.NewMemoryStore(), "same-db-queue", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	d := descriptor(t, "users/same-db-queue")
	if _, err := rt.WithWrite(context.Background(), d, "schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}

	const writers = 10
	start := time.Now()
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := rt.WithWrite(context.Background(), d, fmt.Sprintf("queue-%d", i), func(conn *sql.Conn) error {
				_, err := conn.ExecContext(context.Background(), `INSERT INTO t (id) VALUES (?)`, i)
				return err
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("same-database writers took %v; release notification did not queue them", elapsed)
	}
	if retries := rt.MetricsTotal().WriteRetries; retries != 0 {
		t.Fatalf("write retries = %d, want local queueing with no outer retries", retries)
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
