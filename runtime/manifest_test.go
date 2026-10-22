package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

func manifestTestRuntime(t *testing.T, compaction runtime.CompactionConfig) (*runtime.Runtime, runtime.DatabaseDescriptor, string) {
	t.Helper()
	root := t.TempDir()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = t.TempDir()
	cfg.Litestream.ReplicaCacheBytes = 0
	cfg.Litestream.LevelListingCacheTTL = 0
	cfg.Compaction = compaction

	rt, err := runtime.New(lease.NewMemoryStore(), "manifest-test", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	desc := runtime.DatabaseDescriptor{
		DatabaseID: "users/u1",
		Storage: litestream.Profile{
			Provider:   "file",
			FileRoot:   root,
			RootPrefix: "orgs/o1",
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
	return rt, desc, filepath.Join(root, "orgs/o1/users/u1/replica/manifest.json")
}

func readManifest(t *testing.T, path string) []*ltx.FileInfo {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	files, err := litestream.DecodeManifest(data)
	if err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return files
}

func TestManifestCreatedUsedAndRebuilt(t *testing.T) {
	rt, desc, manifestPath := manifestTestRuntime(t, runtime.CompactionConfig{Enabled: false})
	defer rt.Close()
	ctx := context.Background()

	write := func(key, query string) {
		t.Helper()
		if _, err := rt.WithWrite(ctx, desc, key, func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, query)
			return err
		}); err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
	}
	readCount := func() int {
		t.Helper()
		var n int
		if err := rt.WithRead(ctx, desc, func(conn *sql.Conn) error {
			return conn.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n)
		}); err != nil {
			t.Fatalf("read count: %v", err)
		}
		return n
	}

	write("schema", `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`)
	write("first", `INSERT INTO notes (id, body) VALUES (1, 'one')`)

	files := readManifest(t, manifestPath)
	if len(files) == 0 {
		t.Fatal("manifest was not created after the first write")
	}
	if got := litestream.ManifestMaxTXID(files); got < 2 {
		t.Fatalf("manifest max TXID = %d, want >= 2", got)
	}
	if n := readCount(); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}

	// Legacy fallback: without the manifest, the read path probes and lists
	// the replica, and the next write rebuilds the manifest.
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	if n := readCount(); n != 1 {
		t.Fatalf("legacy rows = %d, want 1", n)
	}
	write("second", `INSERT INTO notes (id, body) VALUES (2, 'two')`)
	if files := readManifest(t, manifestPath); len(files) == 0 {
		t.Fatal("manifest was not rebuilt after a legacy write")
	}
	if n := readCount(); n != 2 {
		t.Fatalf("rows after rebuild = %d, want 2", n)
	}
}

func TestCompactionUpdatesManifest(t *testing.T) {
	rt, desc, manifestPath := manifestTestRuntime(t, runtime.CompactionConfig{
		Enabled:   true,
		Interval:  1,
		Threshold: 2,
		Retention: 0,
		Timeout:   10 * time.Second,
	})
	defer rt.Close()
	ctx := context.Background()

	if _, err := rt.WithWrite(ctx, desc, "schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}
	for i := 0; i < 6; i++ {
		id := i
		if _, err := rt.WithWrite(ctx, desc, fmt.Sprintf("insert-%d", id), func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, `INSERT INTO notes (id, body) VALUES (?, 'x')`, id)
			return err
		}); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}

	before := readManifest(t, manifestPath)
	t.Logf("before compaction: files=%d max=%d", len(before), litestream.ManifestMaxTXID(before))
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rt.MetricsTotal().Compactions > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("metrics: %+v", rt.MetricsTotal())
			t.Fatal("compaction did not run")
		}
		time.Sleep(50 * time.Millisecond)
	}

	files := readManifest(t, manifestPath)
	if len(files) == 0 {
		t.Fatal("manifest is empty after compaction")
	}
	if len(files) >= 7 {
		t.Fatalf("manifest still has %d files after compaction", len(files))
	}
	if got := litestream.ManifestMaxTXID(files); got != 7 {
		t.Fatalf("manifest max TXID = %d, want 7", got)
	}

	var rows int
	if err := rt.WithRead(ctx, desc, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&rows)
	}); err != nil {
		t.Fatalf("read after compaction: %v", err)
	}
	if rows != 6 {
		t.Fatalf("rows after compaction = %d, want 6", rows)
	}
}
