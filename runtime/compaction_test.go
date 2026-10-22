package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/observability"
	"walrusd/runtime"
)

// TestCompactionBoundsReplicaChain writes enough to grow the LTX chain and
// checks that compaction keeps it bounded without losing rows. The file
// provider keeps the test self-contained: no Redis or object store.
func TestCompactionBoundsReplicaChain(t *testing.T) {
	root := t.TempDir()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = t.TempDir()
	cfg.Compaction = runtime.CompactionConfig{
		Enabled:   true,
		Interval:  2,
		Threshold: 2,
		Retention: time.Minute,
		Timeout:   10 * time.Second,
	}

	rt, err := runtime.New(lease.NewMemoryStore(), "compaction-test", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	desc := runtime.DatabaseDescriptor{
		DatabaseID: "users/u1",
		Storage: litestream.Profile{
			Provider:   "file",
			FileRoot:   root,
			RootPrefix: "orgs/o1",
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
	ctx := context.Background()

	write := func(key string, fn func(*sql.Conn) error) {
		t.Helper()
		if _, err := rt.WithWrite(ctx, desc, key, fn); err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
	}

	write("schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`)
		return err
	})

	const writes = 12
	for i := 0; i < writes; i++ {
		id := i
		write(fmt.Sprintf("w-%d", id), func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, `INSERT INTO notes (id, body) VALUES (?, 'x')`, id)
			return err
		})
	}

	// Every flush writes one LTX file; compaction must keep the manifest well
	// below the number of writes. Compaction is asynchronous, so wait for it.
	manifestPath := filepath.Join(root, "orgs/o1/users/u1/replica/manifest.json")
	deadline := time.Now().Add(10 * time.Second)
	var manifestFiles []*ltx.FileInfo
	for {
		manifestFiles = readManifest(t, manifestPath)
		if len(manifestFiles) <= 8 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(manifestFiles) > 8 {
		t.Fatalf("manifest holds %d LTX entries after %d writes, want the chain bounded", len(manifestFiles), writes)
	}

	// Compaction must not lose data: every row is still readable from a
	// freshly opened read session.
	var rows int
	if err := rt.WithRead(ctx, desc, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&rows)
	}); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	if rows != writes {
		t.Fatalf("rows = %d, want %d", rows, writes)
	}
}

// TestCompactionMergesPreexistingChain builds a long chain with compaction
// off, then opens the same database with compaction on and checks that one
// write collapses it. This is the deployment case: the chain already exists
// when the feature is first enabled.
func TestCompactionMergesPreexistingChain(t *testing.T) {
	root := t.TempDir()
	desc := runtime.DatabaseDescriptor{
		DatabaseID: "users/u1",
		Storage: litestream.Profile{
			Provider:   "file",
			FileRoot:   root,
			RootPrefix: "orgs/o1",
		},
		Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
	}
	ctx := context.Background()

	baseCfg := runtime.DefaultConfig()
	baseCfg.Litestream.WriteBufferRootPath = t.TempDir()

	open := func(compaction runtime.CompactionConfig) *runtime.Runtime {
		t.Helper()
		cfg := baseCfg
		cfg.Compaction = compaction
		rt, err := runtime.New(lease.NewMemoryStore(), "test", cfg)
		if err != nil {
			t.Fatalf("new runtime: %v", err)
		}
		return rt
	}

	write := func(rt *runtime.Runtime, key string, fn func(*sql.Conn) error) {
		t.Helper()
		if _, err := rt.WithWrite(ctx, desc, key, fn); err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
	}

	off := open(runtime.CompactionConfig{Enabled: false})
	write(off, "schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`)
		return err
	})
	const writes = 40
	for i := 0; i < writes; i++ {
		id := i
		write(off, fmt.Sprintf("seed-%d", id), func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, `INSERT INTO notes (id, body) VALUES (?, 'x')`, id)
			return err
		})
	}
	manifestPath := filepath.Join(root, "orgs/o1/users/u1/replica/manifest.json")
	before := len(readManifest(t, manifestPath))
	if before < writes {
		t.Fatalf("expected a long chain before compaction, got %d manifest entries", before)
	}
	if err := off.Close(); err != nil {
		t.Fatalf("close off runtime: %v", err)
	}

	on := open(runtime.CompactionConfig{
		Enabled:   true,
		Interval:  1,
		Threshold: 4,
		Retention: time.Minute,
		Timeout:   30 * time.Second,
	})
	defer on.Close()
	write(on, "trigger", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO notes (id, body) VALUES (?, 'y')`, writes)
		return err
	})

	deadline := time.Now().Add(10 * time.Second)
	var total observability.Metrics
	var after int
	for {
		total = on.MetricsTotal()
		after = len(readManifest(t, manifestPath))
		if total.Compactions > 0 && after < before {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if total.CompactionFailures > 0 {
		t.Fatalf("compaction failures = %d", total.CompactionFailures)
	}
	if total.Compactions == 0 {
		t.Fatal("compaction did not run")
	}
	if after >= before {
		t.Fatalf("manifest did not shrink: before=%d after=%d", before, after)
	}
	t.Logf("manifest: %d -> %d entries, compactions=%d levels=%d", before, after, total.Compactions, total.CompactedLevels)

	var rows int
	if err := on.WithRead(ctx, desc, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&rows)
	}); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	if rows != writes+1 {
		t.Fatalf("rows = %d, want %d", rows, writes+1)
	}
}

// TestCompactionSurvivesVFSEviction is the multi-tenant regression: a
// database written once every few tenant touches is evicted from the resident
// LRU between writes. Compaction must trigger from the durable manifest, not
// from a counter that resets with the VFS.
func TestCompactionSurvivesVFSEviction(t *testing.T) {
	root := t.TempDir()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = t.TempDir()
	cfg.MaxReadInstances = 2
	cfg.ReadInstanceIdleTTL = time.Hour
	cfg.RetryPolicy = runtime.RetryPolicy{}
	cfg.Compaction = runtime.CompactionConfig{
		Enabled:   true,
		Interval:  1,
		Threshold: 2,
		Retention: time.Minute,
		Timeout:   10 * time.Second,
		Workers:   2,
	}

	rt, err := runtime.New(lease.NewMemoryStore(), "compaction-eviction", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()
	ctx := context.Background()
	descriptor := func(id string) runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: id,
			Storage: litestream.Profile{
				Provider:   "file",
				FileRoot:   root,
				RootPrefix: "orgs/o1",
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
	}

	a := descriptor("users/evicted")
	write := func(i int) {
		t.Helper()
		if _, err := rt.WithWrite(ctx, a, fmt.Sprintf("evicted-%d", i), func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, `INSERT INTO notes (id) VALUES (?)`, i)
			return err
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if _, err := rt.WithWrite(ctx, a, "schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE notes (id INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}

	const writes = 20
	for i := 0; i < writes; i++ {
		write(i)
		// Evict A from the two-entry LRU between every write, which is what
		// happens to a sparse tenant in a process with many active databases.
		for _, other := range []string{"users/other-b", "users/other-c"} {
			if err := rt.WithRead(ctx, descriptor(other), func(*sql.Conn) error { return nil }); err != nil {
				t.Fatalf("evict %s: %v", other, err)
			}
		}
	}

	manifestPath := filepath.Join(root, "orgs/o1/users/evicted/replica/manifest.json")
	deadline := time.Now().Add(15 * time.Second)
	for {
		counts := manifestLevelCounts(readManifest(t, manifestPath))
		bounded := true
		for level := 0; level < 8; level++ {
			if counts[level] > cfg.Compaction.Threshold {
				bounded = false
				break
			}
		}
		if rt.MetricsTotal().Compactions > 0 && bounded && maxManifestLevel(counts) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("compaction did not bound the evicted chain: levels=%v metrics=%+v", counts, rt.MetricsTotal())
		}
		time.Sleep(50 * time.Millisecond)
	}

	var rows int
	if err := rt.WithRead(ctx, a, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&rows)
	}); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	if rows != writes {
		t.Fatalf("rows = %d, want %d", rows, writes)
	}
}

func manifestLevelCounts(files []*ltx.FileInfo) map[int]int {
	counts := make(map[int]int)
	for _, f := range files {
		counts[f.Level]++
	}
	return counts
}

func maxManifestLevel(counts map[int]int) int {
	max := -1
	for level, n := range counts {
		if n > 0 && level > max {
			max = level
		}
	}
	return max
}
