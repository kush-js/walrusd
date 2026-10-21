package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"walrusd/lease"
	"walrusd/litestream"
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
		Retention: 0,
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

	// Every flush writes one LTX file; compaction must keep the chain well
	// below the number of writes.
	var ltxFiles int
	replicaDir := filepath.Join(root, "orgs/o1/users/u1/replica")
	if err := filepath.WalkDir(replicaDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".ltx") {
			ltxFiles++
		}
		return nil
	}); err != nil {
		t.Fatalf("walk replica: %v", err)
	}
	if ltxFiles > 8 {
		t.Fatalf("replica holds %d LTX files after %d writes, want the chain bounded", ltxFiles, writes)
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
