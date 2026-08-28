//go:build vfs
// +build vfs

package dbmanager

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

// ---- test fakes ----

type fakeStore struct {
	mu   sync.Mutex
	objs map[string]string
}

func newFakeStore() *fakeStore { return &fakeStore{objs: map[string]string{}} }

func (s *fakeStore) Get(_ context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.objs[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return []byte(v), "etag-" + key, nil
}
func (s *fakeStore) CreateIfAbsent(_ context.Context, key string, body []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objs[key]; ok {
		return "", ErrConflict
	}
	s.objs[key] = string(body)
	return "etag-" + key, nil
}
func (s *fakeStore) ReplaceIfVersion(_ context.Context, key string, expected string, body []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objs[key]
	if !ok || "etag-"+key != expected {
		return "", ErrConflict
	}
	s.objs[key] = string(body)
	return expected, nil
}
func (s *fakeStore) PutImmutable(_ context.Context, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objs[key]; ok {
		return ErrConflict
	}
	s.objs[key] = string(body)
	return nil
}

type fakeFencer struct {
	mu      sync.Mutex
	epoch   map[string]uint64
	demoted []string
}

func (f *fakeFencer) Authorize(_ context.Context, dbID string) (uint64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.epoch == nil {
		f.epoch = map[string]uint64{}
	}
	f.epoch[dbID]++
	return f.epoch[dbID], "lease-" + dbID, nil
}
func (f *fakeFencer) Demote(dbID string) { f.demoted = append(f.demoted, dbID) }

type fakeCommitter struct {
	mu   sync.Mutex
	seq  uint64
	fail bool
}

func (c *fakeCommitter) Commit(_ context.Context, _ string, _ uint64, _, _ string, _ []Object) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return 0, ErrFencingLost
	}
	c.seq++
	return c.seq, nil
}

// ---- tests ----

func TestOpenNewDatabaseAndWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	store := newFakeStore()
	m := NewManager(store, "w1", &fakeFencer{}, &fakeCommitter{}, Config{WriteSyncIval: 50 * time.Millisecond})

	ctx := context.Background()
	if err := m.OpenWritable(ctx, "org/o/user/u", "pfx/u"); err != nil {
		t.Fatal(err)
	}
	dsn, err := m.DSN("org/o/user/u")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (x TEXT)`); err != nil {
		t.Fatal("create:", err)
	}
	m.AttachDB("org/o/user/u")
	if _, err := db.Exec(`INSERT INTO t (x) VALUES ('hello-walrus')`); err != nil {
		t.Fatal("insert:", err)
	}
	// Force sync of dirty pages into an LTX object.
	h := m.get("org/o/user/u")
	if err := h.syncAll(ctx); err != nil {
		t.Fatal("sync:", err)
	}
	// Reads go through a fresh connection: the writing connection's pager
	// holds pre-sync page-1 state that the VFS invalidates on sync
	// (litestream VFS behavior); fresh connections always see the durable
	// committed state.
	db.Close()
	db2, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	var got string
	if err := db2.QueryRow(`SELECT x FROM t LIMIT 1`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "hello-walrus" {
		t.Fatalf("bad readback: %q", got)
	}
}

func TestReopenWithoutHydration(t *testing.T) {
	store := newFakeStore()
	fencer := &fakeFencer{}
	m := NewManager(store, "w1", fencer, &fakeCommitter{}, Config{WriteSyncIval: 50 * time.Millisecond})
	ctx := context.Background()
	dbID := "org/o/user/reopen"

	if err := m.OpenWritable(ctx, dbID, "pfx/reopen"); err != nil {
		t.Fatal(err)
	}
	dsn, _ := m.DSN(dbID)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE t (x TEXT)`); err != nil {
		t.Fatal(err)
	}
	m.AttachDB(dbID)
	if _, err := db.Exec(`INSERT INTO t (x) VALUES ('durable')`); err != nil {
		t.Fatal(err)
	}
	h := m.get(dbID)
	if err := h.syncAll(ctx); err != nil {
		t.Fatal(err)
	}
	db.Close()
	m.demote(dbID)

	// Fresh manager (new worker): no local state at all.
	m2 := NewManager(store, "w2", fencer, &fakeCommitter{}, Config{})
	if err := m2.OpenWritable(ctx, dbID, "pfx/reopen"); err != nil {
		t.Fatal(err)
	}
	dsn2, _ := m2.DSN(dbID)
	db2, err := sql.Open("sqlite3", dsn2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	var got string
	if err := db2.QueryRow(`SELECT x FROM t LIMIT 1`).Scan(&got); err != nil {
		t.Fatal("reopen:", err)
	}
	m2.AttachDB(dbID)
	if got != "durable" {
		t.Fatalf("data lost across reopen: %q", got)
	}
}

func TestExecuteWriteDedupeAndFencingLoss(t *testing.T) {
	store := newFakeStore()
	fencer := &fakeFencer{}
	committer := &fakeCommitter{}
	m := NewManager(store, "w1", fencer, committer, Config{WriteSyncIval: 50 * time.Millisecond})
	ctx := context.Background()
	dbID := "org/o/user/idem"

	// Keep the connection open: ExecuteWrite syncs and commits while the
	// connection (and its VFS file) is live; we close it after each call.
	var execConn *sql.DB
	var exec func(string, func() error) error
	exec = func(dsn string, sync func() error) error {
		if execConn != nil {
			execConn.Close()
		}
		conn, err := sql.Open("sqlite3", dsn)
		if err != nil {
			return err
		}
		execConn = conn
		if _, err := execConn.Exec(`CREATE TABLE IF NOT EXISTS t (x TEXT)`); err != nil {
			return err
		}
		if _, err = execConn.Exec(`INSERT INTO t (x) VALUES ('v')`); err != nil {
			return err
		}
		// Sync while the connection is live; closed on the next call/teardown.
		return sync()
	}

	r1, err := m.ExecuteWrite(ctx, dbID, "pfx/idem", "op", "key-1", exec)
	if err != nil {
		t.Fatal("first write:", err)
	}
	// Same idempotency key: dedupe, no re-execution.
	r2, err := m.ExecuteWrite(ctx, dbID, "pfx/idem", "op", "key-1", exec)
	if err != nil {
		t.Fatal("dedupe write:", err)
	}
	if r1.CommitSeq != r2.CommitSeq {
		t.Fatalf("dedupe must return original result: %d != %d", r1.CommitSeq, r2.CommitSeq)
	}

	// Fencing loss at manifest CAS: write must not be acknowledged as durable.
	committer.fail = true
	if _, err := m.ExecuteWrite(ctx, dbID, "pfx/idem", "op", "key-2", exec); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("want ErrNotOwner on fencing loss, got %v", err)
	}
	// The hot instance must be demoted.
	if len(fencer.demoted) == 0 {
		t.Fatal("fencing loss must demote the database")
	}
}

func TestEviction(t *testing.T) {
	store := newFakeStore()
	m := NewManager(store, "w1", &fakeFencer{}, &fakeCommitter{}, Config{IdleTTL: 10 * time.Millisecond})
	ctx := context.Background()
	dbID := "org/o/user/evict"
	if err := m.OpenWritable(ctx, dbID, "pfx/evict"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	m.evictIdle()
	if _, err := m.DSN(dbID); !errors.Is(err, ErrNotWritable) {
		t.Fatalf("evicted db must not be openable, got %v", err)
	}
}

// unused-keeper for the ltx import when tests trim
var _ = ltx.TXID(0)
var _ = sqlite3vfs.RegisterVFS
var _ = os.Getenv
