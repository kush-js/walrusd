package litestream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	ls "github.com/benbjohnson/litestream"
	_ "github.com/mattn/go-sqlite3" // registers the sqlite3 driver with VFS support
	"github.com/superfly/ltx"
)

// Session is one request-scoped SQLite connection over a registered VFS.
// The flush barrier MUST run on the same connection/VFS instance that
// performed the transaction (spec §8).
type Session struct {
	dbVFS      *Database
	dbFileName string
	conn       *sql.Conn
	db         *sql.DB

	// writeVFS is the dedicated write-mode VFS registered for this session
	// (non-nil only for write sessions). It exists only while the lease is
	// held (invariant 5) and is discarded on close.
	writeVFS *wrapperVFS

	// sessionVFS is the wrapper that owns this session's request-scoped VFS
	// state. It is nil for legacy opens that use the shared client.
	sessionVFS *wrapperVFS

	mu        sync.Mutex
	writeFile *ls.VFSFile
	writeOn   bool
	closed    bool
}

var globalSessionSeq atomic.Uint64

// OpenRead opens a read-only session against the remote replica state
// (spec §9). No local database file is hydrated (invariant 11). It uses the
// shared replica client (legacy listing path); OpenReadWithManifest is the
// manifest-backed path.
func (d *Database) OpenRead(ctx context.Context, dbName string) (*Session, error) {
	return d.OpenReadWithManifest(ctx, dbName, nil)
}

// OpenReadWithManifest opens a read session whose restore plan comes from the
// durable manifest. A non-nil files slice (including an empty one) means the
// manifest exists; nil means a legacy replica and the shared client lists
// levels as before.
func (d *Database) OpenReadWithManifest(ctx context.Context, dbName string, files []*ltx.FileInfo) (*Session, error) {
	fileName := fmt.Sprintf("walrusd_%s_%d.db", sanitize(dbName), globalSessionSeq.Add(1))
	var state *vfsSession
	if files != nil {
		mc := newManifestClient(d.client, files)
		state = &vfsSession{inner: d.Bridge.newInnerVFS(mc, false), client: mc}
		d.wrapper.registerSession(fileName, state)
	}
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=ro&_query_only=1", fileName, d.VFSName)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		if state != nil {
			d.wrapper.unregisterSession(fileName)
		}
		return nil, fmt.Errorf("litestream: open read: %w", err)
	}
	db.SetMaxOpenConns(1)
	// The VFS open behind db.Conn can block in waitForRestorePlan past every
	// caller deadline (uncancellable CGO on a sick replica). The caller's own
	// context deadline bounds the wait. The registry is a sync.Map in the
	// vendored sqlite3vfs, so session opens do not serialize.
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		if state != nil {
			d.wrapper.unregisterSession(fileName)
		}
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	return &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db, sessionVFS: d.wrapper}, nil
}

// ReadDSN returns the SQLite DSN for the host's own SQLite connection to
// read this database through the shared litestream VFS (spec §9 read mode).
// The VFS is registered process-wide by the runtime, so any SQLite in the
// process opened with URI filenames (node:sqlite, the sqlite3 CLI, custom
// sqlite builds) can open it natively.
func (d *Database) ReadDSN(ctx context.Context, dbName string) string {
	return fmt.Sprintf("file:walrusd_%s.db?vfs=%s&mode=ro", sanitize(dbName), d.VFSName)
}

// Key identifies this session's database for the runtime's read-instance
// cache (spec §10).
func (s *Session) Key() string { return s.dbVFS.Key }

// HasLTX reports whether the replica holds any LTX files. A read-mode VFS
// open on a zero-LTX replica blocks forever in waitForRestorePlan, so
// callers must probe first and serve the empty-DB fast path instead.
//
// The probe reuses the database's own replica client behind the shared
// read VFS instead of constructing a fresh client per call: a fresh
// client pays for SDK config load, credential resolution, and a new
// HTTP connection on every read, which stalls under load.
//
// The probe checks level 0 (live tail) first, then the snapshot level:
// a healthy replica answers on the first LIST, and level-by-level listing
// keeps each LIST prefix-narrow instead of one broad recursive walk.
func (d *Database) HasLTX(ctx context.Context) (bool, error) {
	client := d.wrapper.client
	if err := client.Init(ctx); err != nil {
		return false, fmt.Errorf("litestream: init replica client: %w", err)
	}
	// Bound the probe: on an unreachable or pathologically slow replica the
	// LIST can stall past every caller deadline (the VFS open it guards is
	// uncancellable CGO), so cap it well inside the caller's budget and
	// report unavailability instead of hanging. Each level gets its own
	// timeout: a stalled first level must not consume the next level's
	// budget, and a fired deadline must not poison the next iterator (its
	// paginator shares the iterator context). The live tail answers a
	// healthy replica on the first LIST; the snapshot level is the second
	// candidate. Compaction (which writes deltas to levels 1..8) can leave
	// both empty while the database is non-empty, so the remaining levels
	// must be checked too. The iterator's context reaches the HTTP request,
	// so a stalled page is cancelled by this deadline rather than needing a
	// watchdog around Next.
	for _, level := range []int{0, ls.SnapshotLevel, 1, 2, 3, 4, 5, 6, 7, 8} {
		lctx, lcancel := context.WithTimeout(ctx, hasLTXProbeTimeout)
		itr, err := client.LTXFiles(lctx, level, 0, false)
		if err != nil {
			lcancel()
			return false, fmt.Errorf("litestream: list LTX files: %w", err)
		}
		has := itr.Next()
		iterErr := itr.Err()
		itr.Close()
		lcancel()
		if iterErr != nil {
			return false, fmt.Errorf("litestream: list LTX files: %w", iterErr)
		}
		if has {
			return true, nil
		}
	}
	return false, nil
}

// hasLTXProbeTimeout caps the empty-replica probe. Callers race the whole
// read against their own budget; this keeps the probe from consuming all
// of it on a stalled LIST.
const hasLTXProbeTimeout = 5 * time.Second

// OpenWrite opens a read-write session with VFS write mode enabled from the
// start. Litestream only honors write mode at VFS-open; a read-mode open
// would block waiting for remote LTX files that do not exist for a new
// database. Call only while holding the database lease (invariant 5);
// DisableWrite must follow in the same request (spec §8).
func (d *Database) OpenWrite(ctx context.Context, dbName string) (*Session, error) {
	return d.OpenWriteWithManifest(ctx, dbName, nil)
}

// OpenWriteWithManifest opens a read-write session whose restore plan comes
// from the durable manifest. A nil files slice selects the legacy listing
// path. Call only while holding the database lease (invariant 5).
func (d *Database) OpenWriteWithManifest(ctx context.Context, dbName string, files []*ltx.FileInfo) (*Session, error) {
	// One write VFS per database profile; each transaction gets its own
	// connection and VFSFile on that shared registration.
	w, err := d.ensureWriteVFS()
	if err != nil {
		return nil, err
	}
	if _, err := w.ensureBufferRoot(); err != nil {
		return nil, err
	}

	seq := globalSessionSeq.Add(1)
	fileName := fmt.Sprintf("walrusd_%s_%d.db", sanitize(dbName), seq)
	var state *vfsSession
	if files != nil {
		mc := newManifestClient(d.client, files)
		state = &vfsSession{inner: d.Bridge.newInnerVFS(mc, true), client: mc}
		if root, err := w.ensureBufferRoot(); err == nil {
			// Keep this session's Litestream write buffer inside the wrapper's
			// removable directory instead of letting Litestream create its
			// own litestream-vfs-* temp dir per session.
			state.inner.WriteBufferPath = filepath.Join(root, fmt.Sprintf("write-buffer-%d", seq))
		}
		w.registerSession(fileName, state)
	}
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=rw", fileName, d.writeVFSName)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		if state != nil {
			w.unregisterSession(fileName)
		}
		_ = w.discardBuffers()
		return nil, fmt.Errorf("litestream: open write: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		if state != nil {
			w.unregisterSession(fileName)
		}
		_ = w.discardBuffers()
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	s := &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db, writeVFS: w, sessionVFS: w}
	if err := s.EnableWrite(); err != nil {
		conn.Close()
		db.Close()
		if state != nil {
			w.unregisterSession(fileName)
		}
		_ = w.discardBuffers()
		return nil, err
	}
	return s, nil
}

// TXID returns the remote Litestream TXID this session observes (spec §9
// read-after-write).
func (s *Session) TXID() (string, error) {
	var txid string
	if err := s.Run(context.Background(), func(conn *sql.Conn) error {
		return conn.QueryRowContext(context.Background(), "PRAGMA litestream_txid").Scan(&txid)
	}); err != nil {
		return "", fmt.Errorf("litestream: read txid: %w", err)
	}
	return txid, nil
}

// NextTXID returns the TXID this session's flush will write: the last
// synced TXID plus one. Valid only while the transaction is the session's
// only pending write (the runtime's contract: one write transaction per
// session, one flush at disable).
func (s *Session) NextTXID() (string, error) {
	file, err := s.vfsFile()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", uint64(file.Pos().TXID)+1), nil
}

// EnableWrite turns on VFS write mode for this session's file.
func (s *Session) EnableWrite() error {
	file, err := s.vfsFile()
	if err != nil {
		return err
	}
	if err := file.SetWriteEnabled(true); err != nil {
		return fmt.Errorf("litestream: enable write: %w", err)
	}
	s.mu.Lock()
	s.writeFile = file
	s.writeOn = true
	s.mu.Unlock()
	return nil
}

// DisableWrite is the MANDATORY flush barrier (spec §8): synchronously
// flushes dirty pages as a remote LTX file; returns an error on upload
// failure or Litestream conflict. Same VFS file as the transaction.
func (s *Session) DisableWrite() error {
	s.mu.Lock()
	file := s.writeFile
	s.mu.Unlock()
	if file == nil {
		return errors.New("litestream: write mode not enabled on this session")
	}
	if err := file.SetWriteEnabled(false); err != nil {
		return fmt.Errorf("litestream: flush barrier failed: %w", err)
	}
	s.mu.Lock()
	s.writeOn = false
	s.mu.Unlock()
	return nil
}

// SQLConn exposes the dedicated database/sql connection for the request
// callback. All SQL MUST run on this single connection so the flush barrier
// lands on the same VFS file (spec §8).
func (s *Session) SQLConn() *sql.Conn { return s.conn }

// Run executes fn while the process-global VFS registry is stable.
func (s *Session) Run(ctx context.Context, fn func(*sql.Conn) error) error {
	s.mu.Lock()
	conn := s.conn
	closed := s.closed
	s.mu.Unlock()
	if closed || conn == nil {
		return errors.New("litestream: session is closed")
	}
	return fn(conn)
}

// Exec runs a statement on this session's dedicated connection.
func (s *Session) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := s.Run(ctx, func(conn *sql.Conn) error {
		var err error
		result, err = conn.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

// Query runs a query on this session's dedicated connection.
func (s *Session) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	err := s.Run(ctx, func(conn *sql.Conn) error {
		var err error
		rows, err = conn.QueryContext(ctx, query, args...)
		return err
	})
	return rows, err
}

// Close closes the session. If write mode is still enabled the caller lost
// the flush barrier; Close surfaces that as an error after cleanup.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	writeOn := s.writeOn
	s.writeFile = nil
	s.mu.Unlock()

	var errs []error
	if writeOn {
		errs = append(errs, errors.New("litestream: session closed with write mode still enabled (flush barrier skipped)"))
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		s.conn = nil
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
		s.db = nil
	}
	if s.writeVFS != nil {
		s.writeVFS.forgetFile(s.dbFileName)
		if err := s.writeVFS.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
		s.writeVFS = nil
	} else if s.dbVFS != nil {
		s.dbVFS.wrapper.forgetFile(s.dbFileName)
		if err := s.dbVFS.wrapper.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.sessionVFS != nil {
		s.sessionVFS.unregisterSession(s.dbFileName)
		s.sessionVFS = nil
	}
	return errors.Join(errs...)
}

// ManifestFiles returns the file list captured by this session's manifest
// client, including the file just flushed. It returns nil for legacy opens.
func (s *Session) ManifestFiles() []*ltx.FileInfo {
	if s.sessionVFS == nil {
		return nil
	}
	sess := s.sessionVFS.session(s.dbFileName)
	if sess == nil || sess.client == nil {
		return nil
	}
	return sess.client.Files()
}

// WriteOn reports whether write mode is currently enabled.
func (s *Session) WriteOn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeOn
}

// vfsFile returns the concrete litestream VFSFile captured by the wrapper
// for this session's database file.
func (s *Session) vfsFile() (*ls.VFSFile, error) {
	w := s.writeVFS
	if w == nil {
		w = s.dbVFS.wrapper
	}
	return w.File(s.dbFileName)
}
