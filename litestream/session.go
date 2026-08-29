package litestream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	ls "github.com/benbjohnson/litestream"
	_ "github.com/mattn/go-sqlite3" // registers the sqlite3 driver with VFS support
	"github.com/psanford/sqlite3vfs"
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

	mu        sync.Mutex
	writeFile *ls.VFSFile
	writeOn   bool
}

// OpenRead opens a read-only session against the remote replica state
// (spec §9). No local database file is hydrated (invariant 11).
func (d *Database) OpenRead(ctx context.Context, dbName string) (*Session, error) {
	fileName := "walrus_" + sanitize(dbName) + ".db"
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=ro&_query_only=1", fileName, d.VFSName)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("litestream: open read: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	return &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db}, nil
}

// ReadDSN returns the SQLite DSN for the host's own SQLite connection to
// read this database through the shared litestream VFS (spec §9 read mode).
// The VFS is registered process-wide by the runtime, so any SQLite in the
// process (bun:sqlite, custom sqlite builds) can open it natively.
func (d *Database) ReadDSN(ctx context.Context, dbName string) string {
	return fmt.Sprintf("file:walrus_%s.db?vfs=%s&mode=ro", sanitize(dbName), d.VFSName)
}

// Key identifies this session's database for the runtime's read-instance
// cache (spec §10).
func (s *Session) Key() string { return s.dbVFS.Key }

// OpenWrite opens a read-write session with VFS write mode enabled from the
// start. Litestream only honors write mode at VFS-open; a read-mode open
// would block waiting for remote LTX files that do not exist for a new
// database. Call only while holding the database lease (invariant 5);
// DisableWrite must follow in the same request (spec §8).
func (d *Database) OpenWrite(ctx context.Context, dbName string) (*Session, error) {
	// Dedicated write VFS: write mode on from open, unique per session.
	client, err := d.Bridge.ReplicaClient(d.Key, d.ReplicaPrefix, d.Profile)
	if err != nil {
		return nil, fmt.Errorf("litestream: replica client: %w", err)
	}
	w := &wrapperVFS{
		inner: ls.NewVFS(client, noOpLogger()),
		files: make(map[string]*ls.VFSFile),
	}
	w.inner.PollInterval = litestreamPollInterval
	w.inner.CacheSize = d.Bridge.cfg.VFSPageCacheBytes
	w.inner.HydrationEnabled = d.Bridge.cfg.HydrationEnabled
	w.inner.WriteEnabled = true // this VFS exists only under a held lease
	w.inner.WriteSyncInterval = 0
	if d.Bridge.cfg.WriteBufferRootPath != "" {
		if err := ensureTempRoot(d.Bridge.cfg.WriteBufferRootPath); err != nil {
			return nil, err
		}
		w.inner.WriteBufferPath = d.Bridge.cfg.WriteBufferRootPath + "/buffer-" + sanitize(dbName)
	}
	name := fmt.Sprintf("walrus_w%d", d.Bridge.seq.Add(1))
	if err := sqlite3vfs.RegisterVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register write vfs: %w", err)
	}

	fileName := "walrus_" + sanitize(dbName) + ".db"
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=rw", fileName, name)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("litestream: open write: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	s := &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db, writeVFS: w}
	if err := s.EnableWrite(); err != nil {
		conn.Close()
		db.Close()
		return nil, err
	}
	return s, nil
}

// TXID returns the remote Litestream TXID this session observes (spec §9
// read-after-write).
func (s *Session) TXID() (string, error) {
	var txid string
	if err := s.conn.QueryRowContext(context.Background(), "PRAGMA litestream_txid").Scan(&txid); err != nil {
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

// Exec runs a statement on this session's dedicated connection.
func (s *Session) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.conn.ExecContext(ctx, query, args...)
}

// Query runs a query on this session's dedicated connection.
func (s *Session) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.conn.QueryContext(ctx, query, args...)
}

// Close closes the session. If write mode is still enabled the caller lost
// the flush barrier; Close surfaces that as an error after cleanup.
func (s *Session) Close() error {
	s.mu.Lock()
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
		s.writeVFS.discardBuffers()
		s.writeVFS = nil
	}
	return errors.Join(errs...)
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
