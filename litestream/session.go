package litestream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	ls "github.com/benbjohnson/litestream"
	_ "github.com/mattn/go-sqlite3" // registers the sqlite3 driver with VFS support
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
	closed    bool
}

var globalSessionSeq atomic.Uint64

// OpenRead opens a read-only session against the remote replica state
// (spec §9). No local database file is hydrated (invariant 11).
func (d *Database) OpenRead(ctx context.Context, dbName string) (*Session, error) {
	fileName := fmt.Sprintf("walrusd_%s_%d.db", sanitize(dbName), globalSessionSeq.Add(1))
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=ro&_query_only=1", fileName, d.VFSName)
	vfsRegistryMu.RLock()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		vfsRegistryMu.RUnlock()
		return nil, fmt.Errorf("litestream: open read: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	vfsRegistryMu.RUnlock()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	return &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db}, nil
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
func (d *Database) HasLTX(ctx context.Context) (bool, error) {
	client, err := d.Bridge.ReplicaClient(d.Key, d.ReplicaPrefix, d.Profile)
	if err != nil {
		return false, fmt.Errorf("litestream: replica client: %w", err)
	}
	if err := client.Init(ctx); err != nil {
		return false, fmt.Errorf("litestream: init replica client: %w", err)
	}
	itr, err := client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return false, fmt.Errorf("litestream: list LTX files: %w", err)
	}
	defer itr.Close()
	return itr.Next(), nil
}

// OpenWrite opens a read-write session with VFS write mode enabled from the
// start. Litestream only honors write mode at VFS-open; a read-mode open
// would block waiting for remote LTX files that do not exist for a new
// database. Call only while holding the database lease (invariant 5);
// DisableWrite must follow in the same request (spec §8).
func (d *Database) OpenWrite(ctx context.Context, dbName string) (*Session, error) {
	// One write VFS per database profile; each transaction gets its own
	// connection and VFSFile on that shared registration.
	w, err := d.ensureWriteVFS()
	if err != nil {
		return nil, err
	}
	if _, err := w.ensureBufferRoot(); err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf("walrusd_%s_%d.db", sanitize(dbName), globalSessionSeq.Add(1))
	dsn := fmt.Sprintf("file:%s?vfs=%s&mode=rw", fileName, d.writeVFSName)
	vfsRegistryMu.RLock()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		vfsRegistryMu.RUnlock()
		_ = w.discardBuffers()
		return nil, fmt.Errorf("litestream: open write: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	vfsRegistryMu.RUnlock()
	if err != nil {
		db.Close()
		_ = w.discardBuffers()
		return nil, fmt.Errorf("litestream: conn: %w", err)
	}
	s := &Session{dbVFS: d, dbFileName: fileName, conn: conn, db: db, writeVFS: w}
	if err := s.EnableWrite(); err != nil {
		conn.Close()
		db.Close()
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
	vfsRegistryMu.RLock()
	defer vfsRegistryMu.RUnlock()
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
		vfsRegistryMu.RLock()
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		vfsRegistryMu.RUnlock()
		s.conn = nil
	}
	if s.db != nil {
		vfsRegistryMu.RLock()
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
		vfsRegistryMu.RUnlock()
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
