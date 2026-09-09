package litestream

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	ls "github.com/benbjohnson/litestream"
	"github.com/psanford/sqlite3vfs"
)

// wrapperVFS delegates to a litestream VFS and captures each opened main-DB
// file so the runtime can drive the flush barrier on the exact file the
// SQLite connection uses (spec §8).
type wrapperVFS struct {
	inner *ls.VFS

	mu    sync.Mutex
	files map[string]*ls.VFSFile // filename -> file
}

func (w *wrapperVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, retFlags, err := w.inner.Open(name, flags)
	if err != nil {
		return nil, retFlags, err
	}
	if flags&sqlite3vfs.OpenMainDB != 0 {
		if vf, ok := file.(*ls.VFSFile); ok {
			w.mu.Lock()
			w.files[name] = vf
			w.mu.Unlock()
		}
	}
	return file, retFlags, nil
}

func (w *wrapperVFS) Delete(name string, dirSync bool) error { return w.inner.Delete(name, dirSync) }
func (w *wrapperVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	return w.inner.Access(name, flags)
}
func (w *wrapperVFS) FullPathname(name string) string { return w.inner.FullPathname(name) }

// File returns the captured file for filename.
func (w *wrapperVFS) File(name string) (*ls.VFSFile, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.files[name]
	if !ok {
		return nil, fmt.Errorf("litestream: no file captured for %q", name)
	}
	return f, nil
}

// Database is one registered database VFS in this process.
type Database struct {
	Bridge        *Bridge
	VFSName       string  // name passed as ?vfs= in the DSN
	Key           string  // canonical database key
	ReplicaPrefix string  // object-store replica prefix
	Profile       Profile // trusted storage profile (credentials resolved)
	wrapper       *wrapperVFS
}

// RegisterDatabase registers a unique VFS for one database replica.
func (b *Bridge) RegisterDatabase(dbKey, replicaPrefix string, p Profile) (*Database, error) {
	client, err := b.ReplicaClient(dbKey, replicaPrefix, p)
	if err != nil {
		return nil, err
	}
	w := &wrapperVFS{
		inner: ls.NewVFS(client, noOpLogger()),
		files: make(map[string]*ls.VFSFile),
	}
	// Litestream requires a positive poll interval (its monitor builds a
	// ticker from it) and a positive sync interval when write mode is on.
	w.inner.PollInterval = litestreamPollInterval
	w.inner.CacheSize = b.cfg.VFSPageCacheBytes
	w.inner.HydrationEnabled = b.cfg.HydrationEnabled
	w.inner.WriteEnabled = false
	w.inner.WriteSyncInterval = 0
	if b.cfg.WriteBufferRootPath != "" {
		if err := os.MkdirAll(b.cfg.WriteBufferRootPath, 0o700); err != nil {
			return nil, fmt.Errorf("litestream: create write buffer root: %w", err)
		}
		w.inner.WriteBufferPath = b.cfg.WriteBufferRootPath + "/buffer-" + sanitize(dbKey)
	}
	name := fmt.Sprintf("walrus_%d", globalVFSSeq.Add(1))
	if err := sqlite3vfs.RegisterVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register vfs %s: %w", name, err)
	}
	return &Database{Bridge: b, VFSName: name, Key: dbKey, ReplicaPrefix: replicaPrefix, Profile: p, wrapper: w}, nil
}

// noOpLogger discards VFS logs; tenant data must never reach them (spec §13).
func noOpLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(debugWriter{}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// debugWriter routes VFS internal logs to stderr when WALRUS_VFS_DEBUG is
// set, else discards. VFS log lines contain only page/TXID metadata.
type debugWriter struct{}

func (debugWriter) Write(p []byte) (int, error) {
	if os.Getenv("WALRUS_VFS_DEBUG") != "" {
		return os.Stderr.Write(p)
	}
	return len(p), nil
}

// ensureTempRoot creates the temp write-buffer root.
func ensureTempRoot(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("litestream: create write buffer root: %w", err)
	}
	return nil
}

// discardBuffers removes this VFS instance's temp buffer files.
func (w *wrapperVFS) discardBuffers() {
	// Litestream removes per-file buffers on file close; nothing retained
	// after the connection drops. Left as a hook for buffer bookkeeping.
}

// litestreamPollInterval is the replica polling cadence for read/write VFS
// instances. Kept small so reads observe fresh remote state quickly.
const litestreamPollInterval = 500 * time.Millisecond
