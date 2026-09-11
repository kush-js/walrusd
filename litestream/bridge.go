package litestream

import (
	"errors"
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

	fileMu sync.Mutex
	files  map[string]*ls.VFSFile // filename -> file

	tempMu         sync.Mutex
	bufferBase     string
	bufferRoot     string
	tempFiles      map[string]string
	activeFiles    int
	cleanupPending bool
	accountedBytes int64

	budget *tempBudget
}

func (w *wrapperVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	if w.isTempFlags(flags) {
		file, retFlags, err := w.openTempFile(name, flags)
		if err != nil {
			return nil, retFlags, err
		}
		return &limitedFile{File: file, vfs: w}, retFlags, nil
	}

	file, retFlags, err := w.inner.Open(name, flags)
	if err != nil {
		return nil, retFlags, err
	}
	if flags&sqlite3vfs.OpenMainDB != 0 {
		if vf, ok := file.(*ls.VFSFile); ok {
			w.fileMu.Lock()
			w.files[name] = vf
			w.fileMu.Unlock()
			w.tempMu.Lock()
			w.activeFiles++
			w.tempMu.Unlock()
		}
		return &trackedFile{
			File: &limitedFile{File: file, vfs: w},
			vfs:  w,
		}, retFlags, nil
	}
	return &limitedFile{File: file, vfs: w}, retFlags, nil
}

func (w *wrapperVFS) Delete(name string, dirSync bool) error {
	if err := w.deleteTempFile(name); err == nil {
		return nil
	} else if err != errTempFileNotFound {
		return err
	}
	return w.inner.Delete(name, dirSync)
}

func (w *wrapperVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	if ok, err := w.accessTempFile(name); err == nil {
		return ok, nil
	} else if err != errTempFileNotFound {
		return false, err
	}
	return w.inner.Access(name, flags)
}
func (w *wrapperVFS) FullPathname(name string) string { return w.inner.FullPathname(name) }

// File returns the captured file for filename.
func (w *wrapperVFS) File(name string) (*ls.VFSFile, error) {
	w.fileMu.Lock()
	defer w.fileMu.Unlock()
	f, ok := w.files[name]
	if !ok {
		return nil, fmt.Errorf("litestream: no file captured for %q", name)
	}
	return f, nil
}

func (w *wrapperVFS) forgetFile(name string) {
	w.fileMu.Lock()
	delete(w.files, name)
	w.fileMu.Unlock()
}

// Database is one registered database VFS in this process.
type Database struct {
	Bridge        *Bridge
	VFSName       string  // name passed as ?vfs= in the DSN
	Key           string  // canonical database key
	ReplicaPrefix string  // object-store replica prefix
	Profile       Profile // trusted storage profile (credentials resolved)
	wrapper       *wrapperVFS

	writeMu      sync.Mutex
	writeVFSName string
	writeWrapper *wrapperVFS
}

// RegisterDatabase registers a unique VFS for one database replica.
func (b *Bridge) RegisterDatabase(dbKey, replicaPrefix string, p Profile) (*Database, error) {
	client, err := b.ReplicaClient(dbKey, replicaPrefix, p)
	if err != nil {
		return nil, err
	}
	w := b.newWrapperVFS(client, false)
	// Litestream requires a positive poll interval (its monitor builds a
	// ticker from it) and a positive sync interval when write mode is on.
	name := fmt.Sprintf("walrusd_%d", globalVFSSeq.Add(1))
	if err := registerVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register vfs %s: %w", name, err)
	}
	return &Database{Bridge: b, VFSName: name, Key: dbKey, ReplicaPrefix: replicaPrefix, Profile: p, wrapper: w}, nil
}

func (b *Bridge) newWrapperVFS(client ls.ReplicaClient, write bool) *wrapperVFS {
	w := &wrapperVFS{
		inner:      ls.NewVFS(client, noOpLogger()),
		files:      make(map[string]*ls.VFSFile),
		tempFiles:  make(map[string]string),
		bufferBase: b.cfg.WriteBufferRootPath,
		budget:     b.buffers,
	}
	w.inner.PollInterval = litestreamPollInterval
	w.inner.CacheSize = b.cfg.VFSPageCacheBytes
	w.inner.HydrationEnabled = b.cfg.HydrationEnabled
	w.inner.WriteEnabled = write
	w.inner.WriteSyncInterval = 0
	return w
}

// ensureWriteVFS registers the database's write-capable VFS exactly once.
func (d *Database) ensureWriteVFS() (*wrapperVFS, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.writeWrapper != nil {
		return d.writeWrapper, nil
	}
	client, err := d.Bridge.ReplicaClient(d.Key, d.ReplicaPrefix, d.Profile)
	if err != nil {
		return nil, fmt.Errorf("litestream: replica client: %w", err)
	}
	w := d.Bridge.newWrapperVFS(client, true)
	name := fmt.Sprintf("walrusd_w%d", globalVFSSeq.Add(1))
	if err := registerVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register write vfs %s: %w", name, err)
	}
	d.writeVFSName = name
	d.writeWrapper = w
	return w, nil
}

// Close releases process-local files owned by this database VFS. The
// sqlite3vfs registry has no unregister operation, so the registrations
// themselves remain process-global.
func (d *Database) Close() error {
	var errs []error
	if d.wrapper != nil {
		if err := d.wrapper.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
	}
	d.writeMu.Lock()
	w := d.writeWrapper
	d.writeMu.Unlock()
	if w != nil {
		if err := w.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// noOpLogger discards VFS logs; tenant data must never reach them (spec §13).
func noOpLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(debugWriter{}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// debugWriter routes VFS internal logs to stderr when WALRUSD_VFS_DEBUG is
// set, else discards. VFS log lines contain only page/TXID metadata.
type debugWriter struct{}

func (debugWriter) Write(p []byte) (int, error) {
	if os.Getenv("WALRUSD_VFS_DEBUG") != "" {
		return os.Stderr.Write(p)
	}
	return len(p), nil
}

// discardBuffers removes this VFS instance's temp buffer files.
func (w *wrapperVFS) discardBuffers() error {
	w.tempMu.Lock()
	if w.activeFiles > 0 {
		w.cleanupPending = true
		w.tempMu.Unlock()
		return nil
	}
	root := w.bufferRoot
	w.bufferRoot = ""
	w.tempFiles = make(map[string]string)
	w.tempMu.Unlock()

	if w.budget != nil {
		w.budget.release(w)
	}
	if root == "" {
		return nil
	}
	if err := os.RemoveAll(root); err != nil {
		return fmt.Errorf("litestream: remove write buffer %s: %w", root, err)
	}
	return nil
}

// litestreamPollInterval is the replica polling cadence for read/write VFS
// instances. Kept small so reads observe fresh remote state quickly.
const litestreamPollInterval = 500 * time.Millisecond
