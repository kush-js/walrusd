package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	ls "github.com/benbjohnson/litestream"
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

// wrapperVFS delegates to a litestream VFS and captures each opened main-DB
// file so the runtime can drive the flush barrier on the exact file the
// SQLite connection uses (spec §8).
type wrapperVFS struct {
	client ls.ReplicaClient
	inner  *ls.VFS

	fileMu sync.Mutex
	files  map[string]*ls.VFSFile // filename -> file

	sessMu   sync.Mutex
	sessions map[string]*vfsSession // filename -> session-scoped VFS

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

	inner := w.inner
	if sess := w.session(name); sess != nil {
		inner = sess.inner
	}
	file, retFlags, err := inner.Open(name, flags)
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
	VFSName       string  // name passed to ?vfs= in the DSN
	Key           string  // canonical database key
	ReplicaPrefix string  // object-store replica prefix
	Profile       Profile // trusted storage profile (credentials resolved)
	wrapper       *wrapperVFS
	client        ls.ReplicaClient // shared by the read and write VFS
	manifest      ManifestStore    // durable LTX index, written after each flush

	writeMu      sync.Mutex
	writeVFSName string
	writeWrapper *wrapperVFS

	maintainMu   sync.Mutex
	lastMaintain [maxMaintainLevel]time.Time // last compaction per level
}

// RegisterDatabase registers a unique VFS for one database replica.
func (b *Bridge) RegisterDatabase(dbKey, replicaPrefix string, p Profile) (*Database, error) {
	client, err := b.ReplicaClient(dbKey, replicaPrefix, p)
	if err != nil {
		return nil, err
	}
	shared := client
	if b.files != nil || b.listingTTL > 0 {
		shared = newCachingClient(client, dbKey, b.files, b.listingTTL)
	}
	manifest, err := b.NewManifestStore(replicaPrefix, p)
	if err != nil {
		return nil, err
	}
	w := b.newWrapperVFS(shared, false)
	// Litestream requires a positive poll interval (its monitor builds a
	// ticker from it) and a positive sync interval when write mode is on.
	name := fmt.Sprintf("walrusd_%d", globalVFSSeq.Add(1))
	if err := registerVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register vfs %s: %w", name, err)
	}
	return &Database{Bridge: b, VFSName: name, Key: dbKey, ReplicaPrefix: replicaPrefix, Profile: p, wrapper: w, client: shared, manifest: manifest}, nil
}

func (b *Bridge) newWrapperVFS(client ls.ReplicaClient, write bool) *wrapperVFS {
	return &wrapperVFS{
		client:     client,
		inner:      b.newInnerVFS(client, write),
		files:      make(map[string]*ls.VFSFile),
		sessions:   make(map[string]*vfsSession),
		tempFiles:  make(map[string]string),
		bufferBase: b.cfg.WriteBufferRootPath,
		budget:     b.buffers,
	}
}

// newInnerVFS builds one Litestream VFS with the runtime's settings. A
// session gets its own instance so its replica client can be backed by a
// per-request manifest without sharing state across requests.
func (b *Bridge) newInnerVFS(client ls.ReplicaClient, write bool) *ls.VFS {
	v := ls.NewVFS(client, noOpLogger())
	v.PollInterval = litestreamPollInterval
	v.CacheSize = b.cfg.VFSPageCacheBytes
	v.HydrationEnabled = b.cfg.HydrationEnabled
	v.WriteEnabled = write
	v.WriteSyncInterval = 0
	return v
}

// vfsSession is one request-scoped VFS. For manifest-backed opens, inner
// uses a manifestClient that answers LTXFiles from the durable manifest.
type vfsSession struct {
	inner  *ls.VFS
	client *manifestClient
}

func (w *wrapperVFS) registerSession(name string, sess *vfsSession) {
	w.sessMu.Lock()
	w.sessions[name] = sess
	w.sessMu.Unlock()
}

func (w *wrapperVFS) unregisterSession(name string) {
	w.sessMu.Lock()
	delete(w.sessions, name)
	w.sessMu.Unlock()
}

func (w *wrapperVFS) session(name string) *vfsSession {
	w.sessMu.Lock()
	defer w.sessMu.Unlock()
	return w.sessions[name]
}

// ensureWriteVFS registers the database's write-capable VFS exactly once.
func (d *Database) ensureWriteVFS() (*wrapperVFS, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.writeWrapper != nil {
		return d.writeWrapper, nil
	}
	w := d.Bridge.newWrapperVFS(d.client, true)
	name := fmt.Sprintf("walrusd_w%d", globalVFSSeq.Add(1))
	if err := registerVFS(name, w); err != nil {
		return nil, fmt.Errorf("litestream: register write vfs %s: %w", name, err)
	}
	d.writeVFSName = name
	d.writeWrapper = w
	return w, nil
}

// maxMaintainLevel is the deepest compaction level; level 9 is reserved for
// snapshots. Eight levels make the chain logarithmic in write count, so
// cold opens walk a bounded number of LTX files no matter the write volume.
const maxMaintainLevel = 8

// maintainIntervals is the compaction cadence per destination level
// (index i = level i+1), mirroring litestream's DefaultCompactionLevels
// shape: each level aggregates the one below over a longer window. Tests
// shorten these; production keeps upstream-scale cadence.
var maintainIntervals = [maxMaintainLevel]time.Duration{
	30 * time.Second,
	5 * time.Minute,
	1 * time.Hour,
	6 * time.Hour,
	24 * time.Hour,
	72 * time.Hour,
	240 * time.Hour,
	720 * time.Hour,
}

// pruneAge is how long a source LTX file stays after a higher level covers
// it. Readers' page indexes refresh from the replica every poll interval
// (500ms), so a generous age leaves no reader pointing at pruned files.
// Tests shorten this.
var pruneAge = 2 * time.Minute

// maintainBudget caps one maintenance pass so a slow replica cannot pin a
// write request; unfinished compaction simply runs on a later write.
const maintainBudget = 10 * time.Second

// Maintain compacts the replica chain and prunes source files already
// covered by higher levels. walrusd writes one LTX file per transaction;
// without this the chain grows forever and cold opens walk every file ever
// written. Levels are compacted on their own cadence so each level
// aggregates the one below it instead of growing per write.
//
// Call only while holding the database lease: compaction and pruning write
// to the replica, and the lease is what serializes writers. Errors are
// returned for observability; they must never fail the flushed write.
func (d *Database) Maintain(ctx context.Context, file *ls.VFSFile) error {
	d.maintainMu.Lock()
	var due []int
	for i, interval := range maintainIntervals {
		if time.Since(d.lastMaintain[i]) >= interval {
			d.lastMaintain[i] = time.Now()
			due = append(due, i)
		}
	}
	d.maintainMu.Unlock()
	if len(due) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, maintainBudget)
	defer cancel()

	var errs []error
	for _, i := range due {
		if _, err := file.Compact(ctx, i+1); err != nil &&
			!errors.Is(err, ls.ErrNoCompaction) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, err)
		}
		if ctx.Err() != nil {
			return errors.Join(errs...)
		}
	}
	if err := d.pruneCovered(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// pruneCovered deletes source LTX files older than pruneAge whose whole
// TXID range sits inside one file of the next level. Containment in a
// single file always holds for our compaction — each Compact merges whole
// source files into one destination file — so pruning can never remove
// state a restore plan needs. The newest deletable file per level is kept
// so every level always has a starting point.
func (d *Database) pruneCovered(ctx context.Context) error {
	client := d.writeWrapper.client
	var errs []error
	for level := 0; level < maxMaintainLevel; level++ {
		covered, err := listLTXFiles(ctx, client, level+1)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		sources, err := listLTXFiles(ctx, client, level)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var stale []*ltx.FileInfo
		for _, s := range sources {
			if time.Since(s.CreatedAt) < pruneAge {
				continue
			}
			for _, n := range covered {
				if n.MinTXID <= s.MinTXID && s.MaxTXID <= n.MaxTXID {
					stale = append(stale, s)
					break
				}
			}
		}
		if len(stale) > 1 {
			if err := client.DeleteLTXFiles(ctx, stale[:len(stale)-1]); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// listLTXFiles materializes the file list for one level, ascending.
func listLTXFiles(ctx context.Context, client ls.ReplicaClient, level int) ([]*ltx.FileInfo, error) {
	itr, err := client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return nil, err
	}
	var infos []*ltx.FileInfo
	for itr.Next() {
		infos = append(infos, itr.Item())
	}
	return infos, itr.Close()
}

// Close releases process-local files and process-global VFS registrations
// owned by this database.
func (d *Database) Close() error {
	var errs []error
	if d.wrapper != nil {
		if err := d.wrapper.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
	}
	d.writeMu.Lock()
	w := d.writeWrapper
	writeVFSName := d.writeVFSName
	d.writeMu.Unlock()
	if w != nil {
		if err := w.discardBuffers(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := unregisterVFS(d.VFSName); err != nil {
		errs = append(errs, err)
	}
	if writeVFSName != "" {
		if err := unregisterVFS(writeVFSName); err != nil {
			errs = append(errs, err)
		}
	}
	if d.Bridge.files != nil {
		d.Bridge.files.dropDB(d.Key)
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
