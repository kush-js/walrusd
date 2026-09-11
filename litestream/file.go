package litestream

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/psanford/sqlite3vfs"
)

// ErrTempWriteBufferExceeded reports that a write would exceed the
// process-local temporary write-buffer limit.
var ErrTempWriteBufferExceeded = errors.New("litestream: max temp write buffer exceeded")

// tempBudget accounts for temporary files across all VFS instances owned by
// one Bridge. sqlite3vfs registrations are process-global, but the runtime
// configuration and its buffer root are Bridge-scoped.
type tempBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

func (b *tempBudget) check(w *wrapperVFS, additional int64) error {
	if b == nil || b.limit <= 0 {
		return nil
	}
	current, err := w.bufferSize()
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.used += current - w.accountedBytes
	w.accountedBytes = current
	if b.used < 0 {
		b.used = 0
	}
	if b.used+additional > b.limit {
		return fmt.Errorf("%w: %d bytes in use, %d byte write requested, limit %d",
			ErrTempWriteBufferExceeded, b.used, additional, b.limit)
	}
	return nil
}

func (b *tempBudget) reconcile(w *wrapperVFS) error {
	if b == nil || b.limit <= 0 {
		return nil
	}
	current, err := w.bufferSize()
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.used += current - w.accountedBytes
	w.accountedBytes = current
	if b.used < 0 {
		b.used = 0
	}
	over := b.used > b.limit
	used := b.used
	limit := b.limit
	b.mu.Unlock()
	if over {
		return fmt.Errorf("%w: %d bytes in use, limit %d", ErrTempWriteBufferExceeded, used, limit)
	}
	return nil
}

func (b *tempBudget) release(w *wrapperVFS) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.used -= w.accountedBytes
	w.accountedBytes = 0
	if b.used < 0 {
		b.used = 0
	}
	b.mu.Unlock()
}

// limitedFile enforces the temporary-buffer budget before delegating a
// write and reconciles actual disk use after it completes.
type limitedFile struct {
	sqlite3vfs.File

	mu  sync.Mutex
	vfs *wrapperVFS
}

func (f *limitedFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.vfs.budget.check(f.vfs, int64(len(p))); err != nil {
		return 0, err
	}
	n, err := f.File.WriteAt(p, off)
	if budgetErr := f.vfs.budget.reconcile(f.vfs); err == nil {
		err = budgetErr
	}
	return n, err
}

func (f *limitedFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.File.Truncate(size)
	if budgetErr := f.vfs.budget.reconcile(f.vfs); err == nil {
		err = budgetErr
	}
	return err
}

func (f *limitedFile) FileControl(op int, pragmaName string, pragmaValue *string) (*string, error) {
	controller, ok := f.File.(sqlite3vfs.FileController)
	if !ok {
		return nil, nil
	}
	return controller.FileControl(op, pragmaName, pragmaValue)
}

// trackedFile decrements the wrapper's active-file count after SQLite closes
// the main database file.
type trackedFile struct {
	sqlite3vfs.File

	vfs       *wrapperVFS
	closeOnce sync.Once
	closeErr  error
}

func (f *trackedFile) FileControl(op int, pragmaName string, pragmaValue *string) (*string, error) {
	controller, ok := f.File.(sqlite3vfs.FileController)
	if !ok {
		return nil, nil
	}
	return controller.FileControl(op, pragmaName, pragmaValue)
}

func (f *trackedFile) Close() error {
	f.closeOnce.Do(func() {
		f.closeErr = f.File.Close()
		f.vfs.fileClosed()
	})
	return f.closeErr
}

var errTempFileNotFound = errors.New("litestream: temp file not tracked")

func (w *wrapperVFS) isTempFlags(flags sqlite3vfs.OpenFlag) bool {
	const tempMask = sqlite3vfs.OpenTempDB |
		sqlite3vfs.OpenTempJournal |
		sqlite3vfs.OpenSubJournal |
		sqlite3vfs.OpenSuperJournal |
		sqlite3vfs.OpenTransientDB |
		sqlite3vfs.OpenMainJournal
	return flags&tempMask != 0 || flags&sqlite3vfs.OpenDeleteOnClose != 0
}

func (w *wrapperVFS) ensureBufferRoot() (string, error) {
	w.tempMu.Lock()
	defer w.tempMu.Unlock()
	if w.bufferRoot != "" {
		return w.bufferRoot, nil
	}

	base := w.bufferBase
	if base == "" {
		base = os.TempDir()
	} else if err := os.MkdirAll(base, 0o700); err != nil {
		return "", fmt.Errorf("litestream: create write buffer root: %w", err)
	}
	pattern := "walrusd-tmp-*"
	if w.inner.WriteEnabled {
		pattern = "walrusd-write-*"
	}
	root, err := os.MkdirTemp(base, pattern)
	if err != nil {
		return "", fmt.Errorf("litestream: create temp dir: %w", err)
	}
	w.bufferRoot = root
	if w.inner.WriteEnabled {
		// Keep Litestream's per-file write buffers inside the directory this
		// wrapper removes at session close.
		w.inner.WriteBufferPath = filepath.Join(root, "write-buffer")
	}
	if w.inner.HydrationEnabled && w.inner.HydrationPath == "" {
		w.inner.HydrationPath = filepath.Join(root, "hydration.db")
	}
	return root, nil
}

func (w *wrapperVFS) openTempFile(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	root, err := w.ensureBufferRoot()
	if err != nil {
		return nil, flags, err
	}

	deleteOnClose := flags&sqlite3vfs.OpenDeleteOnClose != 0 || name == ""
	var f *os.File
	if name == "" {
		f, err = os.CreateTemp(root, "temp-*")
	} else {
		canonical := filepath.Clean(name)
		if canonical == "." || canonical == string(filepath.Separator) {
			return nil, flags, sqlite3vfs.CantOpenError
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(canonical))
		path := filepath.Join(root, fmt.Sprintf("%s-%016x", filepath.Base(canonical), h.Sum64()))
		osFlags := openFlagToOSFlag(flags)
		if osFlags == 0 {
			osFlags = os.O_RDWR
		}
		f, err = os.OpenFile(path, osFlags|os.O_CREATE, 0o600)
		if err == nil {
			w.tempMu.Lock()
			w.tempFiles[canonical] = path
			w.tempMu.Unlock()
		}
	}
	if err != nil {
		return nil, flags, sqlite3vfs.CantOpenError
	}
	return newLocalTempFile(f, deleteOnClose, func() { w.tempFileClosed() }), flags, nil
}

func openFlagToOSFlag(flags sqlite3vfs.OpenFlag) int {
	var out int
	if flags&sqlite3vfs.OpenReadWrite != 0 {
		out |= os.O_RDWR
	} else if flags&sqlite3vfs.OpenReadOnly != 0 {
		out |= os.O_RDONLY
	}
	if flags&sqlite3vfs.OpenCreate != 0 {
		out |= os.O_CREATE
	}
	if flags&sqlite3vfs.OpenExclusive != 0 {
		out |= os.O_EXCL
	}
	return out
}

func (w *wrapperVFS) deleteTempFile(name string) error {
	canonical := filepath.Clean(name)
	w.tempMu.Lock()
	path, ok := w.tempFiles[canonical]
	if ok {
		delete(w.tempFiles, canonical)
	}
	w.tempMu.Unlock()
	if !ok {
		return errTempFileNotFound
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	w.tempFileClosed()
	return nil
}

func (w *wrapperVFS) accessTempFile(name string) (bool, error) {
	canonical := filepath.Clean(name)
	w.tempMu.Lock()
	path, ok := w.tempFiles[canonical]
	w.tempMu.Unlock()
	if !ok {
		return false, errTempFileNotFound
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (w *wrapperVFS) tempFileClosed() {
	_ = w.budget.reconcile(w)
}

func (w *wrapperVFS) fileClosed() {
	w.tempMu.Lock()
	if w.activeFiles > 0 {
		w.activeFiles--
	}
	cleanup := w.activeFiles == 0
	var root string
	if cleanup {
		w.cleanupPending = false
		root = w.bufferRoot
		w.bufferRoot = ""
	}
	w.tempMu.Unlock()
	if cleanup && root != "" {
		w.budget.release(w)
		_ = os.RemoveAll(root)
	}
}

func (w *wrapperVFS) bufferSize() (int64, error) {
	w.tempMu.Lock()
	root := w.bufferRoot
	w.tempMu.Unlock()
	if root == "" {
		return 0, nil
	}

	var size int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		size += info.Size()
		return nil
	})
	if os.IsNotExist(err) {
		return 0, nil
	}
	return size, err
}

type localTempFile struct {
	f             *os.File
	deleteOnClose bool
	onClose       func()

	mu       sync.Mutex
	lockType sqlite3vfs.LockType
	close    sync.Once
	closeErr error
}

func newLocalTempFile(f *os.File, deleteOnClose bool, onClose func()) *localTempFile {
	return &localTempFile{f: f, deleteOnClose: deleteOnClose, onClose: onClose}
}

func (f *localTempFile) Close() error {
	f.close.Do(func() {
		f.closeErr = f.f.Close()
		if f.deleteOnClose {
			if err := os.Remove(f.f.Name()); err != nil && !os.IsNotExist(err) && f.closeErr == nil {
				f.closeErr = err
			}
		}
		if f.onClose != nil {
			f.onClose()
		}
	})
	return f.closeErr
}

func (f *localTempFile) ReadAt(p []byte, off int64) (int, error) {
	return f.f.ReadAt(p, off)
}

func (f *localTempFile) WriteAt(p []byte, off int64) (int, error) {
	return f.f.WriteAt(p, off)
}

func (f *localTempFile) Truncate(size int64) error {
	return f.f.Truncate(size)
}

func (f *localTempFile) Sync(sqlite3vfs.SyncType) error {
	return f.f.Sync()
}

func (f *localTempFile) FileSize() (int64, error) {
	info, err := f.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (f *localTempFile) Lock(lock sqlite3vfs.LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lock < f.lockType {
		return fmt.Errorf("invalid lock downgrade: current=%s target=%s", f.lockType, lock)
	}
	f.lockType = lock
	return nil
}

func (f *localTempFile) Unlock(lock sqlite3vfs.LockType) error {
	f.mu.Lock()
	f.lockType = lock
	f.mu.Unlock()
	return nil
}

func (f *localTempFile) CheckReservedLock() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lockType >= sqlite3vfs.LockReserved, nil
}

func (f *localTempFile) SectorSize() int64 { return 0 }

func (f *localTempFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic { return 0 }
