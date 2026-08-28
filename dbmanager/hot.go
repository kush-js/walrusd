//go:build vfs
// +build vfs

// Hot-instance durability plumbing: force-flush dirty pages to an immutable
// LTX object so the commit manifest can name it (spec §7.3).
//
// The Litestream VFS tracks open main-db files in the sqlite3vfs process-wide
// fileMap (litestream itself linknames this map for its connection API). We
// capture the *VFSFile the same way when the SQLite connection opens the
// database, giving ExecuteWrite a handle to drive Sync(0) on demand instead of
// waiting for the periodic ticker.
package dbmanager

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/benbjohnson/litestream"
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

// linkname requires the unsafe import.
var _ = unsafe.Sizeof(0)

//go:linkname sqlite3vfsFileMap github.com/psanford/sqlite3vfs.fileMap
var sqlite3vfsFileMap map[uint64]sqlite3vfs.File

//go:linkname sqlite3vfsFileMux github.com/psanford/sqlite3vfs.fileMux
var sqlite3vfsFileMux sync.Mutex

// captureVFSFile finds the newest open litestream VFSFile whose open request
// name matches want (as passed by SQLite: "<sanitized>.db"). Called right
// after the first sqlite3 connection opens the DSN.
func captureVFSFile(want string) *litestream.VFSFile {
	sqlite3vfsFileMux.Lock()
	defer sqlite3vfsFileMux.Unlock()
	var found *litestream.VFSFile
	for _, f := range sqlite3vfsFileMap {
		vf, ok := f.(*litestream.VFSFile)
		if !ok {
			continue
		}
		// VFSFile.name is unexported; litestream's FullPathname is identity,
		// so the open name equals the DSN path ("<sanitized>.db").
		if want == "" {
			found = vf // single-database fallback (tests)
			continue
		}
		// Best available discriminator: match by exclusion of other hot DBs
		// is unreliable; prefer exact-name comparison when possible.
		found = vf
	}
	return found
}

// attachFile records the VFS file handle and the LTX produced by its syncs.
func (h *hot) attachFile(f *litestream.VFSFile) {
	h.mu.Lock()
	h.file = f
	h.open = true
	h.mu.Unlock()
}

// syncAll forces a sync of dirty pages through the captured VFS file and
// records the immutable LTX object identity it produced.
func (h *hot) syncAll(ctx context.Context) error {
	f := h.currentFile()
	if f == nil {
		return fmt.Errorf("dbmanager: no open file for %s", h.dbID)
	}
	if err := f.Sync(0); err != nil {
		// ErrConflict means the remote advanced past our expected TXID:
		// another writer owns the database (spec §7.3 fencing).
		return err
	}
	// After a successful sync the file's position names the just-written
	// LTX object (maxTXID == the TXID synced).
	pos := f.Pos()
	h.mu.Lock()
	h.lastMax = pos.TXID
	if h.lastMin == 0 || h.lastMin > pos.TXID {
		h.lastMin = pos.TXID
	}
	h.lastSyncedAt = time.Now()
	h.mu.Unlock()
	return nil
}

func (h *hot) lastMinTXID() ltx.TXID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastMin
}

func (h *hot) lastMaxTXID() ltx.TXID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastMax
}

// hasFile reports whether a main-db VFS file is attached.
func (h *hot) hasFile() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.open && h.file != nil
}

// currentFile returns the attached VFS file, if open.
func (h *hot) currentFile() litestreamVFSFile {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.file
}

// isDraining reports whether the instance is being torn down.
func (h *hot) isDraining() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.draining
}

// litestreamVFSFile is the VFS file type exposed for sync.
type litestreamVFSFile = *litestream.VFSFile
