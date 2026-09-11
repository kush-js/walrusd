package litestream

/*
#include <stdlib.h>

typedef struct sqlite3_vfs sqlite3_vfs;
extern sqlite3_vfs *sqlite3_vfs_find(const char*);
extern int sqlite3_vfs_unregister(sqlite3_vfs*);
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/psanford/sqlite3vfs"
)

// Keep the dependency's unexported registry in sync with SQLite itself.
// sqlite3vfs dispatches callbacks by the sqlite3_vfs name, so removing this
// entry before SQLite stops dispatching to it would leave a live SQLite VFS
// pointing at a missing Go object and crash in a later callback.
//
//go:linkname vfsMap github.com/psanford/sqlite3vfs.vfsMap
var vfsMap map[string]sqlite3vfs.ExtendedVFSv1

// unregisterVFS reclaims one process-global sqlite3vfs registration.
//
// The caller must ensure that no SQLite connection created with this VFS is
// still open. An open connection retains its sqlite3_vfs pointer and would
// dispatch callbacks into the deleted Go map entry, causing SIGSEGV.
//
// sqlite3vfs allocated the sqlite3_vfs struct and its name and has no delete
// path, so this intentionally leaves that small allocation behind. Freeing
// it here would risk a use-after-free for only a few hundred bytes per
// registration.
func unregisterVFS(name string) error {
	vfsRegistryMu.Lock()
	defer vfsRegistryMu.Unlock()

	if p := findSQLiteVFS(name); p != nil {
		if rc := C.sqlite3_vfs_unregister(p); rc != 0 {
			return fmt.Errorf("litestream: unregister sqlite vfs %q: sqlite code %d", name, int(rc))
		}
	}
	if _, ok := vfsMap[name]; ok {
		delete(vfsMap, name)
		decrementVFSCount()
	}
	return nil
}

func findSQLiteVFS(name string) *C.sqlite3_vfs {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return C.sqlite3_vfs_find(cName)
}

func sqliteVFSRegistered(name string) bool {
	vfsRegistryMu.RLock()
	defer vfsRegistryMu.RUnlock()
	return findSQLiteVFS(name) != nil
}

func decrementVFSCount() {
	for {
		current := vfsRegistrations.Load()
		if current == 0 || vfsRegistrations.CompareAndSwap(current, current-1) {
			return
		}
	}
}
