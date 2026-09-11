package litestream

import (
	"fmt"
	"testing"

	"github.com/psanford/sqlite3vfs"
)

type unregisterTestVFS struct{}

func (unregisterTestVFS) Open(string, sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	return nil, 0, nil
}

func (unregisterTestVFS) Delete(string, bool) error { return nil }

func (unregisterTestVFS) Access(string, sqlite3vfs.AccessFlag) (bool, error) {
	return false, nil
}

func (unregisterTestVFS) FullPathname(name string) string { return name }

func TestUnregisterVFS(t *testing.T) {
	name := fmt.Sprintf("walrusd_test_unregister_%d", globalVFSSeq.Add(1))
	before := RegisteredVFSCount()

	if err := registerVFS(name, unregisterTestVFS{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := RegisteredVFSCount(); got != before+1 {
		t.Fatalf("count after register = %d, want %d", got, before+1)
	}
	if !sqliteVFSRegistered(name) {
		t.Fatal("sqlite3_vfs_find did not find registered VFS")
	}
	vfsRegistryMu.RLock()
	_, inMap := vfsMap[name]
	vfsRegistryMu.RUnlock()
	if !inMap {
		t.Fatal("registered VFS missing from sqlite3vfs map")
	}

	if err := unregisterVFS(name); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if got := RegisteredVFSCount(); got != before {
		t.Fatalf("count after unregister = %d, want %d", got, before)
	}
	if sqliteVFSRegistered(name) {
		t.Fatal("sqlite3_vfs_find still found unregistered VFS")
	}
	vfsRegistryMu.RLock()
	_, inMap = vfsMap[name]
	vfsRegistryMu.RUnlock()
	if inMap {
		t.Fatal("unregistered VFS remains in sqlite3vfs map")
	}

	if err := unregisterVFS(name); err != nil {
		t.Fatalf("second unregister: %v", err)
	}
	if got := RegisteredVFSCount(); got != before {
		t.Fatalf("count after second unregister = %d, want %d", got, before)
	}
}

var _ sqlite3vfs.VFS = unregisterTestVFS{}
