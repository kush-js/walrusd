package litestream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	ls "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// fakeReplica is a minimal in-memory replica that stores raw LTX bytes
// without parsing them, so a test can use any payload.
type fakeReplica struct {
	mu      sync.Mutex
	objects map[string][]byte
	opens   atomic.Int64
	lists   atomic.Int64
}

var _ ls.ReplicaClient = (*fakeReplica)(nil)

func newFakeReplica() *fakeReplica {
	return &fakeReplica{objects: make(map[string][]byte)}
}

func (f *fakeReplica) Type() string               { return "fake" }
func (f *fakeReplica) Init(context.Context) error { return nil }
func (f *fakeReplica) SetLogger(*slog.Logger)     {}

func (f *fakeReplica) key(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%04d/%016x-%016x", level, minTXID, maxTXID)
}

func (f *fakeReplica) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	f.lists.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	infos := make([]*ltx.FileInfo, 0)
	for key, data := range f.objects {
		var l int
		var minTXID, maxTXID ltx.TXID
		if _, err := fmt.Sscanf(key, "%04d/%016x-%016x", &l, &minTXID, &maxTXID); err != nil || l != level {
			continue
		}
		if minTXID < seek {
			continue
		}
		infos = append(infos, &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data))})
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (f *fakeReplica) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	f.opens.Add(1)
	f.mu.Lock()
	data, ok := f.objects[f.key(level, minTXID, maxTXID)]
	f.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return sliceReader(data, offset, size), nil
}

func (f *fakeReplica) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[f.key(level, minTXID, maxTXID)] = data
	f.mu.Unlock()
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data))}, nil
}

func (f *fakeReplica) DeleteLTXFiles(_ context.Context, files []*ltx.FileInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, file := range files {
		delete(f.objects, f.key(file.Level, file.MinTXID, file.MaxTXID))
	}
	return nil
}

func (f *fakeReplica) DeleteAll(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects = make(map[string][]byte)
	return nil
}

func newTestCacheClient(t *testing.T) (*cachingClient, *fakeReplica) {
	t.Helper()
	inner := newFakeReplica()
	return newCachingClient(inner, "orgs/o1/users/u1", newFileCache(1<<20)), inner
}

func writeTestFile(t *testing.T, client ls.ReplicaClient, level int, min, max ltx.TXID, body []byte) {
	t.Helper()
	if _, err := client.WriteLTXFile(context.Background(), level, min, max, bytes.NewReader(body)); err != nil {
		t.Fatalf("write ltx file: %v", err)
	}
}

func TestCachingClient_ServesFileRangesFromCache(t *testing.T) {
	client, inner := newTestCacheClient(t)
	ctx := context.Background()
	body := []byte("0123456789abcdef")
	writeTestFile(t, client, 0, 1, 1, body)

	// The write cached the bytes; every range of it is served from memory.
	for _, window := range []struct{ offset, size int64 }{{0, 4}, {4, 4}, {0, 0}} {
		rc, err := client.OpenLTXFile(ctx, 0, 1, 1, window.offset, window.size)
		if err != nil {
			t.Fatalf("open ltx file: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read ltx file: %v", err)
		}
		want := body[window.offset:]
		if window.size > 0 {
			want = body[window.offset : window.offset+window.size]
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("range %+v = %q, want %q", window, got, want)
		}
	}

	if got := inner.opens.Load(); got != 0 {
		t.Fatalf("OpenLTXFile calls = %d, want 0 after the write cached the file", got)
	}
}

func TestCachingClient_CachesRemoteFileOnFirstRead(t *testing.T) {
	client, inner := newTestCacheClient(t)
	ctx := context.Background()
	// Populate the replica behind the cache, as another instance would.
	if _, err := inner.WriteLTXFile(ctx, 0, 1, 1, bytes.NewReader([]byte("remote"))); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	for range 3 {
		rc, err := client.OpenLTXFile(ctx, 0, 1, 1, 0, 0)
		if err != nil {
			t.Fatalf("open ltx file: %v", err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
	if got := inner.opens.Load(); got != 1 {
		t.Fatalf("OpenLTXFile calls = %d, want 1", got)
	}
}

func TestCachingClient_DeleteDropsFile(t *testing.T) {
	client, inner := newTestCacheClient(t)
	ctx := context.Background()
	writeTestFile(t, client, 0, 1, 1, []byte("gone soon"))

	if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); err != nil {
		t.Fatalf("delete ltx files: %v", err)
	}
	if _, err := client.OpenLTXFile(ctx, 0, 1, 1, 0, 0); err == nil {
		t.Fatal("open after delete succeeded, want not-exist")
	}
	if got := inner.opens.Load(); got != 1 {
		t.Fatalf("OpenLTXFile calls = %d, want 1 (cache dropped)", got)
	}
}

func TestCachingClient_ListsTailEveryTimeAndStableLevelsOnce(t *testing.T) {
	client, inner := newTestCacheClient(t)
	ctx := context.Background()

	for range 3 {
		if _, err := client.LTXFiles(ctx, 0, 0, false); err != nil {
			t.Fatalf("list level 0: %v", err)
		}
		if _, err := client.LTXFiles(ctx, 9, 0, false); err != nil {
			t.Fatalf("list level 9: %v", err)
		}
	}

	// Three opens: the live tail is listed each time, the snapshot level once.
	if got := inner.lists.Load(); got != 4 {
		t.Fatalf("LTXFiles calls = %d, want 4 (3 tail + 1 snapshot)", got)
	}
}
