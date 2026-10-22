package litestream

import (
	"bytes"
	"container/list"
	"context"
	"io"
	"sync"
	"time"

	litestream "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// Litestream re-reads every file in the replica chain on each VFS open: the
// header, the page index, and the trailer arrive as separate ranged GETs.
// WALrus now bounds the chain with compaction and indexes it in a durable
// manifest, but these caches remain available for callers that opt in.
//
// LTX files are immutable once written: a (level, minTXID, maxTXID) key names
// the same bytes forever. Caching the whole file (they are per-user segments,
// not the whole database) serves every range of it from memory. A retried
// flush may rewrite the same TXID, so WriteLTXFile replaces the entry; deletes
// drop theirs.
const defaultFileCacheBytes = 32 << 20

// maxCachedFileBytes bounds one entry. Larger files stream through uncached.
const maxCachedFileBytes = 8 << 20

// defaultLevelListingTTL bounds how long a non-tail level's listing is
// trusted. The TTL is a backstop against an out-of-process compactor, and
// every in-process write/delete invalidates its level. Listing entries are
// metadata only — a few hundred bytes per database.
const defaultLevelListingTTL = 30 * time.Second

type fileKey struct {
	dbKey   string
	level   int
	minTXID ltx.TXID
	maxTXID ltx.TXID
}

type fileEntry struct {
	key  fileKey
	data []byte
}

// fileCache is a byte-bounded LRU of LTX file contents.
type fileCache struct {
	mu      sync.Mutex
	order   *list.List // front = most recently used
	entries map[fileKey]*list.Element
	bytes   int64
	limit   int64
}

func newFileCache(limit int64) *fileCache {
	return &fileCache{
		order:   list.New(),
		entries: make(map[fileKey]*list.Element),
		limit:   limit,
	}
}

func (c *fileCache) get(key fileKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*fileEntry).data, true
}

func (c *fileCache) put(key fileKey, data []byte) {
	if c.limit <= 0 || int64(len(data)) > c.limit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.order.MoveToFront(el)
		return
	}
	el := c.order.PushFront(&fileEntry{key: key, data: data})
	c.entries[key] = el
	c.bytes += int64(len(data))
	for c.bytes > c.limit {
		back := c.order.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*fileEntry)
		c.order.Remove(back)
		delete(c.entries, entry.key)
		c.bytes -= int64(len(entry.data))
	}
}

func (c *fileCache) drop(key fileKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return
	}
	c.order.Remove(el)
	delete(c.entries, key)
	c.bytes -= int64(len(el.Value.(*fileEntry).data))
}

// dropDB removes every cached file of one database.
func (c *fileCache) dropDB(dbKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, el := range c.entries {
		if key.dbKey != dbKey {
			continue
		}
		c.order.Remove(el)
		delete(c.entries, key)
		c.bytes -= int64(len(el.Value.(*fileEntry).data))
	}
}

// cachingClient memoizes immutable LTX files and stable level listings in
// front of a replica client.
type cachingClient struct {
	litestream.ReplicaClient
	dbKey      string
	files      *fileCache    // nil = LTX file bytes are not cached
	listingTTL time.Duration // <= 0 = level listings are not cached
	listMu     sync.Mutex
	listings   map[int]listingEntry
}

type listingEntry struct {
	infos []*ltx.FileInfo
	at    time.Time
}

func newCachingClient(inner litestream.ReplicaClient, dbKey string, files *fileCache, listingTTL time.Duration) *cachingClient {
	return &cachingClient{
		ReplicaClient: inner,
		dbKey:         dbKey,
		files:         files,
		listingTTL:    listingTTL,
		listings:      make(map[int]listingEntry),
	}
}

// LTXFiles serves levels 1..9 from a short-lived listing. CalcRestorePlan opens
// an iterator for every level on every VFS open; only level 0 (the live tail)
// changes in a non-compacting replica, so the other nine become one LIST per
// TTL instead of nine per open.
func (c *cachingClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if level == 0 || c.listingTTL <= 0 {
		return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
	}

	c.listMu.Lock()
	entry, ok := c.listings[level]
	c.listMu.Unlock()
	if ok && time.Since(entry.at) < c.listingTTL {
		return ltx.NewFileInfoSliceIterator(filterFrom(entry.infos, seek)), nil
	}

	itr, err := c.ReplicaClient.LTXFiles(ctx, level, 0, useMetadata)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	infos := make([]*ltx.FileInfo, 0, 8)
	for itr.Next() {
		infos = append(infos, itr.Item())
	}
	if err := itr.Err(); err != nil {
		return nil, err
	}

	c.listMu.Lock()
	c.listings[level] = listingEntry{infos: infos, at: time.Now()}
	c.listMu.Unlock()
	return ltx.NewFileInfoSliceIterator(filterFrom(infos, seek)), nil
}

func filterFrom(infos []*ltx.FileInfo, seek ltx.TXID) []*ltx.FileInfo {
	if seek == 0 {
		return infos
	}
	filtered := make([]*ltx.FileInfo, 0, len(infos))
	for _, info := range infos {
		if info.MinTXID >= seek {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

// DropListings forgets every cached level listing. Compaction rewrites levels
// behind the client's back, so it clears them before deciding what to merge.
func (c *cachingClient) DropListings() {
	c.listMu.Lock()
	c.listings = make(map[int]listingEntry)
	c.listMu.Unlock()
}

func (c *cachingClient) dropLevel(level int) {
	c.listMu.Lock()
	delete(c.listings, level)
	c.listMu.Unlock()
}

func (c *cachingClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if c.files == nil {
		return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	}
	key := fileKey{dbKey: c.dbKey, level: level, minTXID: minTXID, maxTXID: maxTXID}
	if data, ok := c.files.get(key); ok {
		return sliceReader(data, offset, size), nil
	}

	// Fetch the file once and serve the requested window from it. A ranged
	// read would leave the rest of the file to be fetched again for the next
	// window, which is exactly the per-open pattern this cache removes.
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) <= maxCachedFileBytes {
		c.files.put(key, data)
	}
	return sliceReader(data, offset, size), nil
}

func sliceReader(data []byte, offset, size int64) io.ReadCloser {
	if offset >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil))
	}
	window := data[offset:]
	if size > 0 && int64(len(window)) > size {
		window = window[:size]
	}
	return io.NopCloser(bytes.NewReader(window))
}

func (c *cachingClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.dropLevel(level)
	if c.files == nil {
		return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
	}
	// The flush passes the whole file, so cache the bytes we are about to
	// write: the next open reads the file we just produced.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	key := fileKey{dbKey: c.dbKey, level: level, minTXID: minTXID, maxTXID: maxTXID}
	c.files.drop(key)
	info, err := c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) <= maxCachedFileBytes {
		c.files.put(key, data)
	}
	return info, nil
}

func (c *cachingClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
	for _, file := range files {
		if c.files != nil {
			c.files.drop(fileKey{dbKey: c.dbKey, level: file.Level, minTXID: file.MinTXID, maxTXID: file.MaxTXID})
		}
		c.dropLevel(file.Level)
	}
	return c.ReplicaClient.DeleteLTXFiles(ctx, files)
}

func (c *cachingClient) DeleteAll(ctx context.Context) error {
	if c.files != nil {
		c.files.dropDB(c.dbKey)
	}
	c.listMu.Lock()
	c.listings = make(map[int]listingEntry)
	c.listMu.Unlock()
	return c.ReplicaClient.DeleteAll(ctx)
}
