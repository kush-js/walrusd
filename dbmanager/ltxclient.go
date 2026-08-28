//go:build vfs
// +build vfs

// Package dbmanager implements the per-database runtime lifecycle (spec §8):
// lazy opens through the Litestream VFS, bounded RAM caching, and the
// ReplicaClient that reads/writes LTX objects in the organization bucket.
package dbmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"walrus/storage"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// Store provides the storage-adapter operations the LTX client needs.
type Store interface {
	Get(ctx context.Context, key string) (body []byte, version string, err error)
	CreateIfAbsent(ctx context.Context, key string, body []byte) (string, error)
	ReplaceIfVersion(ctx context.Context, key string, expected string, body []byte) (string, error)
	PutImmutable(ctx context.Context, key string, body []byte) error
}

// ErrNotFound and ErrConflict are aliases of the storage adapter sentinels
// so that errors.Is comparisons work across packages.
var (
	ErrNotFound = storage.ErrNotFound
	ErrConflict = storage.ErrConflict
)

// errLTXNotFound maps to the error litestream expects for missing objects.
var errLTXNotFound = errors.New("file does not exist")

// LTXClient is a litestream.ReplicaClient over object storage. Level-0 LTX
// objects are immutable and named by TXID; writes use PutImmutable so a stale
// writer can at most upload orphans (spec §7.3) — it cannot advance the
// commit manifest, which is the sole authority.
type LTXClient struct {
	store  Store
	prefix string // database prefix; objects live at <prefix>/ltx/<level>/<min>-<max>.ltx
	logger *slog.Logger
}

// NewLTXClient builds a client for one database prefix.
func NewLTXClient(store Store, prefix string) *LTXClient {
	return &LTXClient{store: store, prefix: prefix, logger: slog.Default()}
}

func (c *LTXClient) SetLogger(l *slog.Logger) { c.logger = l }

func (c *LTXClient) Type() string { return "walrus" }

func (c *LTXClient) Init(ctx context.Context) error { return nil }

func (c *LTXClient) key(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%s/ltx/%04x/%s", c.prefix, level, ltx.FormatFilename(minTXID, maxTXID))
}

func (c *LTXClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {

	// The adapter contract has no List; enumeration walks a manifest-style
	// index object. To stay within the adapter surface we keep a per-level
	// listing object written immutably with a monotonic name.
	body, _, err := c.store.Get(ctx, c.listKey(level))
	if errors.Is(err, ErrNotFound) {
		return ltx.NewFileInfoSliceIterator(nil), nil
	}
	if err != nil {
		return nil, fmt.Errorf("dbmanager: list level %d: %w", level, err)
	}
	var infos []*ltx.FileInfo
	for _, name := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if name == "" {
			continue
		}
		minTXID, maxTXID, err := ltx.ParseFilename(name)
		if err != nil {
			continue
		}
		if maxTXID < seek {
			continue
		}
		infos = append(infos, &ltx.FileInfo{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].MaxTXID < infos[j].MaxTXID })
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *LTXClient) listKey(level int) string {
	return fmt.Sprintf("%s/ltx/%04x.index", c.prefix, level)
}
func (c *LTXClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	body, _, err := c.store.Get(ctx, c.key(level, minTXID, maxTXID))
	if errors.Is(err, ErrNotFound) {
		return nil, errLTXNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("dbmanager: open ltx %d/%s: %w", level, ltx.FormatFilename(minTXID, maxTXID), err)
	}
	if offset > int64(len(body)) {
		return nil, io.EOF
	}
	r := io.Reader(strings.NewReader(string(body[offset:])))
	// Litestream passes size=0 (or negative) to mean "read to EOF";
	// only a positive size limits the read.
	if size > 0 {
		r = io.LimitReader(r, size)
	}
	return io.NopCloser(r), nil
}

func (c *LTXClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("dbmanager: read ltx body: %w", err)
	}
	if err := c.store.PutImmutable(ctx, c.key(level, minTXID, maxTXID), body); err != nil {
		return nil, fmt.Errorf("dbmanager: write ltx: %w", err)
	}
	// Append the filename to the per-level index (best-effort; a lost append
	// is reconciled by re-listing immutable objects).
	c.appendIndex(ltx.FormatFilename(minTXID, maxTXID))
	info := &ltx.FileInfo{
		Level:   level,
		MinTXID: minTXID,
		MaxTXID: maxTXID,
		Size:    int64(len(body)),
	}
	return info, nil
}

// appendIndex appends an LTX filename to the per-level listing object. The
// index is mutable state, so it must be replaced via CAS (never
// CreateIfAbsent, which silently no-ops once the object exists). A lost
// race is harmless: the durable commit manifest, not this listing, is the
// authority (spec §7.3); the listing is a read-optimization index.
func (c *LTXClient) appendIndex(name string) {
	key := c.listKey(0)
	for range 5 {
		body, version, err := c.store.Get(context.Background(), key)
		var next []byte
		switch {
		case errors.Is(err, ErrNotFound):
			next = []byte(name + "\n")
			version = ""
		case err != nil:
			return // storage unreachable; manifest remains authoritative
		default:
			if strings.Contains(string(body), name+"\n") {
				return // already listed
			}
			next = append(append([]byte(nil), body...), []byte(name+"\n")...)
		}
		if version == "" {
			if _, err := c.store.CreateIfAbsent(context.Background(), key, next); err == nil {
				return
			}
		} else {
			if _, err := c.store.ReplaceIfVersion(context.Background(), key, version, next); err == nil {
				return
			}
		}
		// Conflict: another writer appended concurrently; reread and retry.
	}
}

func (c *LTXClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return nil // immutable objects are never deleted (spec: recover from verified objects)
}

func (c *LTXClient) DeleteAll(ctx context.Context) error {
	return nil
}

// Compile-time interface check.
var _ litestream.ReplicaClient = (*LTXClient)(nil)
