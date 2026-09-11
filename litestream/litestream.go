// Package litestream bridges the Go core to Litestream's CGO-backed VFS
// (spec §6, §8). It builds a per-database replica client from a trusted
// storage profile, registers one VFS per database, and exposes the mandatory
// flush barrier: write-mode disable on the SAME VFS file that performed the
// transaction (spec §8, invariant 6).
package litestream

import (
	"fmt"
	"sync"
	"sync/atomic"

	litestream "github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
	"github.com/psanford/sqlite3vfs"
)

// Profile is the storage slice of the trusted DatabaseDescriptor (spec §4,
// §11). It never comes from end users.
type Profile struct {
	Provider        string // "s3" (S3-compatible incl. R2) or "file" (tests/local)
	Endpoint        string
	Region          string
	Bucket          string
	RootPrefix      string
	AccessKeyID     string
	SecretAccessKey string
	// FileRoot is the local filesystem root when Provider == "file".
	FileRoot string
}

// Config controls VFS behavior (spec §16 litestream section).
type Config struct {
	WriteSyncInterval   int64 // ms between background syncs; disable = forced flush
	VFSPageCacheBytes   int
	HydrationEnabled    bool // must be false in normal operation (invariant 11)
	WriteBufferRootPath string
	MaxTempWriteBuffer  int64 // per-process temp write-buffer cap; <= 0 disables
}

// DefaultConfig returns the starting configuration from spec §16.
func DefaultConfig() Config {
	return Config{
		WriteSyncInterval:  1000,
		VFSPageCacheBytes:  10 * 1024 * 1024,
		HydrationEnabled:   false,
		MaxTempWriteBuffer: 268435456,
	}
}

// Bridge owns VFS registration for databases in this process.
type Bridge struct {
	cfg Config

	mu      sync.Mutex
	names   map[string]string // database key -> registered vfs name
	buffers *tempBudget
}

// globalVFSSeq issues process-wide unique VFS names. One process can host
// many Bridges (one WALrusDatabase handle each); per-Bridge counters
// collide on the process-global sqlite3vfs registry and shadow each
// other's databases.
var globalVFSSeq atomic.Uint64

// vfsRegistryMu serializes sqlite3vfs.RegisterVFS writes with SQLite VFS
// callbacks. sqlite3vfs's process-global vfsMap has no mutex of its own.
var (
	vfsRegistryMu    sync.RWMutex
	vfsRegistrations atomic.Uint64
)

// RegisteredVFSCount reports how many process-global sqlite3vfs instances
// this package has registered. Registration is intentionally irreversible;
// the count is used to verify that writes reuse their database VFS.
func RegisteredVFSCount() uint64 { return vfsRegistrations.Load() }

func registerVFS(name string, v sqlite3vfs.VFS) error {
	vfsRegistryMu.Lock()
	defer vfsRegistryMu.Unlock()
	if err := sqlite3vfs.RegisterVFS(name, v); err != nil {
		return err
	}
	vfsRegistrations.Add(1)
	return nil
}

// NewBridge builds a VFS bridge.
func NewBridge(cfg Config) *Bridge {
	if cfg.WriteSyncInterval == 0 {
		cfg = DefaultConfig()
	}
	return &Bridge{
		cfg:     cfg,
		names:   make(map[string]string),
		buffers: &tempBudget{limit: cfg.MaxTempWriteBuffer},
	}
}

// ReplicaClient builds a Litestream replica client rooted at the
// database's replica prefix. Per-tenant credentials come from the trusted
// descriptor only (spec §13).
func (b *Bridge) ReplicaClient(dbKey, replicaPrefix string, p Profile) (litestream.ReplicaClient, error) {
	switch p.Provider {
	case "file":
		// Local filesystem replica: tests and single-node development only.
		root := p.FileRoot
		if root == "" {
			return nil, fmt.Errorf("litestream: file provider requires FileRoot")
		}
		return file.NewReplicaClient(root + "/" + replicaPrefix), nil
	case "s3":
	default:
		return nil, fmt.Errorf("litestream: unsupported provider %q", p.Provider)
	}
	client := s3.NewReplicaClient()
	client.AccessKeyID = p.AccessKeyID
	client.SecretAccessKey = p.SecretAccessKey
	client.Bucket = p.Bucket
	client.Path = replicaPrefix
	client.Region = p.Region
	if p.Endpoint != "" {
		client.Endpoint = p.Endpoint
		client.ForcePathStyle = true
	}
	return client, nil
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 60 {
		out = out[len(out)-60:]
	}
	return string(out)
}
