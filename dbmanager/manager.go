//go:build vfs
// +build vfs

// DatabaseManager: lazy per-database opens, bounded hot registry, idle and
// pressure eviction, and the durable write path (spec §8, §10).
package dbmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

// Config bounds per-worker database state (spec §14 defaults).
type Config struct {
	IdleTTL       time.Duration // DB_IDLE_TTL, default 60s
	MaxHotDBs     int           // max_hot_databases, default 500
	VFSPageCache  int           // vfs_page_cache_bytes, default 10MiB
	WriteSyncIval time.Duration // VFS write sync interval, default 200ms
	BufferPath    string        // temp write-buffer dir; empty = os temp
}

func (c Config) withDefaults() Config {
	if c.IdleTTL == 0 {
		c.IdleTTL = 60 * time.Second
	}
	if c.MaxHotDBs == 0 {
		c.MaxHotDBs = 500
	}
	if c.VFSPageCache == 0 {
		c.VFSPageCache = 10 * 1024 * 1024
	}
	if c.WriteSyncIval == 0 {
		c.WriteSyncIval = 200 * time.Millisecond
	}
	return c
}

// Fencer is the authority interface the writer supplies: it acquires/renews
// ownership and returns the current (epoch, lease) for a database.
type Fencer interface {
	// Authorize returns the valid ownership epoch/lease or an error.
	Authorize(ctx context.Context, databaseID string) (epoch uint64, leaseID string, err error)
	// Demote is called when fencing is lost mid-write.
	Demote(databaseID string)
}

// Committer advances the commit manifest (the commit package Coordinator).
type Committer interface {
	Commit(ctx context.Context, databaseID string, epoch uint64, leaseID, writerID string, objs []Object) (commitSeq uint64, err error)
}

// Object identifies one immutable LTX object in a manifest.
type Object struct {
	MinTXID  ltx.TXID
	MaxTXID  ltx.TXID
	Checksum string
}

// hot is one open database instance (spec §8 hot state).
type hot struct {
	dbID         string
	vfsName      string
	client       *LTXClient
	vfs          *litestream.VFS
	lastUsed     time.Time
	draining     bool
	mu           sync.Mutex
	open         bool
	file         *litestream.VFSFile
	lastSyncedAt time.Time
	lastMin      ltx.TXID
	lastMax      ltx.TXID
}

// Manager owns hot databases for one writer.
type Manager struct {
	cfg       Config
	store     Store
	workerID  string
	fencer    Fencer
	committer Committer
	logger    *slog.Logger

	mu    sync.Mutex
	hotDB map[string]*hot
	idem  *idempotency
	// opening serializes per-database opens (single-flight, spec §8).
	opening map[string]*sync.Once
}

// NewManager builds the manager. store must be a CAS-capable adapter.
func NewManager(store Store, workerID string, fencer Fencer, committer Committer, cfg Config) *Manager {
	cfg = cfg.withDefaults()
	return &Manager{
		cfg:       cfg,
		store:     store,
		workerID:  workerID,
		fencer:    fencer,
		committer: committer,
		logger:    slog.Default(),
		hotDB:     map[string]*hot{},
		idem:      newIdempotency(),
		opening:   map[string]*sync.Once{},
	}
}

// ErrNotWritable is returned for writes without a valid hot writable instance.
var ErrNotWritable = errors.New("dbmanager: database not writable here")

// OpenWritable lazily opens a database in write mode (spec §8 opening):
// authorize ownership first, then open the VFS against the database prefix.
// The VFS name is unique per database; registration is idempotent via
// single-flight.
func (m *Manager) OpenWritable(ctx context.Context, dbID, prefix string) error {
	m.mu.Lock()
	once, ok := m.opening[dbID]
	if !ok {
		once = &sync.Once{}
		m.opening[dbID] = once
	}
	m.mu.Unlock()

	var openErr error
	once.Do(func() {
		openErr = m.openWritable(ctx, dbID, prefix)
		m.mu.Lock()
		delete(m.opening, dbID)
		m.mu.Unlock()
	})
	return openErr
}

func (m *Manager) openWritable(ctx context.Context, dbID, prefix string) error {
	m.mu.Lock()
	if h, ok := m.hotDB[dbID]; ok && !h.isDraining() {
		h.lastUsed = time.Now()
		m.mu.Unlock()
		return nil
	}
	if len(m.hotDB) >= m.cfg.MaxHotDBs {
		m.mu.Unlock()
		m.evictForPressure()
		m.mu.Lock()
	}
	m.mu.Unlock()

	// Ownership is authorization (spec §6.3): routing never suffices.
	epoch, leaseID, err := m.fencer.Authorize(ctx, dbID)
	if err != nil {
		return fmt.Errorf("dbmanager: open %s: %w", dbID, err)
	}
	_ = epoch
	_ = leaseID

	client := NewLTXClient(m.store, prefix)
	vfs := litestream.NewVFS(client, m.logger)
	vfs.PollInterval = time.Second
	vfs.CacheSize = m.cfg.VFSPageCache
	vfs.WriteEnabled = true
	vfs.WriteSyncInterval = m.cfg.WriteSyncIval
	if m.cfg.BufferPath != "" {
		vfs.WriteBufferPath = m.cfg.BufferPath
	}
	vfs.HydrationEnabled = false // never full-hydrate (spec §8)

	vfsName := "walrus_" + sanitize(dbID)
	if err := sqlite3vfs.RegisterVFS(vfsName, vfs); err != nil {
		return fmt.Errorf("dbmanager: register vfs %s: %w", vfsName, err)
	}

	m.mu.Lock()
	m.hotDB[dbID] = &hot{dbID: dbID, vfsName: vfsName, client: client, vfs: vfs, lastUsed: time.Now()}
	m.mu.Unlock()
	return nil
}

// DSN returns the SQLite DSN for an opened writable database.
func (m *Manager) DSN(dbID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hotDB[dbID]
	if !ok {
		return "", ErrNotWritable
	}
	h.lastUsed = time.Now()
	return fmt.Sprintf("file:%s.db?vfs=%s", sanitize(dbID), h.vfsName), nil
}

// AttachDB binds the VFS file created by the first SQLite connection for
// dbID to the hot instance. Call it immediately after sql.Open succeeds; the
// captured handle is what ExecuteWrite drives for durable flushes.
func (m *Manager) AttachDB(dbID string) {
	m.mu.Lock()
	h := m.hotDB[dbID]
	m.mu.Unlock()
	if h == nil {
		return
	}
	if f := captureVFSFile(""); f != nil {
		h.attachFile(f)
	}
}

// EvictLoop runs until ctx is cancelled, evicting idle databases.
func (m *Manager) EvictLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.evictIdle()
		}
	}
}

func (m *Manager) evictIdle() {
	m.mu.Lock()
	var victims []string
	now := time.Now()
	for id, h := range m.hotDB {
		if now.Sub(h.lastUsed) > m.cfg.IdleTTL {
			victims = append(victims, id)
		}
	}
	m.mu.Unlock()
	for _, id := range victims {
		m.close(id)
	}
}

func (m *Manager) evictForPressure() {
	m.mu.Lock()
	var oldest string
	var oldestTime time.Time
	for id, h := range m.hotDB {
		if oldest == "" || h.lastUsed.Before(oldestTime) {
			oldest = id
			oldestTime = h.lastUsed
		}
	}
	m.mu.Unlock()
	if oldest != "" {
		m.close(oldest)
	}
}

// close demotes and closes one hot instance (spec §8 eviction steps).
func (m *Manager) close(dbID string) {
	m.mu.Lock()
	delete(m.hotDB, dbID)
	m.mu.Unlock()
	m.fencer.Demote(dbID)
	// Explicit release is best-effort; lease expiry is acceptable (spec §8).
}

func sanitize(dbID string) string {
	out := make([]byte, 0, len(dbID))
	for i := 0; i < len(dbID); i++ {
		c := dbID[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

// get returns the hot instance or nil.
func (m *Manager) get(dbID string) *hot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hotDB[dbID]
}

// demote removes the instance and notifies the fencer (spec §7.4).
func (m *Manager) demote(dbID string) {
	m.mu.Lock()
	delete(m.hotDB, dbID)
	m.mu.Unlock()
	m.fencer.Demote(dbID)
}
