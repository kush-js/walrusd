// Package lease implements the Redis/Valkey-backed lease (spec §7): CAS
// acquisition, release, expiry takeover, and bounded jittered retry. The
// store's fencing token is the CAS authority; epoch is
// diagnostics/generation bookkeeping only.
package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"walrusd/identity"
	"walrusd/walrusderr"
)

// FormatVersion is the lease record format (spec §7.1).
const FormatVersion = 1

// State is the lease state.
type State string

const (
	StateHeld     State = "held"
	StateReleased State = "released"
)

// Record is the lease.json object (spec §7.1).
type Record struct {
	FormatVersion int       `json:"format_version"`
	DatabaseID    string    `json:"database_id"`
	State         State     `json:"state"`
	Owner         string    `json:"owner"`
	LeaseID       string    `json:"lease_id"`
	Epoch         uint64    `json:"epoch"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// Config controls lease timing (spec §7.4).
type Config struct {
	Duration           time.Duration // 30s
	ClockSkewAllowance time.Duration // 2s
	AcquireRetryBudget time.Duration // 3s
	RetryBackoffMin    time.Duration // 25ms
	RetryBackoffMax    time.Duration // 500ms
	// Now overrides the clock for tests.
	Now func() time.Time
}

// DefaultConfig returns the starting configuration from spec §7.4.
func DefaultConfig() Config {
	return Config{
		Duration:           30 * time.Second,
		ClockSkewAllowance: 2 * time.Second,
		AcquireRetryBudget: 3 * time.Second,
		RetryBackoffMin:    25 * time.Millisecond,
		RetryBackoffMax:    500 * time.Millisecond,
	}
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Manager acquires and releases leases through store CAS.
type Manager struct {
	store Store
	cfg   Config
	owner string
	keys  keyFunc

	// singleFlight serializes acquisition for the same database ID inside
	// one process (spec §7.2).
	flightMu sync.Mutex
	flights  map[string]*sync.Mutex
}

// keyFunc derives the lease key for a database.
type keyFunc func(d identity.DatabaseID) string

// NewManager builds a lease manager. owner is a unique API-process instance
// ID used for diagnostics only (spec §7.1). keyFn derives the lease key;
// nil uses the canonical identity.LeaseKey with the root prefix passed to
// Acquire.
func NewManager(store Store, owner string, cfg Config, keyFn keyFunc) *Manager {
	if cfg.Duration == 0 {
		cfg = DefaultConfig()
	}
	return &Manager{
		store:   store,
		cfg:     cfg,
		owner:   owner,
		keys:    keyFn,
		flights: make(map[string]*sync.Mutex),
	}
}

// Held is a successfully acquired lease. The internal version is the
// fencing token used for every conditional replace: renewal rotates it, so
// callers must go through Manager.Release/Abandon rather than reading it.
type Held struct {
	key string

	mu      sync.Mutex
	record  Record
	version string
	lost    bool

	stop      chan struct{} // closed to end the renewal loop
	done      chan struct{} // closed when the renewal loop has exited
	maxExpiry time.Time     // hard cap on renewal; beyond this the lease expires
}

// LeaseID returns the acquired lease ID.
func (h *Held) LeaseID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.record.LeaseID
}

// Epoch returns the ownership generation.
func (h *Held) Epoch() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.record.Epoch
}

// ExpiresAt returns the current logical expiry, extended by renewal.
func (h *Held) ExpiresAt() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.record.ExpiresAt
}

// Lost reports whether renewal observed a lost lease (conflict or store
// failure). A lost lease must never flush or release.
func (h *Held) Lost() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lost
}

// snapshot returns the current record and fencing token.
func (h *Held) snapshot() (Record, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.record, h.version
}

// markLost flags the lease as no longer provably ours and ends renewal.
func (h *Held) markLost() {
	h.mu.Lock()
	h.lost = true
	h.mu.Unlock()
}

// Acquire acquires the lease for db before any SQLite write begins
// (spec §7.2). It is single-flight per database ID inside this process and
// bounded by cfg.AcquireRetryBudget; on timeout it returns ClassBusy.
// AcquireKey acquires a lease stored under an explicit key. It is used for
// auxiliary locks (for example a database's compaction lock) that must not
// collide with the write lease.
func (m *Manager) AcquireKey(ctx context.Context, db identity.DatabaseID, key string) (*Held, error) {
	if key == "" {
		return nil, errors.New("lease: key is required")
	}
	return m.acquireSingleFlight(ctx, db, key)
}

func (m *Manager) Acquire(ctx context.Context, db identity.DatabaseID, rootPrefix ...string) (*Held, error) {
	key := ""
	if m.keys != nil {
		key = m.keys(db)
	} else {
		prefix := ""
		if len(rootPrefix) > 0 {
			prefix = rootPrefix[0]
		}
		key = db.LeaseKey(prefix)
	}
	return m.acquireSingleFlight(ctx, db, key)
}

func (m *Manager) acquireSingleFlight(ctx context.Context, db identity.DatabaseID, key string) (*Held, error) {
	m.flightMu.Lock()
	fl, ok := m.flights[key]
	if !ok {
		fl = &sync.Mutex{}
		m.flights[key] = fl
	}
	m.flightMu.Unlock()

	fl.Lock()
	held, err := m.acquireLocked(ctx, db, key)
	fl.Unlock()
	// Drop the per-key mutex once uncontended. Acquisition correctness
	// lives in the store CAS, so a later caller creating a fresh mutex can
	// at worst contend twice inside one process — the map stays bounded by
	// in-flight acquisitions, not by databases ever seen.
	m.flightMu.Lock()
	if m.flights[key] == fl {
		delete(m.flights, key)
	}
	m.flightMu.Unlock()
	return held, err
}

func (m *Manager) acquireLocked(ctx context.Context, db identity.DatabaseID, key string) (*Held, error) {
	start := time.Now() // monotonic; the budget must not depend on cfg.Now
	var lastErr error
	for {
		held, err := m.acquireOnce(ctx, db, key)
		if err == nil {
			return held, nil
		}
		if !errors.Is(err, errRetry) {
			return nil, err
		}
		lastErr = err
		if time.Since(start) > m.cfg.AcquireRetryBudget || ctx.Err() != nil {
			break
		}
		// Bounded, jittered backoff (spec §7.4: 25ms..500ms).
		select {
		case <-time.After(jitter(m.cfg.RetryBackoffMin, m.cfg.RetryBackoffMax)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	_ = lastErr
	return nil, walrusderr.Busy("lease held by another owner", int64(m.cfg.RetryBackoffMax/time.Millisecond))
}

// errRetry signals "reread and retry" (CAS conflict, busy lease, lost race).
var errRetry = errors.New("lease: retry")

func (m *Manager) acquireOnce(ctx context.Context, db identity.DatabaseID, key string) (*Held, error) {
	body, version, err := m.store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		rec := m.newRecord(db, 0)
		rec.Epoch = 1
		return m.create(ctx, key, rec)
	}
	if err != nil {
		return nil, walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "read lease", err)
	}
	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, walrusderr.Wrap(walrusderr.ClassConfigurationInvalid, "corrupt lease record", err)
	}
	if rec.DatabaseID != db.String() {
		return nil, walrusderr.New(walrusderr.ClassConfigurationInvalid, "lease record database_id mismatch")
	}

	now := m.cfg.now()
	safelyExpired := rec.ExpiresAt.Before(now.Add(-m.cfg.ClockSkewAllowance))
	if rec.State == StateHeld && !safelyExpired {
		return nil, fmt.Errorf("%w: held and unexpired", errRetry)
	}

	// Present and (released or safely expired): conditional replace with a
	// new lease ID and epoch+1 (spec §7.2 step 3).
	next := m.newRecord(db, rec.Epoch)
	return m.replace(ctx, key, version, next)
}

func (m *Manager) newRecord(db identity.DatabaseID, prevEpoch uint64) Record {
	now := m.cfg.now()
	return Record{
		FormatVersion: FormatVersion,
		DatabaseID:    db.String(),
		State:         StateHeld,
		Owner:         m.owner,
		LeaseID:       NewID(),
		Epoch:         prevEpoch + 1,
		ExpiresAt:     now.Add(m.cfg.Duration),
	}
}

func (m *Manager) create(ctx context.Context, key string, rec Record) (*Held, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	version, err := m.store.CreateIfAbsent(ctx, key, body)
	if errors.Is(err, ErrAlreadyExists) || errors.Is(err, ErrConflict) {
		// Lost a concurrent create: reread and retry (spec §7.2 step 5).
		return nil, fmt.Errorf("%w: lost create race", errRetry)
	}
	if err != nil {
		return nil, walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "create lease", err)
	}
	return m.newHeld(key, rec, version), nil
}

func (m *Manager) replace(ctx context.Context, key, expectedVersion string, rec Record) (*Held, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	version, err := m.store.ReplaceIfToken(ctx, key, expectedVersion, body)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		// Never overwrite without the retrieved token (spec §7.2 step 5).
		return nil, fmt.Errorf("%w: CAS conflict on acquire", errRetry)
	}
	if err != nil {
		return nil, walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "replace lease", err)
	}
	return m.newHeld(key, rec, version), nil
}

// newHeld wraps an acquired record with its renewal loop.
func (m *Manager) newHeld(key string, rec Record, version string) *Held {
	held := &Held{key: key, record: rec, version: version, maxExpiry: rec.ExpiresAt.Add(2 * m.cfg.Duration)}
	held.stop = make(chan struct{})
	held.done = make(chan struct{})
	go m.renewLoop(held)
	return held
}

// renewLoop heartbeats the lease while it is held. Renewal is what keeps a
// slow-but-correct transaction or flush from outliving its lease and
// letting a successor fork the LTX chain (spec §7.4): the fence stays valid
// as long as this process is alive and the store is reachable. Renewal
// stops at maxExpiry so a holder wedged in an uncancellable call still
// loses the lease eventually — takeover-after-expiry is the crash safety
// net and must survive a stuck-but-alive process. The loop also ends at
// Release/Abandon, or when renewal fails — after which the lease expires on
// its own and Held.Lost reports that it is no longer ours.
func (m *Manager) renewLoop(held *Held) {
	defer close(held.done)
	interval := m.cfg.Duration / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-held.stop:
			return
		case <-ticker.C:
		}
		rec, version := held.snapshot()
		next := m.cfg.now().Add(m.cfg.Duration)
		if next.After(held.maxExpiry) {
			return // hold window exhausted; let the lease expire
		}
		rec.ExpiresAt = next
		body, err := json.Marshal(rec)
		if err != nil {
			held.markLost()
			return
		}
		newVersion, err := m.store.ReplaceIfToken(context.Background(), held.key, version, body)
		if err != nil {
			held.markLost()
			return
		}
		held.mu.Lock()
		held.record.ExpiresAt = rec.ExpiresAt
		held.version = newVersion
		held.mu.Unlock()
	}
}

// stopRenewal ends the renewal loop and waits for any in-flight renewal so
// the fencing token is stable afterwards.
func (m *Manager) stopRenewal(held *Held) {
	if held.stop == nil {
		return
	}
	select {
	case <-held.stop:
	default:
		close(held.stop)
	}
	<-held.done
}

// Release conditionally replaces the lease with state=released using the
// fencing token held by the lease owner (spec §7.3). It keeps the record
// and its epoch history. A conflict (or a vanished key, e.g. store data
// loss) returns ClassLeaseConflict: never ack on a token that is not
// provably still ours — the caller retries idempotently.
func (m *Manager) Release(ctx context.Context, held *Held) error {
	if held == nil {
		return nil
	}
	m.stopRenewal(held)
	rec, version := held.snapshot()
	rec.State = StateReleased
	rec.ExpiresAt = m.cfg.now() // released leases are immediately takeable
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = m.store.ReplaceIfToken(ctx, held.key, version, body)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return walrusderr.New(walrusderr.ClassLeaseConflict, "stale lease release (successor owns lease)")
	}
	if err != nil {
		return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "release lease", err)
	}
	return nil
}

// Abandon ends renewal without touching the store: the lease is left to
// expire on its own. Use on paths that must not release (a flush that was
// not confirmed may have forked the chain; releasing would hand the lease
// to a successor while our upload is still ambiguous).
func (m *Manager) Abandon(held *Held) {
	if held == nil {
		return
	}
	m.stopRenewal(held)
}

// jitter returns a random duration in [min, max].
func jitter(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	span := int64(max - min)
	n, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return min
	}
	return min + time.Duration(n.Int64())
}

// NewID returns a random UUID (spec §7.1: lease_id is a random UUID per
// successful acquisition).
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("lease: entropy unavailable: %w", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
