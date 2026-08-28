// Package ownership implements the ownership and fencing protocol (spec §7):
// object-storage CAS plus a monotonic epoch is the final write authority.
// Consul membership is advisory and never consulted here.
package ownership

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"walrus/storage"
)

// Record is the ownership object stored at ownership/current.json.
// The object version protects the update; Epoch is the durable fence.
type Record struct {
	FormatVersion  int       `json:"format_version"`
	DatabaseID     string    `json:"database_id"`
	Epoch          uint64    `json:"epoch"`
	OwnerWorkerID  string    `json:"owner_worker_id"`
	LeaseID        string    `json:"lease_id"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	IssuedAt       time.Time `json:"issued_at"`
}

// Manager acquires and renews ownership through storage CAS.
type Manager struct {
	adapter  Adapter
	leases   LeaseConfig
	now      func() time.Time
	workerID string
}

// Adapter is the storage subset needed for ownership CAS. Get must return
// storage.ErrNotFound when the object is absent.
type Adapter interface {
	Get(ctx context.Context, key string) (body []byte, version Version, err error)
	CreateIfAbsent(ctx context.Context, key string, body []byte) (Version, error)
	ReplaceIfVersion(ctx context.Context, key string, expected Version, body []byte) (Version, error)
}

// Version is an opaque object version.
type Version = string

// LeaseConfig holds conservative fencing timing.
type LeaseConfig struct {
	LeaseDuration time.Duration // e.g. 30s
	RenewInterval time.Duration // e.g. 10s
	MaxClockSkew  time.Duration // e.g. 2s
	Now           func() time.Time
}

func (c LeaseConfig) validate() error {
	if c.RenewInterval+c.MaxClockSkew >= c.LeaseDuration {
		return fmt.Errorf("ownership: renew_interval + max_clock_skew must be < lease_duration")
	}
	return nil
}

// ErrNotOwner is a retryable not-owner/unavailable response (spec §7.4).
var ErrNotOwner = errors.New("ownership: not the current owner")

// errRecordMismatch means the stored record's database_id does not match:
// a deterministic cross-tenant safety failure, never retryable.
var errRecordMismatch = errors.New("ownership: record database_id mismatch")

// ErrTakeoverPending means the previous lease is not yet takeover-eligible.
var ErrTakeoverPending = errors.New("ownership: previous lease not eligible for takeover")

// New creates a manager for one worker identity.
func New(adapter Adapter, workerID string, cfg LeaseConfig) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Manager{adapter: adapter, leases: cfg, now: cfg.Now, workerID: workerID}, nil
}

// Result of AcquireOrRenew with the object version needed for read-back
// confirmation and later manifest fencing.
type Result struct {
	Record  Record
	Version Version // object version of the confirmed record
}

// ErrNotFound is the not-found sentinel returned by the storage adapter;
// aliased so callers can compare against either name.
var ErrNotFound = storage.ErrNotFound

// ErrConflict is the conditional-write conflict sentinel from the storage
// adapter.
var ErrConflict = storage.ErrConflict

// AcquireOrRenew runs the single-flight acquisition/renewal operation
// (spec §7.2). It is safe to call concurrently; concurrent callers conflict
// at the CAS step, reread, and retry with bounded jitter (spec §7.2).
func (m *Manager) AcquireOrRenew(ctx context.Context, databaseID, key string) (Result, error) {
	const maxAttempts = 4
	var lastErr error = ErrNotOwner
	for attempt := range maxAttempts {
		if attempt > 0 {
			// Bounded jitter before rereading (spec §7.2 takeover_backoff).
			time.Sleep(time.Duration(attempt) * 25 * time.Millisecond)
		}
		res, err := m.acquireOnce(ctx, databaseID, key)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, ErrTakeoverPending) || errors.Is(err, errRecordMismatch) {
			return Result{}, err // deterministic; no point retrying
		}
		lastErr = err // conflicts and lost races: reread and retry
	}
	return Result{}, lastErr
}

// acquireOnce performs one read-modify-CAS cycle against the record.
func (m *Manager) acquireOnce(ctx context.Context, databaseID, key string) (Result, error) {
	body, version, err := m.adapter.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return m.create(ctx, databaseID, key)
	}
	if err != nil {
		return Result{}, err
	}
	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return Result{}, fmt.Errorf("ownership: corrupt record: %w", err)
	}
	if rec.DatabaseID != databaseID {
		return Result{}, fmt.Errorf("%w: %q != %q", errRecordMismatch, rec.DatabaseID, databaseID)
	}

	now := m.now()
	// Case 4: same worker, lease still valid → renew without epoch bump.
	if rec.OwnerWorkerID == m.workerID && rec.LeaseExpiresAt.After(now.Add(m.leases.MaxClockSkew)) {
		rec.LeaseExpiresAt = now.Add(m.leases.LeaseDuration)
		return m.writeBack(ctx, key, version, rec)
	}
	// Case 5: takeover — only after the old lease has expired (with skew).
	if rec.LeaseExpiresAt.After(now.Add(-m.leases.MaxClockSkew)) {
		return Result{}, ErrTakeoverPending
	}
	rec.Epoch++
	rec.OwnerWorkerID = m.workerID
	rec.LeaseID = newLeaseID()
	rec.IssuedAt = now
	rec.LeaseExpiresAt = now.Add(m.leases.LeaseDuration)
	return m.writeBack(ctx, key, version, rec)
}

func (m *Manager) create(ctx context.Context, databaseID, key string) (Result, error) {
	now := m.now()
	rec := Record{
		FormatVersion:  1,
		DatabaseID:     databaseID,
		Epoch:          1,
		OwnerWorkerID:  m.workerID,
		LeaseID:        newLeaseID(),
		IssuedAt:       now,
		LeaseExpiresAt: now.Add(m.leases.LeaseDuration),
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return Result{}, err
	}
	version, err := m.adapter.CreateIfAbsent(ctx, key, body)
	if errors.Is(err, ErrConflict) || errors.Is(err, storage.ErrAlreadyExists) {
		// Lost a concurrent create: the reread loop will pick up the winner
		// and take the renew-or-takeover path (spec §7.2).
		return Result{}, ErrNotOwner
	}
	if err != nil {
		return Result{}, err
	}
	// Step 6: read-back confirmation before opening write mode.
	return m.readBack(ctx, key, version, rec)
}

func (m *Manager) writeBack(ctx context.Context, key string, version Version, rec Record) (Result, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return Result{}, err
	}
	newVersion, err := m.adapter.ReplaceIfVersion(ctx, key, version, body)
	if errors.Is(err, ErrConflict) {
		// Never overwrite a newer record (spec §7.2): discard authority.
		return Result{}, ErrNotOwner
	}
	if err != nil {
		return Result{}, err
	}
	return m.readBack(ctx, key, newVersion, rec)
}

// readBack re-fetches and confirms contents/version before returning authority.
func (m *Manager) readBack(ctx context.Context, key string, version Version, want Record) (Result, error) {
	body, gotVersion, err := m.adapter.Get(ctx, key)
	if err != nil {
		return Result{}, err
	}
	var got Record
	if err := json.Unmarshal(body, &got); err != nil {
		return Result{}, fmt.Errorf("ownership: read-back corrupt: %w", err)
	}
	if gotVersion != version || stripMono(got) != stripMono(want) {
		return Result{}, ErrNotOwner
	}
	return Result{Record: got, Version: version}, nil
}

// stripMono removes monotonic clock readings and normalizes to UTC so
// struct equality compares wall-clock instants regardless of the Local zone
// the process was started with. distroless containers ship no tzdata, so a
// record written in one environment parses with a different Location in
// another; time.Time == compares Location too.
func stripMono(r Record) Record {
	r.IssuedAt = r.IssuedAt.Round(0).UTC()
	r.LeaseExpiresAt = r.LeaseExpiresAt.Round(0).UTC()
	return r
}

// Valid reports whether rec still grants authority to this worker at time now,
// guarding the "stop before the lease safety window ends" rule (§7.2).
func (m *Manager) Valid(rec Record) bool {
	return rec.OwnerWorkerID == m.workerID &&
		rec.LeaseExpiresAt.After(m.now().Add(m.leases.MaxClockSkew))
}

func newLeaseID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("ownership: entropy unavailable: %w", err))
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}
