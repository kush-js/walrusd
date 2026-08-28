// Package commit implements the durable commit coordinator (spec §7.3): the
// only component authorized to advance commits/current.json. A transaction is
// durable only after the epoch-aware CAS on the current manifest succeeds.
package commit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"walrus/storage"
)

// Manifest is the CAS-protected current commit manifest.
type Manifest struct {
	FormatVersion int             `json:"format_version"`
	DatabaseID    string          `json:"database_id"`
	Epoch         uint64          `json:"epoch"`
	CommitSeq     uint64          `json:"commit_sequence"`
	PrevVersion   string          `json:"previous_manifest_version"`
	Objects       []ObjectPointer `json:"objects"`
	WriterID      string          `json:"writer_worker_id"`
	LeaseID       string          `json:"lease_id"`
	IssuedAt      time.Time       `json:"issued_at"`
}

// ObjectPointer names one immutable LTX/state object with its checksum.
type ObjectPointer struct {
	Key      string `json:"key"`
	Checksum string `json:"checksum"` // sha256 hex
	MinTXID  uint64 `json:"min_txid"`
	MaxTXID  uint64 `json:"max_txid"`
}

// ErrConflict is the conditional-write conflict sentinel from the storage
// adapter.
var ErrConflict = storage.ErrConflict

// ErrNotFound is the not-found sentinel returned by the storage adapter.
var ErrNotFound = storage.ErrNotFound

// Adapter is the storage subset needed for manifest CAS. CreateIfAbsent
// initializes the manifest of a fresh database; ReplaceIfVersion advances it.
type Adapter interface {
	Get(ctx context.Context, key string) (body []byte, version Version, err error)
	CreateIfAbsent(ctx context.Context, key string, body []byte) (Version, error)
	ReplaceIfVersion(ctx context.Context, key string, expected Version, body []byte) (Version, error)
}

// Version is an opaque object version.
type Version = string

// ErrFencingLost means the manifest CAS failed: another epoch advanced the
// manifest. The old writer must demote and reconcile (spec §7.3).
var ErrFencingLost = errors.New("commit: manifest CAS conflict; authority lost")

// ErrStaleEpoch means the caller's epoch is behind the current manifest.
var ErrStaleEpoch = errors.New("commit: stale epoch cannot advance manifest")

// New builds a coordinator.
func New(adapter Adapter) *Coordinator { return &Coordinator{adapter: adapter} }

// Current reads the current manifest and its object version.
// A missing manifest is a fresh database: returns (nil, "", nil).
func (c *Coordinator) Current(ctx context.Context, key string) (*Manifest, Version, error) {
	body, version, err := c.adapter.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, version, fmt.Errorf("commit: corrupt manifest: %w", err)
	}
	return &m, version, nil
}

// Coordinator advances manifests via storage CAS.
type Coordinator struct {
	adapter Adapter
}

// CommitInput describes one durable transaction.
type CommitInput struct {
	DatabaseID string
	Key        string // commits/current.json object key
	Epoch      uint64 // fencing epoch from the ownership record
	LeaseID    string
	WriterID   string
	Objects    []ObjectPointer
}

// Commit atomically advances the manifest. Never acknowledges before the CAS
// succeeds; a stale writer can upload orphan objects but cannot advance the
// manifest because ReplaceIfVersion requires both the current object version
// and an epoch not behind the stored manifest.
func (c *Coordinator) Commit(ctx context.Context, in CommitInput) (Manifest, Version, error) {
	current, version, err := c.Current(ctx, in.Key)
	if err != nil {
		return Manifest{}, "", err
	}
	if current != nil {
		if in.Epoch < current.Epoch {
			return Manifest{}, "", ErrStaleEpoch
		}
		if current.Epoch == in.Epoch && current.WriterID != in.WriterID {
			// Same epoch, different writer: a takeover that renewed without
			// bumping is fenced by the lease/ownership check upstream; here
			// treat as conflict to stay safe.
			return Manifest{}, "", ErrFencingLost
		}
	}

	var prevVersion Version
	var prevSeq uint64
	if current != nil {
		prevVersion = version
		prevSeq = current.CommitSeq
		if version == "" {
			return Manifest{}, "", fmt.Errorf("commit: current manifest missing object version")
		}
	} else {
		prevVersion = ""
	}

	next := Manifest{
		FormatVersion: 1,
		DatabaseID:    in.DatabaseID,
		Epoch:         in.Epoch,
		CommitSeq:     prevSeq + 1,
		PrevVersion:   string(prevVersion),
		Objects:       in.Objects,
		WriterID:      in.WriterID,
		LeaseID:       in.LeaseID,
		IssuedAt:      time.Now().UTC(),
	}
	body, err := json.Marshal(next)
	if err != nil {
		return Manifest{}, "", err
	}

	if version == "" {
		// Fresh database: create the manifest conditionally so two writers
		// racing to initialize cannot both succeed (spec §7.3).
		newVersion, err := c.adapter.CreateIfAbsent(ctx, in.Key, body)
		if errors.Is(err, ErrConflict) {
			return Manifest{}, "", ErrFencingLost
		}
		if err != nil {
			return Manifest{}, "", err
		}
		return next, newVersion, nil
	}
	newVersion, err := c.adapter.ReplaceIfVersion(ctx, in.Key, version, body)
	if errors.Is(err, ErrConflict) {
		return Manifest{}, "", ErrFencingLost
	}
	if err != nil {
		return Manifest{}, "", err
	}
	return next, newVersion, nil
}

// Checksum returns the sha256 hex checksum required for object pointers
// (spec §14: immutable_object_checksum = sha256).
func Checksum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
