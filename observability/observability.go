// Package observability implements the privacy-safe metrics surface (spec
// §14). Counters are process-local and keyed by database HASH — never by
// database ID, credentials, or SQL text. Exporters pull the snapshot; the
// runtime never pushes tenant-identifying data anywhere.
package observability

import (
	"hash/fnv"
	"sync"
)

// Metrics is the walrusd counter set (spec §14 required signals). All fields
// are monotonically increasing counts except the gauges.
type Metrics struct {
	// Lease signals.
	LeaseAcquireAttempts  uint64
	LeaseAcquireSucceeded uint64
	LeaseCASConflicts     uint64
	LeaseBusyRetries      uint64
	LeaseExpiryTakeovers  uint64
	LeaseReleased         uint64
	LeaseReleaseConflicts uint64

	// Write-path signals.
	WriteTransactions        uint64
	WriteTransactionFailures uint64
	FlushSuccesses           uint64
	FlushFailures            uint64
	FlushConflictErrors      uint64
	FlushDurationMicros      uint64 // cumulative; divide by FlushSuccesses

	// Idempotency signals.
	IdempotencyDedupeHits        uint64
	IdempotencyAmbiguousOutcomes uint64

	// Read-path signals.
	ReadOpens          uint64
	ReadCacheHits      uint64
	ReadCacheEvictions uint64
	ReadFailures       uint64

	// Gauges.
	ActiveReadInstances uint64
}

// Registry records metrics keyed by a privacy-safe database hash (spec §14).
type Registry struct {
	mu      sync.Mutex
	metrics map[uint64]*Metrics
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{metrics: make(map[uint64]*Metrics)}
}

// Hash derives the privacy-safe database key: FNV-1a of the database ID.
// Database IDs themselves are never stored (spec §14).
func Hash(databaseID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(databaseID))
	return h.Sum64()
}

// For returns (creating if needed) the metrics bucket for a database hash.
func (r *Registry) For(databaseID string) *Metrics {
	key := Hash(databaseID)
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metrics[key]
	if !ok {
		m = &Metrics{}
		r.metrics[key] = m
	}
	return m
}

// Record applies fn to the metrics bucket for databaseID under the
// registry lock, making compound read-modify-write updates atomic.
func (r *Registry) Record(databaseID string, fn func(*Metrics)) {
	key := Hash(databaseID)
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metrics[key]
	if !ok {
		m = &Metrics{}
		r.metrics[key] = m
	}
	fn(m)
}

// Snapshot copies the current per-hash metrics. The map is keyed by the
// privacy-safe hash (spec §14): organization IDs are not included.
func (r *Registry) Snapshot() map[uint64]Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint64]Metrics, len(r.metrics))
	for k, v := range r.metrics {
		out[k] = *v
	}
	return out
}

// Total sums every bucket (process-wide rollup).
func (r *Registry) Total() Metrics {
	var total Metrics
	for _, m := range r.Snapshot() {
		total.LeaseAcquireAttempts += m.LeaseAcquireAttempts
		total.LeaseAcquireSucceeded += m.LeaseAcquireSucceeded
		total.LeaseCASConflicts += m.LeaseCASConflicts
		total.LeaseBusyRetries += m.LeaseBusyRetries
		total.LeaseExpiryTakeovers += m.LeaseExpiryTakeovers
		total.LeaseReleased += m.LeaseReleased
		total.LeaseReleaseConflicts += m.LeaseReleaseConflicts
		total.WriteTransactions += m.WriteTransactions
		total.WriteTransactionFailures += m.WriteTransactionFailures
		total.FlushSuccesses += m.FlushSuccesses
		total.FlushFailures += m.FlushFailures
		total.FlushConflictErrors += m.FlushConflictErrors
		total.FlushDurationMicros += m.FlushDurationMicros
		total.IdempotencyDedupeHits += m.IdempotencyDedupeHits
		total.IdempotencyAmbiguousOutcomes += m.IdempotencyAmbiguousOutcomes
		total.ReadOpens += m.ReadOpens
		total.ReadCacheHits += m.ReadCacheHits
		total.ReadCacheEvictions += m.ReadCacheEvictions
		total.ReadFailures += m.ReadFailures
		total.ActiveReadInstances += m.ActiveReadInstances
	}
	return total
}
