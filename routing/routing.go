// Package routing implements API-local rendezvous (HRW) routing (spec §6.2)
// with the canonical sha256-hrw-v1 algorithm shared by all implementations.
package routing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Writer is one eligible member of a routing snapshot.
type Writer struct {
	WorkerID string
	Endpoint string
	Weight   int // >=1; 1 = uniform
}

// Snapshot is an immutable membership view captured at request start.
type Snapshot struct {
	Generation uint64
	Writers    []Writer // already filtered to healthy/compatible/non-draining
}

// salt is the canonical seed for sha256-hrw-v1. All implementations must use
// the same byte encoding: HMAC-SHA256(key=salt, msg=generation(8 BE) ||
// database_id || 0x00 || worker_id), weighted by ceil(score * weight).
var salt = []byte("basemnt-hrw-v1")

// Select returns the highest-scoring eligible writer and the generation used.
func Select(snap *Snapshot, databaseID string) (*Writer, uint64, error) {
	if snap == nil || len(snap.Writers) == 0 {
		return nil, snap.Generation, fmt.Errorf("routing: no eligible writers")
	}
	var best *Writer
	var bestScore uint64
	for i := range snap.Writers {
		w := &snap.Writers[i]
		s := score(databaseID, w.WorkerID, snap.Generation)
		// Weighted rendezvous: replicate weight by scoring against
		// worker_id#wN so higher-weight writers win proportionally more
		// often without a modulo scheme.
		weight := w.Weight
		if weight < 1 {
			weight = 1
		}
		for n := range weight {
			s = score(databaseID, fmt.Sprintf("%s#%d", w.WorkerID, n), snap.Generation)
			if s > bestScore || best == nil {
				bestScore = s
				best = w
			}
		}
	}
	if best == nil {
		return nil, snap.Generation, fmt.Errorf("routing: no eligible writers")
	}
	return best, snap.Generation, nil
}

func score(databaseID, workerID string, generation uint64) uint64 {
	h := hmac.New(sha256.New, salt)
	var gen [8]byte
	binary.BigEndian.PutUint64(gen[:], generation)
	h.Write(gen[:])
	h.Write([]byte(databaseID))
	h.Write([]byte{0})
	h.Write([]byte(workerID))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// SortWriters gives snapshots a stable canonical order.
func SortWriters(w []Writer) {
	sort.Slice(w, func(i, j int) bool { return w[i].WorkerID < w[j].WorkerID })
}
