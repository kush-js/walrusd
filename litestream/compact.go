package litestream

import (
	"context"
	"errors"
	"fmt"
	"time"

	ls "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// compactionTopLevel is the highest level compaction writes to. Level 9 is
// Litestream's snapshot level: a file there is treated as a full database
// image, and a merge of deltas must never be written as one. Levels 1..8 are
// ordinary contiguous deltas, which is exactly what ltx.Compactor produces.
const compactionTopLevel = 8

// Compact merges the replica chain down to at most `threshold` files per
// level, starting at level 0 and cascading upward. It must be called while
// holding the database's write lease: it rewrites the chain that the next
// writer will append to.
//
// Merged source files are deleted only after `retention`, so a read that
// started before the compaction can finish against the files it planned on.
// Returns the number of levels merged.
func (d *Database) Compact(ctx context.Context, threshold int, retention time.Duration) (int, error) {
	if threshold <= 0 {
		threshold = 1
	}
	if invalidator, ok := d.client.(interface{ DropListings() }); ok {
		invalidator.DropListings()
	}

	compactor := ls.NewCompactor(d.client, noOpLogger())
	merged := 0
	for level := 1; level <= compactionTopLevel; level++ {
		count, err := countLTXFiles(ctx, d.client, level-1)
		if err != nil {
			return merged, fmt.Errorf("litestream: count level %d: %w", level-1, err)
		}
		if count <= threshold {
			break
		}

		info, err := compactor.Compact(ctx, level)
		if errors.Is(err, ls.ErrNoCompaction) {
			break
		}
		if err != nil {
			return merged, fmt.Errorf("litestream: compact level %d: %w", level, err)
		}
		merged++

		if err := deleteCovered(ctx, d.client, level-1, info.MaxTXID, retention); err != nil {
			return merged, fmt.Errorf("litestream: delete compacted level %d: %w", level-1, err)
		}
	}
	return merged, nil
}

// countLTXFiles returns how many files a level holds.
func countLTXFiles(ctx context.Context, client ls.ReplicaClient, level int) (int, error) {
	itr, err := client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return 0, err
	}
	defer itr.Close()
	n := 0
	for itr.Next() {
		n++
	}
	return n, itr.Err()
}

// deleteCovered removes the source files a merge replaced. Files newer than
// the merge, or younger than the retention window, stay: an in-flight reader
// may still be using them.
func deleteCovered(ctx context.Context, client ls.ReplicaClient, level int, maxTXID ltx.TXID, retention time.Duration) error {
	if retention < 0 {
		retention = 0
	}
	cutoff := time.Now().Add(-retention)

	itr, err := client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return err
	}
	var covered []*ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		if info.MaxTXID > maxTXID {
			continue
		}
		if info.CreatedAt.After(cutoff) {
			continue
		}
		covered = append(covered, info)
	}
	if err := itr.Close(); err != nil {
		return err
	}
	if len(covered) == 0 {
		return nil
	}
	return client.DeleteLTXFiles(ctx, covered)
}
