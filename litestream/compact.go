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
	merged, _, err := d.CompactWithClient(ctx, d.client, threshold, retention, true)
	return merged, err
}

// CompactWithClient merges with an explicit replica client. A manifest-backed
// client makes the level listings come from the durable manifest instead of
// object-store LISTs. When deleteSources is false the caller owns deletion,
// which lets it write the new manifest before removing files that older
// manifests still reference.
func (d *Database) CompactWithClient(ctx context.Context, client ls.ReplicaClient, threshold int, retention time.Duration, deleteSources bool) (int, []*ltx.FileInfo, error) {
	if threshold <= 0 {
		threshold = 1
	}
	if invalidator, ok := client.(interface{ DropListings() }); ok {
		invalidator.DropListings()
	}

	compactor := ls.NewCompactor(client, noOpLogger())
	merged := make([]*ltx.FileInfo, 0, compactionTopLevel)
	for level := 1; level <= compactionTopLevel; level++ {
		count, err := countLTXFiles(ctx, client, level-1)
		if err != nil {
			return len(merged), merged, fmt.Errorf("litestream: count level %d: %w", level-1, err)
		}
		if count <= threshold {
			break
		}

		info, err := compactor.Compact(ctx, level)
		if errors.Is(err, ls.ErrNoCompaction) {
			break
		}
		if err != nil {
			return len(merged), merged, fmt.Errorf("litestream: compact level %d: %w", level, err)
		}
		merged = append(merged, info)

		if deleteSources {
			if err := deleteCovered(ctx, client, level-1, info.MaxTXID, retention); err != nil {
				return len(merged), merged, fmt.Errorf("litestream: delete compacted level %d: %w", level-1, err)
			}
		}
	}
	return len(merged), merged, nil
}

// CompactManifest runs compaction against a manifest snapshot and returns the
// new active set plus the source files that are no longer referenced. The
// caller writes the manifest before deleting sources, so a failed manifest
// update can never leave readers pointing at deleted files.
func (d *Database) CompactManifest(ctx context.Context, files []*ltx.FileInfo, threshold int, retention time.Duration) (updated, merged, sources []*ltx.FileInfo, err error) {
	mc := newManifestClient(d.client, files)
	_, merged, err = d.CompactWithClient(ctx, mc, threshold, retention, false)
	if err != nil {
		return nil, merged, nil, err
	}
	updated = cloneFileInfos(files)
	for _, info := range merged {
		var gone []*ltx.FileInfo
		for _, f := range updated {
			if f.Level == info.Level-1 && f.MaxTXID <= info.MaxTXID {
				gone = append(gone, f)
			}
		}
		sources = append(sources, gone...)
		updated = applyManifestDelta(updated, []*ltx.FileInfo{info}, gone)
	}
	return updated, merged, sources, nil
}

// DeleteFiles removes the given files once they are older than retention.
// Composite files that are too young stay for the garbage collector.
func (d *Database) DeleteFiles(ctx context.Context, files []*ltx.FileInfo, retention time.Duration) error {
	if len(files) == 0 {
		return nil
	}
	if retention < 0 {
		retention = 0
	}
	cutoff := time.Now().Add(-retention)
	var old []*ltx.FileInfo
	for _, f := range files {
		if !f.CreatedAt.After(cutoff) {
			old = append(old, f)
		}
	}
	if len(old) == 0 {
		return nil
	}
	return d.client.DeleteLTXFiles(ctx, old)
}

// GC removes objects that are not referenced by the current manifest and are
// older than retention. It lists every level, so it is a background task, not
// part of a request.
func (d *Database) GC(ctx context.Context, retention time.Duration) error {
	active, ok, err := d.ReadManifest(ctx)
	if err != nil || !ok {
		return err
	}
	all, err := d.BuildManifest(ctx)
	if err != nil {
		return err
	}
	if retention < 0 {
		retention = 0
	}
	cutoff := time.Now().Add(-retention)
	var garbage []*ltx.FileInfo
	for _, f := range all {
		if manifestContains(active, f) {
			continue
		}
		if f.CreatedAt.After(cutoff) {
			continue
		}
		garbage = append(garbage, f)
	}
	if len(garbage) == 0 {
		return nil
	}
	return d.client.DeleteLTXFiles(ctx, garbage)
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
