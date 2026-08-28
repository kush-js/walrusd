package commit

import (
	"context"
	"errors"
	"testing"
)

func TestCommitCreatesSeq1(t *testing.T) {
	c := New(NewMemAdapter())
	ctx := context.Background()
	m, _, err := c.Commit(ctx, CommitInput{
		DatabaseID: "org/o/user/u", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1",
		Objects: []ObjectPointer{{Key: "obj1", Checksum: "abc", MinTXID: 1, MaxTXID: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.CommitSeq != 1 || m.Epoch != 1 {
		t.Fatalf("bad first manifest: %+v", m)
	}
}

func TestCommitAdvancesSeq(t *testing.T) {
	c := New(NewMemAdapter())
	ctx := context.Background()
	m1, v1, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	m2, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if m2.CommitSeq != m1.CommitSeq+1 || m2.PrevVersion != v1 {
		t.Fatalf("bad advance: seq=%d prev=%q", m2.CommitSeq, m2.PrevVersion)
	}
}

func TestStaleEpochCannotAdvance(t *testing.T) {
	c := New(NewMemAdapter())
	ctx := context.Background()
	// Epoch 2 commits first (e.g. after a takeover).
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 2, LeaseID: "L2", WriterID: "w2"}); err != nil {
		t.Fatal(err)
	}
	// The old epoch-1 writer now tries to commit: refused outright.
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}
	// Even with the current object version in hand, epoch still gates.
	cur, v, _ := c.Current(ctx, "k")
	if cur == nil {
		t.Fatal("manifest missing")
	}
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"}); err == nil {
		t.Fatal("stale epoch must never advance manifest")
	}
	_ = v
}

func TestFencingLostOnCASConflict(t *testing.T) {
	c := New(NewMemAdapter())
	ctx := context.Background()
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"}); err != nil {
		t.Fatal(err)
	}
	// A competing writer (same epoch, different worker) commits.
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L2", WriterID: "w2"}); !errors.Is(err, ErrFencingLost) {
		t.Fatalf("want ErrFencingLost for same-epoch different writer, got %v", err)
	}
	// Object version changed underneath: w1's next CAS conflicts.
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 2, LeaseID: "L3", WriterID: "w3"}); err != nil {
		t.Fatal(err)
	}
	// Verify no acknowledgement happened for w2: sequence must not include it.
	cur, _, _ := c.Current(ctx, "k")
	if cur.WriterID == "w2" {
		t.Fatal("w2 must not have advanced the manifest")
	}
}

func TestHigherEpochTakeoverCanAdvance(t *testing.T) {
	c := New(NewMemAdapter())
	ctx := context.Background()
	if _, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 1, LeaseID: "L1", WriterID: "w1"}); err != nil {
		t.Fatal(err)
	}
	// Epoch 2 owner legitimately advances.
	m, _, err := c.Commit(ctx, CommitInput{DatabaseID: "d", Key: "k", Epoch: 2, LeaseID: "L2", WriterID: "w2"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 2 || m.CommitSeq != 2 {
		t.Fatalf("bad takeover commit: %+v", m)
	}
}
