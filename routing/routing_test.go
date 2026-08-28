package routing

import (
	"fmt"
	"testing"
)

func TestSelectDeterministic(t *testing.T) {
	snap := &Snapshot{Generation: 7, Writers: []Writer{
		{WorkerID: "w1", Endpoint: "e1"}, {WorkerID: "w2", Endpoint: "e2"}, {WorkerID: "w3", Endpoint: "e3"},
	}}
	want, _, _ := Select(snap, "org/a/user/b")
	for range 10 {
		got, _, _ := Select(snap, "org/a/user/b")
		if got.WorkerID != want.WorkerID {
			t.Fatalf("non-deterministic: %s != %s", got.WorkerID, want.WorkerID)
		}
	}
}

func TestSelectLimitedRemap(t *testing.T) {
	snap := &Snapshot{Generation: 1, Writers: []Writer{
		{WorkerID: "w1"}, {WorkerID: "w2"}, {WorkerID: "w3"},
	}}
	const n = 300
	before := map[string]string{}
	for i := range n {
		db := fmt.Sprintf("org/o/user/u%d", i)
		w, _, _ := Select(snap, db)
		before[db] = w.WorkerID
	}
	// Add w4: only databases whose winner changes should move.
	bigger := &Snapshot{Generation: 1, Writers: append(append([]Writer{}, snap.Writers...), Writer{WorkerID: "w4"})}
	moved := 0
	for i := range n {
		db := fmt.Sprintf("org/o/user/u%d", i)
		w, _, _ := Select(bigger, db)
		if w.WorkerID != before[db] {
			moved++
		}
	}
	// Rendezvous moves ~n/(N+1); modulo would move ~n*N/(N+1). Bound loosely.
	if moved > n/3 {
		t.Fatalf("too much remapping on join: %d/%d", moved, n)
	}
	// Remove w1: databases on w1 must move; others stay.
	less := &Snapshot{Generation: 1, Writers: []Writer{{WorkerID: "w2"}, {WorkerID: "w3"}}}
	moved = 0
	for i := range n {
		db := fmt.Sprintf("org/o/user/u%d", i)
		w, _, _ := Select(less, db)
		if w.WorkerID != before[db] {
			moved++
		}
	}
	if moved > n/3 {
		t.Fatalf("too much remapping on leave: %d/%d", moved, n)
	}
}

func TestSelectNoWriters(t *testing.T) {
	if _, _, err := Select(&Snapshot{}, "org/a/user/b"); err == nil {
		t.Fatal("want error for empty membership")
	}
}

func TestWeightBias(t *testing.T) {
	// A weight-4 writer should win substantially more than a weight-1 one.
	snap := &Snapshot{Generation: 3, Writers: []Writer{
		{WorkerID: "heavy", Weight: 4}, {WorkerID: "light", Weight: 1},
	}}
	heavy := 0
	const n = 500
	for i := range n {
		w, _, _ := Select(snap, fmt.Sprintf("org/o/user/u%d", i))
		if w.WorkerID == "heavy" {
			heavy++
		}
	}
	if heavy < n/2 {
		t.Fatalf("weight not honored: heavy won %d/%d", heavy, n)
	}
}
