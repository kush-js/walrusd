package observability

import "testing"

func TestHashStableAndPrivacySafe(t *testing.T) {
	a, b := Hash("org_1/users/user_9"), Hash("org_1/users/user_9")
	if a != b {
		t.Fatal("hash must be stable")
	}
	if a == Hash("org_1/users/user_10") {
		t.Fatal("hash must distinguish IDs")
	}
	// The registry must never retain the raw ID: snapshot keys are hashes.
	r := NewRegistry()
	r.For("org_1/users/user_9")
	for k := range r.Snapshot() {
		if k == 0 {
			t.Fatal("unexpected zero key")
		}
	}
}

func TestRecordAndTotal(t *testing.T) {
	r := NewRegistry()
	r.Record("db_a", func(m *Metrics) { m.WriteTransactions++; m.FlushSuccesses++ })
	r.Record("db_b", func(m *Metrics) { m.WriteTransactions += 2 })
	total := r.Total()
	if total.WriteTransactions != 3 || total.FlushSuccesses != 1 {
		t.Fatalf("total = %+v", total)
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	r := NewRegistry()
	r.Record("db_a", func(m *Metrics) { m.FlushFailures = 1 })
	snap := r.Snapshot()
	for k, v := range snap {
		v.FlushFailures = 99
		snap[k] = v
	}
	if got := r.Total().FlushFailures; got != 1 {
		t.Fatalf("snapshot mutated the registry: %d", got)
	}
}
