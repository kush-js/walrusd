package runtime_test

import (
	"context"
	"database/sql"
	"testing"

	"walrus/observability"
)

func TestMetricsSnapshotAndReadSignals(t *testing.T) {
	rt := newTestRuntime(t, "api-metrics")
	d := descriptor(t, "users/metrics")

	if _, err := rt.WithWrite(context.Background(), d, "create", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
			rows, err := conn.QueryContext(context.Background(), `SELECT * FROM t`)
			if err != nil {
				return err
			}
			return rows.Close()
		}); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
		var value string
		return conn.QueryRowContext(context.Background(), `SELECT * FROM missing_table`).Scan(&value)
	}); err == nil {
		t.Fatal("failing read returned nil error")
	}

	key := observability.Hash(d.DatabaseID)
	snap := rt.MetricsSnapshot()
	got := snap[key]
	if got.ReadOpens != 1 {
		t.Fatalf("ReadOpens = %d, want 1", got.ReadOpens)
	}
	if got.ReadCacheHits != 2 {
		t.Fatalf("ReadCacheHits = %d, want 2", got.ReadCacheHits)
	}
	if got.ReadFailures != 1 {
		t.Fatalf("ReadFailures = %d, want 1", got.ReadFailures)
	}
	if got.ReadCacheEvictions != 1 {
		t.Fatalf("ReadCacheEvictions = %d, want 1", got.ReadCacheEvictions)
	}
	if got.ActiveReadInstances != 0 {
		t.Fatalf("ActiveReadInstances = %d, want 0", got.ActiveReadInstances)
	}

	total := rt.MetricsTotal()
	if total.ReadOpens != got.ReadOpens ||
		total.ReadCacheHits != got.ReadCacheHits ||
		total.ReadFailures != got.ReadFailures ||
		total.ReadCacheEvictions != got.ReadCacheEvictions ||
		total.ActiveReadInstances != got.ActiveReadInstances {
		t.Fatalf("total read metrics = %+v, snapshot = %+v", total, got)
	}
}
