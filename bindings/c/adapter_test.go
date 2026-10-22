package c

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"walrusd/lease"
	"walrusd/runtime"
)

// newTestAdapter builds an adapter over an in-memory lease store and memory
// replica provider. The cap parameter sets MaxReadResultBytes.
func newTestAdapter(t *testing.T, maxReadResultBytes int64) *Adapter {
	t.Helper()
	cfg := runtime.DefaultConfig()
	cfg.MaxReadResultBytes = maxReadResultBytes
	a, err := NewAdapter(lease.NewMemoryStore(), "api-test", cfg)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func testDescriptor(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf(`{
		"database_id": "users/adapter",
		"storage": {"provider": "memory", "root_prefix": "adapter-%d"},
		"credentials": {}
	}`, time.Now().UnixNano())
}

func writeEnvelope(t *testing.T, a *Adapter, descriptor, key string, statements string) error {
	t.Helper()
	req := fmt.Sprintf(`{"descriptor": %s, "idempotency_key": %q, "statements": %s}`, descriptor, key, statements)
	_, err := a.WithWriteBytes(handledCtx{}, []byte(req))
	return err
}

func readEnvelope(t *testing.T, a *Adapter, descriptor, sql string, params string) (ReadResult, error) {
	t.Helper()
	req := fmt.Sprintf(`{"descriptor": %s, "sql": %q, "params": %s}`, descriptor, sql, params)
	out, err := a.WithReadBytes(handledCtx{}, []byte(req))
	if err != nil {
		return ReadResult{}, err
	}
	var res ReadResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("unmarshal read result: %v", err)
	}
	return res, nil
}

// BLOBs must round trip as base64, big integer literals must survive JSON
// exactly, and $blob parameters must bind as raw bytes.
func TestAdapterBlobAndBigIntRoundTrip(t *testing.T) {
	a := newTestAdapter(t, 0)
	d := testDescriptor(t)

	if err := writeEnvelope(t, a, d, "schema", `[{"sql": "CREATE TABLE t (k INTEGER, v TEXT, b BLOB)"}]`); err != nil {
		t.Fatalf("schema write: %v", err)
	}
	// The 9007199254740993 literal cannot survive a float64 round trip.
	stmts := `[{"sql": "INSERT INTO t (k, v, b) VALUES (?, ?, ?)", "params": [9007199254740993, "x", {"$blob": "aGVsbG8="}]}]`
	if err := writeEnvelope(t, a, d, "insert", stmts); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	res, err := readEnvelope(t, a, d, `SELECT k, v, b FROM t`, `[]`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}
	var row map[string]any
	if err := json.Unmarshal(res.Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if got := row["k"]; got != float64(9007199254740993) && fmt.Sprint(got) != "9007199254740993" {
		t.Fatalf("k = %v (%T), want exact 9007199254740993", got, got)
	}
	if got := row["b"]; got != "aGVsbG8=" {
		t.Fatalf("b = %v, want base64 aGVsbG8=", got)
	}
}

// Duplicate column names must not silently collapse into one value.
func TestAdapterDuplicateColumns(t *testing.T) {
	a := newTestAdapter(t, 0)
	d := testDescriptor(t)
	if err := writeEnvelope(t, a, d, "schema", `[{"sql": "CREATE TABLE t (k INTEGER)"}]`); err != nil {
		t.Fatalf("schema write: %v", err)
	}
	if err := writeEnvelope(t, a, d, "insert", `[{"sql": "INSERT INTO t (k) VALUES (7)", "params": []}]`); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	res, err := readEnvelope(t, a, d, `SELECT k AS id, k AS id FROM t`, `[]`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal(res.Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row["id"] != float64(7) || row["id_2"] != float64(7) {
		t.Fatalf("row = %v, want both id and id_2 = 7", row)
	}
}

// A runaway read must fail cleanly instead of buffering without bound.
func TestAdapterReadResultCap(t *testing.T) {
	a := newTestAdapter(t, 64)
	d := testDescriptor(t)
	if err := writeEnvelope(t, a, d, "schema", `[{"sql": "CREATE TABLE t (v TEXT)"}]`); err != nil {
		t.Fatalf("schema write: %v", err)
	}
	big := strings.Repeat("x", 256)
	stmts := fmt.Sprintf(`[{"sql": "INSERT INTO t (v) VALUES (?)", "params": [%q]}]`, big)
	if err := writeEnvelope(t, a, d, "insert", stmts); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	_, err := readEnvelope(t, a, d, `SELECT v FROM t`, `[]`)
	if err == nil {
		t.Fatal("expected capped read to fail")
	}
	if !strings.Contains(err.Error(), "read result exceeds") {
		t.Fatalf("error = %v, want read result cap message", err)
	}
}
