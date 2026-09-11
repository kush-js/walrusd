package runtime

import (
	"context"
	"database/sql"

	"walrusd/walruserr"
)

// idempotencySchema ensures the user database records mutation results in
// the same SQLite transaction as the mutation itself (spec §8).
const idempotencySchema = `
CREATE TABLE IF NOT EXISTS _walrusd_idempotency (
	idempotency_key TEXT PRIMARY KEY,
	txid TEXT NOT NULL
)`

// lookupIdempotent returns the recorded TXID for a key, or "" when the key
// is new. A retry after a timeout or crash obtains the lease, reads the
// prior result, and returns it rather than repeating the operation.
func lookupIdempotent(ctx context.Context, conn *sql.Conn, key string) (string, error) {
	if _, err := conn.ExecContext(ctx, idempotencySchema); err != nil {
		return "", walruserr.Wrap(walruserr.ClassConflict, "prepare idempotency table", err)
	}
	var txid string
	err := conn.QueryRowContext(ctx,
		`SELECT txid FROM _walrusd_idempotency WHERE idempotency_key = ?`, key).Scan(&txid)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", walruserr.Wrap(walruserr.ClassConflict, "read idempotency result", err)
	}
	return txid, nil
}

// recordIdempotent stores the key's result inside the same transaction.
func recordIdempotent(ctx context.Context, conn *sql.Conn, key, txid string) error {
	_, err := conn.ExecContext(ctx,
		`INSERT INTO _walrusd_idempotency (idempotency_key, txid) VALUES (?, ?)`, key, txid)
	if err != nil {
		return walruserr.Wrap(walruserr.ClassConflict, "record idempotency result", err)
	}
	return nil
}
