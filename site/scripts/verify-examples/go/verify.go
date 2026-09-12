//go:build verify_examples

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
	"walrusd/walrusderr"
)

func exampleDescriptor() runtime.DatabaseDescriptor {
keyID, secret := "", ""
d := runtime.DatabaseDescriptor{
    DatabaseID: "users/user_1a4b", // your ID; objects land at <root_prefix>/users/user_1a4b/
    Storage: litestream.Profile{
        Provider:   "s3",         // "s3" (any S3-compatible) or "file"
        Endpoint:   "https://<account>.r2.cloudflarestorage.com",
        Bucket:     "my-org-bucket",
        RootPrefix: "tenants/acme",
    },
    Credentials: runtime.StaticCredentials{AccessKeyID: keyID, SecretAccessKey: secret},
}
return d
}

func readExample(
	ctx context.Context,
	rt *runtime.Runtime,
	d runtime.DatabaseDescriptor,
) error {
err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
    var body string
    return conn.QueryRowContext(ctx,
        `SELECT body FROM events WHERE id = ?`, 1).Scan(&body)
})
return err
}

func writeEvents(
	ctx context.Context,
	rt *runtime.Runtime,
	d runtime.DatabaseDescriptor,
) (runtime.WriteResult, error) {
// First write for a fresh database: create the schema as its own write.
_, err := rt.WithWrite(ctx, d, "create-schema", func(conn *sql.Conn) error {
    _, err := conn.ExecContext(ctx,
        `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)`)
    return err
})
if err != nil { panic(err) }

res, err := rt.WithWrite(ctx, d, "create-event-42", func(conn *sql.Conn) error {
    if _, err := conn.ExecContext(ctx,
        `INSERT INTO events (id, body) VALUES (?, ?)`, 42, "hello"); err != nil {
        return err
    }
    return nil
})
if err != nil { /* see error handling below */ }
fmt.Println("durable at txid", res.TXID, "deduplicated:", res.Deduplicated)
return res, err
}

func handleWriteError(
	ctx context.Context,
	rt *runtime.Runtime,
	d runtime.DatabaseDescriptor,
	key string,
	fn func(*sql.Conn) error,
) {
_, err := rt.WithWrite(ctx, d, key, fn)
if walrusderr.ClassOf(err) == walrusderr.ClassFlushFailed {
    // safe: retry with the same idempotency key
}
}

func readValue(
	ctx context.Context,
	rt *runtime.Runtime,
	d runtime.DatabaseDescriptor,
) (string, error) {
	var body string
	err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx,
			`SELECT body FROM events WHERE id = ?`, 42).Scan(&body)
	})
	return body, err
}

func init() {
	root := os.Getenv("WALRUSD_EXAMPLE_ROOT")
	bufferRoot := os.Getenv("WALRUSD_BUFFER_ROOT")
	if root == "" || bufferRoot == "" {
		panic("WALRUSD_EXAMPLE_ROOT and WALRUSD_BUFFER_ROOT are required")
	}

	ctx := context.Background()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = bufferRoot
	rt, err := runtime.New(lease.NewMemoryStore(), "api-pod-7", cfg)
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	descriptor := runtime.DatabaseDescriptor{
		DatabaseID: "users/user_1a4b",
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: root,
		},
		Credentials: runtime.StaticCredentials{},
	}

	result, err := writeEvents(ctx, rt, descriptor)
	if err != nil {
		panic(err)
	}
	body, err := readValue(ctx, rt, descriptor)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Go durable at txid %s\n", result.TXID)
	fmt.Printf("Go read: %s\n", body)
}
