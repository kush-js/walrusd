// Command e2e is the walrusd end-to-end check for the Go core against real
// object storage (MinIO/S3) and a real Redis/Valkey lease store.
//
// It exercises the full loop for one fresh database: a read before any write
// (no lease), a schema write, an insert write, an idempotent retry, and
// read-after-write. Around every step it inspects the lease key directly in
// Redis to prove the runtime alone acquires and releases the lease and that
// reads never touch it.
//
// Run:
//
//	go run -tags vfs ./e2e/go
//
// Configuration (all optional):
//
//	WALRUSD_E2E_ENDPOINT, WALRUSD_E2E_REGION, WALRUSD_E2E_BUCKET,
//	WALRUSD_E2E_ACCESS_KEY, WALRUSD_E2E_SECRET_KEY, WALRUSD_E2E_REDIS,
//	WALRUSD_E2E_ROOT_PREFIX (object-storage namespace, default "walrusd-e2e")
//
// WALRUSD_E2E_ROOT_PREFIX is the only per-deployment knob: the per-run
// database ID lives under it as "users/<library>_<random>" and never repeats
// the prefix. Every language's e2e script follows this layout.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"

	"github.com/redis/go-redis/v9"

	"walrusd/identity"
	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

type leaseRecord struct {
	State string `json:"state"`
	Owner string `json:"owner"`
	Epoch uint64 `json:"epoch"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL go: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS go")
}

func run() error {
	ctx := context.Background()
	rootPrefix := envOrDefault("WALRUSD_E2E_ROOT_PREFIX", "walrusd-e2e")
	redisAddr := envOrDefault("WALRUSD_E2E_REDIS", "127.0.0.1:6379")
	databaseID := fmt.Sprintf("users/go_%d", rand.Int64())

	id, err := identity.NewDatabaseID(databaseID)
	if err != nil {
		return err
	}
	leaseKey := id.LeaseKey(rootPrefix)

	// Lease inspection goes straight to Redis: the runtime never exposes the
	// lease, and the test must not use the runtime's own machinery for it.
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	store, err := lease.NewRedisStore(ctx, lease.RedisOptions{Addr: redisAddr})
	if err != nil {
		return fmt.Errorf("redis lease store: %w", err)
	}
	defer store.Close()

	rt, err := runtime.New(store, "e2e-go-owner", runtime.DefaultConfig())
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	defer rt.Close()

	d := runtime.DatabaseDescriptor{
		DatabaseID: databaseID,
		Storage: litestream.Profile{
			Provider:   "s3",
			Endpoint:   envOrDefault("WALRUSD_E2E_ENDPOINT", "http://127.0.0.1:9000"),
			Region:     envOrDefault("WALRUSD_E2E_REGION", "us-east-1"),
			Bucket:     envOrDefault("WALRUSD_E2E_BUCKET", "walrusd-e2e"),
			RootPrefix: rootPrefix,
		},
		Credentials: runtime.StaticCredentials{
			AccessKeyID:     envOrDefault("WALRUSD_E2E_ACCESS_KEY", "walrusd"),
			SecretAccessKey: envOrDefault("WALRUSD_E2E_SECRET_KEY", "walrusdsecret"),
		},
	}

	// 1. A read on a database with no flushed state must not take a lease.
	var count int
	if err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master`).Scan(&count)
	}); err != nil {
		return fmt.Errorf("read before write: %w", err)
	}
	if count != 0 {
		return fmt.Errorf("read before write: expected empty database, got %d objects", count)
	}
	if err := requireLeaseAbsent(ctx, redisClient, leaseKey, "read before write"); err != nil {
		return err
	}
	fmt.Println("ok read-before-write: empty database served, no lease created")

	// 2. Schema write: the runtime acquires the lease, commits, flushes to
	// object storage, releases the lease, and only then acknowledges.
	schema, err := rt.WithWrite(ctx, d, "e2e-schema", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`)
		return err
	})
	if err != nil {
		return fmt.Errorf("schema write: %w", err)
	}
	if schema.TXID == "" {
		return fmt.Errorf("schema write: empty txid")
	}
	if err := requireLeaseReleased(ctx, redisClient, leaseKey, "schema write"); err != nil {
		return err
	}
	fmt.Printf("ok schema write: txid=%s lease released\n", schema.TXID)

	// 3. Insert write with a fresh idempotency key.
	value := fmt.Sprintf("value-%d", rand.Int64())
	insertKey := fmt.Sprintf("e2e-insert-%d", rand.Int64())
	insert, err := rt.WithWrite(ctx, d, insertKey, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO kv (k, v) VALUES (?, ?)`, "greeting", value)
		return err
	})
	if err != nil {
		return fmt.Errorf("insert write: %w", err)
	}
	if insert.TXID <= schema.TXID {
		return fmt.Errorf("insert write: txid %s did not advance past %s", insert.TXID, schema.TXID)
	}
	released, err := leaseState(ctx, redisClient, leaseKey)
	if err != nil {
		return err
	}
	if released.State != "released" {
		return fmt.Errorf("insert write: lease state %q, want released", released.State)
	}
	if released.Epoch < 2 {
		return fmt.Errorf("insert write: epoch %d, want >= 2 (one acquisition per write)", released.Epoch)
	}
	fmt.Printf("ok insert write: txid=%s epoch=%d lease released\n", insert.TXID, released.Epoch)

	// 4. Retrying the same idempotency key is deduplicated: same TXID, no
	// second mutation, and the lease still ends released.
	retry, err := rt.WithWrite(ctx, d, insertKey, func(*sql.Conn) error {
		return fmt.Errorf("callback must not run for a deduplicated write")
	})
	if err != nil {
		return fmt.Errorf("idempotent retry: %w", err)
	}
	if retry.TXID != insert.TXID {
		return fmt.Errorf("idempotent retry: txid %s, want %s", retry.TXID, insert.TXID)
	}
	if !retry.Deduplicated {
		return fmt.Errorf("idempotent retry: Deduplicated=false")
	}
	if err := requireLeaseReleased(ctx, redisClient, leaseKey, "idempotent retry"); err != nil {
		return err
	}
	fmt.Printf("ok idempotent retry: txid=%s deduplicated\n", retry.TXID)

	// 5. Reads see the flushed state and leave the lease record untouched.
	before, err := leaseState(ctx, redisClient, leaseKey)
	if err != nil {
		return err
	}
	var got string
	if err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT v FROM kv WHERE k = ?`, "greeting").Scan(&got)
	}); err != nil {
		return fmt.Errorf("read after write: %w", err)
	}
	if got != value {
		return fmt.Errorf("read after write: got %q, want %q", got, value)
	}
	if err := requireLeaseUnchanged(ctx, redisClient, leaseKey, before, "read after write"); err != nil {
		return err
	}
	fmt.Printf("ok read after write: value=%s lease untouched (epoch=%d)\n", got, before.Epoch)

	// 6. A second read is served from the cached read session; still no lease.
	if err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM kv`).Scan(&count)
	}); err != nil {
		return fmt.Errorf("second read: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("second read: %d rows, want 1", count)
	}
	if err := requireLeaseUnchanged(ctx, redisClient, leaseKey, before, "second read"); err != nil {
		return err
	}
	fmt.Println("ok second read: cached session, lease untouched")

	return nil
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// leaseState reads the lease record the runtime keeps in Redis.
func leaseState(ctx context.Context, client *redis.Client, key string) (leaseRecord, error) {
	var rec leaseRecord
	fields, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return rec, fmt.Errorf("read lease record: %w", err)
	}
	data, ok := fields["data"]
	if !ok {
		return rec, fmt.Errorf("lease record %s not found", key)
	}
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		return rec, fmt.Errorf("decode lease record: %w", err)
	}
	return rec, nil
}

func requireLeaseAbsent(ctx context.Context, client *redis.Client, key, step string) error {
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("%s: check lease key: %w", step, err)
	}
	if exists != 0 {
		return fmt.Errorf("%s: lease key %s exists; the operation took a lease it should not have", step, key)
	}
	return nil
}

func requireLeaseReleased(ctx context.Context, client *redis.Client, key, step string) error {
	rec, err := leaseState(ctx, client, key)
	if err != nil {
		return fmt.Errorf("%s: %w", step, err)
	}
	if rec.State != "released" {
		return fmt.Errorf("%s: lease state %q, want released", step, rec.State)
	}
	return nil
}

func requireLeaseUnchanged(ctx context.Context, client *redis.Client, key string, want leaseRecord, step string) error {
	got, err := leaseState(ctx, client, key)
	if err != nil {
		return fmt.Errorf("%s: %w", step, err)
	}
	if got != want {
		return fmt.Errorf("%s: lease record changed (%+v -> %+v)", step, want, got)
	}
	return nil
}
