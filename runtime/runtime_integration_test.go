package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"walrus/litestream"
	"walrus/runtime"
	"walrus/storage"
)

// TestWithWriteS3EndToEnd runs the full spec §8 write path against a real
// S3-compatible store (Cloudflare R2 verified): lease over conditional
// writes, litestream VFS transaction, synchronous LTX flush, and durable
// read-back from a fresh runtime. Skipped unless WALRUS_TEST_S3_* is set.
func TestWithWriteS3EndToEnd(t *testing.T) {
	endpoint := os.Getenv("WALRUS_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALRUS_TEST_S3_BUCKET")
	keyID := os.Getenv("WALRUS_TEST_S3_ACCESS_KEY_ID")
	secret := os.Getenv("WALRUS_TEST_S3_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || keyID == "" || secret == "" {
		t.Skip("WALRUS_TEST_S3_* not set; skipping live end-to-end test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two runtimes = two API instances; both must see the same durable state.
	// The lease store is the same R2 bucket used for the replica.
	newRuntime := func(owner string) *runtime.Runtime {
		t.Helper()
		store, err := storage.NewS3(ctx, storage.S3Options{
			Endpoint:        endpoint,
			Bucket:          bucket,
			AccessKeyID:     keyID,
			SecretAccessKey: secret,
		})
		if err != nil {
			t.Fatalf("new s3 store: %v", err)
		}
		cfg := runtime.DefaultConfig()
		cfg.Litestream.WriteBufferRootPath = t.TempDir()
		rt, err := runtime.New(store, owner, cfg)
		if err != nil {
			t.Fatalf("new runtime: %v", err)
		}
		return rt
	}

	prefix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	desc := func() runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: prefix + "/users/user_1",
			Storage: litestream.Profile{
				Provider:   "s3",
				Endpoint:   endpoint,
				Region:     "auto",
				Bucket:      bucket,
				RootPrefix: prefix,
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: keyID, SecretAccessKey: secret},
		}
	}

	// Write on instance 1.
	w := newRuntime("api-1")
	res, err := w.WithWrite(ctx, desc(), "seed", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx,
			"CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes (body) VALUES ('hello from walrus');")
		return err
	})
	if err != nil {
		t.Fatalf("with write: %v", err)
	}
	if res.TXID == "" || res.Deduplicated {
		t.Fatalf("unexpected result: %+v", res)
	}
	t.Logf("flushed txid: %s", res.TXID)

	// Retry with the same key on another instance must deduplicate.
	r2 := newRuntime("api-2")
	res2, err := r2.WithWrite(ctx, desc(), "seed", func(conn *sql.Conn) error {
		t.Fatal("callback must not run for deduplicated write")
		return nil
	})
	if err != nil {
		t.Fatalf("deduplicated write: %v", err)
	}
	if !res2.Deduplicated || res2.TXID != res.TXID {
		t.Fatalf("dedup result = %+v, want prior txid %s", res2, res.TXID)
	}

	// Read on a third instance must see the flushed state (spec §9).
	r3 := newRuntime("api-3")
	var body string
	err = r3.WithRead(ctx, desc(), func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, "SELECT body FROM notes LIMIT 1").Scan(&body)
	})
	if err != nil {
		t.Fatalf("with read: %v", err)
	}
	if body != "hello from walrus" {
		t.Fatalf("read body = %q", body)
	}
}
