package ownership

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
	"walrus/storage"
)

// ownershipAdapter adapts the storage package to the Adapter interface for
// integration tests. Skipped unless R2_* env vars are set (see storage_r2_test).
type r2Adapter struct {
	get            func(ctx context.Context, key string) ([]byte, string, error)
	createIfAbsent func(ctx context.Context, key string, body []byte) (string, error)
	replaceIfVer   func(ctx context.Context, key string, expected string, body []byte) (string, error)
}

func (a *r2Adapter) Get(ctx context.Context, key string) ([]byte, string, error) {
	return a.get(ctx, key)
}
func (a *r2Adapter) CreateIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	return a.createIfAbsent(ctx, key, body)
}
func (a *r2Adapter) ReplaceIfVersion(ctx context.Context, key, expected string, body []byte) (string, error) {
	return a.replaceIfVer(ctx, key, expected, body)
}

// TestRealStorageLifecycle runs the full acquire → read-back → renew sequence
// against the real storage adapter. Integration test: skips without R2_* env.
func TestRealStorageLifecycle(t *testing.T) {
	endpoint := testEnv("R2_ENDPOINT")
	bucket := testEnv("R2_BUCKET")
	ak := testEnv("R2_ACCESS_KEY_ID")
	sk := testEnv("R2_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || ak == "" || sk == "" {
		t.Skip("R2_* env not set; skipping integration test")
	}
	store := newRealStore(t, endpoint, bucket, ak, sk)
	ctx := context.Background()
	key := fmt.Sprintf("basemnt/v1/databases/dbg-lifecycle-%d/ownership/current.json", time.Now().UnixNano())

	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewInterval: 10 * time.Second, MaxClockSkew: 2 * time.Second}
	m1, err := New(store, "worker-A", cfg)
	if err != nil {
		t.Fatal(err)
	}
	res1, err := m1.AcquireOrRenew(ctx, "org/dbg/user/x", key)
	t.Logf("acquire: epoch=%d err=%v", res1.Record.Epoch, err)
	if err != nil {
		return
	}

	m2, _ := New(store, "worker-A", cfg)
	res2, err := m2.AcquireOrRenew(ctx, "org/dbg/user/x", key)
	t.Logf("renew: epoch=%d err=%v", res2.Record.Epoch, err)
	if err != nil {
		body, ver, _ := store.Get(ctx, key)
		t.Logf("stored body=%s ver=%s", body, ver)
		var rec Record
		_ = json.Unmarshal(body, &rec)
		t.Logf("stored owner=%q lease_expires=%s", rec.OwnerWorkerID, rec.LeaseExpiresAt)
	}
}

// testEnv reads an environment variable.
func testEnv(k string) string { return os.Getenv(k) }

// newRealStore builds an Adapter over the real S3-compatible endpoint by
// binding the storage package's adapter methods as function values (no
// import cycle: storage does not import ownership).
func newRealStore(t *testing.T, endpoint, bucket, ak, sk string) Adapter {
	t.Helper()
	a, err := storage.NewS3(endpoint, "auto", bucket, ak, sk)
	if err != nil {
		t.Fatal(err)
	}
	return &r2Adapter{
		get:            a.Get,
		createIfAbsent: a.CreateIfAbsent,
		replaceIfVer:   a.ReplaceIfVersion,
	}
}
