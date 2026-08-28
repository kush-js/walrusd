package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// integration env: R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
func integrationAdapter(t *testing.T) *S3Adapter {
	t.Helper()
	endpoint := os.Getenv("R2_ENDPOINT")
	bucket := os.Getenv("R2_BUCKET")
	key := os.Getenv("R2_ACCESS_KEY_ID")
	secret := os.Getenv("R2_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || key == "" || secret == "" {
		t.Skip("R2_* env not set; skipping integration test")
	}
	a, err := NewS3(endpoint, "auto", bucket, key, secret)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestR2IntegrationCAS(t *testing.T) {
	a := integrationAdapter(t)
	ctx := context.Background()
	key := fmt.Sprintf("walrus-test/%d/cas.json", time.Now().UnixNano())

	// Create-if-absent succeeds on empty.
	v1, err := a.CreateIfAbsent(ctx, key, []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Second create conflicts.
	if _, err := a.CreateIfAbsent(ctx, key, []byte(`{"n":2}`)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	// Replace with wrong version conflicts.
	if _, err := a.ReplaceIfVersion(ctx, key, `"bogus"`, []byte(`{"n":3}`)); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	// Replace with right version succeeds.
	v2, err := a.ReplaceIfVersion(ctx, key, v1, []byte(`{"n":3}`))
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	// Strong read-after-write.
	body, got, err := a.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(body, []byte(`{"n":3}`)) || got != v2 {
		t.Fatalf("read-back mismatch: body=%q version=%q want %q", body, got, v2)
	}
	// PutImmutable: first wins, same content is fine, different content rejected.
	ikey := key + ".immutable"
	if err := a.PutImmutable(ctx, ikey, []byte("aa")); err != nil {
		t.Fatalf("put immutable: %v", err)
	}
	err = a.PutImmutable(ctx, ikey, []byte("bb"))
	if !errors.Is(err, ErrExistsWithDifferentContent) {
		t.Fatalf("want ErrExistsWithDifferentContent, got %v", err)
	}
}
