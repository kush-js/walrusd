package storage_test

import (
	"context"
	"os"
	"testing"

	"walrus/storage"
)

// TestS3StoreConformance runs the full storage conformance suite against a
// real S3-compatible endpoint (Cloudflare R2 verified). It is skipped unless
// WALRUS_TEST_S3_* env vars are set (spec §15: providers must pass this
// suite before customer onboarding).
func TestS3StoreConformance(t *testing.T) {
	endpoint := os.Getenv("WALRUS_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALRUS_TEST_S3_BUCKET")
	keyID := os.Getenv("WALRUS_TEST_S3_ACCESS_KEY_ID")
	secret := os.Getenv("WALRUS_TEST_S3_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || keyID == "" || secret == "" {
		t.Skip("WALRUS_TEST_S3_* not set; skipping live S3 conformance")
	}
	store, err := storage.NewS3(context.Background(), storage.S3Options{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     keyID,
		SecretAccessKey: secret,
	})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	RunConformance(t, func(t *testing.T) storage.ConditionalStore {
		return store
	})
}
