package litestream

import (
	"testing"

	"github.com/benbjohnson/litestream/s3"
)

// The S3 signing region resolves through: explicit profile region, then
// AWS_REGION, then AWS_DEFAULT_REGION, then us-east-1. R2's dashboard
// label "auto" is normalized away at every level: left in place, the SDK
// signs ListObjects with a bogus region and the replica probe hangs.
func TestResolveS3Region(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	if got := normalizeS3Region("eu-west-1"); got != "eu-west-1" {
		t.Fatalf("explicit region = %q, want eu-west-1", got)
	}

	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
	if got := normalizeS3Region(""); got != "us-west-2" {
		t.Fatalf("AWS_REGION fallback = %q, want us-west-2", got)
	}
	if got := normalizeS3Region("eu-west-1"); got != "eu-west-1" {
		t.Fatalf("explicit region must win over env: got %q", got)
	}

	t.Setenv("AWS_REGION", "")
	if got := normalizeS3Region(""); got != "ap-southeast-1" {
		t.Fatalf("AWS_DEFAULT_REGION fallback = %q, want ap-southeast-1", got)
	}

	t.Setenv("AWS_DEFAULT_REGION", "")
	if got := normalizeS3Region(""); got != "us-east-1" {
		t.Fatalf("no region configured = %q, want us-east-1", got)
	}
	if got := normalizeS3Region("auto"); got != "us-east-1" {
		t.Fatalf("dashboard label must not survive: got %q", got)
	}

	t.Setenv("AWS_REGION", "auto")
	if got := normalizeS3Region(""); got != "us-east-1" {
		t.Fatalf("auto in env must not survive: got %q", got)
	}
}

// The bridge must plumb the env fallback into the S3 client it builds.
func TestReplicaClientS3RegionFallback(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-central-1")
	t.Setenv("AWS_DEFAULT_REGION", "")

	b := NewBridge(DefaultConfig())
	client, err := b.ReplicaClient("db", "prefix/replica", Profile{
		Provider: "s3",
		Bucket:   "bucket",
		Endpoint: "http://127.0.0.1:9000",
	})
	if err != nil {
		t.Fatalf("replica client: %v", err)
	}
	sc, ok := client.(*s3.ReplicaClient)
	if !ok {
		t.Fatalf("client type = %T, want *s3.ReplicaClient", client)
	}
	if sc.Region != "eu-central-1" {
		t.Fatalf("s3 client region = %q, want eu-central-1", sc.Region)
	}
	if sc.Endpoint != "http://127.0.0.1:9000" || !sc.ForcePathStyle {
		t.Fatalf("endpoint plumbing broken: endpoint=%q pathStyle=%v", sc.Endpoint, sc.ForcePathStyle)
	}
}

func TestReplicaClientMemoryProvider(t *testing.T) {
	b := NewBridge(DefaultConfig())
	client, err := b.ReplicaClient("db", "prefix/replica", Profile{Provider: "memory"})
	if err != nil {
		t.Fatalf("memory provider: %v", err)
	}
	if client.Type() != "memory" {
		t.Fatalf("client type = %q, want memory", client.Type())
	}
}

func TestNewBridge_ReplicaCacheOption(t *testing.T) {
	if b := NewBridge(DefaultConfig()); b.files == nil {
		t.Fatal("default bridge has no replica cache")
	}

	cfg := DefaultConfig()
	cfg.ReplicaCacheBytes = 0
	if b := NewBridge(cfg); b.files != nil {
		t.Fatal("replica cache allocated when disabled")
	}
}
