package runtime

import (
	"testing"
	"time"

	"walrusd/walrusderr"
)

func TestDefaultRetryPolicySequence(t *testing.T) {
	p := DefaultRetryPolicy()
	want := []time.Duration{
		time.Second, time.Second, time.Second, time.Second, time.Second,
		time.Second, time.Second, time.Second, time.Second, time.Second,
		2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 64 * time.Second,
	}
	for i, wantDelay := range want {
		if got := p.baseDelay(i + 1); got != wantDelay {
			t.Fatalf("retry %d delay = %s, want %s", i+1, got, wantDelay)
		}
	}
}

func TestWriteRetryClassification(t *testing.T) {
	retryable := []walrusderr.Class{
		walrusderr.ClassBusy,
		walrusderr.ClassLeaseConflict,
		walrusderr.ClassFlushFailed,
		walrusderr.ClassRemoteUnavailable,
		walrusderr.ClassConflict,
	}
	for _, class := range retryable {
		if !isRetryableWrite(walrusderr.New(class, "retryable")) {
			t.Fatalf("%s is not retryable", class)
		}
	}

	terminal := []walrusderr.Class{
		walrusderr.ClassInvalidArgument,
		walrusderr.ClassConfigurationInvalid,
		walrusderr.ClassIdempotencyMismatch,
	}
	for _, class := range terminal {
		if isRetryableWrite(walrusderr.New(class, "terminal")) {
			t.Fatalf("%s is retryable", class)
		}
	}
}

func TestRetryPolicyHonorsLongerRetryAfter(t *testing.T) {
	p := DefaultRetryPolicy()
	p.FixedDelay = time.Millisecond
	p.MaxDelay = time.Second
	err := walrusderr.Busy("busy", 500)
	if got := p.delay(1, err); got != 500*time.Millisecond {
		t.Fatalf("delay = %s, want Retry-After hint 500ms", got)
	}
}
