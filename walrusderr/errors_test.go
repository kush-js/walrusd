package walrusderr_test

import (
	"errors"
	"testing"

	"walrusd/walrusderr"
)

func TestClassOf(t *testing.T) {
	err := walrusderr.New(walrusderr.ClassInvalidArgument, "bad request")
	if got := walrusderr.ClassOf(err); got != walrusderr.ClassInvalidArgument {
		t.Fatalf("ClassOf() = %q, want %q", got, walrusderr.ClassInvalidArgument)
	}
	if got := walrusderr.ClassOf(errors.New("plain")); got != "" {
		t.Fatalf("ClassOf(plain) = %q, want empty", got)
	}
}

func TestWrapPreservesCause(t *testing.T) {
	cause := errors.New("underlying")
	err := walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, "read replica", cause)
	if !errors.Is(err, cause) {
		t.Fatal("wrapped error does not preserve its cause")
	}
	if got := walrusderr.ClassOf(err); got != walrusderr.ClassRemoteUnavailable {
		t.Fatalf("ClassOf() = %q, want %q", got, walrusderr.ClassRemoteUnavailable)
	}
	want := "DB_REMOTE_UNAVAILABLE: read replica: underlying"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestRetryAfterHint(t *testing.T) {
	var retryErr walrusderr.RetryAfterHint = walrusderr.Busy("database is busy", 250)
	if got, ok := retryErr.RetryAfter(); !ok || got != 250 {
		t.Fatalf("RetryAfter() = %d, %v; want 250, true", got, ok)
	}

	for _, err := range []*walrusderr.Error{
		walrusderr.New(walrusderr.ClassBusy, "busy"),
		walrusderr.Busy("busy", 0),
		walrusderr.New(walrusderr.ClassConflict, "conflict"),
	} {
		if got, ok := err.RetryAfter(); ok || got != 0 {
			t.Fatalf("RetryAfter() = %d, %v; want 0, false", got, ok)
		}
	}
}
