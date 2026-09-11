package walruserr_test

import (
	"errors"
	"testing"

	"walrus/walruserr"
)

func TestClassOf(t *testing.T) {
	err := walruserr.New(walruserr.ClassInvalidArgument, "bad request")
	if got := walruserr.ClassOf(err); got != walruserr.ClassInvalidArgument {
		t.Fatalf("ClassOf() = %q, want %q", got, walruserr.ClassInvalidArgument)
	}
	if got := walruserr.ClassOf(errors.New("plain")); got != "" {
		t.Fatalf("ClassOf(plain) = %q, want empty", got)
	}
}

func TestWrapPreservesCause(t *testing.T) {
	cause := errors.New("underlying")
	err := walruserr.Wrap(walruserr.ClassRemoteUnavailable, "read replica", cause)
	if !errors.Is(err, cause) {
		t.Fatal("wrapped error does not preserve its cause")
	}
	if got := walruserr.ClassOf(err); got != walruserr.ClassRemoteUnavailable {
		t.Fatalf("ClassOf() = %q, want %q", got, walruserr.ClassRemoteUnavailable)
	}
	want := "DB_REMOTE_UNAVAILABLE: read replica: underlying"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestRetryAfterHint(t *testing.T) {
	var retryErr walruserr.RetryAfterHint = walruserr.Busy("database is busy", 250)
	if got, ok := retryErr.RetryAfter(); !ok || got != 250 {
		t.Fatalf("RetryAfter() = %d, %v; want 250, true", got, ok)
	}

	for _, err := range []*walruserr.Error{
		walruserr.New(walruserr.ClassBusy, "busy"),
		walruserr.Busy("busy", 0),
		walruserr.New(walruserr.ClassConflict, "conflict"),
	} {
		if got, ok := err.RetryAfter(); ok || got != 0 {
			t.Fatalf("RetryAfter() = %d, %v; want 0, false", got, ok)
		}
	}
}
