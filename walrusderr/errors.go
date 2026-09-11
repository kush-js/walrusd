// Package walrusderr defines the classified error model (spec §11). Every
// error surfaced by the runtime maps to exactly one class so callers and
// bindings can make retry decisions without string matching.
package walrusderr

import "errors"

// Class is a stable, machine-readable error classification.
type Class string

const (
	// ClassBusy: lease held by a non-expired owner; safe to retry
	// (optionally after Retry-After).
	ClassBusy Class = "DB_BUSY"
	// ClassLeaseConflict: CAS conflict or stale release; safe to retry
	// after refreshing.
	ClassLeaseConflict Class = "DB_LEASE_CONFLICT"
	// ClassFlushFailed: SQL may be locally committed but the remote LTX
	// flush was not confirmed; retry with the same idempotency key.
	ClassFlushFailed Class = "DB_FLUSH_FAILED"
	// ClassRemoteUnavailable: object store cannot be safely read/written;
	// safe to retry later.
	ClassRemoteUnavailable Class = "DB_REMOTE_UNAVAILABLE"
	// ClassConflict: Litestream observed another writer; close/reopen and
	// retry only through the idempotent flow.
	ClassConflict Class = "DB_CONFLICT"
	// ClassConfigurationInvalid: storage profile/provider capability
	// failure; not retryable without remediation.
	ClassConfigurationInvalid Class = "DB_CONFIGURATION_INVALID"
	// ClassInvalidArgument: caller passed an unusable descriptor, key, or
	// statement batch; not retryable as-is.
	ClassInvalidArgument Class = "DB_INVALID_ARGUMENT"
	// ClassIdempotencyMismatch: same idempotency key reused with a
	// different request payload.
	ClassIdempotencyMismatch Class = "DB_IDEMPOTENCY_MISMATCH"
)

// RetryAfterHint is an optional retry hint carried by ClassBusy errors.
type RetryAfterHint interface {
	RetryAfter() (d int64, ok bool)
}

// Error is a classified walrusd error.
type Error struct {
	Class   Class
	Message string
	// Cause is the underlying error, if any. Never logged with tenant data.
	Cause error
	// retryAfterMs is the Retry-After hint for ClassBusy, in milliseconds.
	retryAfterMs int64
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return string(e.Class) + ": " + e.Message + ": " + e.Cause.Error()
	}
	return string(e.Class) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// RetryAfter implements RetryAfterHint.
func (e *Error) RetryAfter() (int64, bool) {
	if e.Class == ClassBusy && e.retryAfterMs > 0 {
		return e.retryAfterMs, true
	}
	return 0, false
}

// New builds a classified error.
func New(class Class, message string) *Error {
	return &Error{Class: class, Message: message}
}

// Wrap builds a classified error wrapping cause.
func Wrap(class Class, message string, cause error) *Error {
	return &Error{Class: class, Message: message, Cause: cause}
}

// Busy builds a ClassBusy error with a Retry-After hint in milliseconds.
func Busy(message string, retryAfterMs int64) *Error {
	return &Error{Class: ClassBusy, Message: message, retryAfterMs: retryAfterMs}
}

// ClassOf returns the classification of err, or "" when err is not a
// walrusd error.
func ClassOf(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ""
}
