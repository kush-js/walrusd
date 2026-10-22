package runtime

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/mattn/go-sqlite3"

	"walrusd/litestream"
	"walrusd/walrusderr"
)

// RetryPolicy controls the runtime-owned outer retry loop around WithWrite.
// A zero policy (MaxTotal <= 0) disables retries; it never retries forever.
type RetryPolicy struct {
	FixedDelay   time.Duration // 1s
	FixedRetries int           // 10
	Multiplier   float64       // 2
	MaxDelay     time.Duration // 64s
	MaxTotal     time.Duration // 64s
}

// DefaultRetryPolicy returns the required runtime write-retry defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		FixedDelay:   1 * time.Second,
		FixedRetries: 10,
		Multiplier:   2,
		MaxDelay:     64 * time.Second,
		MaxTotal:     64 * time.Second,
	}
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxTotal <= 0 {
		return RetryPolicy{}
	}
	def := DefaultRetryPolicy()
	if p.FixedDelay <= 0 {
		p.FixedDelay = def.FixedDelay
	}
	if p.FixedRetries < 0 {
		p.FixedRetries = 0
	}
	if p.Multiplier <= 0 {
		p.Multiplier = def.Multiplier
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = def.MaxDelay
	}
	return p
}

// baseDelay returns the unjittered delay before retry number n (1-based).
func (p RetryPolicy) baseDelay(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	delay := p.FixedDelay
	if n > p.FixedRetries {
		steps := n - p.FixedRetries
		for i := 0; i < steps; i++ {
			delay = time.Duration(float64(delay) * p.Multiplier)
			if p.MaxDelay > 0 && delay >= p.MaxDelay {
				delay = p.MaxDelay
				break
			}
		}
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}

// delay returns the jittered delay before retry number n. Retry-After wins
// when it is longer than the computed delay.
func (p RetryPolicy) delay(n int, err error) time.Duration {
	delay := jitterDelay(p.baseDelay(n))
	if hint := retryAfter(err); hint > delay {
		delay = hint
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}

func jitterDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return delay
	}
	const jitter = 0.20
	factor := 1 + (rand.Float64()*2-1)*jitter
	return time.Duration(float64(delay) * factor)
}

func retryAfter(err error) time.Duration {
	var hint walrusderr.RetryAfterHint
	if !errors.As(err, &hint) {
		return 0
	}
	ms, ok := hint.RetryAfter()
	if !ok || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// isRetryableWrite classifies only the explicitly retryable write errors.
// Unknown or unclassified errors are terminal by default.
func isRetryableWrite(err error) bool {
	switch walrusderr.ClassOf(err) {
	case walrusderr.ClassBusy,
		walrusderr.ClassLeaseConflict,
		walrusderr.ClassFlushFailed,
		walrusderr.ClassRemoteUnavailable,
		walrusderr.ClassConflict:
		return true
	default:
		return false
	}
}

// classifySQLError maps an error from the runtime's SQL/session layer to
// the class that drives both the caller-visible code and the outer retry
// decision. A deterministic SQLite failure (constraint violation,
// read-only database, corruption) must not burn the retry budget re-running
// a transaction that will fail identically every time.
func classifySQLError(err error) walrusderr.Class {
	if errors.Is(err, litestream.ErrRegistryBusy) {
		return walrusderr.ClassBusy
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Attempt-scoped timeouts repeat with the same idempotency key.
		return walrusderr.ClassBusy
	}
	var se sqlite3.Error
	if errors.As(err, &se) {
		switch se.Code {
		case sqlite3.ErrBusy, sqlite3.ErrLocked:
			return walrusderr.ClassBusy
		case sqlite3.ErrConstraint, sqlite3.ErrRange, sqlite3.ErrMismatch:
			return walrusderr.ClassInvalidArgument
		case sqlite3.ErrReadonly, sqlite3.ErrPerm, sqlite3.ErrFull,
			sqlite3.ErrTooBig, sqlite3.ErrCorrupt, sqlite3.ErrNotADB:
			return walrusderr.ClassConfigurationInvalid
		}
	}
	return walrusderr.ClassConflict
}

// wrapSessionOpen classifies a failed session open: a busy VFS registry is
// retryable contention; anything else is remote unavailability.
func wrapSessionOpenError(msg string, err error) error {
	if errors.Is(err, litestream.ErrRegistryBusy) {
		return walrusderr.Wrap(walrusderr.ClassBusy, msg, err)
	}
	return walrusderr.Wrap(walrusderr.ClassRemoteUnavailable, msg, err)
}
