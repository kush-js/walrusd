package runtime

import (
	"errors"
	"math/rand"
	"time"

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
