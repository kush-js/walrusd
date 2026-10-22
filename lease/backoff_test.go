package lease

import (
	"testing"
	"time"
)

func TestRetryDelayBacksOffWithinBounds(t *testing.T) {
	m := NewManager(NewMemoryStore(), "test", Config{
		Duration:        30 * time.Second,
		RetryBackoffMin: 25 * time.Millisecond,
		RetryBackoffMax: 500 * time.Millisecond,
	}, nil)

	if got := m.retryDelay(0); got != 25*time.Millisecond {
		t.Fatalf("first retry delay = %v, want 25ms", got)
	}
	previous := time.Duration(0)
	for attempt := 0; attempt < 10; attempt++ {
		got := m.retryDelay(attempt)
		if got < 25*time.Millisecond || got > 500*time.Millisecond {
			t.Fatalf("attempt %d delay = %v, want within 25ms..500ms", attempt, got)
		}
		if attempt > 0 && got < previous/2 {
			t.Fatalf("attempt %d delay = %v, regressed from %v", attempt, got, previous)
		}
		previous = got
	}
	if got := m.retryDelay(10); got < 250*time.Millisecond {
		t.Fatalf("capped delay = %v, want >= 250ms", got)
	}
}
