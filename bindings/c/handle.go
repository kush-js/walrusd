package c

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"time"
	"unsafe"

	"walrus/runtime"
	"walrus/storage"
)

// InitRequest configures one runtime instance.
type InitRequest struct {
	Owner  string        `json:"owner"`
	Config runtimeConfig `json:"config,omitempty"`
}

type runtimeConfig struct {
	RequestTimeoutMs     int64  `json:"request_timeout_ms,omitempty"`
	LeaseDurationMs      int64  `json:"lease_duration_ms,omitempty"`
	ClockSkewMs          int64  `json:"clock_skew_ms,omitempty"`
	AcquireRetryBudgetMs int64  `json:"acquire_retry_budget_ms,omitempty"`
	RetryBackoffMinMs    int64  `json:"retry_backoff_min_ms,omitempty"`
	RetryBackoffMaxMs    int64  `json:"retry_backoff_max_ms,omitempty"`
	WriteBufferRootPath  string `json:"write_buffer_root_path,omitempty"`
}

// walrus_runtime_init creates a runtime over an in-process store registry.
// The store provider is chosen by name ("memory", "s3") with connection
// details in the request. Returns the handle id as a JSON envelope.
//
//export walrus_runtime_init
func walrus_runtime_init(requestBytes *C.char, n C.int) C.uint64_t {
	req := C.GoBytes(unsafe.Pointer(requestBytes), n)
	var r InitRequest
	if err := json.Unmarshal(req, &r); err != nil {
		return 0
	}
	cfg := runtime.DefaultConfig()
	if v := r.Config.RequestTimeoutMs; v > 0 {
		cfg.RequestTimeout = time.Duration(v) * time.Millisecond
	}
	if v := r.Config.LeaseDurationMs; v > 0 {
		cfg.Lease.Duration = time.Duration(v) * time.Millisecond
	}
	if v := r.Config.ClockSkewMs; v > 0 {
		cfg.Lease.ClockSkewAllowance = time.Duration(v) * time.Millisecond
	}
	if v := r.Config.AcquireRetryBudgetMs; v > 0 {
		cfg.Lease.AcquireRetryBudget = time.Duration(v) * time.Millisecond
	}
	if v := r.Config.RetryBackoffMinMs; v > 0 {
		cfg.Lease.RetryBackoffMin = time.Duration(v) * time.Millisecond
	}
	if v := r.Config.RetryBackoffMaxMs; v > 0 {
		cfg.Lease.RetryBackoffMax = time.Duration(v) * time.Millisecond
	}
	if p := r.Config.WriteBufferRootPath; p != "" {
		cfg.Litestream.WriteBufferRootPath = p
	}

	var store storage.ConditionalStore
	switch r.Owner {
	default:
		store = storage.NewMemoryStore()
	}

	adapter, err := NewAdapter(store, r.Owner, cfg)
	if err != nil {
		return 0
	}
	handleMu.Lock()
	defer handleMu.Unlock()
	h := nextHandle
	nextHandle++
	runtimes[h] = runtimeEntry{rt: adapter}
	return C.uint64_t(h)
}
