package c

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"time"
	"unsafe"

	"walrusd/lease"
	"walrusd/runtime"
)

// InitRequest configures one runtime instance.
type InitRequest struct {
	Owner  string        `json:"owner"`
	Config runtimeConfig `json:"config,omitempty"`
}

type runtimeConfig struct {
	RequestTimeoutMs      int64    `json:"request_timeout_ms,omitempty"`
	LeaseDurationMs       int64    `json:"lease_duration_ms,omitempty"`
	ClockSkewMs           int64    `json:"clock_skew_ms,omitempty"`
	AcquireRetryBudgetMs  int64    `json:"acquire_retry_budget_ms,omitempty"`
	RetryBackoffMinMs     int64    `json:"retry_backoff_min_ms,omitempty"`
	RetryBackoffMaxMs     int64    `json:"retry_backoff_max_ms,omitempty"`
	RetryFixedDelayMs     *int64   `json:"retry_fixed_delay_ms,omitempty"`
	RetryFixedCount       *int     `json:"retry_fixed_count,omitempty"`
	RetryMultiplier       *float64 `json:"retry_multiplier,omitempty"`
	RetryMaxDelayMs       *int64   `json:"retry_max_delay_ms,omitempty"`
	RetryMaxTotalMs       *int64   `json:"retry_max_total_ms,omitempty"`
	WriteBufferRootPath   string   `json:"write_buffer_root_path,omitempty"`
	MaxReadInstances      *int     `json:"max_read_instances,omitempty"`
	ReadInstanceIdleTTLMs *int64   `json:"read_instance_idle_ttl_ms,omitempty"`
	VFSPageCacheBytes     *int     `json:"vfs_page_cache_bytes,omitempty"`
	WriteSyncIntervalMs   *int64   `json:"write_sync_interval_ms,omitempty"`
	MaxTempWriteBuffer    *int64   `json:"max_temp_write_buffer,omitempty"`
	// Redis backing for leases (shared deployments). Empty address falls
	// back to an in-process memory store: fine for dev and single-process
	// use, but it serializes only within this handle — cross-process
	// writers need Redis/Valkey with persistence (AOF) enabled.
	RedisAddr     string `json:"redis_address,omitempty"`
	RedisPassword string `json:"redis_password,omitempty"`
	RedisDB       int    `json:"redis_db,omitempty"`
}

// walrusd_runtime_init creates a runtime over a lease store chosen by config.
// The store provider is Redis/Valkey when redis_address is set, else an
// in-process memory store. Returns the handle id as a JSON envelope.
//
//export walrusd_runtime_init
func walrusd_runtime_init(requestBytes *C.char, n C.int) C.uint64_t {
	req := C.GoBytes(unsafe.Pointer(requestBytes), n)
	var r InitRequest
	if err := json.Unmarshal(req, &r); err != nil {
		return 0
	}
	cfg := buildRuntimeConfig(r.Config)

	var store lease.Store = lease.NewMemoryStore()
	if addr := r.Config.RedisAddr; addr != "" {
		rs, err := lease.NewRedisStore(context.Background(), lease.RedisOptions{
			Addr:     addr,
			Password: r.Config.RedisPassword,
			DB:       r.Config.RedisDB,
		})
		if err != nil {
			return 0
		}
		store = rs
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

func buildRuntimeConfig(r runtimeConfig) runtime.Config {
	cfg := runtime.DefaultConfig()
	if v := r.RequestTimeoutMs; v > 0 {
		cfg.RequestTimeout = time.Duration(v) * time.Millisecond
	}
	if v := r.LeaseDurationMs; v > 0 {
		cfg.Lease.Duration = time.Duration(v) * time.Millisecond
	}
	if v := r.ClockSkewMs; v > 0 {
		cfg.Lease.ClockSkewAllowance = time.Duration(v) * time.Millisecond
	}
	if v := r.AcquireRetryBudgetMs; v > 0 {
		cfg.Lease.AcquireRetryBudget = time.Duration(v) * time.Millisecond
	}
	if v := r.RetryBackoffMinMs; v > 0 {
		cfg.Lease.RetryBackoffMin = time.Duration(v) * time.Millisecond
	}
	if v := r.RetryBackoffMaxMs; v > 0 {
		cfg.Lease.RetryBackoffMax = time.Duration(v) * time.Millisecond
	}
	if v := r.RetryFixedDelayMs; v != nil {
		cfg.RetryPolicy.FixedDelay = time.Duration(*v) * time.Millisecond
	}
	if v := r.RetryFixedCount; v != nil {
		cfg.RetryPolicy.FixedRetries = *v
	}
	if v := r.RetryMultiplier; v != nil {
		cfg.RetryPolicy.Multiplier = *v
	}
	if v := r.RetryMaxDelayMs; v != nil {
		cfg.RetryPolicy.MaxDelay = time.Duration(*v) * time.Millisecond
	}
	if v := r.RetryMaxTotalMs; v != nil {
		cfg.RetryPolicy.MaxTotal = time.Duration(*v) * time.Millisecond
	}
	if p := r.WriteBufferRootPath; p != "" {
		cfg.Litestream.WriteBufferRootPath = p
	}
	if v := r.MaxReadInstances; v != nil {
		cfg.MaxReadInstances = *v
	}
	if v := r.ReadInstanceIdleTTLMs; v != nil {
		cfg.ReadInstanceIdleTTL = time.Duration(*v) * time.Millisecond
	}
	if v := r.VFSPageCacheBytes; v != nil {
		cfg.Litestream.VFSPageCacheBytes = *v
	}
	if v := r.WriteSyncIntervalMs; v != nil {
		cfg.Litestream.WriteSyncInterval = *v
	}
	if v := r.MaxTempWriteBuffer; v != nil {
		cfg.MaxTempWriteBuffer = *v
	}
	return cfg
}
