package c

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"sync"
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
	RequestTimeoutMs       int64    `json:"request_timeout_ms,omitempty"`
	LeaseDurationMs        int64    `json:"lease_duration_ms,omitempty"`
	ClockSkewMs            int64    `json:"clock_skew_ms,omitempty"`
	AcquireRetryBudgetMs   int64    `json:"acquire_retry_budget_ms,omitempty"`
	RetryBackoffMinMs      int64    `json:"retry_backoff_min_ms,omitempty"`
	RetryBackoffMaxMs      int64    `json:"retry_backoff_max_ms,omitempty"`
	RetryFixedDelayMs      *int64   `json:"retry_fixed_delay_ms,omitempty"`
	RetryFixedCount        *int     `json:"retry_fixed_count,omitempty"`
	RetryMultiplier        *float64 `json:"retry_multiplier,omitempty"`
	RetryMaxDelayMs        *int64   `json:"retry_max_delay_ms,omitempty"`
	RetryMaxTotalMs        *int64   `json:"retry_max_total_ms,omitempty"`
	WriteBufferRootPath    string   `json:"write_buffer_root_path,omitempty"`
	MaxReadInstances       *int     `json:"max_read_instances,omitempty"`
	ReadInstanceIdleTTLMs  *int64   `json:"read_instance_idle_ttl_ms,omitempty"`
	VFSPageCacheBytes      *int     `json:"vfs_page_cache_bytes,omitempty"`
	MaxTempWriteBuffer     *int64   `json:"max_temp_write_buffer,omitempty"`
	MaxReadResultBytes     *int64   `json:"max_read_result_bytes,omitempty"`
	ReplicaCacheBytes      *int64   `json:"replica_cache_bytes,omitempty"`
	LevelListingCacheTTLMs *int64   `json:"level_listing_cache_ttl_ms,omitempty"`
	CompactionEnabled      *bool    `json:"compaction_enabled,omitempty"`
	CompactionInterval     *int     `json:"compaction_interval,omitempty"`
	CompactionThreshold    *int     `json:"compaction_threshold,omitempty"`
	CompactionRetentionMs  *int64   `json:"compaction_retention_ms,omitempty"`
	CompactionTimeoutMs    *int64   `json:"compaction_timeout_ms,omitempty"`
	CompactionWorkers      *int     `json:"compaction_workers,omitempty"`
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
// in-process memory store. Returns the handle id as a JSON envelope; on
// failure it returns 0 and walrusd_runtime_init_error reports why.
//
//export walrusd_runtime_init
func walrusd_runtime_init(requestBytes *C.char, n C.int) C.uint64_t {
	setInitError("invalid init request")
	req := C.GoBytes(unsafe.Pointer(requestBytes), n)
	var r InitRequest
	if err := json.Unmarshal(req, &r); err != nil {
		setInitError(err.Error())
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
			setInitError(err.Error())
			return 0
		}
		store = rs
	}

	adapter, err := NewAdapter(store, r.Owner, cfg)
	if err != nil {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		setInitError(err.Error())
		return 0
	}
	handleMu.Lock()
	defer handleMu.Unlock()
	h := nextHandle
	nextHandle++
	runtimes[h] = runtimeEntry{rt: adapter}
	setInitError("")
	return C.uint64_t(h)
}

var initErrMu sync.Mutex
var initErrMessage string

func setInitError(msg string) {
	initErrMu.Lock()
	initErrMessage = msg
	initErrMu.Unlock()
}

// walrusd_runtime_init_error returns the last walrusd_runtime_init failure
// message (empty string when the last init succeeded).
//
//export walrusd_runtime_init_error
func walrusd_runtime_init_error() *C.char {
	initErrMu.Lock()
	defer initErrMu.Unlock()
	return C.CString(initErrMessage)
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
	if v := r.MaxTempWriteBuffer; v != nil {
		cfg.MaxTempWriteBuffer = *v
	}
	if v := r.ReplicaCacheBytes; v != nil {
		cfg.Litestream.ReplicaCacheBytes = *v
	}
	if v := r.LevelListingCacheTTLMs; v != nil {
		cfg.Litestream.LevelListingCacheTTL = time.Duration(*v) * time.Millisecond
	}
	if v := r.CompactionEnabled; v != nil {
		cfg.Compaction.Enabled = *v
	}
	if v := r.CompactionInterval; v != nil {
		cfg.Compaction.Interval = *v
	}
	if v := r.CompactionThreshold; v != nil {
		cfg.Compaction.Threshold = *v
	}
	if v := r.CompactionRetentionMs; v != nil {
		cfg.Compaction.Retention = time.Duration(*v) * time.Millisecond
	}
	if v := r.CompactionTimeoutMs; v != nil {
		cfg.Compaction.Timeout = time.Duration(*v) * time.Millisecond
	}
	if v := r.CompactionWorkers; v != nil {
		cfg.Compaction.Workers = *v
	}
	if v := r.MaxReadResultBytes; v != nil {
		cfg.MaxReadResultBytes = *v
	}
	return cfg
}
