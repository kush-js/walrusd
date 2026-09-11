package c

import (
	"encoding/json"
	"testing"
	"time"

	"walrusd/runtime"
)

func TestBuildRuntimeConfigReadAndWriteKnobs(t *testing.T) {
	const requestJSON = `{
		"owner": "api-test",
		"config": {
			"max_read_instances": 7,
			"read_instance_idle_ttl_ms": 1250,
			"vfs_page_cache_bytes": 1048576,
			"write_sync_interval_ms": 250,
			"max_temp_write_buffer": 4194304
		}
	}`

	var req InitRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		t.Fatalf("unmarshal init request: %v", err)
	}
	cfg := buildRuntimeConfig(req.Config)

	if got, want := cfg.MaxReadInstances, 7; got != want {
		t.Errorf("MaxReadInstances = %d, want %d", got, want)
	}
	if got, want := cfg.ReadInstanceIdleTTL, 1250*time.Millisecond; got != want {
		t.Errorf("ReadInstanceIdleTTL = %s, want %s", got, want)
	}
	if got, want := cfg.Litestream.VFSPageCacheBytes, 1048576; got != want {
		t.Errorf("VFSPageCacheBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Litestream.WriteSyncInterval, int64(250); got != want {
		t.Errorf("WriteSyncInterval = %d, want %d", got, want)
	}
	if got, want := cfg.MaxTempWriteBuffer, int64(4194304); got != want {
		t.Errorf("MaxTempWriteBuffer = %d, want %d", got, want)
	}
}

func TestBuildRuntimeConfigReadAndWriteKnobsDefaultWhenOmitted(t *testing.T) {
	cfg := buildRuntimeConfig(runtimeConfig{})
	defaults := runtime.DefaultConfig()

	if cfg.MaxReadInstances != defaults.MaxReadInstances {
		t.Errorf("MaxReadInstances = %d, want default %d", cfg.MaxReadInstances, defaults.MaxReadInstances)
	}
	if cfg.ReadInstanceIdleTTL != defaults.ReadInstanceIdleTTL {
		t.Errorf("ReadInstanceIdleTTL = %s, want default %s", cfg.ReadInstanceIdleTTL, defaults.ReadInstanceIdleTTL)
	}
	if cfg.Litestream.VFSPageCacheBytes != defaults.Litestream.VFSPageCacheBytes {
		t.Errorf("VFSPageCacheBytes = %d, want default %d", cfg.Litestream.VFSPageCacheBytes, defaults.Litestream.VFSPageCacheBytes)
	}
	if cfg.Litestream.WriteSyncInterval != defaults.Litestream.WriteSyncInterval {
		t.Errorf("WriteSyncInterval = %d, want default %d", cfg.Litestream.WriteSyncInterval, defaults.Litestream.WriteSyncInterval)
	}
	if cfg.MaxTempWriteBuffer != defaults.MaxTempWriteBuffer {
		t.Errorf("MaxTempWriteBuffer = %d, want default %d", cfg.MaxTempWriteBuffer, defaults.MaxTempWriteBuffer)
	}
}

func TestBuildRuntimeConfigReadAndWriteKnobsPreserveExplicitZero(t *testing.T) {
	const requestJSON = `{
		"owner": "api-test",
		"config": {
			"max_read_instances": 0,
			"read_instance_idle_ttl_ms": 0,
			"vfs_page_cache_bytes": 0,
			"write_sync_interval_ms": 0,
			"max_temp_write_buffer": 0
		}
	}`

	var req InitRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		t.Fatalf("unmarshal init request: %v", err)
	}
	cfg := buildRuntimeConfig(req.Config)

	if cfg.MaxReadInstances != 0 {
		t.Errorf("MaxReadInstances = %d, want 0", cfg.MaxReadInstances)
	}
	if cfg.ReadInstanceIdleTTL != 0 {
		t.Errorf("ReadInstanceIdleTTL = %s, want 0", cfg.ReadInstanceIdleTTL)
	}
	if cfg.Litestream.VFSPageCacheBytes != 0 {
		t.Errorf("VFSPageCacheBytes = %d, want 0", cfg.Litestream.VFSPageCacheBytes)
	}
	if cfg.Litestream.WriteSyncInterval != 0 {
		t.Errorf("WriteSyncInterval = %d, want 0", cfg.Litestream.WriteSyncInterval)
	}
	if cfg.MaxTempWriteBuffer != 0 {
		t.Errorf("MaxTempWriteBuffer = %d, want 0", cfg.MaxTempWriteBuffer)
	}
}
