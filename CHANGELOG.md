# Changelog

## 0.4.0

- Exposed five previously Go-only runtime tuning knobs through the C ABI and JS bindings: `max_read_instances` (200), `read_instance_idle_ttl_ms` (60000), `vfs_page_cache_bytes` (10485760), `write_sync_interval_ms` (1000), and `max_temp_write_buffer` (268435456). JS consumers previously could not tune these options.
- `max_read_instances` bounds resident databases, while `vfs_page_cache_bytes` applies per database, giving an approximate read-path memory ceiling of `max_read_instances * vfs_page_cache_bytes` (~2 GiB at the defaults). Reads are served lazily page-by-page from object storage; no whole-database download is performed, and Litestream hydration stays disabled.
- Pointer-typed option parsing preserves an explicit `0`: `max_read_instances=0` falls back to 64 resident databases and disables the read-session cache, while `read_instance_idle_ttl_ms=0` disables the read-session cache entirely.
- Rebuilt the Node README as a complete options reference with units, verified defaults, a tuning/limits section, and the error classes.

## 0.3.1

- Fixed the process-global sqlite3vfs registry leak: registrations had no unregister path, so each eviction and re-access added a permanent registration. In testing, live registrations grew by one per distinct database per cycle, from 300 to 1800 over six cycles without plateauing, and tenants exceeding `MaxReadInstances` leaked per request until OOM.
- Registrations are now unregistered from SQLite before removing the map entry. Cached database entries are refcounted so teardown runs only after the last holder releases, and the cached read session is drained first. Residual: sqlite3vfs allocates the `sqlite3_vfs` struct and never frees it.

## 0.3.0

- Added a bounded retry policy for runtime writes: ten 1s delays followed by 2/4/8/16/32/64s delays, with +/-20% jitter and a 64s total budget.
- Retries use the same idempotency key so failed attempts cannot double-apply. Retryable classes are `DB_BUSY`, `DB_LEASE_CONFLICT`, `DB_FLUSH_FAILED`, `DB_REMOTE_UNAVAILABLE`, and `DB_CONFLICT`; caller cancellation still aborts immediately.
- Exposed lease and retry timing knobs in the JavaScript binding.

## 0.2.2

- Stripped the shipped Go library.

## 0.2.1

- Added repository metadata for npm provenance.

## 0.2.0

- Rebranded to walrusd and published the first npm release.
