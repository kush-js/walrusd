# Changelog

## 0.5.9

- Probe level 0 before the snapshot level: the live tail answers healthy replicas on the first LIST instead of paying a wasted level-9 scan.

## 0.5.8


- Isolated the empty-replica probe's per-level timeouts: both level LISTs shared one 5s context, so a stalled level-9 page fired the deadline and level 0 inherited the expired context, failing instantly. Each level now gets its own 5s timeout.

## 0.5.7


- `HasLTX` empty-replica probe checks the snapshot level (9) before level 0, matching the order `CalcRestorePlan` lists on every VFS open. A snapshot short-circuits the scan; each LIST stays prefix-narrow.

## 0.5.6

- Fixed a data race between VFS registration and SQLite VFS dispatch: the process-global registry guard was an `RWMutex` held as a read lock across opens, which does not exclude a concurrent `RegisterVFS` writer (upstream keeps its map without a lock of its own). Every holder now takes the full mutex. Also reverted the helper-goroutine open guard, which raced the same way — acquisition stays on the caller, bounded by its context deadline.

## 0.5.5

- Capped the empty-replica `HasLTX` probe (5s) and the read-mode VFS open behind `OpenRead` (10s). Both could stall past every caller deadline on a sick replica, wedging all content reads until the whole call budget burned. Sick replicas now fail fast with a clear error.

## 0.5.4

- Rebuilt prebuilds from the 0.5.3 sources: the 0.5.3 npm tarball shipped a stale `libwalrusd.so` predating the `HasLTX` replica-client reuse fix, so the stall persisted despite the version bump. No code change beyond the rebuild.

## 0.5.3

- Reused the database's own S3 replica client in the `HasLTX` empty-replica probe instead of constructing a fresh client per read. The fresh client paid SDK config load, credential resolution, and a new HTTP connection on every content read, stalling the probe past all deadlines under load (every read degraded to `DB_REMOTE_UNAVAILABLE`). No API change.

## 0.5.0

- Added an in-process `memory` storage provider (`litestream.Profile{Provider: "memory"}`): a litestream replica held in a process-global map with no object storage and no network. Unit tests now exercise the full lease/transaction/flush/read path without MinIO and Redis, and multiple runtimes in one process share the replica like separate API instances share a bucket. State is lost on exit.
- S3 signing region now falls back to the environment when the descriptor omits it: explicit `region`, then `AWS_REGION`, then `AWS_DEFAULT_REGION`, then Litestream's default (bucket lookup for real S3, `us-east-1` for a custom endpoint). Set `AWS_REGION` to pin signing for deployments whose profiles carry no region.
- Errors for a specific database now name it in the message (`DB_REMOTE_UNAVAILABLE: database_id "users/u1": ...`), so multi-tenant logs and alerts identify which content database failed. The `DB_*` class, cause, and `Retry-After` hint are unchanged.

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
