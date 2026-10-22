# Changelog

## 0.6.1

- **Compaction survives VFS eviction.** The trigger is now the durable manifest written after each successful mutation, not an in-memory write counter tied to the resident database entry. A sparse tenant evicted between writes can no longer grow an unbounded L0 chain. The compaction pass also checks every compactable level instead of stopping at the first level below threshold, so a busy higher level still merges.
- **Compaction runs in a bounded worker pool.** `Compaction.Workers` / `compaction_workers` defaults to 4. A per-database pending marker keeps at most one queued or running pass, so an over-threshold chain cannot flood the queue. `compaction_interval` is retained for configuration compatibility but no longer controls the trigger.
- **Local lease waiters wake on release.** Same-process writers queued behind one database lease now wait on a release notification instead of each paying the 25–500 ms cross-process jittered backoff. Redis/Valkey CAS remains the cross-process authority, and same-database writes remain serialized by design.
- **Lease retry backs off exponentially.** Cross-process waiters start at `retry_backoff_min` (25 ms) and double toward `retry_backoff_max` (500 ms), instead of choosing uniformly across the whole range on every attempt. A momentarily held lease no longer costs a few hundred milliseconds.

## 0.6.0

Production-hardening pass from a full pre-production audit, rebased onto 0.5.17's durable-manifest and async-compaction architecture. Several response values and one config knob change.

- **The LTX chain no longer grows without bound.** Confirmed on top of 0.5.15-0.5.17: compaction and pruning keep the chain bounded (see those entries for the manifest and separate-lease machinery). This pass adds the regression coverage: compaction, pruning, and read-back after pruning.
- **Leases are heartbeated while held.** A slow-but-correct transaction or flush can no longer outlive its lease and fork the LTX chain under a successor. Renewal is bounded to three lease durations, so a wedged process is still takeable by expiry — crash takeover semantics are unchanged. `lease.Manager` gains `Abandon` for paths that must leave a lease to expire; unconfirmed flushes use it.
- **Deterministic SQL errors are terminal.** Constraint violations, read-only databases, and similar failures now surface once as `DB_INVALID_ARGUMENT` / `DB_CONFIGURATION_INVALID` instead of being retried for the full 64s budget as `DB_CONFLICT`. Busy/locked/timeout errors stay retryable.
- **A write attempt that outlives its deadline is rolled back and fails** instead of racing sqlite's context interruption to commit after the caller stopped waiting.
- **Read rows: BLOB values are now base64 strings.** Previously they were coerced through a UTF-8 JSON string, silently corrupting non-UTF-8 bytes. Duplicate column names now get `_2`, `_3`, ... suffixes instead of collapsing (`SELECT a.id, b.id` keeps both).
- **Parameters: exact 64-bit integers.** Number literals are bound as int64 instead of float64, so JSON clients that emit exact literals no longer round above 2^53. TypeScript callers get a clear `DB_INVALID_ARGUMENT` for unsafe integers and bigints (pass decimal strings; INTEGER affinity converts losslessly), and `Uint8Array`/`Buffer` parameters bind as BLOB (`{"$blob": "<base64>"}` on the wire).
- **Read results are capped** by `MaxReadResultBytes` (default 64 MiB, `max_read_result_bytes` in the C ABI, `maxReadResultBytes` in `@walrusd/db`): a runaway query fails cleanly instead of exhausting process memory.
- **Node/Bun concurrency:** blocking core calls moved off libuv's 4-thread pool onto dedicated worker threads with threadsafe-function delivery. Concurrent database calls no longer throttle each other or starve file/DNS work. Shared-library symbol resolution is serialized; `init` failures now surface the underlying error message via `walrusd_runtime_init_error`.
- Removed `write_sync_interval_ms` / `WriteSyncInterval`: the knob was plumbed but never reached the VFS (writes always flush synchronously before lease release — that is the durability barrier). Removed from the C ABI config, `RuntimeOptions`, and docs. This applies the removal 0.5.17 already made to the `litestream` package; superseded is the old belief that the registry lock is held across a session callback (a bounded semaphore was never needed once `sqlite3vfs` moved to a `sync.Map`).
- Smaller fixes: Redis client closed on failed adapter construction; `replica_url` query values are URL-escaped; `failWithLease` releases on a non-cancellable context so a just-expired caller deadline does not leak the lease window; release conflicts after a confirmed flush are handled without acknowledging an ambiguous write; the lease single-flight map is pruned (bounded by in-flight acquisitions, not databases ever seen).

## 0.5.17

- **VFS registry concurrency**: vendored `psanford/sqlite3vfs` with a `sync.Map` dispatch table. The process-global registry mutex is no longer held across VFS opens or SQL execution, so requests for different databases run concurrently again.
- **Durable manifest**: every database gets a `replica/manifest.json` written after each flush. Opening a database is one manifest GET instead of `HasLTX` plus ten level LISTs (and one per level for the restore plan). Legacy replicas fall back to listing and are migrated on the next write; a level-0 tail check repairs an unindexed file after a failed manifest PUT.
- **Async compaction**: merges run in a background worker under a separate compaction lease, outside the write lease. The manifest is updated under the write lease before source files are deleted; an opportunistic GC removes superseded objects after retention.
- **No page cache by default**: `VFSPageCacheBytes` now defaults to 0. Callers that want an in-process page cache opt in explicitly.
- **Auxiliary leases**: `lease.Manager.AcquireKey` for compaction locks that must not collide with the write lease.
- Manifest and async-compaction tests, including legacy fallback, read-after-write, compaction update, and a manifest client that never lists.

## 0.5.16

- `HasLTX` now probes every compaction level, not just the live tail (0) and snapshot (9). Compaction writes deltas to levels 1..8, so a compacted database could have both 0 and 9 empty while holding data; reads then treated it as an empty database and failed with "no such table".
- `MetricsTotal` sums the compaction counters, so `Compactions`/`CompactedLevels`/`CompactionFailures` no longer read as zero when a compaction ran.

## 0.5.15

- Compact the replica chain under the write lease. Every `compaction_interval` writes, each level with more than `compaction_threshold` files is merged into the next (L0 through L8; L9 is Litestream's snapshot level and is never written by a merge), and merged sources are deleted after `compaction_retention_ms` so in-flight readers can finish. Without this the chain grew by one file per write forever, and every write re-read the whole chain.

## 0.5.14

- Split the replica cache in two: `replica_cache_bytes` now covers LTX file bytes only, and `level_listing_cache_ttl_ms` (default 30s) covers the compaction-level listings. The listings are metadata, so a process can keep `replica_cache_bytes` at 0 to hold no file data while still skipping nine of the ten level LISTs per VFS open.

## 0.5.13


## 0.5.12

- Reverted two guards added while chasing a stall that turned out to live elsewhere (the deployment image shipped no CA certificates, so Go could not verify the object store over TLS). The probe timeout is back to 5s: a healthy replica answers in ~500ms, and the larger budget only delayed the report of a stall. The probe also calls the SDK iterator directly again instead of on a watched goroutine — the iterator context already reaches the HTTP request, so the goroutine guarded nothing and could leak one per stalled probe.

## 0.5.11


- Run the empty-replica probe's first-page LIST on its own goroutine with a timeout race: the SDK paginator only watches its context between retries, so a hung response body stalls inside one attempt past every deadline.

## 0.5.10


- Empty-replica probe timeout 5s to 12s: the first LIST pays connection setup and the store's tail latency, tripping 5s while warm probes answer in ~100ms. Stays inside the 15s native attempt budget.

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
