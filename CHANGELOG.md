# Changelog

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
