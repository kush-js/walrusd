# @walrusd/db

Node.js and Bun bindings for the walrusd embedded SQLite runtime.
Writes run as one SQLite transaction under a Redis/Valkey lease and are
acknowledged only after the transaction is flushed to object storage.
Reads see remote-committed state through Litestream's VFS without taking a
lease and without downloading a local copy of the database.

The JavaScript API is batch-oriented. All SQLite, VFS, lease, and flush work
happens in the Go core behind the C ABI.

## Install

```sh
npm install @walrusd/db
```

The published package includes prebuilt native artifacts for `linux-x64`
and `linux-arm64` under `prebuilds/<platform>-<arch>/`. Both
`walrusd.node` and `libwalrusd.so` must be present for the host platform.
Other platforms must build from source with Go and CGO enabled.

## Minimal Example

```ts
import { WalrusdDatabase } from "@walrusd/db";

const db = new WalrusdDatabase({ owner: "my-api-instance" });
const database = {
  database_id: "users/user_1",
  storage: { provider: "file", file_root: "/tmp/walrusd-example" },
  credentials: {},
};

await db.write({
  database,
  idempotencyKey: "schema",
  statements: [{
    sql: "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)",
  }],
});

await db.write({
  database,
  idempotencyKey: "set-greeting",
  statements: [{
    sql: "INSERT OR REPLACE INTO kv (k, v) VALUES ('greeting', 'hello')",
  }],
});

const { rows } = await db.read({
  database,
  sql: "SELECT v FROM kv WHERE k = 'greeting'",
});

console.log(rows[0].v);
await db.close();
```

In production, use an S3-compatible storage profile and set
`redisAddress` so all API instances share leases. The `file` provider is
intended for local development and tests.

## RuntimeOptions

Every option is optional except `owner`. Omitted fields keep the Go core
defaults listed below. Units are part of the option contract.

### Lease

| Option | Default | Unit | Purpose |
|---|---:|---|---|
| `owner` | required | string | Lease owner identity for this runtime handle |
| `requestTimeoutMs` | `20000` | milliseconds | Per-write-attempt timeout and the default read/write operation deadline |
| `leaseDurationMs` | `30000` | milliseconds | Lease duration; must exceed `requestTimeoutMs + clockSkewMs` |
| `clockSkewMs` | `2000` | milliseconds | Lease expiry safety margin |
| `acquireRetryBudgetMs` | `3000` | milliseconds | Inner lease-acquisition retry window |

### Retry

| Option | Default | Unit | Purpose |
|---|---:|---|---|
| `retryBackoffMinMs` | `25` | milliseconds | Minimum inner lease retry backoff |
| `retryBackoffMaxMs` | `500` | milliseconds | Maximum inner lease retry backoff |
| `retryFixedDelayMs` | `1000` | milliseconds | First outer write retry delay |
| `retryFixedCount` | `10` | count | Fixed outer retry delays before exponential backoff |
| `retryMultiplier` | `2` | multiplier | Delay multiplier after the fixed retries |
| `retryMaxDelayMs` | `64000` | milliseconds | Maximum single outer retry delay |
| `retryMaxTotalMs` | `64000` | milliseconds | Whole outer retry wall-clock budget; `0` disables automatic retries |

The default outer schedule is ten `1s` delays followed by `2s`, `4s`, `8s`,
`16s`, `32s`, and `64s`, with +/-20% jitter and a `64s` total wall-clock cap.

### Read Path

| Option | Default | Unit | Purpose |
|---|---:|---|---|
| `maxReadInstances` | `200` | databases | Maximum resident database read instances (an LRU). Values at or below `0` use the runtime fallback of 64 resident databases and disable the read-session cache |
| `readInstanceIdleTtlMs` | `60000` | milliseconds | Close a database's read session after this much idle time. Values at or below `0` disable the read-session cache entirely |
| `vfsPageCacheBytes` | `10485760` | bytes | Per-database SQLite page cache size |

### Write Buffer

| Option | Default | Unit | Purpose |
|---|---:|---|---|
| `writeBufferRootPath` | system temp | path | Root for VFS temporary write buffers |
| `writeSyncIntervalMs` | `1000` | milliseconds | Background VFS sync interval forwarded to Litestream configuration; the synchronous flush before lease release remains the durability barrier |
| `maxTempWriteBuffer` | `268435456` | bytes | Per-process temporary write-buffer cap. Values at or below `0` disable the cap |

### Redis

| Option | Default | Unit | Purpose |
|---|---:|---|---|
| `redisAddress` | unset | `host:port` | Redis/Valkey address for shared leases. Unset uses an in-process memory store, which serializes only within this handle |
| `redisPassword` | unset | string | Redis/Valkey password |
| `redisDB` | `0` | database number | Redis/Valkey database number |

## Tuning And Limits

`maxReadInstances` is an LRU limit on resident database read instances. Each
resident entry keeps its VFS, page cache, and replica client until it is
evicted. `readInstanceIdleTtlMs` separately closes a cached read session
after the configured idle period. A non-positive TTL disables the
read-session cache entirely, so every read opens a fresh session.

Reads are served lazily, page by page, from object storage through the
per-database `vfsPageCacheBytes` cache. The runtime never downloads the
whole database; Litestream hydration, which would materialize a local
database file, is disabled.

The approximate read-path memory ceiling is:

```text
maxReadInstances * vfsPageCacheBytes
```

At the defaults, 200 resident databases with fully warm 10 MiB page caches
can use about 2 GiB (`200 * 10485760` bytes), plus the resident VFS and
replica-client overhead for each database. Use a smaller page cache, a
smaller resident limit, or both when memory is constrained.

Eviction is safe. The durable data is in object storage; evicting a database
drops its VFS, page cache, and replica client, and the next access opens a
fresh read path and re-fetches pages on demand.

## Errors

Failed operations reject with `WalrusdError`. Its `code` is one of the
stable `DB_*` classes below, and busy errors may include `retryAfterMs`.
The runtime automatically retries the five retryable classes using the same
idempotency key. The other three are terminal until the caller or
configuration is corrected.

| Code | Retryable | Meaning |
|---|---|---|
| `DB_BUSY` | Yes | Lease held by a non-expired owner |
| `DB_LEASE_CONFLICT` | Yes | CAS conflict or stale lease release |
| `DB_FLUSH_FAILED` | Yes | SQL may be locally committed, but the remote flush was not confirmed. Retry with the same idempotency key |
| `DB_REMOTE_UNAVAILABLE` | Yes | Object storage is temporarily unusable |
| `DB_CONFLICT` | Yes | Litestream observed another writer |
| `DB_CONFIGURATION_INVALID` | No | Invalid storage profile, capability, or runtime configuration |
| `DB_INVALID_ARGUMENT` | No | Invalid descriptor, idempotency key, or SQL request |
| `DB_IDEMPOTENCY_MISMATCH` | No | The same idempotency key was reused with different statements |
