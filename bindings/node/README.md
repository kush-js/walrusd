# @walrusd/db

Node.js and Bun binding for the walrusd embedded SQLite runtime.

## Installing

```sh
npm install @walrusd/db
```

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
  statements: [{ sql: "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)" }],
});

await db.write({
  database,
  idempotencyKey: "set-greeting",
  statements: [{ sql: "INSERT OR REPLACE INTO kv (k, v) VALUES ('greeting', 'hello')" }],
});

const { rows } = await db.read({
  database,
  sql: "SELECT v FROM kv WHERE k = 'greeting'",
});

console.log(rows[0].v);
await db.close();
```

The package includes prebuilt native artifacts for `linux-x64` and
`linux-arm64` under `prebuilds/<platform>-<arch>/`. Both `walrusd.node` and
`libwalrusd.so` must be present for the host platform. Unsupported platforms
must build from source with Go and CGO enabled.

## RuntimeOptions

All timing knobs are forwarded to the Go core. Defaults are applied when a
field is omitted; `retryMaxTotalMs: 0` disables automatic outer retries.

| Option | Default | Purpose |
|---|---:|---|
| `owner` | required | Lease owner identity for this runtime handle |
| `writeBufferRootPath` | system temp | Optional root for VFS temporary write buffers |
| `requestTimeoutMs` | `20000` | Per-write-attempt timeout and default operation deadline |
| `leaseDurationMs` | `30000` | Lease duration; must exceed `requestTimeoutMs + clockSkewMs` |
| `clockSkewMs` | `2000` | Lease expiry safety margin |
| `acquireRetryBudgetMs` | `3000` | Inner lease-acquisition retry window |
| `retryBackoffMinMs` | `25` | Minimum inner lease retry backoff |
| `retryBackoffMaxMs` | `500` | Maximum inner lease retry backoff |
| `retryFixedDelayMs` | `1000` | First outer write retry delay |
| `retryFixedCount` | `10` | Number of fixed outer retry delays |
| `retryMultiplier` | `2` | Multiplier after the fixed retries |
| `retryMaxDelayMs` | `64000` | Maximum single outer retry delay |
| `retryMaxTotalMs` | `64000` | Whole outer retry wall-clock budget; `0` disables retries |
| `redisAddress` | unset | Redis/Valkey address for shared leases; unset uses process memory |
| `redisPassword` | unset | Redis/Valkey password |
| `redisDB` | `0` | Redis/Valkey database number |

The outer retry schedule is ten `1s` delays, then `2s`, `4s`, `8s`, `16s`,
`32s`, and `64s`, with ±20% jitter and a `64s` total wall-clock cap. The
runtime retries `DB_BUSY`, `DB_LEASE_CONFLICT`, `DB_FLUSH_FAILED`,
`DB_REMOTE_UNAVAILABLE`, and `DB_CONFLICT` using the same idempotency key.
Terminal classes such as `DB_INVALID_ARGUMENT` are returned immediately, and
the caller's deadline always wins.
