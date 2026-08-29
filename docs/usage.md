# WALrus Usage Guide

How to embed and use the WALrus runtime: the Go core, the Node.js/Bun
binding (`@walrus/db`), and the C ABI. For the design and invariants behind
this API, read `docs/specs.md` first — every section below cites it.

## The mental model

- One logical SQLite database per user; object storage (R2/S3) is the
  durable state, held as Litestream LTX files (spec §2–3).
- Any API instance can serve any request. There is no writer fleet, no
  sticky routing (spec §1).
- **Reads** see only remote-committed state. No lease is taken (spec §9).
- **Writes** are serialized per database by a conditional object-storage
  lease, run inside one SQLite transaction, and are acknowledged only after
  the LTX flush is confirmed. A mutation is durable when the flush is
  confirmed — never on a timer (spec §7–8).

```
WithWrite:
  acquire lease (CAS) -> enable VFS write mode -> run transaction ->
  disable write mode (MANDATORY flush barrier) -> release lease (CAS) ->
  return TXID
```

Callers never touch the lease or `litestream_write_enabled` directly; the
runtime owns the whole lifecycle (spec §11).

## Object layout

Everything for a user database lives under one prefix in the org bucket
(spec §4):

```
<root_prefix>/walrus/v1/users/<encoded-user-id>/
  lease.json        # lease record (CAS authority: object ETag)
  replica/          # Litestream replica (LTX files)
```

IDs must be path-safe: no `/`, `?`, `#`, and no percent-encoding needed.
Bucket names, prefixes, and credentials are issued by the control plane —
**never accept them from end users** (spec §11).

---

## Go core

### Build

The core requires CGO (Litestream VFS). Always build/test with the `vfs`
tag:

```sh
go build -tags vfs ./...
go test  -tags vfs ./...
```

### Create a runtime

```go
package main

import (
    "context"

    "walrus/litestream"
    "walrus/runtime"
    "walrus/storage"
)

func main() {
    ctx := context.Background()
    keyID, secret := "...", "..." // from your control plane / secret store

    // The store is the org bucket. Credentials are org-scoped and
    // short-lived; rotate them by rebuilding the descriptor per call.
    store, err := storage.NewS3(ctx, storage.S3Options{
        Endpoint:        "https://<account>.r2.cloudflarestorage.com",
        Bucket:          "my-org-bucket",
        AccessKeyID:     keyID,
        SecretAccessKey: secret,
    })
    if err != nil {
        panic(err)
    }

    rt, err := runtime.New(store, "api-pod-7", runtime.DefaultConfig())
    if err != nil {
        panic(err)
    }
    _ = rt
}
```

`runtime.New` validates configuration and rejects anything that could
release a lease without a confirmed flush — `RequireFlushBeforeRelease`
must stay `true` (spec §16). The lease duration must exceed the request
timeout plus skew allowance; defaults (30s lease, 20s request timeout,
2s skew) satisfy this.

### Describe a database

```go
d := runtime.DatabaseDescriptor{
    OrganizationID: "org_9f2c",   // path-safe
    UserID:         "user_1a4b",  // path-safe
    Storage: litestream.Profile{
        Provider:   "s3",         // "s3" (R2/S3-compatible) or "file"
        Endpoint:   "https://<account>.r2.cloudflarestorage.com",
        Bucket:     "my-org-bucket",
        RootPrefix: "tenants/acme",
    },
    Credentials: runtime.StaticCredentials{AccessKeyID: keyID, SecretAccessKey: secret},
}
```

`Credentials` is a `CredentialSource` interface, so you can plug a token
refresher instead of `StaticCredentials`. It is resolved at call time and
never persisted (spec §11).

### Read

```go
err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
    var body string
    return conn.QueryRowContext(ctx,
        `SELECT body FROM events WHERE id = ?`, 1).Scan(&body)
})
```

You get a real `*sql.Conn` speaking SQLite through the Litestream VFS. No
lease is acquired; the connection observes only flushed remote state
(spec §9). Read instances are cached per database in-process (bounded by
`MaxReadInstances`, idle-evicted after `ReadInstanceIdleTTL`).

### Write

```go
// First write for a fresh database: create the schema as its own write.
_, err := rt.WithWrite(ctx, d, "create-schema", func(conn *sql.Conn) error {
    _, err := conn.ExecContext(ctx,
        `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)`)
    return err
})
if err != nil { panic(err) }

res, err := rt.WithWrite(ctx, d, "create-event-42", func(conn *sql.Conn) error {
    if _, err := conn.ExecContext(ctx,
        `INSERT INTO events (id, body) VALUES (?, ?)`, 42, "hello"); err != nil {
        return err
    }
    return nil
})
if err != nil { /* see error handling below */ }
fmt.Println("durable at txid", res.TXID, "deduplicated:", res.Deduplicated)
```

Rules the runtime enforces:

- `idempotencyKey` is **required** (spec §8). Retrying the same key after
  a timeout/crash returns the recorded result instead of re-running
  (`Deduplicated: true`, same `TXID`). The key and its TXID are recorded in
  the same SQLite transaction as the mutation; a dedup retry returns the
  original TXID, never a placeholder.
- The callback runs in one transaction. Keep it short: the lease, the VFS
  write buffer, and the flush window all span the callback.
- On return, the flush is confirmed and the lease released. `res.TXID` is
  the remote transaction ID — hand it to subsequent reads if you need
  explicit read-after-write reasoning (spec §9).

### Errors and retry

All errors are classified (`walruserr`); match on `Class`, not on strings
(spec §11):

| Class | Meaning | What to do |
|---|---|---|
| `DB_BUSY` | Lease held by a non-expired owner | Retry; honor `Retry-After` hint if present |
| `DB_LEASE_CONFLICT` | CAS conflict or stale release | Retry the whole operation |
| `DB_FLUSH_FAILED` | SQL may be locally committed, remote flush **not** confirmed | Retry with the **same** idempotency key. The lease is left to expire; never treat the write as durable |
| `DB_REMOTE_UNAVAILABLE` | Object store unusable | Back off, retry |
| `DB_CONFLICT` | Litestream observed another writer | Close/reopen; retry |
| `DB_CONFIGURATION_INVALID` | Bad profile/capability/config | Do not retry; fix configuration |
| `DB_INVALID_ARGUMENT` | Bad descriptor, key, or SQL | Do not retry; fix the caller |
| `DB_IDEMPOTENCY_MISMATCH` | Same key reused with different statements | Do not retry; caller bug |

```go
_, err := rt.WithWrite(ctx, d, key, fn)
if walruserr.ClassOf(err) == walruserr.ClassFlushFailed {
    // safe: retry with the same idempotency key
}
```
`DB_FLUSH_FAILED` is the critical case: the write may or may not have
reached object storage. The idempotency key makes the retry idempotent —
that is the whole contract.

---

## Node.js and Bun (`@walrus/db`)

One Node-API addon, loaded by Node directly and by Bun through its
Node-API compatibility layer (spec §11). All SQLite, VFS, lease, and flush
work happens in the Go core behind the C ABI; JavaScript cannot bypass the
lease or the flush barrier.

### Build

```sh
# 1. Go shared library
go build -buildmode=c-shared -tags vfs -o bindings/node/lib/libwalrus.dylib ./bindings/c/lib

# 2. Native addon
cd bindings/node/native && npx node-gyp rebuild

# 3. TypeScript wrapper
cd bindings/node && npx tsc -p .
```

### Use

```ts
import { WALrusDatabase, WALrusError } from "@walrus/db";

const db = new WALrusDatabase({
  owner: "api-pod-7",                 // this instance's identity (lease owner)
  requestTimeoutMs: 20_000,
  // writeBufferRootPath: "/tmp/walrus-buffers",  // optional
});

const descriptor = {
  organization_id: "org_9f2c",
  user_id: "user_1a4b",
  storage: {
    provider: "s3",
    endpoint: "https://<account>.r2.cloudflarestorage.com",
    bucket: "my-org-bucket",
    root_prefix: "tenants/acme",
  },
  credentials: { access_key_id: K, secret_access_key: S },
};

// Write: one batch = one transaction = one lease = one flush.
const { txid } = await db.write({
  database: descriptor,
  idempotencyKey: `create-event-${eventId}`,   // required
  statements: [
    { sql: "INSERT INTO events (id, body) VALUES (?, ?)", params: [eventId, "hello"] },
  ],
});

// Read: remote-committed state only.
const { rows } = await db.read({
  database: descriptor,
  sql: "SELECT * FROM events WHERE id = ?",
  params: [eventId],
});

await db.close();
```

### Errors

`WALrusError` carries `code` (the `DB_*` class) and an optional
`retryAfterMs`:

```ts
try {
  await db.write({ database: d, idempotencyKey: key, statements });
} catch (e) {
  if (e instanceof WALrusError) {
    if (e.code === "DB_FLUSH_FAILED") {
      // retry with the SAME idempotency key — the retry is a no-op if the
      // first attempt actually committed
    } else if (e.code === "DB_BUSY") {
      // retry after e.retryAfterMs
    }
  }
}
```

The same code runs unchanged under Bun — `bindings/bun/walrus.test.ts` is
the executable reference (write+flush+read-back, idempotency validation,
concurrent writers serializing through the lease).

---

## C ABI (other languages)

Six exports, data-oriented: JSON envelopes in, JSON envelopes out, no raw
SQLite pointers ever cross the boundary (spec §11). Build the library:

```sh
go build -buildmode=c-shared -tags vfs -o libwalrus.dylib ./bindings/c/lib
```

```c
const char *walrus_runtime_version(void);
uint64_t    walrus_runtime_init(const char *req, int n);
const char *walrus_runtime_write(uint64_t h, const char *req, int n, long long deadline_ms);
const char *walrus_runtime_read (uint64_t h, const char *req, int n, long long deadline_ms);
const char *walrus_runtime_close(uint64_t h);
void        walrus_free(char *p);   // free every returned string
```

Every response is an envelope:

```json
{ "api_version": 1, "ok": true,  "result": { ... } }
{ "api_version": 1, "ok": false, "error": { "class": "DB_BUSY", "message": "...", "retry_after_ms": 250 } }
```

`init` request (selects the store provider; timing knobs optional and
defaulted from spec §16):

```json
{
  "owner": "api-pod-7",
  "config": { "request_timeout_ms": 20000, "write_buffer_root_path": "/tmp/walrus" }
}
```

`write` request:

```json
{
  "descriptor": { "organization_id": "org", "user_id": "user", "storage": { "provider": "s3", "endpoint": "...", "bucket": "b", "root_prefix": "p" }, "credentials": { "access_key_id": "...", "secret_access_key": "..." } },
  "idempotency_key": "create-event-42",
  "statements": [ { "sql": "INSERT INTO events VALUES (?, ?)", "params": [42, "hello"] } ]
}
```

`deadline_ms` is epoch milliseconds; the core maps it to a context
deadline. Check `api_version` on every response — mismatch means the addon
and core are from different generations (spec §16 gate).

---

## Storage provider conformance

Before onboarding any object-store provider, run the conditional-write
conformance suite against it (spec §5, §15). R2 is verified; `file` is the
local reference implementation:

```sh
# In-memory + file providers run always:
go test -tags vfs ./storage/

# Live provider (env-gated):
WALRUS_TEST_S3_ENDPOINT="https://<account>.r2.cloudflarestorage.com" \
WALRUS_TEST_S3_BUCKET="my-org-bucket" \
WALRUS_TEST_S3_ACCESS_KEY_ID="..." \
WALRUS_TEST_S3_SECRET_ACCESS_KEY="..." \
go test -tags vfs ./storage/ -run TestS3StoreConformance -v
```

The suite asserts exactly the operations the lease protocol depends on:
read-after-write byte consistency, create-if-absent, replace-if-version
(CAS), and delete-if-version. Note: R2 accepts but does not enforce
`If-Match` on `DeleteObject`; the adapter enforces the guard via an atomic
CAS-to-tombstone, so `DeleteIfVersion` remains correct on R2 (see
`storage/s3.go`).

## Configuration reference

Go `runtime.Config` (defaults from spec §16):

| Field | Default | Notes |
|---|---|---|
| `RequestTimeout` | 20s | Per-operation deadline |
| `Lease.Duration` | 30s | Must exceed request timeout + skew (validated) |
| `Lease.ClockSkewAllowance` | 2s | Expiry safety margin |
| `Lease.AcquireRetryBudget` | 3s | Total bounded retry window |
| `Lease.RetryBackoffMin/Max` | 25ms / 500ms | Jittered backoff |
| `ReadInstanceIdleTTL` | 60s | Read-connection cache eviction |
| `MaxReadInstances` | 200 | Bounded per-process cache |
| `MaxTempWriteBuffer` | 256 MiB | Per-process write-buffer cap |
| `RequireFlushBeforeRelease` | `true` | **Must stay true**; runtime rejects `false` |

Litestream (`cfg.Litestream`): `WriteSyncInterval` (1s) only bounds
background syncs; durability always comes from the disable-path flush.
`HydrationEnabled` must stay `false` in normal operation (invariant 11).

## Operational checklist (spec §17–18)

- Every process runs the same core; any instance can serve any request.
- Never log tenant data: the VFS logger is a no-op by design (spec §13).
- Alert on `DB_FLUSH_FAILED` and `DB_REMOTE_UNAVAILABLE` rates; a rising
  flush-failure rate means storage trouble, and unacked writes must be
  retried by clients with their original idempotency keys.
- Expired-lease takeover is automatic: a crashed writer's lease expires
  after `Duration` (+ skew), then another instance CAS-acquires it.
- Version gates: match `api_version` (envelope) and `core_version`
  (`walrus_runtime_version`) across rolling deploys; keep a rollback plan
  (spec §16).
