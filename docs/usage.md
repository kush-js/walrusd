# walrusd Usage Guide

How to embed and use the walrusd runtime: the Go core, the Node.js/Bun
binding (`@walrusd/db`), and the C ABI. For the design and invariants behind
this API, read `docs/specs.md` first — every section below cites it.

## The mental model

- One logical SQLite database per user; cheap object storage (no
  conditional writes needed) is the durable state, held as Litestream LTX
  files (spec §2–5). Redis/Valkey holds the write leases (spec §7).
- Any API instance can serve any request. There is no writer fleet, no
  sticky routing (spec §1).
- **Reads** see only remote-committed state. No lease is taken (spec §9).
- **Writes** are serialized per database by a Redis/Valkey lease, run inside one SQLite transaction, and are acknowledged only after
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

Everything for a database lives under one prefix derived from your database
ID (spec §4):

```
<root_prefix>/<database_id>/
  replica/          # Litestream replica (LTX files, plain PUTs)

Leases live in Redis/Valkey under the lease key
`<root_prefix>/<database_id>/lease.json` (fencing token = CAS authority).
Object storage never sees lease traffic, so providers without conditional
writes are fine.
```

The database ID is yours to choose — `user_1a4b`, `users/u1`,
`acme/agents/a7` — any clean, path-safe relative path (no leading/trailing
or duplicate slashes, no `.`/`..` segments, no `?`/`#`, nothing needing
percent-encoding). Databases with different IDs are fully independent:
separate lease, separate replica chain. Bucket names, prefixes, and
credentials are issued by the control plane — **never accept them from end
users** (spec §11).

---

## Go core

### Build

The core requires CGO (Litestream VFS is behind the `vfs` build tag).
Always build, test, and vet with the tag; commands without it fail:

```sh
go build -tags vfs ./...
go test  -tags vfs ./...
go vet   -tags vfs ./...
```

### Create a runtime

:::variants
```go title="create_runtime.go"
package main

import (
    "context"

    "walrusd/lease"
    "walrusd/runtime"
)

func main() {
    ctx := context.Background()

    // Leases live in Redis/Valkey shared by every API instance (one small
    // instance with persistence/AOF is enough). Memory store is dev-only:
    // it serializes within this process but not across instances.
    store, err := lease.NewRedisStore(ctx, lease.RedisOptions{
        Addr: "127.0.0.1:6379",
    })
    if err != nil {
        panic(err)
    }
    defer store.Close()

    rt, err := runtime.New(store, "api-pod-7", runtime.DefaultConfig())
    if err != nil {
        panic(err)
    }
    _ = rt
}
```
```ts title="create_runtime.ts"
import { WalrusdDatabase, WalrusdError } from "@walrusd/db";

async function createRuntime() {
  const db = new WalrusdDatabase({
    owner: "api-pod-7",                 // this instance's identity (lease owner)
    requestTimeoutMs: 20_000,           // per-attempt timeout; default 20_000
    // redisAddress: "127.0.0.1:6379", // optional shared leases
    // (unset = in-process memory leases: dev/single-process only)
  });

  await db.close();
}
```
```c title="create_runtime.c"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *walrusd_runtime_version(void);
uint64_t walrusd_runtime_init(const char *req, int n);
const char *walrusd_runtime_write(uint64_t h, const char *req, int n,
                                  long long deadline_ms);
const char *walrusd_runtime_read(uint64_t h, const char *req, int n,
                                 long long deadline_ms);
const char *walrusd_runtime_close(uint64_t h);
void walrusd_free(char *p);

static void check_ok(const char *json) {
    if (strstr(json, "\"ok\":true") != NULL) return;

    const char *class_start = strstr(json, "\"class\":\"");
    if (class_start != NULL) {
        class_start += strlen("\"class\":\"");
        const char *class_end = strchr(class_start, '"');
        fprintf(stderr, "walrusd error class: %.*s\n",
                (int)(class_end - class_start), class_start);
    } else {
        fprintf(stderr, "walrusd response without ok=true: %s\n", json);
    }
    exit(EXIT_FAILURE);
}

uint64_t create_runtime(const char *buffer_root) {
    const char *version = walrusd_runtime_version();
    check_ok(version);
    walrusd_free((char *)version);

    char request[1024];
    snprintf(request, sizeof(request),
             "{\"owner\":\"api-pod-7\",\"config\":{"
             "\"request_timeout_ms\":20000,"
             "\"write_buffer_root_path\":\"%s\"}}",
             buffer_root);
    uint64_t handle = walrusd_runtime_init(
        request, (int)strlen(request));
    if (handle == 0) {
        fprintf(stderr, "walrusd_runtime_init failed\n");
        exit(EXIT_FAILURE);
    }
    return handle;
}

void close_runtime(uint64_t handle) {
    const char *response = walrusd_runtime_close(handle);
    check_ok(response);
    walrusd_free((char *)response);
}
```
```python title="create_runtime.py"
import ctypes
import json
import os

lib = ctypes.CDLL(os.environ["WALRUSD_LIBRARY"])
buffer_root = os.environ.get("WALRUSD_BUFFER_ROOT", "/tmp/walrusd-buffers")


class WalrusdError(RuntimeError):
    def __init__(self, payload):
        super().__init__(payload["message"])
        self.code = payload["class"]
        self.retry_after_ms = payload.get("retry_after_ms")


lib.walrusd_runtime_version.restype = ctypes.c_void_p
lib.walrusd_runtime_init.argtypes = [ctypes.c_char_p, ctypes.c_int]
lib.walrusd_runtime_init.restype = ctypes.c_uint64
lib.walrusd_runtime_write.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong
]
lib.walrusd_runtime_write.restype = ctypes.c_void_p
lib.walrusd_runtime_read.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong
]
lib.walrusd_runtime_read.restype = ctypes.c_void_p
lib.walrusd_runtime_close.argtypes = [ctypes.c_uint64]
lib.walrusd_runtime_close.restype = ctypes.c_void_p
lib.walrusd_free.argtypes = [ctypes.c_void_p]


def consume(pointer):
    if not pointer:
        raise RuntimeError("walrusd returned a null response")
    envelope = json.loads(ctypes.string_at(pointer).decode())
    lib.walrusd_free(pointer)
    if not envelope["ok"]:
        raise WalrusdError(envelope["error"])
    return envelope.get("result")


def create_runtime():
    consume(lib.walrusd_runtime_version())
    init_request = json.dumps({
        "owner": "api-pod-7",
        "config": {
            "request_timeout_ms": 20_000,
            "write_buffer_root_path": buffer_root,
        },
    }).encode()
    handle = lib.walrusd_runtime_init(init_request, len(init_request))
    if handle == 0:
        raise RuntimeError("walrusd_runtime_init failed")
    return handle


def close_runtime(handle):
    consume(lib.walrusd_runtime_close(handle))
```
:::

`runtime.New` validates configuration and rejects anything that could
release a lease without a confirmed flush — `RequireFlushBeforeRelease`
must stay `true` (spec §16). The lease duration must exceed the request
timeout plus skew allowance; defaults (30s lease, 20s request timeout,
2s skew) satisfy this.

### Describe a database

:::variants
```go
d := runtime.DatabaseDescriptor{
    DatabaseID: "users/user_1a4b", // your ID; objects land at <root_prefix>/users/user_1a4b/
    Storage: litestream.Profile{
        Provider:   "s3",         // "s3" (any S3-compatible) or "file"
        Endpoint:   "https://<account>.r2.cloudflarestorage.com",
        Bucket:     "my-org-bucket",
        RootPrefix: "tenants/acme",
    },
    Credentials: runtime.StaticCredentials{AccessKeyID: keyID, SecretAccessKey: secret},
}
```
```ts title="describe_database.ts"
const root = process.env.WALRUSD_EXAMPLE_ROOT ?? "/tmp/walrusd-example";

const descriptor = {
  database_id: "users/user_1a4b",
  storage: { provider: "file", file_root: root },
  credentials: {},
};
```
```c title="describe_database.c"
char *descriptor_json(const char *root) {
    const char *format =
        "{\"database_id\":\"users/user_1a4b\","
        "\"storage\":{\"provider\":\"file\",\"file_root\":\"%s\"},"
        "\"credentials\":{}}";
    int size = snprintf(NULL, 0, format, root);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, root);
    return json;
}
```
```python title="describe_database.py"
root = os.environ.get("WALRUSD_EXAMPLE_ROOT", "/tmp/walrusd-example")

descriptor = {
    "database_id": "users/user_1a4b",
    "storage": {"provider": "file", "file_root": root},
    "credentials": {},
}
```
:::

`Credentials` is a `CredentialSource` interface, so you can plug a token
refresher instead of `StaticCredentials`. It is resolved at call time and
never persisted (spec §11).

### Read

:::variants
```go
err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
    var body string
    return conn.QueryRowContext(ctx,
        `SELECT body FROM events WHERE id = ?`, 1).Scan(&body)
})
```
```ts title="read.ts"
async function readValue(db, descriptor) {
  const { rows } = await db.read({
    database: descriptor,
    sql: "SELECT body FROM events WHERE id = ?",
    params: [1],
  });
  return rows;
}
```
```c title="read.c"
char *read_request(const char *descriptor) {
    const char *format =
        "{\"descriptor\":%s,\"sql\":"
        "\"SELECT body FROM events WHERE id = 1\"}";
    int size = snprintf(NULL, 0, format, descriptor);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, descriptor);
    return json;
}

char *read_value(uint64_t handle, const char *descriptor,
                 long long deadline) {
    char *body = read_request(descriptor);
    const char *response = walrusd_runtime_read(
        handle, body, (int)strlen(body), deadline);
    check_ok(response);

    const char *value_start = strstr(response, "\"body\":\"");
    if (value_start == NULL) {
        fprintf(stderr, "walrusd read response has no body: %s\n", response);
        exit(EXIT_FAILURE);
    }
    value_start += strlen("\"body\":\"");
    const char *value_end = strchr(value_start, '"');
    if (value_end == NULL) exit(EXIT_FAILURE);
    size_t size = (size_t)(value_end - value_start);
    char *value = malloc(size + 1);
    if (value == NULL) exit(EXIT_FAILURE);
    memcpy(value, value_start, size);
    value[size] = '\0';

    walrusd_free(body);
    walrusd_free((char *)response);
    return value;
}
```
```python title="read.py"
def read_value(handle, descriptor, deadline):
    request = json.dumps({
        "descriptor": descriptor,
        "sql": "SELECT body FROM events WHERE id = ?",
        "params": [1],
    }).encode()
    result = consume(lib.walrusd_runtime_read(
        handle, request, len(request), deadline))
    return result["rows"][0]["body"]
```
:::

You get a real `*sql.Conn` speaking SQLite through the Litestream VFS. No
lease is acquired; the connection observes only flushed remote state
(spec §9). Reads are served lazily page-by-page from object storage through
the per-database `VFSPageCacheBytes` page cache. The whole database is never
downloaded, and Litestream hydration is disabled.

Resident database VFS entries are bounded by `MaxReadInstances` using LRU
eviction. Read sessions are separately cached up to the same limit and are
closed after `ReadInstanceIdleTTL` idle time. A non-positive
`ReadInstanceIdleTTL` disables the read-session cache entirely. A
non-positive `MaxReadInstances` makes the resident-database LRU fall back to
64 entries and also disables the read-session cache.

Eviction is safe: durable state remains in object storage, and the next
access re-opens the database and re-fetches pages on demand. The approximate
read-path memory ceiling is `MaxReadInstances * VFSPageCacheBytes`, plus the
resident VFS and replica-client overhead per database. At the defaults, 200
fully warm 10 MiB caches can use about 2 GiB.

### Write

:::variants
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
```ts title="write.ts"
const { txid } = await db.write({
  database: descriptor,
  idempotencyKey: "create-event-42",
  statements: [
    { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
    { sql: "INSERT INTO events (id, body) VALUES (?, ?)", params: [1, "hello from walrusd"] },
  ],
});
```
```c title="write.c"
char *write_request(const char *descriptor) {
    const char *format =
        "{\"descriptor\":%s,\"idempotency_key\":\"create-event-42\","
        "\"statements\":["
        "{\"sql\":\"CREATE TABLE IF NOT EXISTS events "
        "(id INTEGER PRIMARY KEY, body TEXT)\"},"
        "{\"sql\":\"INSERT INTO events (id, body) VALUES (?, ?)\","
        "\"params\":[1,\"hello from walrusd\"]}]}";
    int size = snprintf(NULL, 0, format, descriptor);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, descriptor);
    return json;
}

char *write_txid(uint64_t handle, const char *descriptor,
                 long long deadline) {
    char *body = write_request(descriptor);
    const char *response = walrusd_runtime_write(
        handle, body, (int)strlen(body), deadline);
    check_ok(response);

    const char *txid_start = strstr(response, "\"txid\":\"");
    if (txid_start == NULL) {
        fprintf(stderr, "walrusd write response has no txid: %s\n", response);
        exit(EXIT_FAILURE);
    }
    txid_start += strlen("\"txid\":\"");
    const char *txid_end = strchr(txid_start, '"');
    if (txid_end == NULL) exit(EXIT_FAILURE);
    size_t size = (size_t)(txid_end - txid_start);
    char *txid = malloc(size + 1);
    if (txid == NULL) exit(EXIT_FAILURE);
    memcpy(txid, txid_start, size);
    txid[size] = '\0';

    walrusd_free(body);
    walrusd_free((char *)response);
    return txid;
}
```
```python title="write.py"
def write_txid(handle, descriptor, deadline):
    request = json.dumps({
        "descriptor": descriptor,
        "idempotency_key": "create-event-42",
        "statements": [
            {"sql": "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)"},
            {"sql": "INSERT INTO events (id, body) VALUES (?, ?)", "params": [1, "hello from walrusd"]},
        ],
    }).encode()
    result = consume(lib.walrusd_runtime_write(
        handle, request, len(request), deadline))
    return result["txid"]
```
:::

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
- `WithWrite` automatically retries `DB_BUSY`, `DB_LEASE_CONFLICT`,
  `DB_FLUSH_FAILED`, `DB_REMOTE_UNAVAILABLE`, and `DB_CONFLICT` with the
  same idempotency key. The default retry schedule is ten 1s delays followed
  by doubling delays (`2s`, `4s`, `8s`, `16s`, `32s`, `64s`) with ±20%
  jitter and a 64s total wall-clock budget. A `Retry-After` hint longer than
  the computed delay is honored.
- The caller's context deadline always wins. If the retry budget is
  exhausted, the final classified error (for example `DB_BUSY`) is
  returned to the caller.

### Errors and retry

All errors are classified (`walrusderr`); match on `Class`, not on strings
(spec §11):

The runtime retries only the five transient classes listed below. Retries
wrap the whole operation, so the same idempotency key is reused and a retry
whose first attempt committed is detected from the transactionally recorded
dedup row and returns `Deduplicated: true` without invoking the callback
again. Terminal errors are never retried.

| Class | Meaning | What to do |
|---|---|---|
| `DB_BUSY` | Lease held by a non-expired owner | Runtime retries automatically; if the retry budget is exhausted, retry later or let the caller deadline win |
| `DB_LEASE_CONFLICT` | CAS conflict or stale release | Runtime retries the whole operation automatically |
| `DB_FLUSH_FAILED` | SQL may be locally committed, remote flush **not** confirmed | Runtime retries with the **same** idempotency key. The lease is left to expire; never treat the write as durable |
| `DB_REMOTE_UNAVAILABLE` | Object store unusable | Runtime backs off and retries automatically |
| `DB_CONFLICT` | Litestream observed another writer | Runtime retries automatically through the idempotent flow |
| `DB_CONFIGURATION_INVALID` | Bad profile/capability/config | Do not retry; fix configuration |
| `DB_INVALID_ARGUMENT` | Bad descriptor, key, or SQL | Do not retry; fix the caller |
| `DB_IDEMPOTENCY_MISMATCH` | Same key reused with different statements | Do not retry; caller bug |

:::variants
```go
_, err := rt.WithWrite(ctx, d, key, fn)
if walrusderr.ClassOf(err) == walrusderr.ClassFlushFailed {
    // safe: retry with the same idempotency key
}
```
```ts title="errors.ts"
function shouldRetry(error: unknown): boolean {
  if (error instanceof WalrusdError) {
    if (error.code === "DB_FLUSH_FAILED") {
      // retry with the SAME idempotency key — the retry is a no-op if the
      // first attempt actually committed
      return true;
    } else if (error.code === "DB_BUSY") {
      // retry after error.retryAfterMs
      return error.retryAfterMs !== undefined;
    }
  }
  return false;
}
```
```c title="errors.c"
void report_retry(const char *json) {
    if (strstr(json, "\"ok\":false") == NULL) return;

    const char *class_start = strstr(json, "\"class\":\"");
    const char *retry_start = strstr(json, "\"retry_after_ms\":");
    if (class_start != NULL && retry_start != NULL) {
        class_start += strlen("\"class\":\"");
        const char *class_end = strchr(class_start, '"');
        retry_start += strlen("\"retry_after_ms\":");
        fprintf(stderr, "walrusd error class: %.*s (retry after %ld ms)\n",
                (int)(class_end - class_start), class_start,
                strtol(retry_start, NULL, 10));
    }
}
```
```python title="errors.py"
def should_retry(error):
    if error.code == "DB_FLUSH_FAILED":
        # safe: retry with the same idempotency key
        return True
    if error.code == "DB_BUSY":
        return error.retry_after_ms is not None
    return False
```
:::
`DB_FLUSH_FAILED` is the critical case: the write may or may not have
reached object storage. The idempotency key makes the retry idempotent —
that is the whole contract. The runtime never acknowledges a mutation or
releases its lease until the same VFS instance confirms the remote flush;
automatic retries repeat the entire operation and never release a lease that
saw a failed flush.

---

## Node.js and Bun (`@walrusd/db`)

One Node-API addon, loaded by Node directly and by Bun through its
Node-API compatibility layer (spec §11). All SQLite, VFS, lease, and flush
work happens in the Go core behind the C ABI; JavaScript cannot bypass the
lease or the flush barrier.

### Build

```sh
# 1. Go shared library
go build -buildmode=c-shared -tags vfs -trimpath -ldflags="-s -w" -o bindings/node/lib/libwalrusd.dylib ./bindings/c/lib

# 2. Native addon
cd bindings/node/native && npx node-gyp rebuild

# 3. TypeScript wrapper
cd bindings/node && npx tsc -p .
```

The package's `test` script invokes Bun, so Bun is required to run
`npm test`.

### Use

```ts
import { WalrusdDatabase, WalrusdError } from "@walrusd/db";

const db = new WalrusdDatabase({
  owner: "api-pod-7",                 // this instance's identity (lease owner)
  requestTimeoutMs: 20_000,           // per-attempt timeout; default 20_000
  // writeBufferRootPath: "/tmp/walrusd-buffers",  // optional
  redisAddress: "127.0.0.1:6379",    // required for shared leases;
  // redisPassword: "...", redisDB: 0,            // optional
  // (unset = in-process memory leases: dev/single-process only)
});

const descriptor = {
  database_id: "users/user_1a4b",
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

`WalrusdError` carries `code` (the `DB_*` class) and an optional
`retryAfterMs`:

```ts
try {
  await db.write({ database: d, idempotencyKey: key, statements });
} catch (e) {
  if (e instanceof WalrusdError) {
    if (e.code === "DB_FLUSH_FAILED") {
      // retry with the SAME idempotency key — the retry is a no-op if the
      // first attempt actually committed
    } else if (e.code === "DB_BUSY") {
      // retry after e.retryAfterMs
    }
  }
}
```

The same code runs unchanged under Bun — `bindings/bun/walrusd.test.ts` is
the executable reference (write+flush+read-back, idempotency validation,
concurrent writers serializing through the lease).

---

## C ABI (other languages)

Six exports, data-oriented: JSON envelopes in, JSON envelopes out, no raw
SQLite pointers ever cross the boundary (spec §11). Build the library:

```sh
go build -buildmode=c-shared -tags vfs -trimpath -ldflags="-s -w" -o libwalrusd.dylib ./bindings/c/lib
```

```c
const char *walrusd_runtime_version(void);
uint64_t    walrusd_runtime_init(const char *req, int n);
const char *walrusd_runtime_write(uint64_t h, const char *req, int n, long long deadline_ms);
const char *walrusd_runtime_read (uint64_t h, const char *req, int n, long long deadline_ms);
const char *walrusd_runtime_close(uint64_t h);
void        walrusd_free(char *p);   // free every returned string
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
  "config": {
    "request_timeout_ms": 20000,
    "lease_duration_ms": 30000,
    "clock_skew_ms": 2000,
    "acquire_retry_budget_ms": 3000,
    "retry_backoff_min_ms": 25,
    "retry_backoff_max_ms": 500,
    "retry_fixed_delay_ms": 1000,
    "retry_fixed_count": 10,
    "retry_multiplier": 2,
    "retry_max_delay_ms": 64000,
    "retry_max_total_ms": 64000,
    "write_buffer_root_path": "/tmp/walrusd",
    "max_read_instances": 200,
    "read_instance_idle_ttl_ms": 60000,
    "vfs_page_cache_bytes": 10485760,
    "write_sync_interval_ms": 1000,
    "max_temp_write_buffer": 268435456,
    "redis_address": "127.0.0.1:6379"
  }
}
```

`retry_max_total_ms: 0` disables automatic outer retries. The existing
`retry_backoff_min_ms` / `retry_backoff_max_ms` fields configure the inner
lease-acquisition loop; the `retry_fixed_*`, `retry_multiplier`,
`retry_max_delay_ms`, and `retry_max_total_ms` fields configure the outer
whole-operation retry policy.

`max_read_instances` bounds the resident database LRU and read-session
cache. A value at or below 0 uses the runtime fallback of 64 resident
databases and disables the read-session cache. `read_instance_idle_ttl_ms`
closes an idle read session; a value at or below 0 disables the read-session
cache entirely. `vfs_page_cache_bytes` is the per-database page-cache size
in bytes. `write_sync_interval_ms` is the background VFS sync interval
forwarded to Litestream configuration; the synchronous flush before lease
release remains the durability barrier. `max_temp_write_buffer` caps
per-process temporary write-buffer bytes, and a value at or below 0 disables
the cap.

`write` request:

```json
{
  "descriptor": { "database_id": "users/user_1", "storage": { "provider": "s3", "endpoint": "...", "bucket": "b", "root_prefix": "p" }, "credentials": { "access_key_id": "...", "secret_access_key": "..." } },
  "idempotency_key": "create-event-42",
  "statements": [ { "sql": "INSERT INTO events VALUES (?, ?)", "params": [42, "hello"] } ]
}
```

`deadline_ms` is epoch milliseconds; the core maps it to a context
deadline. Check `api_version` on every response — mismatch means the addon
and core are from different generations (spec §16 gate).

---

## Lease-store conformance

Before onboarding any lease backend, run the CAS conformance suite
(`lease/conformance_test.go`, spec §15). Memory runs always; Redis/Valkey
runs env-gated (any RESP-compatible server, e.g. Valkey):

```sh
go test -tags vfs ./lease/

WALRUSD_TEST_REDIS_ADDR="127.0.0.1:6379" \
go test -tags vfs ./lease/ -run 'TestRedis' -v
```

The suite asserts exactly the operations the lease protocol depends on:
read-after-write, create-if-absent exclusivity, token-gated replace with
token rotation, and exactly-one-winner under concurrent create/replace.

Object-storage providers need no CAS suite: reads must observe completed
PUTs and listings must expose the expected LTX state (`file` is the local
reference implementation).

## Configuration reference

Go `runtime.Config` (defaults from spec §16):

| Field | Default | Notes |
|---|---|---|
| `RequestTimeout` | 20s | Per-write-attempt deadline |
| `Lease.Duration` | 30s | Must exceed request timeout + skew (validated) |
| `Lease.ClockSkewAllowance` | 2s | Expiry safety margin |
| `Lease.AcquireRetryBudget` | 3s | Total bounded retry window |
| `Lease.RetryBackoffMin/Max` | 25ms / 500ms | Jittered backoff |
| `RetryPolicy.FixedDelay` | 1s | Initial outer retry delay |
| `RetryPolicy.FixedRetries` | 10 | Fixed-delay retry count before exponential backoff |
| `RetryPolicy.Multiplier` | 2 | Exponential delay multiplier |
| `RetryPolicy.MaxDelay` | 64s | Maximum single outer retry delay |
| `RetryPolicy.MaxTotal` | 64s | Whole outer retry wall-clock budget; `0` disables retries |
| `ReadInstanceIdleTTL` | 60s | Close an idle read session; `<=0` disables the read-session cache |
| `MaxReadInstances` | 200 | Resident-database LRU and read-session cache bound; `<=0` falls back to 64 resident databases and disables the read-session cache |
| `Litestream.VFSPageCacheBytes` | 10 MiB | Per-database SQLite page cache |
| `Litestream.WriteSyncInterval` | 1s | Background VFS sync interval; the synchronous flush before lease release remains authoritative |
| `MaxTempWriteBuffer` | 256 MiB | Per-process write-buffer cap; `<=0` disables the cap |
| `RequireFlushBeforeRelease` | `true` | **Must stay true**; runtime rejects `false` |

Reads fetch only the pages SQLite requests and cache them up to
`VFSPageCacheBytes` per resident database. Litestream's hydrator, which
would materialize a local database file, is compiled in but disabled:
`HydrationEnabled` must stay `false` in normal operation (invariant 11).
The approximate read-path memory ceiling is
`MaxReadInstances * VFSPageCacheBytes`, plus resident VFS and replica-client
overhead per database. At the defaults, 200 fully warm 10 MiB caches can use
about 2 GiB. LRU eviction drops the database's VFS, page cache, and replica
client; data remains durable in object storage and the next access re-opens
and re-fetches pages on demand.

## Operational checklist (spec §17–18)

- Every process runs the same core; any instance can serve any request.
- Never log tenant data: the VFS logger is a no-op by design (spec §13).
- Alert on `DB_FLUSH_FAILED` and `DB_REMOTE_UNAVAILABLE` rates; a rising
  flush-failure rate means storage trouble, and unacked writes must be
  retried by clients with their original idempotency keys.
- Expired-lease takeover is automatic: a crashed writer's lease expires
  after `Duration` (+ skew), then another instance CAS-acquires it.
- Run Redis/Valkey with persistence (AOF): a restart that wipes lease keys
  lets a new owner create while a stale holder still believes it owns the
  lease (spec §7.1).
- Version gates: match `api_version` (envelope) and `core_version`
  (`walrusd_runtime_version`) across rolling deploys; keep a rollback plan
  (spec §16).
