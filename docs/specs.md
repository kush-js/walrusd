# walrusd Embedded SQLite Runtime Specification

**Status:** implementation specification  
**Audience:** engineering team or coding agent  
**Core language:** Go (CGO), with Node.js and Bun bindings  
**Primary principle:** object storage holds durable SQLite state; API processes run disposable database compute.

## 1. Objective

Build **walrusd**, a multi-tenant SQLite runtime for Basemnt in which every user has one logical SQLite database and every organization owns the object-storage bucket that persists its databases.

The runtime is embedded directly in each API process. Its core is written in Go because Litestream VFS requires CGO. Go services import the core package directly; Node.js and Bun services use a thin native binding backed by the same Go core. There is no dedicated writer-worker fleet, no Consul, no centralized database router, no sticky sessions, and no worker-to-worker forwarding. Any API instance can handle any request. Before a mutation, its embedded runtime takes a Redis/Valkey lease for that user's database, performs the SQLite transaction through Litestream VFS write mode, synchronously flushes the resulting LTX data to object storage, and conditionally releases the lease.

The system is deliberately simple:

```text
Load balancer -> any stateless API -> embedded walrusd runtime
                                        (Go package or Node/Bun native binding)
                                                      |
                                               SQLite + Litestream VFS
                                                      |
                                          organization-owned object storage
```

## 2. Goals and non-goals

### Goals

- One logical SQLite database per application-chosen `database_id`.
- One object-storage bucket/configuration per organization.
- Basemnt-managed and customer-controlled object storage.
- Stateless API instances with no affinity requirement.
- A single Go implementation shared by Go, Node.js, and Bun API services.
- Redis/Valkey leases to serialize writes to one user database.
- Litestream VFS reads from remote replica data without normal full local hydration.
- Synchronous remote LTX flush before a successful mutation response or lease release.
- Bounded, ephemeral API-local cache and temporary write-buffer state.
- Clean recovery when a process dies mid-request.

### Non-goals for v1

- A dedicated database writer fleet or a central database router.
- Consul, etcd, Redis, or any separate distributed lock service.
- Multiple concurrent writers to one user database.
- Cross-user/cross-organization transactions.
- A guarantee that stock Litestream VFS alone provides storage-enforced fencing against malicious or buggy writers.
- Python support. It requires a separate binding and is intentionally deferred.

## 3. Important Litestream constraints

Litestream VFS is a CGO-backed SQLite VFS. In write mode it maintains an ephemeral local write buffer and periodically uploads changed pages as incremental LTX files. It assumes a single writer; it detects competing writers but does not prevent them.

This runtime supplies the application-level single-writer coordination that Litestream requires. It must not claim stronger semantics than it implements:

- A conditional lease prevents multiple **well-behaved walrusd runtimes** from intentionally entering write mode at once.
- A successful flush proves the runtime uploaded its LTX data before releasing the lease.
- A lost/expired lease cannot cryptographically stop a faulty process that ignores the protocol and continues uploading LTX data.
- Any future requirement for hard, storage-enforced stale-writer fencing requires a fork/wrapper of Litestream's replica write path or a different storage format.

Litestream already maintains the authoritative logical database state as an ordered LTX chain and transaction ID. Do not add an independent “current database manifest,” duplicate database snapshots per transaction, or an application-defined commit sequence for v1.

## 4. Tenant identity and object layout

The embedding application owns database identity: each database is one application-chosen `database_id` — a canonical, path-safe relative key (for example `user_1a4b`, `users/u1`, or `acme/agents/a7`). A shared Go package validates it and derives all object paths:

```text
database_id = "<application-chosen relative path>"
```

Never use mutable names, a worker/API identity, request IDs, or client-supplied storage keys in this identifier.

Each organization has a storage profile:

```text
provider, endpoint, region, bucket, root_prefix,
credential_reference, encryption_policy, capability_result
```

Bucket layout:

```text
<root_prefix>/<database_id>/
  lease.json
  replica/                 # Litestream LTX files and its own metadata
  metadata/schema.json
```

`database_id` is chosen by the embedding application (for example
`user_1a4b`, `users/u1`, or `acme/agents/a7`) and must be a canonical,
path-safe relative key. walrusd imposes no `users/`-style structure on it.
Database paths and lease paths are always derived by trusted code from the
storage profile and canonical database ID.

## 5. Object storage requirements

Object storage holds only the Litestream replica (LTX chain). It needs
plain GET/PUT/DELETE plus correct list/read semantics for the replica
path — no conditional-write support required. Leases live in Redis/Valkey
(spec §7), so cheap providers without conditional writes (Wasabi,
Backblaze B2, Sliplane) are acceptable as long as reads observe completed
PUTs (read-after-write) and listings expose the expected LTX state.

## 6. Runtime architecture

The Go module is the sole implementation of database semantics. It is packaged for direct Go use and through a small C ABI used by the Node/Bun native addon:

```text
walrusd/
  identity/       canonical database IDs and object paths
  storage/        provider adapters and capability validation
  lease/          CAS lease acquisition, renewal, release, retry policy
  runtime/        request-scoped VFS and SQLite lifecycle
  litestream/     CGO-backed VFS integration and flush adapter
  cache/          bounded process-local read/VFS cache
  observability/  logs, metrics, traces, error classification
  conformance/    storage and lease protocol test vectors

bindings/
  c/              narrow C ABI exported from the CGO build
  node/           Node-API addon and TypeScript package
  bun/            Bun compatibility/integration test package
```

No binding reimplements lease JSON, object-key construction, retry behavior, or Litestream lifecycle handling. Those semantics remain in Go and are tested once.

Do not use the existing Litestream Python/Node loadable extension for this multi-tenant runtime: its replica URL is configured from process environment at startup and is not suitable for selecting a tenant bucket/database per request. The walrusd Node/Bun addon calls the Go core instead, which receives a runtime database descriptor per operation.

## 7. Lease protocol

### 7.1 Lease record

Every database has exactly one lease record, keyed by its lease key
(`<root_prefix>/<database_id>/lease.json` by default) in Redis/Valkey:

```json
{
  "format_version": 1,
  "database_id": "users/user_456",
  "state": "held",
  "owner": "api-7f5c",
  "lease_id": "4b48dac0-1e4a-4418-9c6b-9ac5e51e0b61",
  "epoch": 42,
  "expires_at": "2026-08-28T15:00:30.000Z"
}
```

Fields:

- `owner`: unique API-process instance ID, used for diagnostics only.
- `lease_id`: random UUID generated for each successful acquisition.
- `epoch`: incremented for every successful acquisition; records ownership generations and makes stale activity visible.
- `state`: `held` or `released`.
- `expires_at`: upper bound on the lease's intended lifetime; used only for crash recovery/takeover.

The store's fencing token (minted fresh on every successful write) is the
CAS token. It is not interchangeable with `epoch`. All store mutations are
single-key atomic (Lua); CAS is never emulated with read-then-write.
Lease keys carry no TTL: logical expiry governs takeover, and a TTL firing
early would hand the lease to a successor while the old owner flushes.
Run Redis/Valkey with persistence (AOF): a restart that wipes keys lets a
new owner create while a stale holder still believes it owns the lease.

### 7.2 Acquire

Each logical mutation acquires the lease before any SQLite write begins:

1. `GET` the lease key and capture body + fencing token, or observe `NotFound`.
2. If absent, call `CreateIfAbsent` with `state=held`, a new lease ID, epoch `1`, and a short expiry.
3. If present and `state=released` or the lease is safely expired, call `ReplaceIfToken` using the exact retrieved token. Set `state=held`, a new lease ID, `epoch=previous+1`, and a new expiry.
4. If held and unexpired, do not write. Return a retryable busy result or wait using bounded, jittered retry.
5. On conditional conflict, reread and repeat with backoff. Never overwrite without the retrieved token.

Lease acquisition must be single-flight inside an API process for the same database ID to avoid local thundering herds. The Redis/Valkey CAS remains the authority across processes.

### 7.3 Release and crash recovery

After a successful remote flush, conditionally replace the lease record with `state=released` using the fencing token held by the lease owner. The release keeps the record and its epoch history rather than deleting it.

If the API process crashes, it cannot release the lease. A later requester waits until `expires_at`, applies a conservative clock-skew allowance, then takes over with a conditional replace. Lease expiry is not a success condition for the old request; clients whose requests were interrupted must retry with the same idempotency key.

Do not hold an HTTP connection indefinitely while waiting. A contender uses a bounded internal retry budget, then returns `DB_BUSY` with a `Retry-After` hint. There is no fairness queue.

### 7.4 Lease duration

Start with:

```yaml
lease:
  duration: 30s
  clock_skew_allowance: 2s
  acquire_retry_budget: 3s
  retry_backoff: 25ms..500ms
```

The lease duration must exceed the configured request timeout plus expected remote-flush time plus skew allowance. A request must abort and leave the lease to expire if it cannot complete safely within the duration. v1 does not renew a lease during a normal short mutation; long-running writes must either renew with conditional CAS or be rejected/split into short transactions.

## 8. Write lifecycle

The API handler calls a single high-level operation such as:

```go
err := runtime.WithWrite(ctx, descriptor, idempotencyKey, func(conn *sql.Conn) error {
    // BEGIN, SQL work, COMMIT
    return nil
})
```

`WithWrite` enforces the following exact sequence on one dedicated SQLite connection/VFS instance:

```text
resolve trusted database descriptor
      -> conditionally acquire lease
      -> open/refresh remote Litestream VFS state
      -> enable VFS write mode
      -> run and commit SQLite transaction
      -> disable VFS write mode
      -> wait for synchronous remote LTX flush to succeed
      -> conditionally release lease
      -> acknowledge success
```

The write-mode disable operation is the mandatory flush barrier. The current VFS implementation flushes dirty pages before returning from the disable path and returns an error if synchronization fails. The library must invoke it on the **same connection/VFS instance** that performed the transaction. A generic connection pool must not run the flush or disable operation on a different connection.

If any step from acquisition through flush fails:

- do not report mutation success;
- do not release the lease until the library has resolved the state or the lease expires;
- close/discard the VFS instance and temporary write buffer;
- return a retryable error where appropriate;
- rely on the idempotency key to make the caller retry safe.

Never replace the flush barrier with a fixed sleep. A sync interval schedules work; it does not prove that an upload completed successfully.

### Idempotency

Every mutation must carry an idempotency key, unique for `(database_id, logical operation)`. The callback transaction records the key and its result inside the user database in the same SQLite transaction as the mutation. A retry after a timeout or process crash obtains the lease, reads the prior result, and returns it rather than repeating the operation.

## 9. Read lifecycle

Reads require no write lease. The runtime opens a read-only Litestream VFS connection to the organization replica and serves only state that Litestream has observed remotely.

Read consistency modes:

- **Remote committed:** default. Read the current remote LTX chain. It sees mutations only after their successful flush.
- **Read-after-write:** the mutation response includes the remote Litestream TXID; a following read waits/polls until its VFS observes at least that TXID, or routes through the same request-scoped connection before it is closed.

Reads must never depend on an API-instance local cache for correctness. A cache may reduce remote page fetches but must be invalidated/refreshed according to Litestream's update polling and must have a bounded TTL.

## 10. Local state, caching, and eviction

API processes are stateless in the deployment sense. They may hold bounded, disposable local state:

- VFS page cache in RAM;
- read-only VFS instances for recently used databases;
- short-lived temporary write buffers during a held lease;
- per-database single-flight/open locks;
- no durable tenant database file and no normal hydration.

Recommended starting controls:

```yaml
runtime:
  read_instance_idle_ttl: 60s
  max_read_instances: 200
  vfs_page_cache_bytes: 10485760
  max_total_cache_bytes: 2147483648
  max_temp_write_buffer_bytes: 268435456
  hydration_enabled: false
```

Evict least-recently-used eligible read instances on TTL or resource pressure. A write instance is never cached across lease release unless it is strictly read-only afterward and has refreshed remote state before its next use. Always delete temporary write-buffer files after a successful flush/close or after failure cleanup.

## 11. APIs and error model

### Control-plane descriptor

The API receives a trusted descriptor from the Basemnt control plane:

```go
type DatabaseDescriptor struct {
    DatabaseID     string           // application-chosen, path-safe relative key
    StorageProfile StorageProfile
    Credentials    CredentialSource // short-lived, scoped credentials
}
```

Do not accept bucket names, replica URLs, object prefixes, credentials, or database IDs directly from end users.

### Runtime surface

```go
type Runtime interface {
    WithRead(ctx context.Context, db DatabaseDescriptor, fn func(*sql.Conn) error) error
    WithWrite(ctx context.Context, db DatabaseDescriptor, key string, fn func(*sql.Conn) error) error
}
```

`WithWrite` owns the lease, write-mode enable/disable, remote flush, release, error mapping, and cleanup. Callers must not acquire a lease or toggle `litestream_write_enabled` directly.

### Node.js and Bun interface

Ship one Node-API addon, published as `@walrusd/db`, with prebuilt binaries for supported operating systems and CPU architectures. Node.js loads the `.node` module directly. Bun uses the same package through its Node-API compatibility layer; Bun support is gated by the same integration suite, not assumed from API compatibility alone.

The Node/Bun interface must call the Go core through a narrow C ABI. It must never shell out to a Litestream process, mutate process environment variables, or use the stock `litestream-vfs` Node package.

Expose batch-oriented operations so the native core can safely keep one SQLite connection and one lease for the complete operation:

```ts
import { WalrusdDatabase } from "@walrusd/db";

const database = new WalrusdDatabase({ credentialResolver });

const result = await database.write({
  database: descriptor,
  idempotencyKey: "req_7b39",
  statements: [
    { sql: "INSERT INTO events (id, body) VALUES (?, ?)", params: [eventId, body] },
    { sql: "UPDATE users SET updated_at = unixepoch() WHERE id = ?", params: [userId] }
  ]
});

const rows = await database.read({
  database: descriptor,
  sql: "SELECT * FROM events WHERE id = ?",
  params: [eventId],
  consistency: "remote_committed"
});
```

`write()` runs all statements in one SQLite transaction and then performs the mandatory flush-before-release sequence. Statement values use typed bound parameters; never build SQL with interpolated values.

Do not expose an arbitrary asynchronous JavaScript transaction callback in v1. A callback can pause while the database lease is held, exceed the lease deadline, and make cancellation/cleanup ambiguous. If an application needs read-dependent branching within a write transaction, implement that operation as a named Go core command or add a carefully designed native session API later with a strict non-awaiting contract and lease extension rules.

The binding accepts an opaque, trusted `DatabaseDescriptor` issued by the application/control plane. It may accept short-lived credentials only for the duration of a call. It must not persist credentials in JavaScript objects, disk, logs, or addon-global caches.

### C ABI boundary

Keep the C ABI small, versioned, and data-oriented. It accepts serialized descriptors, operation batches, cancellation/deadline information, and returns serialized result/error envelopes. The addon translates those envelopes to TypeScript values and typed errors.

```text
walrusd_runtime_write(request_bytes, deadline) -> response_bytes
walrusd_runtime_read(request_bytes, deadline)  -> response_bytes
walrusd_runtime_version()                      -> version_bytes
```

The Go core owns all SQLite/VFS handles. The Node/Bun addon never receives a raw SQLite pointer or direct Litestream VFS file handle. This prevents JavaScript code from bypassing the lease and flush protocol.

### Binding compatibility

- Use JSON or protobuf envelopes with explicit `api_version`; choose one and publish a schema.
- Preserve additive compatibility within a major version; reject unknown mandatory fields.
- Include the core version and protocol version in every health/status response.
- Test Go, Node, and Bun against the same lease conformance cases and object-store integration matrix.
- Never allow an older binding to silently downgrade `require_flush_before_release` or idempotency requirements.

Required classified errors:

- `DB_BUSY`: lease held by a non-expired owner; safe to retry.
- `DB_LEASE_CONFLICT`: CAS conflict or stale release; safe to retry after refreshing.
- `DB_FLUSH_FAILED`: SQL may be locally committed but remote LTX flush was not confirmed; retry with the same idempotency key.
- `DB_REMOTE_UNAVAILABLE`: object store cannot be safely read/written; safe to retry later.
- `DB_CONFLICT`: Litestream observed another writer; close/reopen, investigate, and retry only through idempotent flow.
- `DB_CONFIGURATION_INVALID`: storage profile/provider capability failure; not retryable without remediation.

## 12. Failure modes

| Event | Required handling |
| --- | --- |
| API crashes while lease is held | Lease expires; next request conditionally takes over. The interrupted caller retries with the same idempotency key. |
| API crashes before flush | The mutation is not acknowledged. Any buffered state is disposable; no later writer assumes it is durable. |
| Flush fails | Keep or allow lease to expire; return `DB_FLUSH_FAILED`; never release and acknowledge success. |
| API crashes after flush but before release | The mutation is durable. Later takeover sees it through the LTX chain; idempotency prevents duplication. |
| Concurrent write request | Loser sees held lease, retries with jitter or receives `DB_BUSY`. |
| Conditional lease conflict | Reread lease object and retry; never blindly retry an overwrite. |
| Object storage outage | Fail closed for writes. Reads may fail or serve only explicitly allowed cached committed data. |
| Lease expires during long request | Abort before more writes; do not release based on stale token. New owner takes over only through CAS. |
| Litestream write conflict | Treat as protocol violation/concurrency incident; discard local state, refresh from remote, and investigate. |
| API deploy/version skew | All versions must be protocol-compatible; incompatible lease formats require an explicit migration gate. |

## 13. Security and tenant isolation

- Authenticate and authorize every request before deriving its descriptor.
- Use per-organization scoped storage credentials, preferably short-lived and obtained through a secret manager/workload identity.
- Restrict credentials to one organization bucket and walrusd root prefix.
- Enforce TLS/mTLS for control-plane credential delivery and object-store transport.
- Require storage encryption policy validation during organization onboarding.
- Keep credentials, SQL values, object bodies, signed URLs, and raw LTX data out of logs/traces.
- Canonicalize all object paths; reject path traversal and invalid encoded IDs.
- Audit storage-profile changes, failed capability checks, lease takeovers, write conflicts, and privileged operations.
- Use deployment/IAM policy to prevent unrelated application code from directly mutating the walrusd lease and replica prefixes.

## 14. Observability

Use a privacy-safe database hash in metrics/traces; include organization ID only where policy allows. Required signals:

- lease acquire duration, held duration, CAS conflicts, takeover count, busy retries, and expiry takeovers;
- write transaction duration, VFS flush duration, LTX upload bytes, flush errors, and Litestream conflict errors;
- remote page fetches, cache hit rate, read open latency, VFS polling lag, and read-after-write wait time;
- active read instances, cache memory, temporary-buffer bytes, evictions, file descriptors, and GC pressure;
- object-store request latency/error rate by provider/profile;
- idempotency dedupe hits and ambiguous mutation outcomes.

Alert on: any Litestream writer conflict, sustained lease-CAS conflict, a flush failure, an expired lease while a request was active, storage capability regression, temporary-buffer exhaustion, or cross-tenant authorization denial.

## 15. Testing and conformance

### Unit tests

- Canonical IDs and object-key construction.
- Lease JSON encoding/decoding, epoch monotonicity, and state transitions.
- CAS conflict, expiry, clock-skew, retry-backoff, and release behavior.
- Idempotency transaction semantics.
- Resource limits and write-buffer cleanup.

### Lease-store conformance tests

Run against every lease backend before it is supported (`lease/conformance_test.go`):

- concurrent `CreateIfAbsent`: exactly one success;
- concurrent `ReplaceIfToken`: exactly one success;
- stale tokens never overwrite; tokens rotate on every write;
- conditional release cannot overwrite a successor lease;
- transient error classification and retry safety.

Object-storage providers need no CAS suite: post-write reads/listing must
expose the expected LTX state, with read-after-write visibility.

### Integration tests

- Two API processes contend for one database; only one executes its mutation at a time.
- A Go service, Node.js service, and Bun service contend for the same database and preserve the exact same lease behavior.
- A second write begins only after the first performs successful synchronous VFS flush and release.
- Kill the process before flush, during flush, after flush, and before release; validate correct retry/idempotency behavior.
- Reopen from a fresh process and validate SQLite integrity plus expected Litestream TXID.
- Exercise customer-controlled S3-compatible storage and Basemnt-managed storage separately.
- Verify no local full-database hydration file is produced in normal mode.

### Load and chaos tests

- Measure lease contention and p50/p95/p99 write latency for cold, warm, and hot-key databases.
- Inject storage latency, CAS conflicts, failed uploads, lost responses, and API process termination.
- Saturate temporary disk, RAM, and file descriptors.
- Run long-term LTX compaction/retention tests and validate that required LTX history remains available to VFS readers.

## 16. Deployment and configuration

Go API deployments include the walrusd Go runtime and the CGO-enabled Litestream VFS build directly. Node.js and Bun deployments include the `@walrusd/db` Node-API addon, which bundles or dynamically links a version-matched Go core and CGO-enabled VFS build. Do not run a separate router or worker process. The API becomes ready only after it validates its library/core protocol versions, native-addon loadability where applicable, temporary-storage path, control-plane credential source, and supported storage profile.

Release prebuilt Node-API binaries for each supported platform and architecture, with a source-build fallback only for development. Pin the addon to an exact compatible core major/minor version. CI must run the package against supported Node.js LTS versions and the supported Bun version on every release.

Starting configuration:

```yaml
lease:
  duration: 30s
  clock_skew_allowance: 2s
  acquire_retry_budget: 3s
  retry_backoff_min: 25ms
  retry_backoff_max: 500ms

litestream:
  write_sync_interval: 1s        # normal mode; disable triggers forced flush
  hydration_enabled: false
  vfs_page_cache_bytes: 10485760

runtime:
  request_timeout: 20s
  read_instance_idle_ttl: 60s
  max_temp_write_buffer_bytes: 268435456

durability:
  require_flush_before_release: true
  require_idempotency_key_for_mutations: true
```

Validate at startup that request timeout plus expected flush duration is below lease duration after skew allowance. Reject any setting that releases a lease without a confirmed successful flush.

## 17. Invariants

1. Go, Node.js, and Bun bindings all use one canonical application-chosen `database_id`.
2. Object storage is the durable source of the SQLite/LTX replica and lease record.
3. No API process writes a database without first conditionally acquiring its lease.
4. Lease acquisition and release always use the fencing token returned by the prior read/create.
5. The lease owner is the only walrusd runtime, regardless of language binding, allowed to enable Litestream VFS write mode for that database.
6. A mutation is not acknowledged and the lease is not released until the same VFS instance reports successful remote flush.
7. API instances are stateless; no request/session affinity is required.
8. Any API instance may acquire the next lease after release or expiry.
9. A fixed timer never substitutes for successful remote-flush confirmation.
10. Litestream writer conflicts are incidents, not a normal resolution mechanism.
11. Normal operation does not hydrate a full tenant database onto local disk.
12. Per-organization credentials and canonical paths prevent cross-tenant storage access.

## 18. Definition of done

The v1 runtime is complete when:

- Go, Node.js, and Bun API processes can read and write any authorized user database using only the embedded runtime and organization object storage.
- No Consul, database worker fleet, centralized router, sticky session, or worker-to-worker forwarding exists.
- Per-write conditional lease acquisition, explicit VFS write enable, SQLite transaction, synchronous flush-on-disable, conditional release, and idempotency are implemented as one library operation.
- All mutation success responses occur only after confirmed remote LTX upload.
- A second API instance safely waits/retries and sees the first instance's flushed state after it acquires the next lease.
- Crash, flush-failure, CAS-conflict, contention, and object-store outage tests pass without acknowledged data loss or duplicate idempotent mutation.
- The lease store passes the CAS conformance suite (memory always; Redis/Valkey live) before customer onboarding.
- Go direct-package, Node.js addon, and Bun addon integration tests run the identical lease/flush test matrix against each supported object store.
- The Node/Bun addon accepts only batch operations and routes all SQLite/VFS work through the Go core; no JavaScript code can bypass the lease or flush barrier.
- API-local cache, temporary buffer, cleanup, metrics, alerts, security controls, and runbooks are implemented.
- The production rollout includes compatibility/version gates and a tested rollback plan.

## 19. Final model

```text
Any API instance can serve any request.
The object-store lease serializes one user's writes.
Litestream VFS carries the incremental SQLite state as LTX files.
Flush confirmation, not elapsed time, makes a mutation durable.
```

This is an embedded, Go-native database runtime. It favors deployment simplicity and correctness through short serialized writes over the throughput and warm-cache advantages of a separate writer fleet.
