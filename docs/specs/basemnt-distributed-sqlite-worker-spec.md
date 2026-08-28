# Basemnt Distributed SQLite Storage and Database Worker Specification

**Status:** implementation specification  
**Audience:** engineering team or coding agent  
**Primary goal:** object storage is the durable database; workers are disposable compute.

## 1. Purpose

Build a horizontally scalable, multi-tenant SQLite service for Basemnt with one logical SQLite database per user. An organization owns the object-storage bucket that holds its users' durable database state. API and writer processes may be added, drained, restarted, or lost without migrating databases or retaining permanent tenant copies on local disks.

This specification intentionally separates **placement** from **write authorization**:

- **Rendezvous hashing** deterministically selects the writer that should receive a request.
- **Consul** provides writer membership, health, and fast operational coordination.
- **Object-storage conditional writes (CAS) and monotonic fencing epochs** are mandatory and authoritative for write authorization.

Consul is never enough by itself. A writer must fail closed and must not acknowledge a write unless its object-storage ownership epoch remains valid through the durable commit protocol.

## 2. Goals and non-goals

### Goals

- One logical SQLite database per `(organization_id, user_id)`.
- One object-storage bucket/configuration per organization, including customer-controlled storage.
- Durable data, ownership metadata, and durable commit metadata in object storage.
- SQLite access through Litestream VFS, using remote pages and RAM caching without normal full-database local hydration.
- Horizontally scalable dedicated writer fleet; API fleet is independently scalable.
- Read-only requests may be served by API workers directly through a read-only VFS path when consistency requirements permit.
- API-local routers, no centralized router, no sticky sessions, and no writer-to-writer forwarding.
- Bounded per-worker hot-database state with idle-TTL and memory-pressure eviction.
- Safe failover and rebalancing under stale membership, process loss, and network partitions.

### Non-goals for the first implementation

- Multiple concurrent SQLite writers for one user database.
- Cross-user or cross-organization SQL transactions.
- Treating object storage as a generic distributed lock service.
- A permanent local replica, background hydration, or disk-sized-to-database local state.
- Routing based on API session affinity.
- Support for an object store that cannot provide strong reads and conditional create/replace.

## 3. Terminology

| Term | Meaning |
| --- | --- |
| Organization | Basemnt tenant/customer. Owns one configured object-storage bucket. |
| Database ID | Stable identifier: `org/<organization_id>/user/<user_id>`. |
| Writer | Dedicated service that handles mutations for a database selected by routing. |
| Router | Library embedded in each API worker that selects a writer. |
| Epoch | Monotonically increasing fencing number carried by the current owner. |
| Ownership record | Object-storage object naming the current writer, lease, epoch, and generation. |
| Commit manifest | CAS-protected object naming the latest authoritative durable SQLite/LTX state. |
| Hot database | Open SQLite + VFS instance with bounded RAM cache and temporary write-buffer state. |

## 4. High-level architecture

```text
                              Clients
                                |
                         Load balancer
                                |
              +-----------------+-----------------+
              |                 |                 |
            API #1            API #2            API #N
          local router       local router       local router
              |                 |                 |
              +------ rendezvous hashing --------+
                                |
              +-----------------+-----------------+
              |                 |                 |
          Writer #1         Writer #2         Writer #N
       SQLite + VFS       SQLite + VFS       SQLite + VFS
              |                 |                 |
              +----------- object storage -------+
                                |
                     Org-specific bucket
              (customer-owned or Basemnt-managed)

                 Consul: writer membership and health
```

Every API process watches Consul, materializes an immutable local writer-membership snapshot, and computes the same routing decision from the same snapshot. No request passes through a router service and writers never forward a request to a peer.

Object storage is authoritative for tenant data and for fencing. Consul is advisory coordination: it makes failure detection and routing fast but cannot authorize a write.

## 5. Tenant identity and object layout

The control plane resolves an authenticated request to `organization_id` and `user_id`. It forms the canonical database ID exactly once in a shared library:

```text
database_id = "org/<organization_id>/user/<user_id>"
```

Do not include a worker ID, process ID, request ID, bucket name, or mutable user attributes in the database ID.

Each organization has a storage profile containing provider, bucket, endpoint/region, KMS/encryption settings, credential reference, and a storage-provider capability result. A customer-controlled bucket is treated exactly like a Basemnt-managed bucket after it passes capability validation.

Recommended prefix inside an organization bucket:

```text
basemnt/v1/databases/<encoded-user-id>/
  ownership/current.json
  commits/current.json
  ltx/<immutable LTX objects>
  metadata/schema.json
```

`encoded-user-id` must be canonical and path-safe. The implementation must derive all keys from trusted tenant context, never from a client-supplied storage key.

### Storage-provider contract

The storage adapter must expose strongly consistent `GET`, conditional create-if-absent, conditional replace-if-version, immutable-object put, delete, and an opaque version/ETag. The adapter must reject a backend when any required guarantee is unavailable.

The logical operations are:

```text
CreateIfAbsent(key, bytes) -> version | AlreadyExists
ReplaceIfVersion(key, expected_version, bytes) -> new_version | Conflict
Get(key) -> bytes, version | NotFound
PutImmutable(key, bytes) -> success | AlreadyExistsWithDifferentContent
```

Do not emulate CAS with a read followed by an unconditional write. Do not use timestamps alone as a lock.

## 6. Routing and membership

### 6.1 Writer registration

Writers register in Consul under one service name and publish:

- stable `worker_id` (unique for the process lifetime; never reused while a previous incarnation may run);
- mTLS endpoint and protocol version;
- region/zone and optional capacity weight;
- liveness/readiness health checks;
- drain state and admission capacity.

Only healthy, protocol-compatible, non-draining writers appear in a routing snapshot. Consul health changes must be debounced enough to avoid flapping but must not cause the router to consider an unhealthy writer usable.

### 6.2 API-local routing

The router uses highest-random-weight (rendezvous) hashing:

```text
score = H(routing_salt, database_id, writer_id, membership_generation)
selected_writer = eligible writer with highest score
```

All implementations must use the same canonical byte encoding, hash algorithm, seed, ordering, and weighting behavior. The router returns the selected writer endpoint and membership generation used to make the decision.

Use the membership snapshot captured at the beginning of a request. A Consul update during the request affects later requests only. The API retries a safe, idempotent request once after a selected writer is unavailable, using a fresh snapshot; mutations require an idempotency key before retry.

Do not use modulo hashing. Adding or removing a writer should move only the databases whose winning rendezvous score changes.

### 6.3 Routing is not ownership

Routing answers “where should this request go?” Ownership answers “who may durably commit this database now?” A request arriving at the selected writer is still rejected if that writer cannot acquire or retain the authoritative object-storage epoch.

The API does not keep a `database_id -> owner` table, acquire per-database Consul locks, or establish sticky sessions.

## 7. Ownership and fencing protocol

### 7.1 Core rule

Consul membership is advisory. Object-storage CAS plus a monotonically increasing epoch is the final authority. A writer never acknowledges a mutation merely because it is hash-selected, registered, healthy, or believes it owns the database.

An ownership record contains at least:

```json
{
  "format_version": 1,
  "database_id": "org/org_123/user/user_456",
  "epoch": 92,
  "owner_worker_id": "writer-02/instance-7f5c",
  "lease_id": "5cf2f4c0-5c25-4a0d-9ab9-e50f65cbf523",
  "lease_expires_at": "2026-08-28T15:04:30Z",
  "issued_at": "2026-08-28T15:04:00Z"
}
```

The object version/ETag returned by storage is not the fencing epoch. It protects the ownership-record update; `epoch` is the durable, monotonic fence included in all commit manifests.

### 7.2 Acquisition and renewal

For a cold writable database or takeover, the writer performs the following single-flight operation per database:

1. Confirm it is locally eligible to serve writes (healthy, not draining) and that it is the currently selected writer for a fresh membership snapshot. This is a routing gate, not authorization.
2. Read `ownership/current.json` and its object version.
3. If no record exists, conditionally create one with epoch `1`.
4. If the record names this exact `(worker_id, lease_id)` and is still valid, conditionally renew it without changing the epoch.
5. Otherwise, only after the old lease is eligible for takeover, conditionally replace the same object version with a new owner, fresh lease ID, and `epoch = previous_epoch + 1`.
6. Read back the ownership record and confirm its contents, object version, and epoch match the acquired record before opening write mode.

On `Conflict`, discard local authority, reread, and either return a retryable “not owner/temporarily unavailable” response or retry acquisition with bounded jitter. Never overwrite a newer record.

Lease expiry is a liveness aid, not authorization. Use a conservative duration and a bounded clock-skew policy. Where the provider offers authoritative server-time semantics, use it. If object-storage reachability, time confidence, CAS, or read-back confirmation is lost, the writer must stop accepting/acknowledging writes before its last confirmed lease window ends.

### 7.3 Commit fencing

An epoch only provides safety if it fences the *durable commit point*. Therefore the VFS/storage integration must use a CAS-protected current commit manifest. Each acknowledged SQLite transaction follows this sequence:

1. Execute the transaction against the open SQLite/VFS instance.
2. Produce immutable LTX/state objects named with transaction identity and epoch.
3. Read or use the cached version of `commits/current.json`.
4. Verify that `ownership/current.json` still names this worker, lease ID, and epoch; renew/revalidate as configured.
5. Atomically `ReplaceIfVersion` the current commit manifest, including the current epoch and pointer(s) to immutable LTX/state objects.
6. Treat the transaction as durable and return success only after that CAS succeeds and the required object-store durability condition is met.

The commit manifest must include: database ID, epoch, logical commit sequence, previous manifest version/sequence, durable object pointers/checksums, and writer/lease identity. A stale writer may upload immutable orphan LTX objects, but it cannot make them authoritative because it cannot advance the current manifest with the current epoch and expected object version.

If ownership changes between local SQLite commit and manifest CAS, the CAS/revalidation fails. The old writer must not acknowledge success, must close or demote its writable instance, and must reconcile from the current manifest on the next open. This is why write acknowledgements cannot be based on a local buffer alone.

### 7.4 Loss of authority

Immediately demote a writable database to read-only/closed and fail in-flight unacknowledged mutations when any of these occur:

- ownership renewal conflicts or reports a different epoch/lease;
- commit-manifest CAS conflicts unexpectedly;
- the writer cannot read or conditionally write the organization’s storage before lease safety expires;
- the process enters drain mode;
- integrity validation detects a manifest or epoch discontinuity.

Return a retryable error such as `DB_OWNER_UNAVAILABLE` or `DB_RETRY_ON_NEW_OWNER`, never a false success. The client/API retry uses the same idempotency key.

## 8. Database runtime lifecycle

Each writer holds only a bounded set of hot databases:

```text
COLD -> OPENING -> HOT -> IDLE -> EVICTING -> COLD
                         \-> FENCED/FAILED
```

### Opening

One per-database single-flight promise prevents multiple concurrent opens. For a writable open: acquire/revalidate authoritative ownership, load the current commit manifest, open SQLite with Litestream VFS in non-hydrating mode, replay/fetch only required remote state, and register the hot instance. For a read-only open: load a stable manifest and use a read-only VFS handle; do not create a writer ownership record.

### Hot state

The hot instance contains only transient state: SQLite connection pool, Litestream VFS page cache (RAM), temporary write buffer, ownership/manifest versions, in-flight request count, access timestamp, and instrumentation. It is not a durable tenant replica.

The VFS page cache is in memory. The temporary write buffer may be local ephemeral disk, but its size must be bounded and it must never be the sole basis for acknowledging a write. Normal operation must not hydrate/reconstruct a complete SQLite database file on worker disk.

### Eviction

Start with a configurable `DB_IDLE_TTL=60s`. An idle database becomes eligible when it has no active requests, no opening operation, and no pending acknowledged durability work. Eviction performs:

1. block new work for the instance;
2. finish or fail unacknowledged writes according to the fencing protocol;
3. flush and confirm the required durable manifest state;
4. relinquish/demote write ownership when appropriate (expiry is acceptable; explicit release is best-effort only);
5. close SQLite and VFS, delete temporary buffers, and release RAM;
6. remove the instance from the hot registry.

Add a global resource governor. On configured memory, file-descriptor, temporary-disk, or hot-instance pressure, evict least-recently-used eligible databases before admitting more. Do not evict a database in a transaction or with a required commit-manifest CAS in progress.

## 9. Read model

Reads have two explicit consistency modes:

- **Strong/read-your-writes:** route to the current writer and read the writer’s authoritative open state. Use this for requests following a mutation or requiring the newest committed data.
- **Committed snapshot:** an API worker may open a read-only VFS against the latest verified commit manifest in object storage. This returns only data represented by the committed manifest and may lag an in-flight unacknowledged mutation.

The external API must select the mode deliberately; default to strong consistency where application semantics are unclear. Read-only API VFS instances obey the same bounded caching and idle-eviction policy and never write ownership or database data.

## 10. Service APIs and internal interfaces

Use versioned gRPC or HTTP/2 endpoints with mTLS between API workers and writers. The precise transport is an implementation choice; the following semantics are required.

### API-to-writer request

```text
ExecuteWrite(database_id, idempotency_key, expected_membership_generation,
             request_context, operation)
ExecuteRead(database_id, consistency_mode, request_context, query)
```

Every write carries a caller-generated idempotency key scoped to `(database_id, operation)`. Writers persist deduplication results in the user database or another durably fenced per-database record so a retry after ambiguous transport failure cannot duplicate a mutation.

Responses include `database_id`, `commit_sequence` on success, and structured retryability. Do not expose storage credentials, object keys, epochs, or internal topology to external clients.

### Storage interfaces

```text
StorageAdapter.Get(key)
StorageAdapter.CreateIfAbsent(key, body)
StorageAdapter.ReplaceIfVersion(key, expected_version, body)
StorageAdapter.PutImmutable(key, body, checksum)
OwnershipManager.AcquireOrRenew(database_id, candidate_worker)
CommitCoordinator.Commit(database_id, epoch, lease_id, local_transaction_state)
DatabaseManager.GetOrOpen(database_id, mode)
```

The `CommitCoordinator` is the only component authorized to advance `commits/current.json`. Keep the ownership and commit-manifest algorithms in one well-tested package, not duplicated in API handlers or VFS call sites.

## 11. Failure handling

| Event | Required behavior |
| --- | --- |
| Writer process crashes | Consul removes it after health failure; routers select another writer; new writer acquires a higher epoch through object-store CAS before committing. |
| Network partition from Consul only | The writer may continue only while object-storage authority remains valid and it can revalidate/commit; stale routers are still fenced by storage. |
| Network partition from object storage | Fail closed for writes before the confirmed lease safety window expires; never acknowledge based solely on local buffers. |
| Consul split brain/stale snapshot | Requests can reach competing writers, but only the one holding current object-store epoch can advance the commit manifest. |
| Ownership CAS conflict | Do not write or acknowledge; reread state and return retryable/not-owner response. |
| Commit-manifest CAS conflict | Assume fencing loss or concurrent takeover; demote and reconcile. Do not retry blindly against stale local state. |
| Writer joins or leaves | Routers recompute placement; affected databases open lazily on new writers, with no data migration. |
| Writer drain/deploy | Stop being eligible in Consul, reject new write admissions, finish durable commits, demote/close hot DBs, then terminate. |
| Storage object corruption/missing LTX | Stop affected database, alert, and recover only from verified immutable objects/backups; never invent a manifest. |
| Client timeout after write | Client retries with same idempotency key; writer returns original durable result or safely executes once. |

No component may forward an arriving database request to a different writer. The API performs a fresh routing decision and retry instead.

## 12. Security and tenant isolation

- Authenticate end users before deriving tenant context; authorize every database operation against organization and user boundaries.
- Use separate storage credentials or narrowly scoped role assumptions per organization. Restrict each credential to that organization bucket and prefix.
- Store credentials only in the control-plane secret manager; workers receive short-lived scoped credentials, never long-lived plaintext configuration.
- Encrypt in transit with mTLS for API-to-writer, Consul, and storage traffic. Require server identity validation.
- Require encryption at rest using customer-required provider encryption/KMS settings. Record key policy validation at onboarding.
- Use canonical key construction and strict input validation to prevent path traversal/cross-tenant object access.
- Redact SQL values, credentials, signed URLs, object contents, and raw user data from logs, traces, and errors.
- Audit storage-profile changes, ownership takeovers, commit-manifest transitions, privileged reads, and control-plane credential use.
- Rate-limit opens, writes, and expensive read queries per tenant to protect the shared fleet.

## 13. Observability

Emit structured logs and traces keyed by a privacy-safe database hash, organization ID where permitted, request ID, writer ID, membership generation, epoch, lease ID hash, and commit sequence.

Required metrics include:

- router snapshot age, selected writer, routing retries, and membership changes;
- hot database count, open/close latency, idle/pressure eviction count, cache hit rate, RAM, temporary disk, and file descriptors;
- ownership acquire/renew duration, CAS conflicts, fencing losses, epoch advances, and lease-expiry failures;
- commit-manifest CAS duration/failures, durable-ack latency, LTX upload bytes, object-store request/error rates;
- read latency by consistency mode and remote-page fetch rate;
- idempotency dedupe hits and ambiguous write outcomes.

Alert on persistent CAS conflicts, any unplanned fencing loss, expired ownership while requests are active, failure to advance manifests, storage capability failures, excessive snapshot age, memory-pressure eviction storms, and cross-tenant authorization denials.

## 14. Configuration

All values are deployment configuration with safe defaults, validation, and per-tenant overrides only where justified.

```yaml
routing:
  hash_algorithm: "sha256-hrw-v1"
  consul_watch_max_staleness: "15s"
  retryable_route_attempts: 1

ownership:
  lease_duration: "30s"
  renew_interval: "10s"
  max_clock_skew: "2s"
  takeover_backoff: "100ms..2s"

database:
  idle_ttl: "60s"
  max_hot_databases: 500
  vfs_page_cache_bytes: 10485760
  max_worker_cache_bytes: 4294967296
  max_temp_write_buffer_bytes: 1073741824

durability:
  require_manifest_cas_before_ack: true
  immutable_object_checksum: "sha256"
  storage_read_after_write_verification: true
```

Validate `renew_interval + max_clock_skew < lease_duration`. Reject configurations that allow acknowledgement without a successful manifest CAS or without a compatible storage adapter.

## 15. Deployment and operations

### Control plane

The control plane creates the organization storage profile, validates object-store capabilities and permissions, initializes the database prefix and schema metadata, and records the customer’s data-residency/encryption requirements. It must not proxy normal database data.

### API deployment

Deploy API workers statelessly. Each includes the routing library and Consul watch client. API restarts merely rebuild their membership snapshot. The API fleet can scale based on request rate independently of writer memory/database concurrency.

### Writer deployment

Deploy writers as disposable instances with bounded ephemeral disk. At startup, register as not-ready; become ready only after transport, Consul registration, storage adapter, and fencing/commit-coordinator self-tests pass. Never pre-open all tenant databases.

For rolling updates: mark the instance draining in Consul, remove it from eligible routing, stop accepting new write opens, allow bounded completion of already-authorized work, close/demote hot databases, deregister, then terminate. A hard stop is safe because another writer lazily opens from object storage and acquires a new epoch.

Autoscale writers by active hot databases, cache pressure, write throughput, VFS open latency, and temporary-buffer pressure—not simply total registered users.

## 16. Testing strategy

### Unit and property tests

- Canonical database-ID and object-key construction; no cross-tenant collisions.
- Rendezvous hashing determinism, weighted behavior, and limited remapping on membership change.
- Ownership-record serialization, epoch monotonicity, CAS conflict behavior, and single-flight opening.
- Commit-manifest state machine: no acknowledgement before manifest CAS; stale epoch cannot advance it.
- Eviction and resource-governor behavior; never close an in-flight transaction.
- Idempotency behavior for retry, timeout, and duplicate delivery.

### Integration tests

- Run against every supported object-store provider or a faithful compatibility suite verifying strong reads and conditional writes.
- Create, write, close, and reopen a database from a fresh worker without full local hydration.
- Add/remove writers and assert only expected rendezvous assignments change.
- Customer-controlled credential isolation: one organization cannot access another bucket/prefix.

### Fault-injection and chaos tests

- Kill a writer during a local transaction, immutable-object upload, and commit-manifest CAS.
- Partition a writer from Consul, from object storage, and from API workers independently.
- Force stale API membership and concurrent takeover attempts.
- Delay/reorder storage responses; inject CAS conflicts and read-back mismatches.
- Exhaust memory, temporary disk, and file descriptors.
- Verify at most one epoch can advance the authoritative commit manifest and no acknowledged write is lost or duplicated.

### Load tests

Measure hot/cold open latency, cache hit rate, object-store request cost, writer scaling distribution, eviction churn, and p99 durable write latency at representative active-user concurrency. Test long-tail traffic and hot-key databases separately.

## 17. Invariants

These are non-negotiable implementation invariants:

1. A logical user database is identified only by its canonical `(organization_id, user_id)` database ID.
2. The organization bucket is the durable authority for database state, ownership, and commit metadata.
3. Consul is used for membership, health, and coordination only; it never independently authorizes database writes.
4. Routing/placement never implies ownership/write authority.
5. Every writer must acquire and retain a current object-storage ownership record with a monotonic fencing epoch before acknowledging writes.
6. Every acknowledged mutation must have passed the epoch-aware, CAS-protected durable commit-manifest step.
7. A writer that cannot revalidate authority or storage reachability fails closed for writes.
8. At most one epoch can advance an authoritative commit-manifest generation; stale writers may not overwrite it.
9. No sticky sessions, centralized DB router, per-request Consul lock, or writer-to-writer forwarding exists.
10. Normal operation never requires a full hydrated local tenant database; worker-local cache and buffers are bounded and disposable.
11. Idle and resource-pressure eviction never discards an acknowledged-but-not-durable write.
12. Tenant credentials and object paths are scoped so no organization can access another organization’s data.

## 18. Definition of done

The first production-ready implementation is complete when:

- API workers independently and deterministically route writes with rendezvous hashing from watched Consul membership.
- Writers register health/capacity/drain state and can be added, removed, and restarted without tenant data migration.
- One user database opens lazily through Litestream VFS backed by the organization’s object store, with no normal full hydration.
- Hot instances use configurable RAM cache, bounded temporary write buffer, idle-TTL eviction, and global pressure eviction.
- The storage adapter enforces conditional create/replace and startup/onboarding rejects unsupported providers.
- Ownership acquisition/renewal uses object-store CAS, monotonic epochs, and read-back validation.
- The durable commit path uses epoch-aware CAS on a current commit manifest; a write is never acknowledged before it succeeds.
- Partition, stale-router, crash, takeover, and CAS-conflict tests demonstrate that stale writers cannot make durable authoritative commits.
- Read APIs explicitly support strong writer reads and committed-snapshot reads, with documented semantics.
- Idempotent write retries work across writer failure and routing change.
- mTLS, tenant credential isolation, encryption policy, audit logs, redaction, metrics, traces, dashboards, and alerts are in place.
- An operational runbook covers onboarding validation, writer drain, storage outage, fencing conflict, corruption recovery, and incident triage.

## 19. Final implementation principle

```text
Consul selects the eligible fleet.
Rendezvous hashing selects the intended writer.
Object-storage CAS and fencing epochs authorize durable writes.
The commit manifest makes that authorization enforceable.
```

This division keeps the fast path horizontally scalable while ensuring that stale membership, partitions, or faulty workers cannot turn an advisory routing decision into an unsafe write.
