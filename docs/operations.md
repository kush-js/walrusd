# Operations

Translation of the specification (§12-18) into implementation documentation.

## Security and tenant isolation

- Authenticate end users before deriving tenant context; authorize every
  database operation against organization and user boundaries.
- Use separate storage credentials or narrowly scoped role assumptions per
  organization; restrict each credential to that organization's bucket and
  prefix.
- Store credentials only in the control-plane secret manager; workers receive
  short-lived scoped credentials, never long-lived plaintext configuration.
- Encrypt in transit for API-to-writer, membership, and storage traffic;
  require server identity validation.
- Require encryption at rest per the provider's encryption/KMS settings;
  record key-policy validation at onboarding.
- Canonical key construction and strict input validation prevent
  path traversal / cross-tenant object access (`base/` derives keys from
  SHA-256 of the canonical database ID; client input can never reach a key).
- Redact SQL values, credentials, signed URLs, object contents, and raw user
  data from logs, traces, and errors.
- Audit storage-profile changes, ownership takeovers, commit-manifest
  transitions, privileged reads, and control-plane credential use.
- Rate-limit opens, writes, and expensive read queries per tenant to protect
  the shared fleet.

## Observability

Structured logs (JSON, `slog`) are keyed by privacy-safe fields: worker ID,
request context, epoch, and commit sequence — never raw tenant data.

Required metrics (from the spec; expose via a metrics endpoint):

- router snapshot age, selected writer, routing retries, membership changes;
- hot database count, open/close latency, idle/pressure eviction count, cache
  hit rate, RAM, temporary disk, file descriptors;
- ownership acquire/renew duration, CAS conflicts, fencing losses, epoch
  advances, lease-expiry failures;
- commit-manifest CAS duration/failures, durable-ack latency, LTX upload
  bytes, object-store request/error rates;
- read latency by consistency mode and remote-page fetch rate;
- idempotency dedupe hits and ambiguous write outcomes.

Alert on persistent CAS conflicts, any unplanned fencing loss, expired
ownership while requests are active, failure to advance manifests, storage
capability failures, excessive snapshot age, memory-pressure eviction storms,
and cross-tenant authorization denials.

## Configuration

All values are deployment configuration with safe defaults (see the flag
table in the README). Startup validation rejects configurations where
`renew_interval + max_clock_skew >= lease_duration`, and any configuration
that could allow acknowledgement without a successful manifest CAS or without
a compatible storage adapter.

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

## Deployment

### Control plane

The control plane creates the organization storage profile, validates
object-store capabilities and permissions, initializes the database prefix
and schema metadata, and records the customer's data-residency/encryption
requirements. It must not proxy normal database data.

### API deployment

Deploy API workers statelessly. Each embeds the routing library and
membership watch. API restarts merely rebuild their membership snapshot. The
API fleet scales on request rate independently of writer memory/database
concurrency.

### Writer deployment

Deploy writers as disposable instances with bounded ephemeral disk. At
startup, register as not-ready; become ready only after transport,
membership registration, storage adapter, and fencing/commit-coordinator
self-tests pass. Never pre-open all tenant databases.

Rolling updates: mark the instance draining, remove it from eligible routing,
stop accepting new write opens, allow bounded completion of already-authorized
work, close/demote hot databases, deregister, then terminate. A hard stop is
safe because another writer lazily opens from object storage and acquires a
new epoch.

Autoscale writers by active hot databases, cache pressure, write throughput,
VFS open latency, and temporary-buffer pressure — not total registered users.

## Testing

### Unit and property tests (in-repo)

- Canonical database-ID and object-key construction; no cross-tenant
  collisions (`base`).
- Rendezvous hashing determinism, weighted behavior, limited remapping on
  membership change (`routing`).
- Ownership-record serialization, epoch monotonicity, CAS conflict behavior
  (`ownership`).
- Commit-manifest state machine: no acknowledgement before manifest CAS;
  stale epoch cannot advance it (`commit`).
- Eviction and resource-governor behavior; never close an in-flight
  transaction (`dbmanager`).
- Idempotency behavior for retry, timeout, and duplicate delivery.

### Integration tests

- Run against every supported object-store provider or a faithful
  compatibility suite verifying strong reads and conditional writes
  (`storage` tests with `R2_*` env).
- Create, write, close, and reopen a database from a fresh worker without
  full local hydration (`dbmanager` reopen test).
- Add/remove writers and assert only expected rendezvous assignments change
  (`routing` remap tests).
- Customer-controlled credential isolation: one organization cannot access
  another bucket/prefix.

### Fault-injection and chaos tests

- Kill a writer during a local transaction, immutable-object upload, and
  commit-manifest CAS.
- Partition a writer from membership, from object storage, and from API
  workers independently.
- Force stale API membership and concurrent takeover attempts.
- Delay/reorder storage responses; inject CAS conflicts and read-back
  mismatches.
- Exhaust memory, temporary disk, and file descriptors.
- Verify at most one epoch can advance the authoritative commit manifest and
  no acknowledged write is lost or duplicated.

### Load tests

Measure hot/cold open latency, cache hit rate, object-store request cost,
writer scaling distribution, eviction churn, and p99 durable write latency at
representative active-user concurrency. Test long-tail traffic and hot-key
databases separately.

## Invariants

Non-negotiable implementation invariants:

1. A logical user database is identified only by its canonical
   `(organization_id, user_id)` database ID.
2. The organization bucket is the durable authority for database state,
   ownership, and commit metadata.
3. Membership (Consul) is used for health and coordination only; it never
   independently authorizes database writes.
4. Routing/placement never implies ownership/write authority.
5. Every writer must acquire and retain a current object-storage ownership
   record with a monotonic fencing epoch before acknowledging writes.
6. Every acknowledged mutation must have passed the epoch-aware,
   CAS-protected durable commit-manifest step.
7. A writer that cannot revalidate authority or storage reachability fails
   closed for writes.
8. At most one epoch can advance an authoritative commit-manifest generation;
   stale writers may not overwrite it.
9. No sticky sessions, centralized DB router, per-request Consul lock, or
   writer-to-writer forwarding exists.
10. Normal operation never requires a full hydrated local tenant database;
    worker-local cache and buffers are bounded and disposable.
11. Idle and resource-pressure eviction never discards an
    acknowledged-but-not-durable write.
12. Tenant credentials and object paths are scoped so no organization can
    access another organization's data.

## Definition of done (first production-ready implementation)

- API workers independently and deterministically route writes with
  rendezvous hashing from watched membership.
- Writers register health/capacity/drain state and can be added, removed, and
  restarted without tenant data migration.
- One user database opens lazily through the Litestream VFS backed by the
  organization's object store, with no normal full hydration.
- Hot instances use configurable RAM cache, bounded temporary write buffer,
  idle-TTL eviction, and global pressure eviction.
- The storage adapter enforces conditional create/replace; startup/onboarding
  rejects unsupported providers.
- Ownership acquisition/renewal uses object-store CAS, monotonic epochs, and
  read-back validation.
- The durable commit path uses epoch-aware CAS on a current commit manifest;
  a write is never acknowledged before it succeeds.
- Partition, stale-router, crash, takeover, and CAS-conflict tests
  demonstrate that stale writers cannot make durable authoritative commits.
- Read APIs explicitly support strong writer reads and committed-snapshot
  reads, with documented semantics.
- Idempotent write retries work across writer failure and routing change.
- mTLS, tenant credential isolation, encryption policy, audit logs,
  redaction, metrics, traces, dashboards, and alerts are in place.
- An operational runbook covers onboarding validation, writer drain, storage
  outage, fencing conflict, corruption recovery, and incident triage.

## Final implementation principle

```text
Consul selects the eligible fleet.
Rendezvous hashing selects the intended writer.
Object-storage CAS and fencing epochs authorize durable writes.
The commit manifest makes that authorization enforceable.
```
