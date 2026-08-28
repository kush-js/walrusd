# Architecture

Translation of the Basemnt Distributed SQLite Storage and Database Worker
Specification (§1-5) into implementation documentation.

## Purpose

WALrus is a horizontally scalable, multi-tenant SQLite service with one
logical SQLite database per user. An organization owns the object-storage
bucket that holds its users' durable database state. API and writer processes
may be added, drained, restarted, or lost without migrating databases or
retaining permanent tenant copies on local disks.

The specification intentionally separates **placement** from **write
authorization**:

- **Rendezvous hashing** deterministically selects the writer that should
  receive a request (fast path).
- **Object-storage conditional writes (CAS) and monotonic fencing epochs**
  are mandatory and authoritative for write authorization (safety path).

Consul (membership/health) is never enough by itself: a writer fails closed
and does not acknowledge a write unless its object-storage ownership epoch
remains valid through the durable commit protocol.

## Goals and non-goals

Goals:

- One logical SQLite database per `(organization_id, user_id)`.
- One object-storage bucket/configuration per organization, including
  customer-controlled storage.
- Durable data, ownership metadata, and durable commit metadata in object
  storage.
- SQLite access through the Litestream VFS: remote pages, RAM caching, no
  full-database local hydration.
- Horizontally scalable writer fleet; API fleet scales independently.
- Read-only requests may be served directly through a read-only VFS path when
  consistency requirements permit.
- API-local routers: no centralized router, no sticky sessions, no
  writer-to-writer forwarding.
- Bounded per-worker hot-database state with idle-TTL and memory-pressure
  eviction.

Non-goals for the first implementation:

- Multiple concurrent SQLite writers for one user database.
- Cross-user or cross-organization SQL transactions.
- Treating object storage as a generic distributed lock service.
- A permanent local replica, background hydration, or disk-sized-to-database
  local state.
- Routing based on API session affinity.
- Support for object stores without strong reads and conditional
  create/replace.

## High-level architecture

Every API process watches membership, materializes an immutable local
writer-membership snapshot, and computes the same routing decision from the
same snapshot. No request passes through a router service and writers never
forward a request to a peer.

Object storage is authoritative for tenant data and for fencing. Membership
services (Consul in production) are advisory coordination: they make failure
detection and routing fast but cannot authorize a write.

## Tenant identity and object layout

The control plane resolves an authenticated request to
`organization_id` and `user_id`. The canonical database ID is formed exactly
once, in `base/`:

```text
database_id = "org/<organization_id>/user/<user_id>"
```

No worker ID, process ID, request ID, bucket name, or mutable user attribute
is ever part of the database ID.

Each organization has a storage profile: provider, bucket, endpoint/region,
KMS/encryption settings, credential reference, and a capability-validation
result. A customer-controlled bucket is treated exactly like a managed one
after validation.

Object prefix inside an organization bucket:

```text
basemnt/v1/databases/<encoded-user-id>/
  ownership/current.json
  commits/current.json
  ltx/<level>/<min-txid>-<max-txid>.ltx
```

`encoded-user-id` is the SHA-256 of the canonical database ID — canonical and
path-safe by construction. Keys are always derived from trusted tenant
context (`base.NewDatabaseID`), never from client-supplied storage keys.

## Storage-provider contract

The adapter (`storage/`) exposes strongly consistent `GET`, conditional
create-if-absent, conditional replace-if-version, immutable-object put, and
an opaque version/ETag. On startup/onboarding the provider is rejected when
any required guarantee is unavailable.

Logical operations:

```text
CreateIfAbsent(key, bytes) -> version | AlreadyExists/Conflict
ReplaceIfVersion(key, expected_version, bytes) -> new_version | Conflict
Get(key) -> bytes, version | NotFound
PutImmutable(key, bytes) -> success | AlreadyExistsWithDifferentContent
```

CAS is never emulated with a read followed by an unconditional write.
Timestamps are never used as a lock.

Implementation mapping (Cloudflare R2 verified by integration test):

| Contract operation | R2 mechanism |
| --- | --- |
| CreateIfAbsent | `PutObject` with `If-None-Match: *`; 412 → conflict |
| ReplaceIfVersion | `PutObject` with `If-Match: <quoted etag>`; 412 → conflict |
| Get | `GetObject`; typed `NoSuchKey` → `ErrNotFound` |
| PutImmutable | `PutObject` with `If-None-Match: *`; content-addressed by sha256 |
| Consistency | R2 read-after-write is strongly consistent |

ETags are stored and replayed **quoted**, exactly as the provider returns
them.
