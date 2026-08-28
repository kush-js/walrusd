# Ownership and durable commits

Translation of the specification (§7, §11) into implementation documentation.
Implemented in `ownership/` and `commit/`.

## Core rule

Membership is advisory. Object-storage CAS plus a monotonically increasing
epoch is the final authority. A writer never acknowledges a mutation merely
because it is hash-selected, registered, healthy, or believes it owns the
database.

## Ownership record

Stored at `<prefix>/ownership/current.json`:

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

The object version/ETag is **not** the fencing epoch. It protects the
ownership-record update; `epoch` is the durable, monotonic fence included in
all commit manifests.

## Acquisition and renewal

`ownership.Manager.AcquireOrRenew` implements the single-flight operation
(spec §7.2) as a bounded reread loop; each attempt runs one read-modify-CAS
cycle:

1. Read `ownership/current.json` and its object version.
2. No record → conditionally create one with epoch `1`.
3. Record names this exact worker and the lease is still valid → renew it
   (new expiry) **without changing the epoch or lease ID**.
4. Lease not yet expired (plus skew) → `ErrTakeoverPending`; takeover is only
   allowed after the old lease expires.
5. Lease expired → takeover: conditionally replace the same object version
   with a new owner, fresh lease ID, and `epoch = previous_epoch + 1`.
6. Read back the ownership record and confirm contents, object version, and
   epoch match the acquired record before opening write mode.

On CAS `Conflict`: discard local authority, reread, and either return a
retryable not-owner response or retry acquisition with bounded jitter
(25ms × attempt, max 4 attempts). Never overwrite a newer record. A
`database_id` mismatch on the stored record is a deterministic cross-tenant
safety failure and is never retried.

Lease expiry is a liveness aid, not authorization. Configuration is validated
at startup: `renew_interval + max_clock_skew < lease_duration`. If storage
reachability, time confidence, CAS, or read-back confirmation is lost, the
writer stops accepting/acknowledging writes before the last confirmed lease
window ends.

## Commit fencing

An epoch only provides safety if it fences the *durable commit point*. The
VFS/storage integration uses a CAS-protected current commit manifest. Each
acknowledged SQLite transaction follows this sequence
(`dbmanager.Manager.ExecuteWrite`):

1. Execute the transaction against the open SQLite/VFS instance.
2. Force a sync: dirty pages are encoded as an immutable LTX object named
   with transaction identity (TXID range) and uploaded with
   `PutImmutable` (create-if-absent). A stale writer may upload orphan LTX
   objects — it cannot make them authoritative.
3. Read the current `commits/current.json` version.
4. The epoch/lease being committed was authorized before execution; the CAS
   revalidates it against the stored manifest (`ErrStaleEpoch` for an older
   epoch, `ErrFencingLost` on version conflict or same-epoch different
   writer).
5. Atomically advance the current commit manifest — including the current
   epoch and pointers/checksums to the immutable LTX objects.
6. Treat the transaction as durable and return success only after that CAS
   succeeds.

The commit manifest contains: database ID, epoch, logical commit sequence,
previous manifest version, durable object pointers (key + sha256 checksum +
TXID range), and writer/lease identity.

If ownership changes between the local SQLite commit and the manifest CAS,
the CAS fails. The old writer must not acknowledge success, must close or
demote its writable instance, and must reconcile from the current manifest on
the next open. Write acknowledgements are never based on a local buffer.

## Idempotency

Every write carries a caller-generated idempotency key scoped to
`(database_id, operation)`. Results are persisted per database keyed by a
hash of `(database_id, operation, key)`. A retry after ambiguous transport
failure returns the original durable result; a mutation is never duplicated
(spec §10, §11).

## Loss of authority

Immediately demote the writable database to read-only/closed and fail
in-flight unacknowledged mutations when any of these occur:

- ownership renewal conflicts or reports a different epoch/lease;
- commit-manifest CAS conflicts unexpectedly;
- the writer cannot read or conditionally write the organization's storage
  before lease safety expires;
- the process enters drain mode;
- integrity validation detects a manifest or epoch discontinuity.

The API receives a retryable error (`DB_OWNER_UNAVAILABLE` or
`DB_RETRY_ON_NEW_OWNER`), never a false success. The client/API retry uses
the same idempotency key.

## Failure handling

| Event | Required behavior (implemented) |
| --- | --- |
| Writer process crashes | Membership drops it; routers select another writer; the new writer acquires a higher epoch through object-store CAS before committing. |
| Partition from Consul only | The writer may continue only while object-storage authority remains valid and it can revalidate/commit. |
| Partition from object storage | Fail closed for writes before the confirmed lease safety window expires; never acknowledge based solely on local buffers. |
| Stale membership / split brain | Requests can reach competing writers, but only the current epoch can advance the commit manifest. |
| Ownership CAS conflict | Do not write or acknowledge; reread state; return retryable not-owner. |
| Commit-manifest CAS conflict | Fencing loss: demote and reconcile; never retry blindly against stale local state. |
| Writer joins or leaves | Routers recompute placement; affected databases open lazily on new writers; no data migration. |
| Writer drain/deploy | Drain: reject new write admissions, finish durable commits, demote/close hot DBs, deregister, terminate. Hard stop is safe: another writer lazily opens from object storage and acquires a new epoch. |
| Object corruption / missing LTX | Stop the affected database, alert; recover only from verified immutable objects; never invent a manifest. |
| Client timeout after write | Client retries with the same idempotency key; writer returns the original durable result or safely executes once. |

No component ever forwards an arriving database request to a different
writer: the API performs a fresh routing decision and retry.
