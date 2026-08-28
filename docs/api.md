# Service API

Translation of the specification (§10) into implementation documentation.
Exposed by `cmd/walrusd` over HTTP/1.1+ (JSON); the transport is an
implementation choice per spec — the semantics below are required.

## Internal interfaces

```text
StorageAdapter.Get(key)
StorageAdapter.CreateIfAbsent(key, body)
StorageAdapter.ReplaceIfVersion(key, expected_version, body)
StorageAdapter.PutImmutable(key, body, checksum)
OwnershipManager.AcquireOrRenew(database_id, candidate_worker)
CommitCoordinator.Commit(database_id, epoch, lease_id, local_transaction_state)
DatabaseManager.GetOrOpen(database_id, mode)
```

The `CommitCoordinator` is the only component authorized to advance
`commits/current.json`. Ownership and commit-manifest algorithms live in one
well-tested package each (`ownership/`, `commit/`) — never duplicated in API
handlers or VFS call sites.

## Write API

`POST /v1/write`

```json
{
  "organization_id": "org_123",
  "user_id": "user_456",
  "idempotency_key": "unique-per-(database,operation)",
  "sql": "INSERT INTO todos (id, title) VALUES (?, ?)",
  "args": [1, "write the spec"]
}
```

Semantics:

- Every write carries a caller-generated idempotency key scoped to
  `(database_id, operation)`. Writers persist deduplication results so a
  retry after ambiguous transport failure cannot duplicate a mutation.
- The response is sent only after the durable commit manifest CAS succeeds.
- Responses include `database_id` and `commit_sequence` on success, plus
  structured retryability. Storage credentials, object keys, epochs, and
  internal topology are never exposed to external clients.

Success:

```json
{"database_id": "org/org_123/user/user_456", "commit_sequence": 42}
```

Errors (structured, with `retryable` flag):

| HTTP | `error` | Meaning | Client action |
| --- | --- | --- | --- |
| 409 | `DB_RETRY_ON_NEW_OWNER` | Ownership/manifest lost mid-write | Re-route; retry with the same idempotency key |
| 503 | `DB_OWNER_UNAVAILABLE` | Draining, or previous lease pending takeover | Retry shortly with the same key |
| 400 | `BAD_REQUEST` | Invalid input | Fix request |
| 500 | `INTERNAL` | Unexpected failure | Retry with the same key (idempotent) |

## Read API

`POST /v1/read`

```json
{
  "organization_id": "org_123",
  "user_id": "user_456",
  "consistency": "strong",
  "sql": "SELECT id, title FROM todos WHERE id = ?",
  "args": [1]
}
```

- `consistency: "strong"` (default) — served from the writer's authoritative
  open state.
- `consistency: "committed_snapshot"` — only data represented by the latest
  committed manifest; served by read-only API deployments.

Response: JSON array of row objects, e.g. `[{"id": 1, "title": "write the spec"}]`.

## Health and drain

- `GET /healthz` — `200 {"status":"ok"}` or `503 {"status":"draining"}`.
- `POST /drain` — stop admitting new writes; finish in-flight durable
  commits; then terminate (rolling-update sequence, spec §15).
