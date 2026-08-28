# WALrus

WALrus ("Write-Ahead Log in object storage") is a horizontally scalable,
multi-tenant SQLite service with one logical database per user. Object
storage (Cloudflare R2 or any S3-compatible store with conditional writes)
is the durable database; workers are disposable compute. Implementation of
the [Basemnt Distributed SQLite Storage and Database Worker Specification]
(basemnt-distributed-sqlite-worker-spec.md).

```
                          Clients
                             |
                      Load balancer
                             |
              +----------------+----------------+
              |                |                |
           API #1           API #2           API #N
         (router lib)     (router lib)     (router lib)
              +-------- rendezvous hashing ------+
                             |
              +----------------+----------------+
              |                |                |
          Writer #1        Writer #2        Writer #N
       SQLite+VFS+CAS   SQLite+VFS+CAS   SQLite+VFS+CAS
              +------------ object storage -----+
                             |
                   Org-specific bucket (R2)
```

## How it works

- **Canonical identity** — every database is `org/<organization_id>/user/<user_id>`
  (spec §5). Object keys derive from the SHA-256 of this ID; nothing
  client-supplied ever touches a storage key.
- **Real CAS, no emulation** — the storage adapter uses R2 conditional writes:
  `If-None-Match: *` for create-if-absent, `If-Match: <etag>` for
  replace-if-version (spec §5).
- **Fencing beats routing** — rendezvous hashing selects a writer; only a
  writer holding the current ownership epoch (CAS + monotonic epoch + lease +
  read-back validation) may acknowledge writes (spec §6.3, §7).
- **Durable commit manifest** — every acknowledged transaction uploaded an
  immutable LTX object and advanced `commits/current.json` via epoch-aware
  CAS. No acknowledgement precedes the CAS (spec §7.3, invariant 6).
- **No hydration** — reads go through the Litestream VFS: pages are fetched
  from LTX objects on demand into a bounded RAM cache (spec §8-9).
- **Eviction** — idle-TTL and pressure eviction keep per-worker state bounded;
  eviction never discards an acknowledged write (spec §8, invariant 11).

## Repository layout

| Path | Contents |
| --- | --- |
| `base/` | Canonical database IDs and object-key construction |
| `storage/` | S3/R2 adapter: Get, CreateIfAbsent, ReplaceIfVersion, PutImmutable |
| `ownership/` | Ownership manager: acquire, renew, takeover, fencing epochs |
| `commit/` | Commit coordinator: epoch-aware CAS on `commits/current.json` |
| `routing/` | `sha256-hrw-v1` rendezvous hashing over membership snapshots |
| `dbmanager/` | Hot-database lifecycle, LTX-over-R2 replica client, durable write path, idempotency |
| `cmd/walrusd/` | Writer service (HTTP API) |
| `docs/` | Specification translated into topic documentation |

## Installation

Requirements: Go 1.26+ (the VFS integration uses cgo), a C compiler
(`clang`/`gcc`), and make-level basics. No SQLite installation is needed —
`mattn/go-sqlite3` bundles the SQLite amalgamation.

```sh
git clone <repo> walrus && cd walrus
go build -tags vfs -o bin/walrusd ./cmd/walrusd
```

The `-tags vfs` build tag enables the Litestream VFS (cgo). Building without
it compiles the non-VFS packages only (`go build ./...`).

Or with Docker:

```sh
docker build -t walrus:dev .
```

## Running

`walrusd` is a writer. Configure via flags or environment:

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `-listen` | `WALRUS_LISTEN` | `:8080` | HTTP listen address |
| `-worker-id` | `WALRUS_WORKER_ID` | — (required) | Stable worker ID; never reused while a previous incarnation may run |
| `-storage-endpoint` | `WALRUS_STORAGE_ENDPOINT` | — (required) | S3 endpoint host (no bucket in path) |
| `-storage-bucket` | `WALRUS_STORAGE_BUCKET` | — (required) | Organization bucket |
| `-storage-region` | `WALRUS_STORAGE_REGION` | `auto` | Region (R2: `auto`) |
| `-storage-access-key` | `WALRUS_STORAGE_ACCESS_KEY_ID` | — (required) | Access key |
| `-storage-secret-key` | `WALRUS_STORAGE_SECRET_ACCESS_KEY` | — (required) | Secret key |
| `-ownership-lease` | — | `30s` | Lease duration (spec §14) |
| `-ownership-renew` | — | `10s` | Renewal interval |
| `-ownership-max-skew` | — | `2s` | Max clock skew (validated: `renew + skew < lease`) |
| `-db-idle-ttl` | — | `60s` | Idle database TTL |
| `-db-max-hot` | — | `500` | Max hot databases |
| `-db-page-cache` | — | `10485760` | VFS page-cache bytes per database |
| `-log-level` | `WALRUS_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

Example (Cloudflare R2):

```sh
export WALRUS_WORKER_ID=writer-1
export WALRUS_STORAGE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
export WALRUS_STORAGE_BUCKET=my-org-bucket
export WALRUS_STORAGE_ACCESS_KEY_ID=...
export WALRUS_STORAGE_SECRET_ACCESS_KEY=...
./bin/walrusd -listen :8080
```

If your endpoint URL embeds the bucket (`https://<account>.r2.cloudflarestorage.com/my-org-bucket`),
`walrusd` strips it automatically and fails if it disagrees with
`-storage-bucket`.

Container:

```sh
docker run -d -p 8080:8080 \
  -e WALRUS_WORKER_ID=writer-1 \
  -e WALRUS_STORAGE_ENDPOINT=https://<account>.r2.cloudflarestorage.com \
  -e WALRUS_STORAGE_BUCKET=my-org-bucket \
  -e WALRUS_STORAGE_ACCESS_KEY_ID=... \
  -e WALRUS_STORAGE_SECRET_ACCESS_KEY=... \
  walrus:dev
```

## API

### `POST /v1/write` — durable mutation

```json
{
  "organization_id": "org_123",
  "user_id": "user_456",
  "idempotency_key": "client-generated-unique-key",
  "sql": "INSERT INTO todos (id, title) VALUES (?, ?)",
  "args": [1, "write the spec"]
}
```

Response `200`: `{"database_id": "org/org_123/user/user_456", "commit_sequence": 42}`

The write is durable (LTX uploaded, manifest CAS-advanced) before the
response. Retry with the **same idempotency key** after any ambiguous failure;
you get the original result, never a duplicate execution. On
`DB_RETRY_ON_NEW_OWNER` / `DB_OWNER_UNAVAILABLE`, re-route and retry with the
same key (spec §7.4, §11).

### `POST /v1/read` — query

```json
{
  "organization_id": "org_123",
  "user_id": "user_456",
  "consistency": "strong",
  "sql": "SELECT id, title FROM todos WHERE id = ?",
  "args": [1]
}
```

`consistency` defaults to `strong` (writer's authoritative state).
`committed_snapshot` requires a read-only API deployment (see
`docs/api.md`). Responses are JSON arrays of row objects.

### `GET /healthz`, `POST /drain`

Health probe (503 while draining) and graceful drain: stop admitting writes,
finish in-flight durable commits, evict (spec §15).

## Testing

```sh
# unit tests (no network)
go test -tags vfs ./...

# integration against real R2 (or any S3-compatible CAS store)
export R2_ENDPOINT=https://<account>.r2.cloudflarestorage.com
export R2_BUCKET=my-bucket
export R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
go test -tags vfs -count=1 ./storage ./ownership
```

Covered: canonical-ID and key construction, rendezvous determinism and
limited remapping, ownership create/renew/takeover/epoch-monotonicity,
stale-epoch-cannot-advance-manifest, CAS-conflict fencing loss, idempotent
retries, reopen-without-hydration, eviction, and live R2 conditional-write
semantics.

## Documentation

The original specification is translated into topic docs:

- [docs/architecture.md](docs/architecture.md) — purpose, placement vs
  authority, tenancy and object layout (spec §1-5)
- [docs/routing.md](docs/routing.md) — membership, rendezvous hashing,
  "routing is not ownership" (spec §6)
- [docs/ownership-and-commits.md](docs/ownership-and-commits.md) — fencing
  protocol, durable commit path, failure handling (spec §7, §11)
- [docs/database-lifecycle.md](docs/database-lifecycle.md) — hot lifecycle,
  eviction, read model (spec §8-9)
- [docs/api.md](docs/api.md) — service interfaces and HTTP API (spec §10)
- [docs/operations.md](docs/operations.md) — security, observability,
  configuration, deployment, testing, invariants (spec §12-18)
