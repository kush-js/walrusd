# Using WALrus: deployment, clients, and common questions

Practical guide for running WALrus next to your application — including from
a Bun/Node API — and what your client code needs (spoiler: no SQLite
driver).

---

## Do I need a SQLite client in my app?

**No.** That is the point of the architecture. `walrusd` owns SQLite and the
object-storage durability protocol; your application talks to it over
**HTTP + JSON**. You send SQL text and parameters, you get rows back as JSON
objects. Nothing SQLite-related is linked into, or deployed with, your
application.

Works from any language or runtime that can make an HTTP request: Bun,
Node, Python, Go, curl, a cron job.

## Calling WALrus from Bun

Write (a durable mutation):

```ts
const res = await fetch("http://walrus:8080/v1/write", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    organization_id: "org_123",
    user_id: "user_456",
    idempotency_key: crypto.randomUUID(), // unique per logical mutation
    sql: "INSERT INTO todos (id, title) VALUES (?, ?)",
    args: [1, "ship it"],
  }),
});
const body = await res.json();
// { database_id: "org/org_123/user/user_456", commit_sequence: 42 }
```

Read (a query):

```ts
const res = await fetch("http://walrus:8080/v1/read", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    organization_id: "org_123",
    user_id: "user_456",
    sql: "SELECT id, title FROM todos WHERE id = ?",
    args: [1],
  }),
});
const rows = await res.json();
// [{ id: 1, title: "ship it" }]
```

A tiny typed client is ~20 lines; keep the `idempotency_key` discipline (see
[Retries and idempotency](#retries-and-idempotency)).

### Type mapping (JSON → SQLite)

| JSON | SQLite |
| --- | --- |
| `number` | bound as float64 — INTEGER-affinity columns normalize integral values back to integers; for strict integer math, `CAST` in SQL |
| `string` | TEXT |
| `true` / `false` | 1 / 0 |
| `null` | NULL |
| objects/arrays | not supported as bind values; serialize to JSON text yourself |

`args` are positional (`?` placeholders). Named parameters are not exposed
over the HTTP API yet.

### Scope: one statement per request

Each `/v1/write` executes **one statement**, auto-committed, made durable via
the manifest CAS. Multi-statement transactions are not yet exposed over the
HTTP API (a known gap; tracked for the API roadmap). For now, design writes
as single statements, or emulate atomicity with idempotent follow-ups. DDL
(`CREATE TABLE`, migrations) goes through the same endpoint — use
`IF NOT EXISTS` or unique idempotency keys.

## Can I run it alongside my Bun API?

Yes — that is the intended deployment. `walrusd` is a separate service; your
Bun API calls it over the network. Two things stay true:

- **Your API is stateless** and can scale/redeploy freely.
- **WALrus owns the data.** It can crash, restart, or be replaced; committed
  data lives in the R2 bucket and is re-opened from there.

### Recommended: separate containers (docker-compose)

```yaml
services:
  walrus:
    image: walrus:dev
    environment:
      WALRUS_WORKER_ID: "writer-1"
      WALRUS_STORAGE_ENDPOINT: "https://<account>.r2.cloudflarestorage.com"
      WALRUS_STORAGE_BUCKET: "my-bucket"
      WALRUS_STORAGE_ACCESS_KEY_ID: "${R2_ACCESS_KEY_ID}"
      WALRUS_STORAGE_SECRET_ACCESS_KEY: "${R2_SECRET_ACCESS_KEY}"
    ports: ["8080:8080"]

  api:
    build: ./my-bun-api
    environment:
      WALRUS_URL: "http://walrus:8080"
    depends_on: [walrus]
    ports: ["3000:3000"]
```

`depends_on` only orders startup; WALrus does not need the API to be up or
vice versa.

### Can they share ONE container?

Technically yes (run both processes under a small init script), but it is
**discouraged** and the coupling has a real cost:

- **Lifecycle coupling.** Restarting your API restarts the writer. If the
  writer exits, the database lease must expire (`-ownership-lease`, default
  30s) before ownership can be taken over again — every API deploy would add
  a ~30s write window for each database.
- **Resource contention.** The VFS page cache and write buffers compete with
  your API for memory.
- **Scaling.** The spec's model is an API fleet scaling on request rate and
  writers scaling on data pressure — impossible when they share a lifecycle.

The only case where one container makes sense is a throwaway local dev setup.
If you do it, keep the writer's `WALRUS_WORKER_ID` stable so restarts reuse
the lease (see [Worker ID and restarts](#worker-id-and-restarts)).

## Worker ID and restarts (important operational detail)

`WALRUS_WORKER_ID` controls takeover semantics:

- **Same ID as the previous incarnation** (e.g. restarting the same
  container/slot): the renew path applies immediately — the instance reuses
  its still-valid lease and recovers with **no unavailability window**. This
  is the normal case for a single-writer deployment.
- **A different ID** while the old lease is still valid: the writer refuses
  with `DB_OWNER_UNAVAILABLE` until the lease expires (~30s), then takes over
  with a bumped epoch. This is the correct behavior when a *new* instance
  replaces a *dead* one whose ID you retired.

Practical rule: derive the worker ID from the instance slot
(`writer-1`, `writer-2`, …), not from a random UUID, so rolling restarts are
instant and only genuine replacements wait out the lease.

Crash safety: a transaction that was **acknowledged** is in R2 (LTX + commit
manifest) and survives any crash. A transaction that was in flight when the
process died was never acknowledged — the client retries with the same
idempotency key and it executes exactly once on the new owner.

## Topology options

### 1. Dev: one writer, app beside it
The docker-compose setup above. One `walrusd` owns every database. Simplest;
what most applications need for a long time.

### 2. Several writers behind your API (routing from the app layer)
Ownership CAS makes multiple writers **safe** (only the epoch holder can
commit), but you must route each database's requests to its writer, or
writers will thrash ownership. Two options:

- **Route by user in your API.** If each user's traffic is sticky by
  construction (e.g. one writer per tenant/region), no algorithm needed.
- **Rendezvous routing in your API.** The algorithm is fully specified
  (`routing/`, canonical name `sha256-hrw-v1`): with a fixed writer list and
  generation `0`, compute
  `HMAC-SHA256(key="basemnt-hrw-v1", gen(8 bytes big-endian=0) || "org/<org>/user/<user>" || 0x00 || worker_id)`
  and pick the highest-scoring 8-byte prefix as uint64. Re-implementing this
  in Bun is ~10 lines with `Bun.CryptoHasher`/WebCrypto; all writers
  computing the same function agree on the owner. Bump the generation when
  the writer set changes.

What is *not* safe: round-robin or random LB between writers without
routing — writers would fight over ownership (requests fail with
`DB_RETRY_ON_NEW_OWNER` until the winner stabilizes).

### 3. Production fleet
API workers (Bun) + writer fleet + Consul for membership/health, with the
rendezvous router computing placement (spec §6). The routing library ships
in Go (`routing/`); Consul wiring is the remaining integration seam.

## Retries and idempotency

Errors are structured JSON with a `retryable` flag:

```json
{ "error": "DB_RETRY_ON_NEW_OWNER", "message": "not the durable owner", "retryable": true }
```

| `error` | Meaning | What your client does |
| --- | --- | --- |
| `DB_RETRY_ON_NEW_OWNER` (409) | This writer lost (or never had) authority | Retry the same request, same idempotency key, ideally after re-routing |
| `DB_OWNER_UNAVAILABLE` (503) | Draining, or takeover pending lease expiry | Wait (≤ lease duration) and retry, same key |
| `BAD_REQUEST` (400) | Missing fields, bad IDs (`/`, whitespace) | Fix the request; do not retry |
| `QUERY_FAILED` (400) | SQL error from the engine | Fix the query |
| `INTERNAL` (500) | Unexpected failure | Retry once with the same key |

**Rule: every write gets an idempotency key, and a retry always reuses the
same key.** That converts "did my write happen?" from a correctness problem
into a no-op — the server returns the original durable result. Network
timeouts after a write are always safe to retry for this reason. Reads are
safe to retry freely (no side effects).

## Consistency

`/v1/read` defaults to `consistency: "strong"` — served from the writer's
open state, so read-your-writes holds across write→read. `committed_snapshot`
(reading the last manifest without touching the writer) is a read-only-API
feature, not served by the embedded writer binary yet.

## Data lifecycle, backups, migrations

- **The bucket is the database.** Durable state is the LTX objects plus the
  commit manifest; object-storage replication/retention policies are your
  backup story. A lost writer is irrelevant; a lost bucket is total — use
  R2's versioning/replication as you would for any primary datastore.
- **Schema changes** are ordinary writes (`CREATE TABLE`, `ALTER TABLE`).
  Give each migration a stable idempotency key or use `IF NOT EXISTS` so
  re-running is safe.
- **Per-user databases are isolated** by construction: separate prefix,
  separate ownership record, separate manifest. There is no cross-user SQL.
- **Deleting a user's database** currently means removing its prefix in the
  bucket (control-plane operation; not exposed over the HTTP API).

## Local development without R2

The storage adapter requires S3-compatible **conditional writes**
(`If-None-Match` / `If-Match` on PUT). R2 is verified by the test suite. Any
store implementing those headers should work — MinIO supports them but is
untested with WALrus. Plain S3 works (S3 supports the same conditional
headers). Point `WALRUS_STORAGE_ENDPOINT`/`WALRUS_STORAGE_BUCKET` at whatever
you run.

## Health, drain, upgrades

- `GET /healthz` → 200 while serving, 503 while draining. Point your
  orchestrator's liveness/readiness at it.
- `POST /drain` → graceful shutdown path: stop accepting writes, finish
  in-flight durable commits, close databases. Use it before stopping the
  container (docker stop sends SIGTERM which triggers the same path).
- Upgrades: drain → stop → start new version. With a stable worker ID the
  new process renews the existing lease immediately (no takeover window).

## Quick answers

**Do I need a SQLite client/driver in Bun?** No — HTTP + JSON only.

**Can WALrus run in the same container as my API?** Technically yes, but
don't: deploys would restart the writer and trigger lease-expiry windows.
Run it as a separate container and call it over HTTP.

**What happens if WALrus crashes mid-write?** Unacknowledged writes are lost
and re-executed exactly once on retry (idempotency key). Acknowledged writes
were already durable in R2.

**Can two writers serve the same database?** Only the current epoch holder
commits. Give writers the rendezvous route (or one writer per set) — never
blind round-robin.

**How do I run migrations?** `POST /v1/write` with DDL and a stable
idempotency key.

**Is my data safe if the container is destroyed?** Yes — the bucket holds
the database. Any new `walrusd` with the same credentials and bucket reopens
it lazily.
