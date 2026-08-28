# Database lifecycle and read model

Translation of the specification (§8-9) into implementation documentation.
Implemented in `dbmanager/`.

## Lifecycle

Each writer holds only a bounded set of hot databases:

```text
COLD -> OPENING -> HOT -> IDLE -> EVICTING -> COLD
                         \-> FENCED/FAILED
```

### Opening

One per-database single-flight promise (`sync.Once` keyed by database ID)
prevents multiple concurrent opens. For a writable open:

1. Acquire/revalidate authoritative ownership (`fencer.Authorize` — a routing
   gate, not authorization; the object-storage CAS is the authorization).
2. Build the LTX-over-storage replica client for the database prefix.
3. Open the database through the Litestream VFS in non-hydrating mode
   (`HydrationEnabled = false`); only required remote state is fetched.
4. Register the hot instance.

A brand-new database (empty replica) is bootstrapped by the VFS itself; the
first committed transaction produces the first LTX object.

### Hot state

The hot instance contains only transient state: the VFS handle with its RAM
page cache (LRU, bounded by `-db-page-cache`), the temporary write buffer
(ephemeral disk, bounded; never the sole basis for acknowledging a write),
ownership/manifest versions, in-flight request count, and access timestamps.
It is not a durable tenant replica. Normal operation never reconstructs a
complete SQLite database file on worker disk.

### Eviction

`-db-idle-ttl` (default 60s) makes an idle database eligible when it has no
active requests, no opening operation, and no pending acknowledged durability
work. Eviction:

1. Removes the instance from the hot registry (new work is refused);
2. Relinquishes/demotes write ownership (explicit release is best-effort;
   lease expiry is acceptable);
3. Closes the SQLite/VFS instance and releases RAM and buffers.

A global resource governor evicts least-recently-used eligible databases
before admitting more when `-db-max-hot` pressure is reached. A database
with a commit-manifest CAS in progress is never evicted.

## Read model

Reads have two explicit consistency modes:

- **Strong / read-your-writes** — route to the current writer and read the
  writer's authoritative open state. Default wherever application semantics
  are unclear.
- **Committed snapshot** — an API worker may serve reads against the latest
  verified commit manifest in object storage; data may lag an in-flight
  unacknowledged mutation. Requires a read-only API deployment (the
  embedded writer binary reports `UNSUPPORTED_CONSISTENCY`).

Read-only instances obey the same bounded caching and idle-eviction policy
and never write ownership or database data.

## Durable write path (exec contract)

`Manager.ExecuteWrite(ctx, dbID, prefix, operation, idempotencyKey, exec)`
drives the sequence from [ownership-and-commits.md]. The `exec` callback
receives the SQLite DSN and a `sync` callback. The connection **must stay
open** while `sync()` runs — the connection's VFS file is the handle that
flushes dirty pages — and is closed afterwards:

```go
err := mgr.ExecuteWrite(ctx, dbID, prefix, "sql", idemKey,
    func(dsn string, sync func() error) error {
        db, err := sql.Open("sqlite3", dsn)
        if err != nil { return err }
        defer db.Close()
        if _, err := db.Exec(stmt); err != nil { return err }
        return sync() // dirty pages -> immutable LTX -> manifest CAS next
    })
```

`ExecuteWrite` then advances the commit manifest (epoch-aware CAS) and only
then stores the idempotency result. The writing connection's own pager must
not be used for reads after a sync; subsequent reads go through fresh
connections, which always observe the durable committed state.
