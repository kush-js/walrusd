# Routing

Translation of the specification (§6) into implementation documentation.

## Writer registration

Writers register in Consul under one service name and publish:

- stable `worker_id` (unique for the process lifetime; never reused while a
  previous incarnation may run);
- endpoint and protocol version;
- region/zone and optional capacity weight;
- liveness/readiness health checks;
- drain state and admission capacity.

Only healthy, protocol-compatible, non-draining writers appear in a routing
snapshot. Consul health changes are debounced enough to avoid flapping but
never cause the router to consider an unhealthy writer usable.

## API-local routing (rendezvous hashing)

The router uses highest-random-weight (rendezvous) hashing with the
canonical `sha256-hrw-v1` algorithm implemented in `routing/`:

```text
score = HMAC-SHA256(key = "basemnt-hrw-v1",
                    msg = generation(8 bytes BE) || database_id || 0x00 || worker_id)
selected_writer = eligible writer with highest score
```

All implementations must use the same byte encoding, hash algorithm, seed,
ordering, and weighting behavior. Weights are applied by scoring
`worker_id#wN` for N in `[0, weight)` — proportional without any modulo
scheme. The router returns the selected writer endpoint and the membership
generation used for the decision.

Use the membership snapshot captured at the beginning of a request. A Consul
update during the request affects later requests only. The API retries a
safe, idempotent request once after a selected writer is unavailable, using a
fresh snapshot; mutations require an idempotency key before retry.

Modulo hashing is prohibited. Adding or removing a writer moves only the
databases whose winning rendezvous score changes (property-tested in
`routing/routing_test.go`).

## Routing is not ownership

Routing answers "where should this request go?". Ownership answers "who may
durably commit this database now?". A request arriving at the selected writer
is still rejected if that writer cannot acquire or retain the authoritative
object-storage epoch (see [ownership-and-commits.md]).

The API does not keep a `database_id -> owner` table, acquire per-database
Consul locks, or establish sticky sessions.

## Consul integration point

`walrusd` and the routing library consume a membership snapshot
(`routing.Snapshot`). In production a Consul watch materializes that
snapshot (spec §6.1); the snapshot interface is the only seam. Without a
Consul agent (local development, single-writer deployments) the snapshot
contains the one local writer.
