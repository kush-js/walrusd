// walrusd end-to-end check for the Bun binding (@walrusd/db) against real
// object storage (MinIO/S3) and a real Redis/Valkey lease store. Bun loads
// the same Node-API addon through its Node-API compatibility layer.
//
// The full loop for one fresh database: a read before any write (no lease), a
// schema write, an insert write, an idempotent retry, and read-after-write.
// Around every step the lease key is inspected directly in Redis (plain RESP
// over a socket) to prove the runtime alone acquires and releases the lease
// and that reads never touch it.
//
// Run (after the addon is built):
//
//	bun e2e/bun/e2e.ts
//
// Configuration (all optional):
//
//	WALRUSD_E2E_ENDPOINT, WALRUSD_E2E_REGION, WALRUSD_E2E_BUCKET,
//	WALRUSD_E2E_ACCESS_KEY, WALRUSD_E2E_SECRET_KEY, WALRUSD_E2E_REDIS,
//	WALRUSD_E2E_ROOT_PREFIX (object-storage namespace, default "walrusd-e2e")
//
// WALRUSD_E2E_ROOT_PREFIX is the only per-deployment knob: the per-run
// database ID lives under it as "users/<library>_<random>" and never repeats
// the prefix. Every language's e2e script follows this layout.
import net from "node:net";
import { WalrusdDatabase, type DatabaseDescriptor } from "../../bindings/node/src/index";

const ENDPOINT = process.env.WALRUSD_E2E_ENDPOINT ?? "http://127.0.0.1:9000";
const REGION = process.env.WALRUSD_E2E_REGION ?? "us-east-1";
const BUCKET = process.env.WALRUSD_E2E_BUCKET ?? "walrusd-e2e";
const ACCESS_KEY = process.env.WALRUSD_E2E_ACCESS_KEY ?? "walrusd";
const SECRET_KEY = process.env.WALRUSD_E2E_SECRET_KEY ?? "walrusdsecret";
const REDIS = process.env.WALRUSD_E2E_REDIS ?? "127.0.0.1:6379";
const ROOT_PREFIX = process.env.WALRUSD_E2E_ROOT_PREFIX ?? "walrusd-e2e";

const databaseID = `users/bun_${Math.random().toString(36).slice(2)}`;
const leaseKey = `${ROOT_PREFIX}/${databaseID}/lease.json`;

const descriptor: DatabaseDescriptor = {
  database_id: databaseID,
  storage: {
    provider: "s3",
    endpoint: ENDPOINT,
    region: REGION,
    bucket: BUCKET,
    root_prefix: ROOT_PREFIX,
  },
  credentials: { access_key_id: ACCESS_KEY, secret_access_key: SECRET_KEY },
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const db = new WalrusdDatabase({ owner: "e2e-bun-owner", redisAddress: REDIS });

try {
  // 1. A read on a database with no flushed state must not take a lease.
  const empty = await db.read({ database: descriptor, sql: "SELECT count(*) AS n FROM sqlite_master" });
  assert(Number(empty.rows[0].n) === 0, `read before write: expected empty database, got ${JSON.stringify(empty.rows)}`);
  await requireLeaseAbsent("read before write");
  console.log("ok read-before-write: empty database served, no lease created");

  // 2. Schema write: the runtime acquires the lease, commits, flushes to
  // object storage, releases the lease, and only then acknowledges.
  const schema = await db.write({
    database: descriptor,
    idempotencyKey: "e2e-schema",
    statements: [{ sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)" }],
  });
  assert(schema.txid, "schema write: empty txid");
  await requireLeaseReleased("schema write");
  console.log(`ok schema write: txid=${schema.txid} lease released`);

  // 3. Insert write with a fresh idempotency key.
  const value = `value-${Math.random().toString(36).slice(2)}`;
  const insertKey = `e2e-insert-${Math.random().toString(36).slice(2)}`;
  const insert = await db.write({
    database: descriptor,
    idempotencyKey: insertKey,
    statements: [{ sql: "INSERT INTO kv (k, v) VALUES (?, ?)", params: ["greeting", value] }],
  });
  assert(insert.txid! > schema.txid!, `insert write: txid ${insert.txid} did not advance past ${schema.txid}`);
  const released = await leaseState("insert write");
  assert(released.state === "released", `insert write: lease state ${released.state}, want released`);
  assert(released.epoch >= 2, `insert write: epoch ${released.epoch}, want >= 2 (one acquisition per write)`);
  console.log(`ok insert write: txid=${insert.txid} epoch=${released.epoch} lease released`);

  // 4. Retrying the same idempotency key is deduplicated: same TXID, no
  // second mutation, lease still ends released.
  const retry = await db.write({
    database: descriptor,
    idempotencyKey: insertKey,
    statements: [{ sql: "INSERT INTO kv (k, v) VALUES (?, ?)", params: ["greeting", value] }],
  });
  assert(retry.txid === insert.txid, `idempotent retry: txid ${retry.txid}, want ${insert.txid}`);
  await requireLeaseReleased("idempotent retry");
  console.log(`ok idempotent retry: txid=${retry.txid} deduplicated`);

  // 5. Reads see the flushed state and leave the lease record untouched.
  const before = await leaseState("read after write");
  const read = await db.read({ database: descriptor, sql: "SELECT v FROM kv WHERE k = ?", params: ["greeting"] });
  assert(read.rows[0].v === value, `read after write: got ${read.rows[0].v}, want ${value}`);
  await requireLeaseUnchanged("read after write", before);
  console.log(`ok read after write: value=${read.rows[0].v} lease untouched (epoch=${before.epoch})`);

  // 6. A second read is served from the cached read session; still no lease.
  const again = await db.read({ database: descriptor, sql: "SELECT count(*) AS n FROM kv" });
  assert(Number(again.rows[0].n) === 1, `second read: ${JSON.stringify(again.rows)}, want 1 row`);
  await requireLeaseUnchanged("second read", before);
  console.log("ok second read: cached session, lease untouched");

  console.log("PASS bun");
} finally {
  await db.close();
}

// ---- lease inspection (RESP over a socket; HGET only needs bulk strings) ----

interface LeaseRecord {
  state: string;
  owner: string;
  epoch: number;
  lease_id: string;
}

async function redis(command: string): Promise<number | string | null> {
  const [host, port] = REDIS.split(":");
  const { promise, resolve, reject } = Promise.withResolvers<number | string | null>();
  const socket = net.createConnection({ host, port: Number(port) });
  let buffer = Buffer.alloc(0);
  socket.on("error", (error) => {
    socket.destroy();
    reject(error);
  });
  socket.on("connect", () => socket.write(`${command}\r\n`));
  socket.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);
    const reply = parseReply(buffer);
    if (reply === undefined) return;
    socket.end();
    resolve(reply);
  });
  return await promise;
}

// Returns the reply value, or undefined while more bytes are needed.
function parseReply(buffer: Buffer): number | string | null | undefined {
  const end = buffer.indexOf("\r\n");
  if (end < 0) return undefined;
  const header = buffer.subarray(0, end).toString();
  if (header.startsWith(":")) return Number(header.slice(1));
  if (header.startsWith("-")) throw new Error(`redis: ${header.slice(1)}`);
  if (header.startsWith("$")) {
    const length = Number(header.slice(1));
    if (length < 0) return null;
    if (buffer.length < end + 2 + length + 2) return undefined;
    return buffer.subarray(end + 2, end + 2 + length).toString();
  }
  throw new Error(`redis: unsupported reply ${header}`);
}

async function leaseState(step: string): Promise<LeaseRecord> {
  const fields = await redis(`HGET ${leaseKey} data`);
  assert(typeof fields === "string", `${step}: lease record ${leaseKey} not found`);
  return JSON.parse(fields) as LeaseRecord;
}

async function requireLeaseAbsent(step: string): Promise<void> {
  const exists = await redis(`EXISTS ${leaseKey}`);
  assert(exists === 0, `${step}: lease key ${leaseKey} exists; the operation took a lease it should not have`);
}

async function requireLeaseReleased(step: string): Promise<void> {
  const record = await leaseState(step);
  assert(record.state === "released", `${step}: lease state ${record.state}, want released`);
}

async function requireLeaseUnchanged(step: string, before: LeaseRecord): Promise<void> {
  const after = await leaseState(step);
  assert(
    after.state === before.state && after.epoch === before.epoch && after.lease_id === before.lease_id,
    `${step}: lease record changed (${JSON.stringify(before)} -> ${JSON.stringify(after)}); reads must not touch leases`,
  );
}
