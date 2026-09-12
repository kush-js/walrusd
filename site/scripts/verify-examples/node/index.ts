import { WalrusdDatabase } from "@walrusd/db";

const root = process.env.WALRUSD_EXAMPLE_ROOT ?? "/tmp/walrusd-example";

const db = new WalrusdDatabase({
  owner: "api-pod-7",                 // this instance's identity (lease owner)
  requestTimeoutMs: 20_000,           // per-attempt timeout; default 20_000
  // redisAddress: "127.0.0.1:6379", // optional shared leases
  // (unset = in-process memory leases: dev/single-process only)
});

const descriptor = {
  database_id: "users/user_1a4b",
  storage: { provider: "file", file_root: root },
  credentials: {},
};

const { txid } = await db.write({
  database: descriptor,
  idempotencyKey: "create-event-42",
  statements: [
    { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
    { sql: "INSERT INTO events (id, body) VALUES (?, ?)", params: [42, "hello from walrusd"] },
  ],
});

const { rows } = await db.read({
  database: descriptor,
  sql: "SELECT body FROM events WHERE id = ?",
  params: [42],
});

console.log(`Node.js / Bun durable at txid ${txid}`);
console.log(`Node.js / Bun read: ${rows[0].body}`);
await db.close();
