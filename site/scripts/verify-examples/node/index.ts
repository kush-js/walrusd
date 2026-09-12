import { WalrusdDatabase, WalrusdError } from "@walrusd/db";

async function createRuntime() {
  const db = new WalrusdDatabase({
    owner: "api-pod-7",                 // this instance's identity (lease owner)
    requestTimeoutMs: 20_000,           // per-attempt timeout; default 20_000
    // redisAddress: "127.0.0.1:6379", // optional shared leases
    // (unset = in-process memory leases: dev/single-process only)
  });

  await db.close();
}

await createRuntime();

const root = process.env.WALRUSD_EXAMPLE_ROOT ?? "/tmp/walrusd-example";

const descriptor = {
  database_id: "users/user_1a4b",
  storage: { provider: "file", file_root: root },
  credentials: {},
};

const db = new WalrusdDatabase({
  owner: "api-pod-7",
  requestTimeoutMs: 20_000,
  writeBufferRootPath: process.env.WALRUSD_BUFFER_ROOT,
});

async function readValue(db, descriptor) {
  const { rows } = await db.read({
    database: descriptor,
    sql: "SELECT body FROM events WHERE id = ?",
    params: [1],
  });
  return rows;
}

const { txid } = await db.write({
  database: descriptor,
  idempotencyKey: "create-event-42",
  statements: [
    { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
    { sql: "INSERT INTO events (id, body) VALUES (?, ?)", params: [1, "hello from walrusd"] },
  ],
});

function shouldRetry(error: unknown): boolean {
  if (error instanceof WalrusdError) {
    if (error.code === "DB_FLUSH_FAILED") {
      // retry with the SAME idempotency key — the retry is a no-op if the
      // first attempt actually committed
      return true;
    } else if (error.code === "DB_BUSY") {
      // retry after error.retryAfterMs
      return error.retryAfterMs !== undefined;
    }
  }
  return false;
}

const rows = await readValue(db, descriptor);
console.log(`Node.js / Bun durable at txid ${txid}`);
console.log(`Node.js / Bun read: ${rows[0].body}`);
await db.close();
