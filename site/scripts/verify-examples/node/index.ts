import { WalrusdDatabase } from "@walrusd/db";

const db = new WalrusdDatabase({
  owner: "api-pod-7",
  requestTimeoutMs: 20_000,
  writeBufferRootPath: "/tmp/walrusd-example/buffers",
});

import type { DatabaseDescriptor } from "@walrusd/db";

const descriptor: DatabaseDescriptor = {
  database_id: "users/user_1a4b",
  storage: { provider: "file", file_root: "/tmp/walrusd-example" },
  credentials: {},
};

// The documentation puts Read before Write, so seed the database with the
// same idempotent operation first. The Write snippet below returns that txid.
const seed = await db.write({
  database: descriptor,
  idempotencyKey: "create-event-42",
  statements: [
    { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
    {
      sql: "INSERT INTO events (id, body) VALUES (?, ?)",
      params: [1, "hello from walrusd"],
    },
  ],
});
if (seed.txid === undefined) throw new Error("seed write returned no txid");

const { rows } = await db.read({
  database: descriptor,
  sql: "SELECT body FROM events WHERE id = ?",
  params: [1],
});
const body = rows[0].body;

const { txid } = await db.write({
  database: descriptor,
  idempotencyKey: "create-event-42",
  statements: [
    { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
    {
      sql: "INSERT INTO events (id, body) VALUES (?, ?)",
      params: [1, "hello from walrusd"],
    },
  ],
});

import { WalrusdError } from "@walrusd/db";

try {
  await db.write({
    database: descriptor,
    idempotencyKey: "create-event-42",
    statements: [
      { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
      {
        sql: "INSERT INTO events (id, body) VALUES (?, ?)",
        params: [1, "hello from walrusd"],
      },
    ],
  });
} catch (error) {
  if (error instanceof WalrusdError) {
    if (error.code === "DB_FLUSH_FAILED") {
      // retry with the SAME idempotency key
    } else if (error.code === "DB_BUSY") {
      console.log(`retry after ${error.retryAfterMs} ms`);
    }
  }
}

if (txid === undefined) throw new Error("write returned no txid");
if (typeof body !== "string") throw new Error("read returned no body");
console.log(`Node.js / Bun durable at txid ${txid}`);
console.log(`Node.js / Bun read: ${body}`);
await db.close();
