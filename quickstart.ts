import { WalrusdDatabase } from "./bindings/node/src/index";

const db = new WalrusdDatabase({ owner: "my-api-instance" });

// The descriptor is issued by your control plane — never by end users.
const d = {
  database_id: "users/user_1",
  storage: { provider: "file", file_root: "/tmp/walrusd-quickstart" },
  credentials: {},
};

// One write = one batch = one transaction = one lease = one confirmed flush.
await db.write({
  database: d,
  idempotencyKey: "schema",
  statements: [{ sql: "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)" }],
});

const { txid } = await db.write({
  database: d,
  idempotencyKey: "set-greeting",   // retrying this key is safe: deduplicated
  statements: [{ sql: "INSERT OR REPLACE INTO kv (k, v) VALUES ('greeting', 'hello from walrusd')" }],
});
console.log("durable at txid", txid);

// Reads see only flushed, remote-committed state.
const { rows } = await db.read({ database: d, sql: "SELECT v FROM kv WHERE k = 'greeting'" });
console.log("read:", rows[0].v);

await db.close();
