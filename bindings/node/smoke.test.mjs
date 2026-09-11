// Node.js (non-Bun) integration smoke for @walrusd/db (spec §15: Node support
// is gated on this suite, not assumed from Bun API compatibility).
// Run: node --test bindings/node/smoke.test.mjs  (or bun test).
import { test } from "node:test";
import assert from "node:assert/strict";
import { WalrusdDatabase, WalrusdError } from "./dist/index.js";
import { mkdirSync } from "node:fs";

const ROOT = process.env.WALRUSD_TEST_FILE_ROOT ?? "/tmp/walrusd-node-test";
mkdirSync(ROOT, { recursive: true });

test("version", () => {
  const v = WalrusdDatabase.version();
  assert.equal(v.api_version, 1);
});

test("write/flush/read-back on node", async () => {
  const db = new WalrusdDatabase({ owner: "node-smoke" });
  const d = {
    database_id: "users/node_1_" + Math.random().toString(36).slice(2),
    storage: { provider: "file", file_root: ROOT },
    credentials: {},
  };
  const res = await db.write({
    database: d,
    idempotencyKey: "op_" + Math.random().toString(36).slice(2),
    statements: [
      { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
      { sql: "INSERT INTO events (id, body) VALUES (1, 'hello from node')" },
    ],
  });
  assert.ok(res.txid);
  const rows = await db.read({ database: d, sql: "SELECT body FROM events WHERE id = ?", params: [1] });
  assert.equal(rows.rows[0].body, "hello from node");
  await db.close();
});

test("write requires idempotency key", async () => {
  const db = new WalrusdDatabase({ owner: "node-smoke" });
  const d = {
    database_id: "users/node_2",
    storage: { provider: "file", file_root: ROOT },
    credentials: {},
  };
  await assert.rejects(
    () => db.write({ database: d, idempotencyKey: "", statements: [{ sql: "SELECT 1" }] }),
    (e) => e instanceof WalrusdError && e.code === "DB_INVALID_ARGUMENT",
  );
  await db.close();
});
