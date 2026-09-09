// WALrus end-to-end footgun suite (Node, file provider).
// Covers every fixed footgun plus the core contract. Run:
//   cd bindings/node && npx tsc -p . && node --test smoke.test.mjs e2e.test.mjs
// Uses only the public @walrus/db surface (dist/index.js) + file storage,
// so each test is hermetic under its own database_id / file root.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WALrusDatabase, WALrusError } from "./dist/index.js";

const ROOT = mkdtempSync(join(tmpdir(), "walrus-e2e-"));
// Shared Redis leases when available: the same suite then exercises
// cross-handle exclusion instead of the in-process memory fallback.
const REDIS = process.env.WALRUS_TEST_REDIS_ADDR || undefined;
if (REDIS) console.log(`e2e leases: redis ${REDIS}`);
const freshDB = (owner = "e2e") =>
  new WALrusDatabase({ owner: `${owner}-${Math.random().toString(36).slice(2)}`, redisAddress: REDIS });
let seq = 0;
const uniq = (p) => `${p}_${Date.now()}_${seq++}_${Math.random().toString(36).slice(2)}`;
const d = (id, root = ROOT) => ({
  database_id: id,
  storage: { provider: "file", file_root: root },
  credentials: {},
});

describe("write/read contract", () => {
  test("write returns TXID and read sees flushed state", async () => {
    const db = freshDB();
    const id = uniq("users/contract");
    const r1 = await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" }],
    });
    assert.ok(r1.txid, "expected TXID");
    const r2 = await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "INSERT INTO t (v) VALUES ('hello')" }],
    });
    assert.ok(r2.txid > r1.txid, `TXIDs increase: ${r1.txid} -> ${r2.txid}`);
    const { rows } = await db.read({ database: d(id), sql: "SELECT v FROM t LIMIT 1" });
    assert.equal(rows[0].v, "hello");
    await db.close();
  });

  test("write requires an idempotency key", async () => {
    const db = freshDB();
    await assert.rejects(
      db.write({ database: d(uniq("users/nokey")), idempotencyKey: "", statements: [{ sql: "SELECT 1" }] }),
      (e) => e instanceof WALrusError && e.code === "DB_INVALID_ARGUMENT",
    );
    await db.close();
  });

  test("readDsn returns a usable read-only DSN shape", async () => {
    const db = freshDB();
    const id = uniq("users/dsn");
    await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" }],
    });
    const { dsn, vfs } = await db.readDsn({ database: d(id) });
    assert.ok(vfs, "expected vfs name");
    assert.ok(dsn.includes(`vfs=${vfs}`) && dsn.includes("mode=ro"), `DSN carries vfs+ro: ${dsn}`);
    await db.close();
  });
});

describe("single-transaction atomicity", () => {
  test("failing second statement rolls back the whole batch", async () => {
    const db = freshDB();
    const id = uniq("users/atomic");
    await assert.rejects(
      db.write({
        database: d(id), idempotencyKey: uniq("k"),
        statements: [
          { sql: "CREATE TABLE atomic_t (id INTEGER PRIMARY KEY, v TEXT)" },
          { sql: "THIS IS NOT SQL" },
        ],
      }),
    );
    // Table creation was inside the same tx: it must be gone (rows is
    // null when the query returns zero rows).
    const found = await db.read({
      database: d(id),
      sql: "SELECT name FROM sqlite_master WHERE type='table' AND name='atomic_t'",
    });
    assert.equal(found.rows?.length ?? 0, 0, "partial batch must not persist");
    // Same-key retry with valid SQL succeeds (nothing was recorded).
    const res = await db.write({
      database: d(id), idempotencyKey: uniq("k2"),
      statements: [{ sql: "CREATE TABLE atomic_t (id INTEGER PRIMARY KEY, v TEXT)" }],
    });
    assert.ok(res.txid);
    await db.close();
  });

  test("callback must not manage its own transaction", async () => {
    const db = freshDB();
    // No nested-BEGIN path exists through the batch API by construction
    // (statements run inside the runtime-owned tx), but a raw BEGIN in the
    // batch must fail, not half-commit.
    await assert.rejects(
      db.write({
        database: d(uniq("users/nested")), idempotencyKey: uniq("k"),
        statements: [{ sql: "BEGIN" }],
      }),
    );
    await db.close();
  });
});

describe("idempotency", () => {
  test("same-key retry returns same TXID without re-running", async () => {
    const db = freshDB();
    const id = uniq("users/dedup");
    const key = uniq("k");
    const r1 = await db.write({
      database: d(id), idempotencyKey: key,
      statements: [
        { sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" },
        { sql: "INSERT INTO t (v) VALUES ('a')" },
      ],
    });
    const r2 = await db.write({
      database: d(id), idempotencyKey: key,
      statements: [{ sql: "INSERT INTO t (v) VALUES ('SHOULD NOT RUN')" }],
    });
    assert.equal(r2.txid, r1.txid, "dedup returns original TXID");
    const { rows } = await db.read({ database: d(id), sql: "SELECT COUNT(*) AS n FROM t" });
    assert.equal(rows[0].n, 1, "retry must not duplicate the mutation");
    await db.close();
  });
});

describe("concurrency", () => {
  test("parallel writes to one DB serialize with no lost updates", async () => {
    const db = freshDB();
    const id = uniq("users/hotkey");
    await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" }],
    });
    const N = 8;
    const results = await Promise.all(
      Array.from({ length: N }, (_, i) =>
        db.write({
          database: d(id), idempotencyKey: uniq(`w${i}`),
          statements: [{ sql: "INSERT INTO t (v) VALUES (?)", params: [`v${i}`] }],
        }),
      ),
    );
    const txids = new Set(results.map((r) => r.txid));
    assert.equal(txids.size, N, "each write gets a distinct TXID");
    const { rows } = await db.read({ database: d(id), sql: "SELECT COUNT(*) AS n FROM t" });
    assert.equal(rows[0].n, N, "no lost updates under lease serialization");
    await db.close();
  });

  // Cross-handle exclusion needs shared leases (Redis). On the memory
  // fallback each handle is independent, so different databases are used
  // to pin isolation instead.
  test(REDIS ? "two handles on one DB serialize via Redis" : "two instances on different DBs are independent", async () => {
    const a = freshDB("a");
    const b = freshDB("b");
    if (!REDIS) {
      const idA = uniq("users/xa");
      const idB = uniq("users/xb");
      const [r1, r2] = await Promise.all([
        a.write({ database: d(idA), idempotencyKey: uniq("k"), statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" }, { sql: "INSERT INTO t VALUES ('a')" }] }),
        b.write({ database: d(idB), idempotencyKey: uniq("k"), statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" }, { sql: "INSERT INTO t VALUES ('b')" }] }),
      ]);
      assert.ok(r1.txid && r2.txid);
      const ra = await a.read({ database: d(idA), sql: "SELECT v FROM t LIMIT 1" });
      const rb = await b.read({ database: d(idB), sql: "SELECT v FROM t LIMIT 1" });
      assert.equal(ra.rows[0].v, "a");
      assert.equal(rb.rows[0].v, "b");
    } else {
      const id = uniq("users/xshared");
      await a.write({
        database: d(id), idempotencyKey: uniq("k"),
        statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" }],
      });
      const [r1, r2] = await Promise.all([
        a.write({ database: d(id), idempotencyKey: uniq("k"), statements: [{ sql: "INSERT INTO t (v) VALUES ('a')" }] }),
        b.write({ database: d(id), idempotencyKey: uniq("k"), statements: [{ sql: "INSERT INTO t (v) VALUES ('b')" }] }),
      ]);
      assert.ok(r1.txid && r2.txid && r1.txid !== r2.txid);
      const { rows } = await a.read({ database: d(id), sql: "SELECT COUNT(*) AS n FROM t" });
      assert.equal(rows[0].n, 2, "no lost updates across handles via Redis");
    }
    await a.close();
    await b.close();
  });

  test("20 parallel reads on one DB all succeed (no shared-conn corruption)", async () => {
    const db = freshDB();
    const id = uniq("users/parread");
    await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [
        { sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" },
        { sql: "INSERT INTO t (v) VALUES ('x')" },
      ],
    });
    const reads = await Promise.all(
      Array.from({ length: 20 }, () => db.read({ database: d(id), sql: "SELECT v FROM t LIMIT 1" })),
    );
    for (const r of reads) assert.equal(r.rows[0].v, "x");
    await db.close();
  });

  test("reads during writes never fail", async () => {
    const db = freshDB();
    const id = uniq("users/rw");
    await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" }],
    });
    const writers = Array.from({ length: 4 }, (_, i) =>
      db.write({ database: d(id), idempotencyKey: uniq(`w${i}`), statements: [{ sql: "INSERT INTO t (v) VALUES (?)", params: [`v${i}`] }] }),
    );
    const readers = Array.from({ length: 10 }, () =>
      db.read({ database: d(id), sql: "SELECT COUNT(*) AS n FROM t" }).then((r) => assert.ok(r.rows[0].n >= 0)),
    );
    await Promise.all([...writers, ...readers]);
    await db.close();
  });
});

describe("event loop is not blocked", () => {
  test("write/read return real promises", async () => {
    const db = freshDB();
    const id = uniq("users/promise");
    const p = db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" }],
    });
    assert.ok(p instanceof Promise, "write() must be async (Promise), not a blocking string");
    await p;
    const q = db.read({ database: d(id), sql: "SELECT 1 AS n" });
    assert.ok(q instanceof Promise, "read() must be async (Promise)");
    await q;
    await db.close();
  });

  test("timers fire while native work is in flight", async () => {
    const db = freshDB();
    // Heavy batch so the Go core holds the worker thread for a while.
    const bulk = Array.from({ length: 1500 }, (_, i) => ({
      sql: "INSERT INTO t (v) VALUES (?)", params: [`bulk${i}`],
    }));
    const ids = [uniq("users/bulk1"), uniq("users/bulk2")];
    for (const id of ids) {
      await db.write({
        database: d(id), idempotencyKey: uniq("k"),
        statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" }],
      });
    }
    let ticks = 0;
    const timer = setInterval(() => ticks++, 5);
    try {
      await Promise.all(ids.map((id, i) =>
        db.write({ database: d(id), idempotencyKey: uniq(`b${i}`), statements: bulk }),
      ));
    } finally {
      clearInterval(timer);
    }
    // With the old synchronous addon the JS thread was parked for the whole
    // flush and ticks stayed 0. napi_async_work must let the loop breathe.
    assert.ok(ticks > 0, `event loop starved during writes (ticks=${ticks})`);
    await db.close();
  });
});

describe("storage isolation", () => {
  test("same database_id under different roots are independent", async () => {
    const db = freshDB();
    const root2 = mkdtempSync(join(tmpdir(), "walrus-e2e-iso-"));
    const id = uniq("users/same");
    await db.write({
      database: d(id), idempotencyKey: uniq("k"),
      statements: [
        { sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" },
        { sql: "INSERT INTO t VALUES ('root1')" },
      ],
    });
    await db.write({
      database: d(id, root2), idempotencyKey: uniq("k"),
      statements: [
        { sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" },
        { sql: "INSERT INTO t VALUES ('root2')" },
      ],
    });
    const r1 = await db.read({ database: d(id), sql: "SELECT v FROM t LIMIT 1" });
    const r2 = await db.read({ database: d(id, root2), sql: "SELECT v FROM t LIMIT 1" });
    assert.equal(r1.rows[0].v, "root1");
    assert.equal(r2.rows[0].v, "root2");
    await db.close();
  });
});
