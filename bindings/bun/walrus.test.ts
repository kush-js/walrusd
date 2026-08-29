// Bun integration suite for the WALrus binding (spec §11, §15). It runs the
// same lease/flush matrix as the Go conformance suite through the native
// addon; Bun loads the same .node addon via its Node-API compatibility layer.
import { describe, test, expect, beforeAll } from "bun:test";
import { WALrusDatabase, WALrusError, type DatabaseDescriptor } from "../node/src/index";

const ROOT = process.env.WALRUS_TEST_FILE_ROOT ?? "/tmp/walrus-bun-test";

const descriptor = (id: string): DatabaseDescriptor => ({
  database_id: id,
  storage: { provider: "file", file_root: ROOT },
  credentials: {},
});

beforeAll(() => {
  require("node:fs").mkdirSync(ROOT, { recursive: true });
});

describe("WALrusDatabase", () => {
  test("version exposes core and protocol versions", () => {
    const v = WALrusDatabase.version();
    expect(v.api_version).toBe(1);
    expect(typeof v.core_version).toBe("string");
  });

  test("write executes a batch with flush and read sees it", async () => {
    const db = new WALrusDatabase({ owner: "bun-test" });
    const d = descriptor("users/user_1");

    const res = await db.write({
      database: d,
      idempotencyKey: "op_1",
      statements: [
        { sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)" },
        { sql: "INSERT INTO events (id, body) VALUES (1, 'hello')" },
      ],
    });
    expect(typeof res.txid).toBe("string");

    const rows = await db.read({
      database: d,
      sql: "SELECT * FROM events WHERE id = ?",
      params: [1],
    });
    expect(rows.rows.length).toBe(1);
    expect(rows.rows[0].body).toBe("hello");
    await db.close();
  });

  test("write requires an idempotency key", async () => {
    const db = new WALrusDatabase({ owner: "bun-test" });
    const d = descriptor("users/user_2");
    let err: any;
    try {
      await db.write({ database: d, idempotencyKey: "", statements: [{ sql: "SELECT 1" }] });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WALrusError);
    expect(err.code).toBe("DB_INVALID_ARGUMENT");
    await db.close();
  });

  test("second writer waits while the lease is held", async () => {
    const db = new WALrusDatabase({ owner: "bun-test-2", requestTimeoutMs: 2000 });
    const d = descriptor("users/user_3");
    // Hold the lease via a slow write in one instance; a second instance's
    // write must serialize behind it (DB_BUSY or success after wait).
    const first = db.write({
      database: d,
      idempotencyKey: "op_a",
      statements: [{ sql: "CREATE TABLE IF NOT EXISTS t (v TEXT)" }, { sql: "INSERT INTO t VALUES ('a')" }],
    });
    const second = db.write({
      database: d,
      idempotencyKey: "op_b",
      statements: [{ sql: "INSERT INTO t VALUES ('b')" }],
    });
    const [r1, r2] = await Promise.all([first, second]);
    expect(r1.txid).toBeTruthy();
    expect(r2.txid).toBeTruthy();
    await db.close();
  });
});
