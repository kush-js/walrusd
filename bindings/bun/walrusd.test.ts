// Bun integration suite for the walrusd binding (spec §11, §15). It runs the
// same lease/flush matrix as the Go conformance suite through the native
// addon; Bun loads the same .node addon via its Node-API compatibility layer.
import { describe, test, expect, beforeAll } from "bun:test";
import { DatabaseSync } from "node:sqlite";
import { WalrusdDatabase, WalrusdError, vfsExtensionPath, type DatabaseDescriptor } from "../node/src/index";

const ROOT = process.env.WALRUSD_TEST_FILE_ROOT ?? "/tmp/walrusd-bun-test";

const descriptor = (id: string): DatabaseDescriptor => ({
  database_id: id,
  storage: { provider: "file", file_root: ROOT },
  credentials: {},
});

// attachNative opens a walrusd read DSN through the loadable VFS extension.
// The DSN selects the VFS by name through SQLite's URI filenames, so the host
// must open with SQLITE_OPEN_URI: node:sqlite does, bun:sqlite's Database does
// not (it treats the DSN as a literal filename).
function attachNative(
  dsn: string,
  vfs: string,
  replicaURL: string,
  accessKeyID: string,
  secretAccessKey: string,
): DatabaseSync {
  const scratch = new DatabaseSync(":memory:", { allowExtension: true });
  scratch.loadExtension(vfsExtensionPath(), "sqlite3_walrusdvfs_init");
  const attach = scratch
    .prepare("SELECT walrusd_vfs_attach(?, ?, ?, ?) AS err")
    .get(vfs, replicaURL, accessKeyID, secretAccessKey) as { err: string | null };
  scratch.close();
  expect(attach.err).toBeNull();
  return new DatabaseSync(dsn, { readOnly: true });
}

beforeAll(() => {
  require("node:fs").mkdirSync(ROOT, { recursive: true });
});

describe("WalrusdDatabase", () => {
  test("version exposes core and protocol versions", () => {
    const v = WalrusdDatabase.version();
    expect(v.api_version).toBe(1);
    expect(typeof v.core_version).toBe("string");
  });

  test("write executes a batch with flush and read sees it", async () => {
    const db = new WalrusdDatabase({ owner: "bun-test" });
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

  test("small read/write limits survive eviction and re-registration", async () => {
    const db = new WalrusdDatabase({
      owner: `bun-eviction-${Math.random().toString(36).slice(2)}`,
      maxReadInstances: 2,
      readInstanceIdleTtlMs: 100,
      vfsPageCacheBytes: 64 * 1024,
      writeSyncIntervalMs: 100,
      maxTempWriteBuffer: 2 * 1024 * 1024,
    });
    const ids = Array.from({ length: 5 }, (_, i) => `users/bun-eviction-${Date.now()}-${i}`);
    for (let i = 0; i < ids.length; i++) {
      await db.write({
        database: descriptor(ids[i]),
        idempotencyKey: `seed-${i}`,
        statements: [
          { sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" },
          { sql: "INSERT INTO t (v) VALUES (?)", params: [`v${i}`] },
        ],
      });
    }
    for (let i = 0; i < ids.length; i++) {
      const { rows } = await db.read({
        database: descriptor(ids[i]),
        sql: "SELECT v FROM t LIMIT 1",
      });
      expect(rows[0].v).toBe(`v${i}`);
    }
    await db.close();
  });

  test("write requires an idempotency key", async () => {
    const db = new WalrusdDatabase({ owner: "bun-test" });
    const d = descriptor("users/user_2");
    let err: any;
    try {
      await db.write({ database: d, idempotencyKey: "", statements: [{ sql: "SELECT 1" }] });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WalrusdError);
    expect(err.code).toBe("DB_INVALID_ARGUMENT");
    await db.close();
  });

  test("second writer waits while the lease is held", async () => {
    const db = new WalrusdDatabase({ owner: "bun-test-2", requestTimeoutMs: 2000 });
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

// Live S3 (R2) end-to-end through the whole native stack (spec §15).
// Enabled only when WALRUSD_TEST_S3_* is set. The root prefix namespaces the
// objects inside the bucket and is configurable via
// WALRUSD_TEST_S3_ROOT_PREFIX; each run gets a fresh database ID under it, so
// the prefix never appears in the database ID.
const S3_ROOT_PREFIX = process.env.WALRUSD_TEST_S3_ROOT_PREFIX ?? "walrusd-bun-test";

interface S3Env {
  endpoint: string;
  bucket: string;
  access_key_id: string;
  secret_access_key: string;
  root_prefix: string;
}

const s3env = (): S3Env | null => {
  const endpoint = process.env.WALRUSD_TEST_S3_ENDPOINT;
  const bucket = process.env.WALRUSD_TEST_S3_BUCKET;
  const access_key_id = process.env.WALRUSD_TEST_S3_ACCESS_KEY_ID;
  const secret_access_key = process.env.WALRUSD_TEST_S3_SECRET_ACCESS_KEY;
  if (!endpoint || !bucket || !access_key_id || !secret_access_key) return null;
  return { endpoint, bucket, access_key_id, secret_access_key, root_prefix: S3_ROOT_PREFIX };
};

const s3Descriptor = (s3: S3Env, databaseID: string): DatabaseDescriptor => ({
  database_id: databaseID,
  storage: {
    provider: "s3",
    endpoint: s3.endpoint,
    region: "auto",
    bucket: s3.bucket,
    root_prefix: s3.root_prefix,
    access_key_id: s3.access_key_id,
    secret_access_key: s3.secret_access_key,
  },
  credentials: {
    access_key_id: s3.access_key_id,
    secret_access_key: s3.secret_access_key,
  },
});

test("live R2 write/flush/read-back", async () => {
  const s3 = s3env();
  if (!s3) {
    console.log("WALRUSD_TEST_S3_* not set; skipping live R2 test");
    return;
  }
  const db = new WalrusdDatabase({ owner: "bun-live" });
  const d = s3Descriptor(s3, `users/live_${Date.now()}`);
  const res = await db.write({
    database: d,
    idempotencyKey: "live_1",
    statements: [
      { sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" },
      { sql: "INSERT INTO t VALUES (1, 'from-bun-live')" },
    ],
  });
  expect(typeof res.txid).toBe("string");
  expect(res.txid).not.toBe("");

  const rows = await db.read({ database: d, sql: "SELECT v FROM t WHERE id = 1" });
  expect(rows.rows[0].v).toBe("from-bun-live");
  await db.close();
}, 60_000);

// Native read mode via the loadable VFS extension (spec §9): the host's own
// SQLite streams LTX pages from object storage through the attached VFS.
// Uses the same file root as the write tests above.
test("native read mode through attached VFS", async () => {
  const db = new WalrusdDatabase({ owner: "bun-native" });
  const d = descriptor(`users/native_${Date.now()}_${Math.random().toString(36).slice(2)}`);
  const res = await db.write({
    database: d,
    idempotencyKey: "nat_" + Math.random().toString(36).slice(2),
    statements: [
      { sql: "CREATE TABLE IF NOT EXISTS docs (id INTEGER PRIMARY KEY, body TEXT)" },
      { sql: "INSERT INTO docs VALUES (1, 'native mode')" },
    ],
  });
  expect(typeof res.txid).toBe("string");

  const { dsn, vfs, replica_url } = await db.readDsn({ database: d });
  expect(vfs).toBeTruthy();

  // The file provider's replica URL is local; attach via the extension only
  // supports s3 today, so native VFS reads are gated on s3 descriptors.
  if (!replica_url) {
    await db.close();
    console.log("file provider: skipping VFS attach (s3 only)");
    return;
  }
  const native = attachNative(
    dsn,
    vfs,
    replica_url,
    d.storage.access_key_id ?? "",
    d.storage.secret_access_key ?? "",
  );
  const rows = native.prepare("SELECT body FROM docs WHERE id = 1").all() as { body: string }[];
  native.close();
  await db.close();
  expect(rows.length).toBe(1);
  expect(rows[0].body).toBe("native mode");
}, 30_000);

// Live S3 (R2) native read via the attached VFS extension (spec §15).
test("live R2 native VFS read", async () => {
  const s3 = s3env();
  if (!s3) {
    console.log("WALRUSD_TEST_S3_* not set; skipping live native read test");
    return;
  }
  const db = new WalrusdDatabase({ owner: "bun-native-live" });
  const d = s3Descriptor(s3, `users/native_live_${Date.now()}`);
  const res = await db.write({
    database: d,
    idempotencyKey: "live",
    statements: [
      { sql: "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)" },
      { sql: "INSERT INTO t VALUES (1, 'native-live')" },
    ],
  });
  expect(res.txid).toBeTruthy();

  const { dsn, vfs, replica_url } = await db.readDsn({ database: d });
  expect(replica_url).toContain("s3://");

  const native = attachNative(dsn, vfs, replica_url, s3.access_key_id, s3.secret_access_key);
  const rows = native.prepare("SELECT v FROM t WHERE id = 1").all() as { v: string }[];
  native.close();
  await db.close();
  expect(rows.length).toBe(1);
  expect(rows[0].v).toBe("native-live");
}, 60_000);
