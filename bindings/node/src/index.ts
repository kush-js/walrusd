// @walrus/db — WALrus binding for Node.js and Bun (spec §11).
// All SQLite/VFS/lease work happens in the Go core via the C ABI; this
// wrapper only translates JSON envelopes to typed values and errors.

import { join } from "node:path";
import { existsSync, readdirSync } from "node:fs";

interface NativeAPI {
  load(path: string): void;
  version(): string;
  init(configJson: string): number;
  write(handle: number, requestJson: string, deadlineMs: number): Promise<string>;
  read(handle: number, requestJson: string, deadlineMs: number): Promise<string>;
  readDsn(handle: number, requestJson: string, deadlineMs: number): Promise<string>;
  close(handle: number): string;
}

// Envelope protocol (spec §11: JSON with explicit api_version).
const API_VERSION = 1;

interface Envelope {
  api_version: number;
  ok: boolean;
  result?: any;
  error?: { class: string; message: string; retry_after_ms?: number };
}

export interface StorageProfile {
  provider: "s3" | "file";
  endpoint?: string;
  region?: string;
  bucket?: string;
  root_prefix?: string;
  access_key_id?: string;
  secret_access_key?: string;
  file_root?: string;
}

export interface DatabaseDescriptor {
  /** Caller's canonical database ID: a path-safe relative key such as
   *  "user_1a4b" or "acme/agents/a7". Objects live at
   *  "<root_prefix>/<database_id>/". */
  database_id: string;
  storage: StorageProfile;
  credentials: { access_key_id?: string; secret_access_key?: string };
}

export interface Statement {
  sql: string;
  params?: unknown[];
}

export interface WriteOptions {
  database: DatabaseDescriptor;
  idempotencyKey: string;
  statements: Statement[];
}

export interface WriteResult {
  txid?: string;
}

export interface ReadOptions {
  database: DatabaseDescriptor;
  sql: string;
  params?: unknown[];
  consistency?: "remote_committed";
}

export interface ReadResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface RuntimeOptions {
  owner: string;
  writeBufferRootPath?: string;
  requestTimeoutMs?: number;
}

/** Classified WALrus error (spec §11 required errors). */
export class WALrusError extends Error {
  readonly code: string;
  readonly retryAfterMs?: number;
  constructor(code: string, message: string, retryAfterMs?: number) {
    super(message);
    this.code = code;
    this.retryAfterMs = retryAfterMs;
  }
}

function platformDir(): string {
  const platform = `${process.platform}-${process.arch}`;
  return join(__dirname, "..", "prebuilds", platform);
}

function loadNative(): NativeAPI {
  // Prebuilds first (npm package); fall back to in-repo dev builds.
  const candidates = [
    join(platformDir(), "walrus.node"),
    join(__dirname, "..", "native", "build", "Release", "walrus.node"),
    join(__dirname, "..", "..", "..", "bindings", "node", "native", "build", "Release", "walrus.node"),
  ];
  const addonPath = candidates.find(existsSync);
  if (!addonPath) {
    throw new Error("@walrus/db: native addon not built. Run `npm run build`.");
  }
  const native: NativeAPI = require(addonPath);
  // Locate the bundled shared library (Go core).
  const libCandidates = [
    join(platformDir(), "libwalrus.dylib"),
    join(platformDir(), "libwalrus.so"),
    join(__dirname, "..", "lib", findLib(join(__dirname, "..", "lib"))),
    join(__dirname, "..", "..", "..", "libwalrus.dylib"),
  ];
  const libPath = libCandidates.find((p) => p && existsSync(p));
  if (!libPath) {
    throw new Error("@walrus/db: libwalrus shared library not found");
  }
  native.load(libPath);
  return native;
}

/** Absolute path to the bundled litestream VFS read extension for the host
 *  SQLite (bun native read mode, spec §9). Load with:
 *  db.loadExtension(path, "sqlite3_walrusvfs_init"). */
export function vfsExtensionPath(): string {
  const p = join(platformDir(), "libwalrus_vfs.dylib");
  if (!existsSync(p)) {
    throw new Error(`@walrus/db: VFS extension not found for ${process.platform}-${process.arch}`);
  }
  return p;
}

function findLib(dir: string): string {
  if (!existsSync(dir)) return "";
  const files = readdirSync(dir);
  return files.find((f) => f.startsWith("libwalrus.") && !f.endsWith(".h")) ?? "";
}

function unwrap(res: string): any {
  const env = JSON.parse(res) as Envelope;
  if (env.api_version !== API_VERSION) {
    throw new WALrusError("DB_PROTOCOL_MISMATCH", `api_version ${env.api_version} != ${API_VERSION}`);
  }
  if (!env.ok) {
    throw new WALrusError(env.error!.class, env.error!.message, env.error!.retry_after_ms);
  }
  return env.result;
}

/**
 * WALrusDatabase is the batch-oriented interface (spec §11): all statements
 * in a write() run in one SQLite transaction under one lease, followed by
 * the mandatory flush-before-release sequence.
 */
export class WALrusDatabase {
  private native: NativeAPI;
  private handle: number;
  private defaultDeadlineMs: number;

  constructor(options: RuntimeOptions) {
    this.native = loadNative();
    this.handle = this.native.init(
      JSON.stringify({
        owner: options.owner,
        config: {
          request_timeout_ms: options.requestTimeoutMs,
          write_buffer_root_path: options.writeBufferRootPath,
        },
      })
    );
    this.defaultDeadlineMs = options.requestTimeoutMs ?? 20_000;
  }

  /** Static version/core info (spec §11: included in health/status). */
  static version(): { api_version: number; core_version: string } {
    const native = loadNative();
    return unwrap(native.version());
  }

  async write(options: WriteOptions): Promise<WriteResult> {
    const request = JSON.stringify({
      descriptor: options.database,
      idempotency_key: options.idempotencyKey,
      statements: options.statements,
    });
    const deadline = Date.now() + this.defaultDeadlineMs;
    const res = await this.native.write(this.handle, request, deadline);
    return unwrap(res) as WriteResult;
  }

  async read(options: ReadOptions): Promise<ReadResult> {
    const request = JSON.stringify({
      descriptor: options.database,
      sql: options.sql,
      params: options.params,
      consistency: options.consistency,
    });
    const deadline = Date.now() + this.defaultDeadlineMs;
    const res = await this.native.read(this.handle, request, deadline);
    return unwrap(res) as ReadResult;
  }
  /**
   * readDsn returns a SQLite DSN (file:...?vfs=walrus_N&mode=ro) that the
   * HOST's own SQLite can open to read through litestream VFS natively,
   * plus the replica URL for the loadable-extension path. On Bun:
   *   1. Database.setCustomSQLite(...) once
   *   2. scratch = new Database(":memory:");
   *      scratch.loadExtension("<pkg>/prebuilds/libwalrus_vfs", "sqlite3_walrusvfs_init")
   *      scratch.exec(`SELECT walrus_vfs_attach('<vfs>', '<replica_url>', key, secret)`)
   *      scratch.close()
   *   3. new Database("file:...?vfs=<vfs>&mode=ro")
   * Reads stream LTX pages from object storage; no lease, read-only
   * (remote-committed state only, spec §9).
   */
  async readDsn(options: { database: DatabaseDescriptor }): Promise<{ dsn: string; vfs: string; replica_url?: string }> {
    const request = JSON.stringify({ descriptor: options.database });
    const deadline = Date.now() + this.defaultDeadlineMs;
    const res = await this.native.readDsn(this.handle, request, deadline);
    return unwrap(res) as { dsn: string; vfs: string; replica_url?: string };
  }

  async close(): Promise<void> {
    const res = this.native.close(this.handle);
    unwrap(res);
  }
}

export default WALrusDatabase;
