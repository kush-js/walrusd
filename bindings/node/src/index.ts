// @walrusd/db — walrusd binding for Node.js and Bun (spec §11).
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
  /** Per-write-attempt timeout in milliseconds. Default: 20000. */
  requestTimeoutMs?: number;
  /** Lease duration in milliseconds. Must exceed requestTimeoutMs plus
   *  clockSkewMs. Default: 30000. */
  leaseDurationMs?: number;
  /** Lease expiry safety margin in milliseconds. Default: 2000. */
  clockSkewMs?: number;
  /** Inner lease-acquisition retry window in milliseconds. Default: 3000. */
  acquireRetryBudgetMs?: number;
  /** Minimum inner lease retry backoff in milliseconds. Default: 25. */
  retryBackoffMinMs?: number;
  /** Maximum inner lease retry backoff in milliseconds. Default: 500. */
  retryBackoffMaxMs?: number;
  /** First outer retry delays in milliseconds. Default: 1000. */
  retryFixedDelayMs?: number;
  /** Number of fixed outer retry delays before exponential backoff.
   *  Default: 10. */
  retryFixedCount?: number;
  /** Multiplier for delays after the fixed retries. Default: 2. */
  retryMultiplier?: number;
  /** Maximum single outer retry delay in milliseconds. Default: 64000. */
  retryMaxDelayMs?: number;
  /** Total outer retry wall-clock budget in milliseconds. Default: 64000.
   *  Set to 0 to disable automatic write retries. */
  retryMaxTotalMs?: number;
  /** Maximum number of resident database read instances (an LRU).
   *  Default: 200. Values at or below 0 use the runtime fallback of 64
   *  resident databases and disable the read-session cache. */
  maxReadInstances?: number;
  /** Idle time in milliseconds before a database's cached read session is
   *  closed. Default: 60000. Values at or below 0 disable the read-session
   *  cache entirely. */
  readInstanceIdleTtlMs?: number;
  /** Per-database SQLite page cache size in bytes. Default: 10485760 (10 MiB). */
  vfsPageCacheBytes?: number;
  /** Background VFS sync interval in milliseconds. Default: 1000. The
   *  mandatory flush before lease release remains authoritative. */
  writeSyncIntervalMs?: number;
  /** Per-process temporary write-buffer cap in bytes. Default: 268435456
   *  (256 MiB). Values at or below 0 disable the cap. */
  maxTempWriteBuffer?: number;
  /** Redis/Valkey address (host:port) for shared leases. Unset = in-process
   *  memory leases (dev/single-process only; no cross-process exclusion). */
  redisAddress?: string;
  redisPassword?: string;
  redisDB?: number;
}
/** Classified walrusd error (spec §11 required errors). */
export class WalrusdError extends Error {
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
    join(platformDir(), "walrusd.node"),
    join(__dirname, "..", "native", "build", "Release", "walrusd.node"),
    join(__dirname, "..", "..", "..", "bindings", "node", "native", "build", "Release", "walrusd.node"),
  ];
  const addonPath = candidates.find(existsSync);
  if (!addonPath) {
    throw new Error("@walrusd/db: native addon not built. Run `npm run build`.");
  }
  const native: NativeAPI = require(addonPath);
  // Locate the bundled shared library (Go core).
  const libCandidates = [
    join(platformDir(), "libwalrusd.dylib"),
    join(platformDir(), "libwalrusd.so"),
    join(__dirname, "..", "lib", findLib(join(__dirname, "..", "lib"))),
    join(__dirname, "..", "..", "..", "libwalrusd.dylib"),
  ];
  const libPath = libCandidates.find((p) => p && existsSync(p));
  if (!libPath) {
    throw new Error("@walrusd/db: libwalrusd shared library not found");
  }
  native.load(libPath);
  return native;
}

/** Absolute path to the bundled litestream VFS read extension for the host
 *  SQLite (bun native read mode, spec §9). Load with:
 *  db.loadExtension(path, "sqlite3_walrusdvfs_init"). */
export function vfsExtensionPath(): string {
  const filename = process.platform === "darwin" ? "libwalrusd_vfs.dylib" : "libwalrusd_vfs.so";
  const candidates = [
    join(platformDir(), filename),
    join(__dirname, "..", "lib", filename),
    join(__dirname, "..", "..", "..", "bindings", "vfs", filename),
  ];
  const p = candidates.find(existsSync);
  if (!p) {
    throw new Error(`@walrusd/db: VFS extension not found for ${process.platform}-${process.arch}`);
  }
  return p;
}

function findLib(dir: string): string {
  if (!existsSync(dir)) return "";
  const files = readdirSync(dir);
  return files.find((f) => f.startsWith("libwalrusd.") && !f.endsWith(".h")) ?? "";
}

function unwrap(res: string): any {
  const env = JSON.parse(res) as Envelope;
  if (env.api_version !== API_VERSION) {
    throw new WalrusdError("DB_PROTOCOL_MISMATCH", `api_version ${env.api_version} != ${API_VERSION}`);
  }
  if (!env.ok) {
    throw new WalrusdError(env.error!.class, env.error!.message, env.error!.retry_after_ms);
  }
  return env.result;
}

/**
 * WalrusdDatabase is the batch-oriented interface (spec §11): all statements
 * in a write() run in one SQLite transaction under one lease, followed by
 * the mandatory flush-before-release sequence.
 */
export class WalrusdDatabase {
  private native: NativeAPI;
  private handle: number;
  private defaultDeadlineMs: number;
  private defaultWriteDeadlineMs: number;

  constructor(options: RuntimeOptions) {
    this.native = loadNative();
    this.handle = this.native.init(
      JSON.stringify({
        owner: options.owner,
        config: {
          request_timeout_ms: options.requestTimeoutMs,
          lease_duration_ms: options.leaseDurationMs,
          clock_skew_ms: options.clockSkewMs,
          acquire_retry_budget_ms: options.acquireRetryBudgetMs,
          retry_backoff_min_ms: options.retryBackoffMinMs,
          retry_backoff_max_ms: options.retryBackoffMaxMs,
          retry_fixed_delay_ms: options.retryFixedDelayMs,
          retry_fixed_count: options.retryFixedCount,
          retry_multiplier: options.retryMultiplier,
          retry_max_delay_ms: options.retryMaxDelayMs,
          retry_max_total_ms: options.retryMaxTotalMs,
          write_buffer_root_path: options.writeBufferRootPath,
          max_read_instances: options.maxReadInstances,
          read_instance_idle_ttl_ms: options.readInstanceIdleTtlMs,
          vfs_page_cache_bytes: options.vfsPageCacheBytes,
          write_sync_interval_ms: options.writeSyncIntervalMs,
          max_temp_write_buffer: options.maxTempWriteBuffer,
          redis_address: options.redisAddress,
          redis_password: options.redisPassword,
          redis_db: options.redisDB,
        },
      })
    );
    this.defaultDeadlineMs = options.requestTimeoutMs ?? 20_000;
    const writeBudgetMs = options.retryMaxTotalMs && options.retryMaxTotalMs > 0
      ? options.retryMaxTotalMs
      : 20_000;
    this.defaultWriteDeadlineMs = options.requestTimeoutMs ?? writeBudgetMs;
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
    const deadline = Date.now() + this.defaultWriteDeadlineMs;
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
   * readDsn returns a SQLite DSN (file:...?vfs=walrusd_N&mode=ro) that the
   * HOST's own SQLite can open to read through litestream VFS natively,
   * plus the replica URL for the loadable-extension path. The DSN selects
   * the VFS by name through SQLite's URI filenames, so the host must open
   * with SQLITE_OPEN_URI; node:sqlite does, while bun:sqlite's Database
   * treats the DSN as a literal filename. With node:sqlite (Node and Bun):
   *   1. scratch = new DatabaseSync(":memory:", { allowExtension: true });
   *      scratch.loadExtension("<pkg>/prebuilds/libwalrusd_vfs", "sqlite3_walrusdvfs_init");
   *      scratch.prepare("SELECT walrusd_vfs_attach(?, ?, ?, ?)").get(vfs, replica_url, key, secret);
   *      scratch.close();
   *   2. new DatabaseSync(dsn, { readOnly: true })
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

export default WalrusdDatabase;
