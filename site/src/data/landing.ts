// All landing-page copy is derived from README.md and docs/specs.md.
// Edit the landing page here so the claims and quickstart stay easy to review.

const base = import.meta.env.BASE_URL.endsWith("/")
  ? import.meta.env.BASE_URL
  : `${import.meta.env.BASE_URL}/`;

export const landingCopy = {
  meta: {
    title: "walrusd",
    description:
      "A horizontally scalable, multi-tenant SQLite runtime with one logical database per user.",
  },
  hero: {
    eyebrow: "Embedded SQLite runtime",
    title: "walrusd",
    expansion: "Write-Ahead Log in object storage",
    description:
      "A horizontally scalable, multi-tenant SQLite runtime with one logical database per user.",
    primaryAction: {
      label: "Read the docs",
      href: `${base}docs/`,
    },
    secondaryAction: {
      label: "View on GitHub",
      href: "https://github.com/kush-js/walrusd",
    },
    facts: [
      "No writer fleet",
      "No routing layer",
      "No sticky sessions",
    ],
    factsLabel: "Deployment properties",
  },
  architecture: {
    eyebrow: "How it works",
    title: "Stateless compute. Object storage is the database.",
    intro:
      "walrusd embeds the database runtime directly in each API process. Redis/Valkey serializes writes per database, while Litestream's VFS turns object storage into the durable SQLite state.",
    stages: [
      {
        number: "01",
        title: "Your code",
        body: "Go services import the core directly; Node.js and Bun use the same core through @walrusd/db.",
      },
      {
        number: "02",
        title: "Embedded runtime",
        body: "walrusd runs inside a stateless API process, so API instances remain disposable compute.",
      },
      {
        number: "03",
        title: "Redis / Valkey lease",
        body: "A per-database lease uses conditional CAS to serialize writes across API instances.",
      },
      {
        number: "04",
        title: "SQLite + Litestream VFS",
        body: "Writes run through Litestream write mode; reads use the remote replica without normal full local hydration.",
      },
      {
        number: "05",
        title: "LTX in object storage",
        body: "The durable state is an ordered LTX chain in an S3-compatible store; conditional writes are not required.",
      },
    ],
  },
  writePath: {
    eyebrow: "Write lifecycle",
    title: "The write path",
    intro:
      "Every mutation follows the same five steps. Success is acknowledged only after the LTX data is in object storage.",
    steps: [
      {
        number: "01",
        title: "Acquire the lease",
        body: "Before any SQLite write begins, the runtime conditionally acquires the database's Redis/Valkey lease.",
      },
      {
        number: "02",
        title: "Open a write-mode session",
        body: "The runtime opens or refreshes the remote Litestream VFS state and enables write mode.",
      },
      {
        number: "03",
        title: "Run the SQL transaction",
        body: "Your callback or statement batch runs and commits inside one SQLite transaction.",
      },
      {
        number: "04",
        title: "Flush the LTX file",
        body: "Disabling write mode is the mandatory flush barrier. walrusd waits for the synchronous remote flush to succeed.",
      },
      {
        number: "05",
        title: "Release the lease",
        body: "Only after the confirmed flush does the runtime conditionally release the lease and return the remote TXID.",
      },
    ],
    ack: {
      label: "Durability boundary",
      title: "The acknowledgement means the data is in object storage.",
      body:
        "Durability is confirmed by flush, never by elapsed time. Any API instance can serve any request because mutual exclusion comes from the lease, not from request routing.",
    },
    read: {
      label: "Read behavior",
      title: "Reads see only remote-committed state.",
      body:
        "Reads acquire no lease. They observe only state that Litestream has committed remotely through the VFS, with no local hydration.",
    },
  },
  capabilities: {
    eyebrow: "Runtime properties",
    title: "Small surface, explicit guarantees.",
    intro:
      "The runtime keeps coordination narrow and leaves durable state in object storage.",
    cards: [
      {
        kicker: "Placement",
        title: "Any instance",
        body: "Any API instance can serve any request for any user, with no writer fleet, routing layer, or sticky sessions.",
      },
      {
        kicker: "Writes",
        title: "Flush-backed acknowledgement",
        body: "Writes are acknowledged only after the transaction is flushed to object storage. Durability is confirmed by flush, never by elapsed time.",
      },
      {
        kicker: "Reads",
        title: "Remote committed state",
        body: "Reads see only remote-committed state through Litestream's VFS, with no local hydration.",
      },
      {
        kicker: "Bindings",
        title: "One core, three runtimes",
        body: "A Go core (CGO) is exposed to Node.js and Bun through one Node-API addon over a narrow C ABI.",
      },
      {
        kicker: "Retries",
        title: "Idempotent by key",
        body: "Retrying with the same idempotency key is deduplicated; after a timeout or crash, the recorded result is returned instead of re-running the mutation.",
      },
      {
        kicker: "Storage",
        title: "S3-compatible by default",
        body: "Any S3-compatible store can hold the replica, including Wasabi, Backblaze B2, and Sliplane; conditional writes are not required.",
      },
    ],
  },
  quickstart: {
    eyebrow: "Quickstart",
    title: "Write, flush, read.",
    intro:
      "This TypeScript example is the repository quickstart for the Node.js and Bun binding.",
    filename: "quickstart.ts",
    code: `import { WalrusdDatabase } from "./bindings/node/src/index";

const db = new WalrusdDatabase({ owner: "my-api-instance" });

// The descriptor is issued by your control plane - never by end users.
const d = {
  database_id: "users/user_1",   // your ID; objects land at <root_prefix>/users/user_1/
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

await db.close();`,
  },
  closing: {
    eyebrow: "Documentation",
    title: "Read the design. Then embed the runtime.",
    body:
      "The specification explains the invariants; the usage guide covers the Go core, the Node.js and Bun binding, the C ABI, and the error model.",
    action: {
      label: "Read the docs",
      href: `${base}docs/`,
    },
  },
} as const;
