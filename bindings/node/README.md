# @walrus/db

Node.js and Bun binding for the WALrus embedded SQLite runtime.

## Installing

```sh
npm install @walrus/db
```

```ts
import { WALrusDatabase } from "@walrus/db";

const db = new WALrusDatabase({ owner: "my-api-instance" });
const database = {
  database_id: "users/user_1",
  storage: { provider: "file", file_root: "/tmp/walrus-example" },
  credentials: {},
};

await db.write({
  database,
  idempotencyKey: "schema",
  statements: [{ sql: "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)" }],
});

await db.write({
  database,
  idempotencyKey: "set-greeting",
  statements: [{ sql: "INSERT OR REPLACE INTO kv (k, v) VALUES ('greeting', 'hello')" }],
});

const { rows } = await db.read({
  database,
  sql: "SELECT v FROM kv WHERE k = 'greeting'",
});

console.log(rows[0].v);
await db.close();
```

The package includes prebuilt native artifacts for `linux-x64` and
`linux-arm64` under `prebuilds/<platform>-<arch>/`. Both `walrus.node` and
`libwalrus.so` must be present for the host platform. Unsupported platforms
must build from source with Go and CGO enabled.
