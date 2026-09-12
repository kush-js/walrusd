import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import { existsSync, readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const siteDir = path.resolve(scriptDir, "..", "..");
const repoDir = path.resolve(siteDir, "..");
const docsDir = path.join(repoDir, "docs");
const nodePackageDir = path.join(repoDir, "bindings", "node");
const goBin = process.env.GO?.trim() || path.join(os.homedir(), "tools", "go", "bin", "go");
const nodeBin = process.env.NODE?.trim() || process.execPath;
const pythonBin = process.env.PYTHON?.trim() || "python3";
const npmBin = process.env.NPM?.trim() || "npm";
const cCompiler = process.env.CC?.trim() || "cc";

const languages = [
  {
    language: "go",
    label: "Go",
    harness: [
      path.join(scriptDir, "go", "main.go"),
      path.join(scriptDir, "go", "verify.go"),
    ],
  },
  {
    language: "ts",
    label: "Node.js / Bun",
    harness: [path.join(scriptDir, "node", "index.ts")],
  },
  {
    language: "c",
    label: "C ABI",
    harness: [path.join(scriptDir, "c", "main.c")],
  },
  {
    language: "python",
    label: "Python (C ABI)",
    harness: [path.join(scriptDir, "python", "main.py")],
  },
];

const preferredDocs = ["usage.md", "specs.md"];
const expectedUsageSections = [
  "Create a runtime",
  "Describe a database",
  "Read",
  "Write",
  "Errors and retry",
];

function normalize(value) {
  return `${value
    .replace(/\r\n?/g, "\n")
    .split("\n")
    .map((line) => line.trimEnd())
    .join("\n")
    .replace(/^\n+|\n+$/g, "")}\n`;
}

function fenceMatch(line) {
  const match = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
  if (!match) return null;
  return { marker: match[1], language: match[2].trim().split(/\s+/, 1)[0] };
}

function isFenceClose(line, marker) {
  const match = line.match(/^ {0,3}(`{3,}|~{3,})\s*$/);
  return Boolean(
    match && match[1][0] === marker[0] && match[1].length >= marker.length,
  );
}

async function extractSnippets() {
  const sourceFiles = (await fs.readdir(docsDir))
    .filter((source) => source.endsWith(".md"))
    .sort((left, right) => {
      const leftIndex = preferredDocs.indexOf(left);
      const rightIndex = preferredDocs.indexOf(right);
      if (leftIndex !== -1 || rightIndex !== -1) {
        return (
          (leftIndex === -1 ? Number.MAX_SAFE_INTEGER : leftIndex) -
            (rightIndex === -1 ? Number.MAX_SAFE_INTEGER : rightIndex) ||
          left.localeCompare(right)
        );
      }
      return left.localeCompare(right);
    });
  const snippets = new Map(languages.map(({ language }) => [language, []]));
  const regions = [];

  for (const source of sourceFiles) {
    const lines = (await fs.readFile(path.join(docsDir, source), "utf8")).split(
      /\r?\n/,
    );
    let inVariant = false;
    let regionStart = -1;
    let region = null;
    let section = "";

    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      const sectionMatch = line.match(/^###\s+(.+?)\s*$/);
      if (!inVariant && sectionMatch) {
        section = sectionMatch[1];
      }
      if (line === ":::variants") {
        if (inVariant) {
          throw new Error(`${source}:${index + 1}: nested :::variants region`);
        }
        inVariant = true;
        regionStart = index;
        region = {
          source: `${source}:${index + 1}`,
          section,
          snippets: new Map(),
        };
        continue;
      }

      if (!inVariant) continue;
      if (line === ":::") {
        for (const { language, label } of languages) {
          if (!region.snippets.has(language)) {
            throw new Error(
              `${source}:${regionStart + 1}: ${region.section || "(untitled section)"} is missing the ${label} variant`,
            );
          }
        }
        if (region.snippets.keys().next().value !== "go") {
          throw new Error(
            `${source}:${regionStart + 1}: ${region.section || "(untitled section)"} must start with the Go variant`,
          );
        }
        regions.push(region);
        inVariant = false;
        region = null;
        continue;
      }
      if (line.trim() === "") continue;

      const openingFence = fenceMatch(line);
      if (!openingFence) {
        throw new Error(
          `${source}:${index + 1}: variant regions may contain only fenced code blocks`,
        );
      }
      if (!snippets.has(openingFence.language)) {
        throw new Error(
          `${source}:${index + 1}: unknown variant language ${JSON.stringify(openingFence.language)}`,
        );
      }
      if (region.snippets.has(openingFence.language)) {
        const { label } = languages.find(
          ({ language }) => language === openingFence.language,
        );
        throw new Error(
          `${source}:${index + 1}: ${region.section || "(untitled section)"} has more than one ${label} variant`,
        );
      }

      const blockLines = [line];
      let closed = false;
      while (index + 1 < lines.length) {
        index += 1;
        blockLines.push(lines[index]);
        if (isFenceClose(lines[index], openingFence.marker)) {
          closed = true;
          break;
        }
      }
      if (!closed) {
        throw new Error(
          `${source}:${regionStart + 1}: unterminated variant region`,
        );
      }

      const snippet = {
        source: `${source}:${regionStart + 1}`,
        section: region.section,
        language: openingFence.language,
        text: normalize(blockLines.slice(1, -1).join("\n")),
      };
      region.snippets.set(openingFence.language, snippet);
      snippets.get(openingFence.language).push(snippet);
    }

    if (inVariant) {
      throw new Error(
        `${source}:${regionStart + 1}: unterminated variant region`,
      );
    }
  }

  const usageRegions = regions.filter(({ source }) =>
    source.startsWith("usage.md:"),
  );
  const usageSections = new Set(usageRegions.map(({ section }) => section));
  if (
    usageRegions.length !== expectedUsageSections.length ||
    usageSections.size !== expectedUsageSections.length
  ) {
    throw new Error(
      `usage.md must have exactly ${expectedUsageSections.length} distinct variant sections; found ${usageRegions.length} region(s) in ${usageSections.size} section(s)`,
    );
  }
  for (const section of expectedUsageSections) {
    if (!usageSections.has(section)) {
      throw new Error(`usage.md is missing the variant region for ${section}`);
    }
  }

  return { snippets, regions };
}

function renderSnippetDiff(harnessLabel, harness, cursor, snippet) {
  const expectedLines = snippet.text.trimEnd().split("\n");
  const firstLine = expectedLines.find((line) => line.trim() !== "");
  const found = firstLine === undefined ? -1 : harness.indexOf(firstLine, cursor);
  const actualStart = found === -1 ? cursor : found;
  const lineNumber = harness.slice(0, actualStart).split("\n").length;
  const actualLines = harness
    .slice(actualStart)
    .split("\n")
    .slice(0, Math.max(expectedLines.length, 5));

  return [
    `--- expected ${snippet.source}`,
    ...expectedLines.map((line) => `- ${line}`),
    `+++ ${harnessLabel} at or after line ${lineNumber}`,
    ...actualLines.map((line) => `+ ${line}`),
  ].join("\n");
}

function assertSnippets(harnessPaths, snippets) {
  for (const harnessPath of harnessPaths) {
    if (!existsSync(harnessPath)) {
      throw new Error(`missing example harness: ${harnessPath}`);
    }
  }
  const harnessLabel = harnessPaths
    .map((file) => path.relative(repoDir, file))
    .join(" + ");
  const harness = normalize(
    harnessPaths.map((harnessPath) => readFileSync(harnessPath, "utf8")).join("\n"),
  );
  let cursor = 0;

  for (const snippet of snippets) {
    const position = harness.indexOf(snippet.text, cursor);
    if (position === -1) {
      throw new Error(
        [
          `${harnessLabel} is missing or reorders the ${snippet.section} snippet from ${snippet.source}.`,
          "",
          renderSnippetDiff(harnessLabel, harness, cursor, snippet),
        ].join("\n"),
      );
    }
    cursor = position + snippet.text.length;
  }
}

function logCommand(label, command, args) {
  const display = [command, ...args]
    .map((part) => (/\s/.test(part) ? JSON.stringify(part) : part))
    .join(" ");
  console.log(`\n[${label}] $ ${display}`);
}

function run(label, command, args, options = {}) {
  logCommand(label, command, args);
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repoDir,
    env: { ...process.env, ...options.env },
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    timeout: options.timeout ?? 120_000,
  });

  if (result.stdout?.trim()) console.log(result.stdout.trimEnd());
  if (result.stderr?.trim()) console.error(result.stderr.trimEnd());

  if (result.error) {
    throw new Error(`${label} failed to start: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const details = [result.stderr?.trim(), result.stdout?.trim()]
      .filter(Boolean)
      .join("\n");
    throw new Error(
      `${label} exited with status ${result.status}${details ? `\n${details}` : ""}`,
    );
  }
  return result.stdout ?? "";
}

function assertOutput(language, label, output) {
  const expected = {
    go: /^Go durable at txid \S+\nGo read: hello$/m,
    ts: /^Node\.js \/ Bun durable at txid \S+\nNode\.js \/ Bun read: hello from walrusd$/m,
    c: /^C ABI durable at txid \S+\nC ABI read: hello from walrusd$/m,
    python:
      /^Python \(C ABI\) durable at txid \S+\nPython \(C ABI\) read: hello from walrusd$/m,
  }[language];

  if (!expected.test(output)) {
    throw new Error(
      `${label} did not print the expected txid/read evidence:\n${output || "(no output)"}`,
    );
  }
}

async function makeRunEnv(root) {
  const storageRoot = path.join(root, "storage");
  const bufferRoot = path.join(root, "buffers");
  const tempRoot = path.join(root, "tmp");
  await Promise.all(
    [storageRoot, bufferRoot, tempRoot].map((directory) =>
      fs.mkdir(directory, { recursive: true }),
    ),
  );
  return {
    ...process.env,
    WALRUSD_EXAMPLE_ROOT: storageRoot,
    WALRUSD_BUFFER_ROOT: bufferRoot,
    TMPDIR: tempRoot,
    CGO_ENABLED: "1",
    PATH: `${path.dirname(goBin)}${path.delimiter}${process.env.PATH ?? ""}`,
  };
}

async function runGo(root, sharedLibrary) {
  const moduleDir = path.join(root, "module");
  await fs.mkdir(moduleDir, { recursive: true });
  const moduleSource = readFileSync(path.join(repoDir, "go.mod"), "utf8");
  await fs.writeFile(
    path.join(moduleDir, "go.mod"),
    [
      moduleSource.replace(/^module\s+.+$/m, "module walrusd-example-verify").trimEnd(),
      "",
      "require walrusd v0.0.0",
      `replace walrusd => ${repoDir}`,
      "",
    ].join("\n"),
  );
  await fs.copyFile(
    path.join(scriptDir, "go", "main.go"),
    path.join(moduleDir, "main.go"),
  );
  await fs.copyFile(
    path.join(scriptDir, "go", "verify.go"),
    path.join(moduleDir, "verify.go"),
  );
  await fs.copyFile(
    path.join(repoDir, "go.sum"),
    path.join(moduleDir, "go.sum"),
  );
  const overlayPath = path.join(moduleDir, "overlay.json");
  await fs.writeFile(
    overlayPath,
    `${JSON.stringify(
      {
        Replace: {
          [path.join(repoDir, "lease", "redis.go")]: path.join(
            scriptDir,
            "go",
            "redis_memory.go",
          ),
        },
      },
      null,
      2,
    )}\n`,
  );
  const binary = path.join(root, "verify-go");
  run(
    "go build",
    goBin,
    [
      "build",
      "-tags",
      "vfs,verify_examples",
      `-overlay=${overlayPath}`,
      "-o",
      binary,
      ".",
    ],
    {
      cwd: moduleDir,
      env: { CGO_ENABLED: "1" },
    },
  );
  const output = run("go run", binary, [], {
    cwd: moduleDir,
    env: await makeRunEnv(root),
  });
  assertOutput("go", "Go harness", output);
}

async function prepareNodePackage(root, sharedLibrary) {
  if (!existsSync(path.join(nodePackageDir, "node_modules", "node-gyp"))) {
    run("node dependencies", npmBin, ["--prefix", nodePackageDir, "ci"]);
  }

  run("node binding build", npmBin, ["--prefix", nodePackageDir, "run", "build"]);

  const packageDir = path.join(root, "node_modules", "@walrusd", "db");
  await fs.mkdir(path.join(packageDir, "dist"), { recursive: true });
  await fs.mkdir(path.join(packageDir, "lib"), { recursive: true });
  await fs.mkdir(path.join(packageDir, "native", "build", "Release"), {
    recursive: true,
  });
  await fs.copyFile(
    path.join(nodePackageDir, "dist", "index.js"),
    path.join(packageDir, "dist", "index.js"),
  );
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    `${JSON.stringify(
      {
        name: "@walrusd/db",
        private: true,
        main: "dist/index.js",
      },
      null,
      2,
    )}\n`,
  );
  await fs.copyFile(
    path.join(nodePackageDir, "native", "build", "Release", "walrusd.node"),
    path.join(packageDir, "native", "build", "Release", "walrusd.node"),
  );
  await fs.copyFile(
    sharedLibrary,
    path.join(packageDir, "lib", path.basename(sharedLibrary)),
  );
  return packageDir;
}

async function runNode(root, sharedLibrary) {
  await prepareNodePackage(root, sharedLibrary);
  const appDir = path.join(root, "app");
  await fs.mkdir(appDir, { recursive: true });
  await fs.copyFile(
    path.join(scriptDir, "node", "index.ts"),
    path.join(appDir, "index.ts"),
  );
  const output = run(
    "node run",
    nodeBin,
    ["--experimental-strip-types", "index.ts"],
    {
      cwd: appDir,
      env: await makeRunEnv(root),
    },
  );
  assertOutput("ts", "Node.js / Bun harness", output);
}

async function runC(root, sharedLibrary) {
  const binary = path.join(root, "verify-c");
  run(
    "c build",
    cCompiler,
    [
      "-std=c11",
      "-Wall",
      "-Wextra",
      "-Werror",
      "-o",
      binary,
      path.join(scriptDir, "c", "main.c"),
      "-L",
      path.dirname(sharedLibrary),
      "-lwalrusd",
      `-Wl,-rpath,${path.dirname(sharedLibrary)}`,
    ],
    { cwd: root },
  );
  const output = run("c run", binary, [], {
    cwd: root,
    env: await makeRunEnv(root),
  });
  assertOutput("c", "C harness", output);
}

async function runPython(root, sharedLibrary) {
  const output = run("python run", pythonBin, [path.join(scriptDir, "python", "main.py")], {
    cwd: root,
    env: {
      ...(await makeRunEnv(root)),
      WALRUSD_LIBRARY: sharedLibrary,
    },
  });
  assertOutput("python", "Python harness", output);
}

async function main() {
  const { snippets, regions } = await extractSnippets();
  console.log(
    `[verify-examples] checking ${regions.length} variant region(s): ${regions
      .map(({ section }) => section)
      .join(", ")}`,
  );
  for (const { language, harness } of languages) {
    assertSnippets(harness, snippets.get(language));
  }

  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "walrusd-verify-"));
  try {
    const extension = process.platform === "darwin" ? "dylib" : "so";
    const sharedLibrary = path.join(tempRoot, `libwalrusd.${extension}`);
    await fs.mkdir(path.dirname(sharedLibrary), { recursive: true });
    run(
      "shared library",
      goBin,
      [
        "build",
        "-buildmode=c-shared",
        "-tags",
        "vfs",
        "-trimpath",
        "-ldflags=-s -w",
        "-o",
        sharedLibrary,
        "./bindings/c/lib",
      ],
      { env: { CGO_ENABLED: "1" } },
    );

    const runners = [
      ["go", runGo],
      ["node", runNode],
      ["c", runC],
      ["python", runPython],
    ];

    for (const [name, runner] of runners) {
      const root = await fs.mkdtemp(path.join(tempRoot, `${name}-`));
      try {
        await runner(root, sharedLibrary);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }

    console.log(
      `\n[verify-examples] verified ${languages.length} language harnesses and all documentation snippets`,
    );
  } finally {
    await fs.rm(tempRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(
    `\n[verify-examples] ${error instanceof Error ? error.message : error}`,
  );
  process.exitCode = 1;
});
