import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const siteDir = path.resolve(scriptDir, "..");
const repoDir = path.resolve(siteDir, "..");
const sourceDir = path.join(repoDir, "docs");
const outputDir = path.join(siteDir, "src", "content", "docs");
const docsOutputDir = path.join(outputDir, "docs");
const packagePath = path.join(repoDir, "bindings", "node", "package.json");

const preferredOrder = ["usage.md", "specs.md"];
const syncKey = "runtime";
const variantLabels = new Map([
  ["go", "Go"],
  ["ts", "Node.js / Bun"],
  ["c", "C ABI"],
  ["python", "Python (C ABI)"],
]);
const rawBase = process.env.SITE_BASE?.trim() || "/";
const base =
  rawBase === "/"
    ? "/"
    : `/${rawBase.replace(/^\/+|\/+$/g, "")}/`;

function stripInlineMarkdown(value) {
  return value
    .replace(/!\[([^\]]*)\]\([^)]+\)/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/(\*\*|__)(.*?)\1/g, "$2")
    .replace(/(\*|_)(.*?)\1/g, "$2")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseDocument(source, markdown) {
  const lines = markdown.split(/\r?\n/);
  let fence = null;
  let h1Index = -1;
  let title = null;
  let description = null;
  let paragraphStart = -1;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})/);
    if (fenceMatch) {
      const marker = fenceMatch[1][0];
      if (!fence) {
        fence = marker;
      } else if (fence === marker) {
        fence = null;
      }
      continue;
    }

    if (fence) {
      continue;
    }

    const headingMatch = line.match(/^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$/);
    if (headingMatch) {
      const depth = headingMatch[1].length;
      if (depth === 1 && h1Index === -1) {
        h1Index = index;
        title = stripInlineMarkdown(headingMatch[2]);
      }
      continue;
    }

    if (h1Index === -1 || description) {
      continue;
    }

    if (line.trim() === "") {
      if (paragraphStart !== -1) {
        description = stripInlineMarkdown(
          lines.slice(paragraphStart, index).join(" "),
        );
      }
    } else if (paragraphStart === -1) {
      paragraphStart = index;
    }
  }

  if (h1Index === -1 || !title) {
    throw new Error(`${source} has no H1 heading`);
  }

  if (!description && paragraphStart !== -1) {
    description = stripInlineMarkdown(lines.slice(paragraphStart).join(" "));
  }

  if (!description) {
    throw new Error(`${source} has no paragraph after its H1 heading`);
  }

  const bodyLines = [...lines];
  bodyLines.splice(h1Index, 1);
  while (bodyLines[0]?.trim() === "") {
    bodyLines.shift();
  }
  const body = bodyLines.join("\n").trimEnd();

  if (!body.trim()) {
    throw new Error(`${source} would produce an empty body`);
  }

  return {
    slug: source.replace(/\.md$/, ""),
    title,
    description,
    editUrl: `https://github.com/kush-js/walrusd/blob/main/docs/${source}`,
    ...transformVariants(source, body),
  };
}

function fenceMatch(line) {
  const match = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
  if (!match) return null;

  return {
    marker: match[1],
    info: match[2].trim(),
  };
}

function isFenceClose(line, marker) {
  const match = line.match(/^ {0,3}(`{3,}|~{3,})\s*$/);
  return Boolean(
    match && match[1][0] === marker[0] && match[1].length >= marker.length,
  );
}

function escapeMdxText(value) {
  let output = "";
  let codeDelimiter = null;

  for (let index = 0; index < value.length; ) {
    const backtickMatch = value.slice(index).match(/^`+/);
    if (backtickMatch) {
      const delimiter = backtickMatch[0];
      if (codeDelimiter === delimiter) {
        codeDelimiter = null;
      } else if (codeDelimiter === null) {
        codeDelimiter = delimiter;
      }
      output += delimiter;
      index += delimiter.length;
      continue;
    }

    const character = value[index];
    if (codeDelimiter === null && character === "<") {
      output += "&lt;";
    } else if (codeDelimiter === null && character === "{") {
      output += "&#123;";
    } else {
      output += character;
    }
    index += 1;
  }

  return output;
}

function transformVariants(source, body) {
  const lines = body.split("\n");
  const output = [];
  let inVariant = false;
  let variantStart = -1;
  let variants = [];
  let outsideFence = null;
  let usesTabs = false;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];

    if (line === ":::variants") {
      if (inVariant) {
        throw new Error(
          `${source}:${index + 1} has a nested :::variants region`,
        );
      }
      inVariant = true;
      variantStart = index;
      variants = [];
      continue;
    }

    if (inVariant && line === ":::") {
      if (variants.length === 0) {
        throw new Error(
          `${source}:${variantStart + 1} variant region has no code blocks`,
        );
      }

      const labels = new Set();
      for (const variant of variants) {
        if (labels.has(variant.label)) {
          throw new Error(
            `${source}:${variant.line} duplicate variant label ${JSON.stringify(variant.label)} in one region`,
          );
        }
        labels.add(variant.label);
      }

      output.push(`<Tabs syncKey="${syncKey}">`);
      usesTabs = true;
      for (const variant of variants) {
        output.push(`<TabItem label="${variant.label}">`);
        output.push(...variant.lines);
        output.push("</TabItem>");
      }
      output.push("</Tabs>");
      inVariant = false;
      variantStart = -1;
      variants = [];
      continue;
    }

    if (inVariant) {
      if (line.trim() === "") {
        continue;
      }

      const openingFence = fenceMatch(line);
      if (!openingFence) {
        throw new Error(
          `${source}:${index + 1} variant regions may contain only fenced code blocks`,
        );
      }

      const language = openingFence.info.split(/\s+/, 1)[0];
      const label = variantLabels.get(language);
      if (!label) {
        throw new Error(
          `${source}:${index + 1} unknown variant language ${JSON.stringify(language)}`,
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
          `${source}:${variantStart + 1} has an unterminated variant region`,
        );
      }

      variants.push({
        label,
        lines: blockLines,
        line: index - blockLines.length + 2,
      });
      continue;
    }

    const fence = fenceMatch(line);
    if (fence) {
      if (!outsideFence) {
        outsideFence = fence.marker;
      } else if (isFenceClose(line, outsideFence)) {
        outsideFence = null;
      }
      output.push(line);
      continue;
    }

    output.push(outsideFence ? line : escapeMdxText(line));
  }

  if (inVariant) {
    throw new Error(
      `${source}:${variantStart + 1} has an unterminated variant region`,
    );
  }

  return {
    body: output.join("\n"),
    usesTabs,
  };
}

function renderDocument(document) {
  return [
    "---",
    `title: ${JSON.stringify(document.title)}`,
    `description: ${JSON.stringify(document.description)}`,
    `editUrl: ${JSON.stringify(document.editUrl)}`,
    "---",
    "",
    ...(document.usesTabs
      ? ['import { Tabs, TabItem } from "@astrojs/starlight/components";', ""]
      : []),
    document.body,
    "",
  ].join("\n");
}

function escapeMarkdownText(value) {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/([\[\]])/g, "\\$1");
}

function renderDocsHub(description, documents) {
  const documentLinks = documents.map(
    (document) =>
      `- [${escapeMarkdownText(document.title)}](${base}docs/${document.slug}/) - ${escapeMarkdownText(document.description)}`,
  );

  return [
    "---",
    'title: "walrusd documentation"',
    `description: ${JSON.stringify(description)}`,
    "---",
    "",
    "Browse the guides and design documentation for walrusd.",
    "",
    "## Documents",
    "",
    ...documentLinks,
    "",
  ].join("\n");
}

async function syncDocs() {
  const packageJson = JSON.parse(await fs.readFile(packagePath, "utf8"));
  if (!packageJson.description) {
    throw new Error(`${packagePath} has no description`);
  }

  const sourceFiles = (await fs.readdir(sourceDir, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
    .map((entry) => entry.name);

  const preferred = preferredOrder.filter((source) =>
    sourceFiles.includes(source),
  );
  const remaining = sourceFiles
    .filter((source) => !preferredOrder.includes(source))
    .sort((a, b) => a.localeCompare(b));
  const orderedFiles = [...preferred, ...remaining];

  await fs.rm(outputDir, { recursive: true, force: true });
  await fs.mkdir(docsOutputDir, { recursive: true });

  const documents = [];
  for (const source of orderedFiles) {
    const markdown = await fs.readFile(path.join(sourceDir, source), "utf8");
    const document = parseDocument(source, markdown);
    documents.push(document);
    await fs.writeFile(
      path.join(docsOutputDir, `${document.slug}.mdx`),
      renderDocument(document),
      "utf8",
    );
  }

  await fs.writeFile(
    path.join(docsOutputDir, "index.md"),
    renderDocsHub(packageJson.description, documents),
    "utf8",
  );

  console.log(
    `[sync-docs] generated ${orderedFiles.length} document(s) and docs/index.md`,
  );
}

try {
  await syncDocs();
} catch (error) {
  console.error(`[sync-docs] ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
}
