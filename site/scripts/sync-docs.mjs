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
    body,
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
      path.join(docsOutputDir, `${document.slug}.md`),
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
