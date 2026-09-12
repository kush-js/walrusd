# walrusd documentation site

This Astro Starlight site renders the repository's `docs/*.md` files as a
static, dark-only documentation site. It has no runtime API or server
dependency. The root route is a custom landing page; documentation lives
under `/docs/`.

## Local development

Requirements:

- Node.js 22.12 or newer (Node.js 24 LTS is used in CI)
- npm 9.6.5 or newer

From the repository root:

```sh
cd site
npm install
npm run dev
```

`predev` runs `scripts/sync-docs.mjs`, which generates `../docs/*.md` into the
ignored `src/content/docs/docs/` directory, moves each document's H1 into
Starlight frontmatter, adds a source edit URL, and creates the `/docs/` hub
from the parsed document titles and descriptions.

## Build

```sh
cd site
npm ci
npm run build
```

The static output is written to `site/dist/`. `prebuild` always runs the
documentation sync first.

To build under a subpath, set `SITE_BASE` to that path. For example:

```sh
SITE_BASE=/walrusd npm run build
```

The default base path is `/`.

## Cloudflare Pages

Create a Pages project for this repository and use these settings:

| Setting | Value |
| --- | --- |
| Framework preset | Astro |
| Root directory | `site` |
| Build command | `npm run build` |
| Build output directory | `dist` |
| Node version | `24` (`NODE_VERSION=24`) |

Set `SITE_URL` to the production origin, for example
`https://docs.example.com`, to generate stable canonical URLs and a sitemap.
If `SITE_URL` is unset, the build uses Cloudflare Pages' automatically
provided `CF_PAGES_URL` as a fallback. If neither variable is set, no `site`
URL is configured.

No adapter or server runtime is required. The uploaded `dist/` directory
contains plain HTML, CSS, JavaScript, and other static assets.

If the project is hosted below the domain root, add a `SITE_BASE`
environment variable such as `/walrusd`. Leave it unset for root hosting.

## UI translations

Starlight's optional UI-string overrides belong in `src/content/i18n/`. The
collection includes an empty `en.json` because Astro warns when a collection
has no entries. No UI strings are overridden.

## Expected 404 warning

Builds emit `[WARN] [content] Entry docs → 404 was not found.` This is
expected. Starlight looks for an optional user-supplied `404` page in the
`docs` collection and falls back to its built-in 404 page when none exists.
We deliberately do not add a custom 404 entry because `docs/*.md` is the only
source of page content.

## Dark-only theme

`astro.config.mjs` sets `darkOnly` to `true`. This overrides Starlight's theme
provider and theme selector, keeping the site in dark mode and removing the
theme switcher. To re-enable Starlight's theme switcher, change that one line
to `const darkOnly = false;`.

## Design system

The site's layout, type scale, and HUD/ticker-free motifs are inspired by the
author's other site, [basemnt.ai](https://basemnt.ai), but the palette is
walrusd's own navy blue and is maintained as the token block in
`src/styles/starlight.css`. Corner radii live alongside those palette tokens;
the landing page consumes the same tokens.
Fonts are deliberately self-hosted through the `@fontsource/*` packages, and
this build must not make remote font or CDN requests.

The hero terminal lines are the actual output of the repository quickstart.
Run `bun run quickstart.ts` from the repository root to verify the
`durable at txid ...` and `read: hello from walrusd` lines. Re-verify those
strings whenever the quickstart changes; do not hand-edit them to look
plausible.

## Adding a page

Add a Markdown file to the repository-level `docs/` directory. It appears at
`/docs/<filename>/` on the next `npm run dev` or `npm run build` and is listed
on the `/docs/` hub. Every document must start with an H1 heading. The first
paragraph after that H1 becomes the page description.

The sidebar currently lists the overview, `usage.md`, and `specs.md` explicitly
in Starlight configuration. Additional documents are generated and build
successfully, but must be added to the `sidebar` array in `astro.config.mjs`
to appear in the sidebar.
