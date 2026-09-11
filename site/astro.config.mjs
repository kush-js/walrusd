import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";

const rawBase = process.env.SITE_BASE?.trim() || "/";
const base =
  rawBase === "/"
    ? "/"
    : `/${rawBase.replace(/^\/+|\/+$/g, "")}/`;
const darkOnly = true;

export default defineConfig({
  site: process.env.SITE_URL || process.env.CF_PAGES_URL,
  output: "static",
  base,
  trailingSlash: "always",
  build: {
    assets: "assets/_astro",
  },
  integrations: [
    starlight({
      title: "walrusd",
      favicon: "/favicon.svg",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/kush-js/walrusd",
        },
      ],
      sidebar: [
        { label: "Usage Guide", slug: "usage" },
        { label: "Specification", slug: "specs" },
      ],
      expressiveCode: {
        themes: ["github-dark"],
      },
      components: darkOnly
        ? {
            ThemeProvider: "./src/components/DarkThemeProvider.astro",
            ThemeSelect: "./src/components/NoThemeSelect.astro",
          }
        : undefined,
      customCss: darkOnly ? ["./src/styles/starlight.css"] : [],
    }),
  ],
});
