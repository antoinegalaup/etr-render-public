import fs from "node:fs";
import path from "node:path";
import { uniqueStrings } from "./io.js";

function walkFiles(dir, files = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkFiles(fullPath, files);
      continue;
    }
    if (!/\.(php|inc|js|jsx|ts|tsx)$/i.test(entry.name)) continue;
    files.push(fullPath);
  }
  return files;
}

function normalizeSource(filePath, line) {
  return { file: filePath, line };
}

function pluginSlugFromPath(filePath, wpContentDir) {
  const rel = path.relative(wpContentDir, filePath).replaceAll(path.sep, "/");
  const m = rel.match(/^plugins\/([^/]+)\//);
  return m?.[1] ?? null;
}

function pushPluginHook(map, slug, kind, hookName) {
  if (!slug || !hookName) return;
  if (!map[slug]) {
    map[slug] = { actions: [], filters: [] };
  }
  map[slug][kind].push(hookName);
}

export function scanWpContent(wpContentDir) {
  if (!wpContentDir || !fs.existsSync(wpContentDir)) {
    return {
      pluginHooks: {},
      restRoutes: [],
      ajaxActions: [],
      shortcodes: [],
      templates: []
    };
  }

  const files = walkFiles(wpContentDir);
  const pluginHooks = {};
  const restRoutes = [];
  const ajaxActions = [];
  const shortcodes = [];
  const templates = [];

  for (const file of files) {
    const content = fs.readFileSync(file, "utf8");
    const lines = content.split("\n");
    const slug = pluginSlugFromPath(file, wpContentDir);

    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];

      const actionMatch = line.match(/add_action\s*\(\s*["']([^"']+)["']/);
      if (actionMatch) {
        pushPluginHook(pluginHooks, slug, "actions", actionMatch[1]);
      }

      const filterMatch = line.match(/add_filter\s*\(\s*["']([^"']+)["']/);
      if (filterMatch) {
        pushPluginHook(pluginHooks, slug, "filters", filterMatch[1]);
      }

      const restMatch = line.match(
        /register_rest_route\s*\(\s*["']([^"']+)["']\s*,\s*["']([^"']+)["']/
      );
      if (restMatch) {
        const namespace = restMatch[1].replace(/\/+$/, "");
        const routePath = restMatch[2].startsWith("/") ? restMatch[2] : `/${restMatch[2]}`;
        restRoutes.push({
          namespace,
          path: `/${namespace}${routePath}`.replace(/\/{2,}/g, "/"),
          methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
          source: normalizeSource(file, i + 1)
        });
      }

      const shortcodeMatch = line.match(/add_shortcode\s*\(\s*["']([^"']+)["']/);
      if (shortcodeMatch) {
        shortcodes.push({
          tag: shortcodeMatch[1],
          source: normalizeSource(file, i + 1),
          used_in_posts: []
        });
      }

      const ajaxMatch = line.match(/["']wp_ajax(_nopriv)?_([a-zA-Z0-9_]+)["']/);
      if (ajaxMatch) {
        ajaxActions.push({
          action: ajaxMatch[2],
          authenticated: !ajaxMatch[1],
          source: normalizeSource(file, i + 1)
        });
      }

      const templateMatch = line.match(/Template Name:\s*(.+)$/);
      if (templateMatch) {
        templates.push({
          file,
          name: templateMatch[1].trim(),
          type: "page",
          source: normalizeSource(file, i + 1)
        });
      }
    }
  }

  for (const slug of Object.keys(pluginHooks)) {
    pluginHooks[slug].actions = uniqueStrings(pluginHooks[slug].actions);
    pluginHooks[slug].filters = uniqueStrings(pluginHooks[slug].filters);
  }

  return {
    pluginHooks,
    restRoutes: dedupeBy(restRoutes, (r) => `${r.path}|${r.source.file}|${r.source.line}`),
    ajaxActions: dedupeBy(
      ajaxActions,
      (a) => `${a.action}|${a.authenticated}|${a.source.file}|${a.source.line}`
    ),
    shortcodes: dedupeBy(shortcodes, (s) => `${s.tag}|${s.source.file}|${s.source.line}`),
    templates: dedupeBy(templates, (t) => `${t.file}|${t.name}|${t.source.line}`)
  };
}

function dedupeBy(items, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = keyFn(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}
