import { uniqueStrings } from "./io.js";

function toArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [];
}

function toInt(value, fallback = 0) {
  const n = Number.parseInt(value, 10);
  return Number.isNaN(n) ? fallback : n;
}

function parseMethods(methodsValue) {
  if (!methodsValue) return ["GET"];
  if (Array.isArray(methodsValue)) return uniqueStrings(methodsValue.map((m) => `${m}`));
  return uniqueStrings(
    `${methodsValue}`
      .split(/[,\|]/)
      .map((m) => m.trim())
      .filter(Boolean)
  );
}

function parseBlocks(content) {
  if (!content) return [];
  const hits = [...`${content}`.matchAll(/<!--\s*wp:([^\s/]+(?:\/[^\s]+)?)\b/g)];
  return uniqueStrings(hits.map((h) => h[1]));
}

function parseShortcodes(content) {
  if (!content) return [];
  const hits = [...`${content}`.matchAll(/\[([a-zA-Z0-9_-]+)(?=[\s\]\/])/g)];
  return uniqueStrings(hits.map((h) => h[1]));
}

function normalizePlugin(row, pluginHooks) {
  const slug = row.slug || row.name || row.plugin || "unknown";
  const hooks = pluginHooks?.[slug] ?? { actions: [], filters: [] };
  return {
    slug,
    name: row.title || row.name || slug,
    version: row.version || "",
    status: row.status || "unknown",
    update: row.update || "",
    auto_update: row.auto_update || "",
    requires_php: row.requires_php || "",
    requires_wp: row.requires_wp || "",
    hooks
  };
}

function normalizeTheme(row) {
  const slug = row.slug || row.name || "unknown";
  return {
    slug,
    name: row.title || row.name || slug,
    version: row.version || "",
    status: row.status || "unknown",
    update: row.update || "",
    auto_update: row.auto_update || ""
  };
}

function normalizePage(row) {
  const content =
    row.post_content ?? row.content ?? row.content_raw ?? row.excerpt ?? row.description ?? "";
  const metaKeys = Array.isArray(row.meta_keys) ? row.meta_keys : [];
  return {
    id: toInt(row.ID ?? row.id),
    type: row.post_type || row.type || "post",
    status: row.post_status || row.status || "unknown",
    title: row.post_title || row.title || "",
    slug: row.post_name || row.slug || "",
    url: row.guid || row.link || "",
    template: row.page_template || row.template || "",
    blocks: parseBlocks(content),
    shortcodes: parseShortcodes(content),
    meta_keys: metaKeys
  };
}

function normalizeRestRoute(row) {
  const path = row.path || row.route || "";
  const namespace = row.namespace || "";
  return {
    namespace,
    path,
    methods: parseMethods(row.methods),
    source: row.source
  };
}

function normalizeAjaxAction(row) {
  return {
    action: row.action || row.name || "",
    authenticated: Boolean(row.authenticated),
    source: row.source
  };
}

function normalizeShortcode(row) {
  return {
    tag: row.tag || row.shortcode || "",
    source: row.source,
    used_in_posts: toArray(row.used_in_posts).map((id) => toInt(id)).filter((id) => id > 0)
  };
}

function normalizeTemplate(row) {
  return {
    file: row.file || "",
    name: row.name || "",
    type: row.type || "unknown",
    source: row.source
  };
}

function shortcodesFromPages(pages) {
  const index = new Map();
  for (const page of pages) {
    for (const tag of page.shortcodes) {
      if (!index.has(tag)) index.set(tag, []);
      index.get(tag).push(page.id);
    }
  }
  return [...index.entries()].map(([tag, ids]) => ({
    tag,
    used_in_posts: uniqueStrings(ids).map((id) => Number(id))
  }));
}

function mergeShortcodes(primary, secondary) {
  const map = new Map();
  for (const sc of [...secondary, ...primary]) {
    if (!sc.tag) continue;
    if (!map.has(sc.tag)) {
      map.set(sc.tag, { tag: sc.tag, used_in_posts: [] });
    }
    const current = map.get(sc.tag);
    current.source = current.source || sc.source;
    current.used_in_posts = uniqueStrings([
      ...current.used_in_posts,
      ...(sc.used_in_posts || [])
    ]).map((id) => Number(id));
  }
  return [...map.values()];
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

export function buildManifest({
  siteName,
  siteUrl,
  environment,
  schemaVersion,
  inputs,
  scans
}) {
  const plugins = toArray(inputs.plugins).map((row) => normalizePlugin(row, scans.pluginHooks));
  const themes = toArray(inputs.themes).map(normalizeTheme);
  const pages = toArray(inputs.posts).map(normalizePage).filter((p) => p.id > 0);

  const inputRestRoutes = toArray(inputs.restRoutes).map(normalizeRestRoute);
  const restRoutes = dedupeBy(
    [...inputRestRoutes, ...toArray(scans.restRoutes)],
    (r) => `${r.path}|${r.methods.join(",")}`
  ).filter((r) => r.path);

  const inputAjax = toArray(inputs.ajaxActions).map(normalizeAjaxAction);
  const ajaxActions = dedupeBy(
    [...inputAjax, ...toArray(scans.ajaxActions)],
    (a) => `${a.action}|${a.authenticated}`
  ).filter((a) => a.action);

  const inputShortcodes = toArray(inputs.shortcodes).map(normalizeShortcode);
  const derivedShortcodes = shortcodesFromPages(pages);
  const scannedShortcodes = toArray(scans.shortcodes).map(normalizeShortcode);
  const shortcodes = mergeShortcodes(
    inputShortcodes,
    mergeShortcodes(scannedShortcodes, derivedShortcodes)
  ).filter((s) => s.tag);

  const templates = dedupeBy(
    [...toArray(inputs.templates).map(normalizeTemplate), ...toArray(scans.templates)],
    (t) => `${t.file}|${t.name}`
  ).filter((t) => t.file && t.name);

  return {
    schema_version: schemaVersion,
    generated_at: new Date().toISOString(),
    site: {
      name: siteName,
      url: siteUrl,
      environment: environment || "unknown"
    },
    sources: inputs.sources || {},
    plugins,
    themes,
    pages,
    templates,
    rest_routes: restRoutes,
    ajax_actions: ajaxActions,
    shortcodes,
    stats: {
      plugin_count: plugins.length,
      theme_count: themes.length,
      page_count: pages.length,
      template_count: templates.length,
      rest_route_count: restRoutes.length,
      ajax_action_count: ajaxActions.length,
      shortcode_count: shortcodes.length
    }
  };
}
