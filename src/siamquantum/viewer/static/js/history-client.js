import { supabase, supabaseEnabled } from "/static/js/supabase-client.js";

const TABLE = "corpus_history";
const MIN_SAVE_INTERVAL_MS = 5 * 60 * 1000; // max once per 5 min per page

function ok() {
  return Boolean(supabaseEnabled && supabase);
}

function throttled(page) {
  const key = `sq-hist-last-${page}`;
  const last = Number(localStorage.getItem(key) || 0);
  if (Date.now() - last < MIN_SAVE_INTERVAL_MS) return true;
  localStorage.setItem(key, String(Date.now()));
  return false;
}

async function saveSnapshot(page, metrics, feedItems) {
  if (!ok() || throttled(page)) return;
  try {
    await supabase.from(TABLE).insert({
      page,
      metrics: metrics || [],
      feed_items: (feedItems || []).slice(0, 20),
    });
  } catch { /* best-effort */ }
}

async function loadHistory(page, limit = 60) {
  if (!ok()) return [];
  try {
    const { data } = await supabase
      .from(TABLE)
      .select("metrics, feed_items, captured_at")
      .eq("page", page)
      .order("captured_at", { ascending: false })
      .limit(limit);
    return data || [];
  } catch { return []; }
}

async function loadAccumulatedFeed(page, limit = 60) {
  const rows = await loadHistory(page, limit);
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    for (const item of (Array.isArray(row.feed_items) ? row.feed_items : [])) {
      const key = (item.copy || "") + "|" + (item.meta || "");
      if (!seen.has(key)) {
        seen.add(key);
        out.push({ ...item, ts: row.captured_at });
      }
    }
  }
  return out; // newest first (rows are desc ordered)
}

function extractSeries(snapshots, labelSubstr) {
  return [...snapshots]
    .reverse()
    .map((row) => {
      const arr = Array.isArray(row.metrics) ? row.metrics : [];
      const m = arr.find((m) =>
        String(m.label || "").toLowerCase().includes(labelSubstr.toLowerCase())
      );
      if (!m) return null;
      const n = parseFloat(String(m.value || "").replace(/,/g, ""));
      return isNaN(n) ? null : n;
    })
    .filter((v) => v !== null);
}

function sparklineSvg(values, color = "#38BDF8", width = 88, height = 30) {
  if (values.length < 2) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const pad = 4;
  const iw = width - pad * 2;
  const ih = height - pad * 2;
  const pts = values.map((v, i) => {
    const x = (pad + (i / (values.length - 1)) * iw).toFixed(1);
    const y = (pad + ih - ((v - min) / range) * ih).toFixed(1);
    return `${x},${y}`;
  });
  const [lx, ly] = pts[pts.length - 1].split(",");
  const [fx, fy] = pts[0].split(",");
  const trend = values[values.length - 1] >= values[0] ? color : "#f87171";
  const areaPath = `M${fx},${pad + ih} L${pts.join(" L")} L${lx},${pad + ih} Z`;
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" style="display:block;overflow:visible">
    <path d="${areaPath}" fill="${trend}" fill-opacity="0.07"/>
    <polyline points="${pts.join(" ")}" fill="none" stroke="${trend}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${lx}" cy="${ly}" r="2.8" fill="${trend}" fill-opacity="0.9"/>
  </svg>`;
}

function pageName() {
  const p = window.location.pathname;
  if (p === "/dashboard") return "dashboard";
  if (p === "/network") return "network";
  if (p === "/analytics") return "analytics";
  if (p === "/database") return "database";
  if (p.startsWith("/submit") || p === "/community") return "community";
  return "overview";
}

window.SQHistory = {
  saveSnapshot,
  loadHistory,
  loadAccumulatedFeed,
  extractSeries,
  sparklineSvg,
  pageName,
  enabled: ok,
};
