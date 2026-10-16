// A small dependency-free SVG line chart: several series on a date axis, with a hover
// readout. Missing values (null) break the line instead of being drawn as zero.
"use strict";

const SVGNS = "http://www.w3.org/2000/svg";

function el(name, attrs = {}, parent) {
  const node = document.createElementNS(SVGNS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

function niceTicks(lo, hi, count) {
  const span = hi - lo || Math.abs(hi) || 1;
  const raw = span / count;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => span / s <= count) || 10 * mag;
  const ticks = [];
  for (let t = Math.ceil(lo / step) * step; t <= hi + step * 1e-9; t += step) ticks.push(t);
  return ticks;
}

function fmt(v) {
  if (v === null || v === undefined) return "–";
  const a = Math.abs(v);
  if (a >= 1e6) return (v / 1e6).toFixed(2) + "M";
  if (a >= 1e4) return Math.round(v).toLocaleString();
  return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// series: [{name, color, dash, points: [[isoDate, value], ...]}]
function lineChart(container, series, { unit = "" } = {}) {
  container.innerHTML = "";
  const box = container.getBoundingClientRect();
  const W = Math.max(box.width, 300), H = Math.max(box.height, 200);
  const m = { l: 64, r: 16, t: 12, b: 28 };
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" }, container);
  const tip = document.createElement("div");
  tip.className = "tip";
  container.appendChild(tip);

  const parsed = series.map((s) => ({
    ...s,
    pts: s.points.map(([d, v]) => [Date.parse(d + "T00:00:00Z"), v]),
  }));
  const all = parsed.flatMap((s) => s.pts);
  const vals = all.map((p) => p[1]).filter((v) => v !== null);
  if (!all.length || !vals.length) {
    el("text", { x: W / 2, y: H / 2, "text-anchor": "middle", fill: "currentColor" }, svg)
      .textContent = "no data";
    return;
  }
  const x0 = Math.min(...all.map((p) => p[0])), x1 = Math.max(...all.map((p) => p[0]));
  let y0 = Math.min(...vals), y1 = Math.max(...vals);
  const pad = (y1 - y0) * 0.06 || 1;
  y0 -= pad; y1 += pad;
  const X = (t) => m.l + ((t - x0) / (x1 - x0 || 1)) * (W - m.l - m.r);
  const Y = (v) => H - m.b - ((v - y0) / (y1 - y0)) * (H - m.t - m.b);

  const grid = el("g", { class: "grid" }, svg);
  const axis = el("g", { class: "axis" }, svg);
  for (const t of niceTicks(y0, y1, 5)) {
    el("line", { x1: m.l, x2: W - m.r, y1: Y(t), y2: Y(t) }, grid);
    el("text", { x: m.l - 8, y: Y(t) + 4, "text-anchor": "end" }, axis).textContent = fmt(t);
  }
  if (y0 < 0 && y1 > 0) {
    el("line", { x1: m.l, x2: W - m.r, y1: Y(0), y2: Y(0), stroke: "currentColor",
      "stroke-opacity": 0.35 }, svg);
  }
  const yearSpan = (x1 - x0) / (365.25 * 864e5);
  const yearStep = niceTicks(0, yearSpan, 8)[1] || 1;
  const firstYear = new Date(x0).getUTCFullYear();
  for (let y = firstYear; ; y += Math.max(1, Math.round(yearStep))) {
    const t = Date.UTC(y, 0, 1);
    if (t > x1) break;
    if (t < x0) continue;
    el("text", { x: X(t), y: H - 8, "text-anchor": "middle" }, axis).textContent = String(y);
    el("line", { x1: X(t), x2: X(t), y1: m.t, y2: H - m.b }, grid);
  }
  if (unit) el("text", { x: 8, y: m.t + 10 }, axis).textContent = unit;

  for (const s of parsed) {
    let d = "", pen = false;
    for (const [t, v] of s.pts) {
      if (v === null) { pen = false; continue; }
      d += (pen ? "L" : "M") + X(t).toFixed(1) + " " + Y(v).toFixed(1);
      pen = true;
    }
    el("path", { d, fill: "none", stroke: s.color, "stroke-width": 1.4,
      "stroke-dasharray": s.dash || "", "stroke-linejoin": "round" }, svg);
  }

  const cursor = el("line", { y1: m.t, y2: H - m.b, stroke: "currentColor",
    "stroke-opacity": 0.3, visibility: "hidden" }, svg);
  function nearest(pts, t) {
    let lo = 0, hi = pts.length - 1;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (pts[mid][0] < t) lo = mid + 1; else hi = mid; }
    if (lo > 0 && Math.abs(pts[lo - 1][0] - t) < Math.abs(pts[lo][0] - t)) lo -= 1;
    return pts[lo];
  }
  svg.addEventListener("mousemove", (e) => {
    const r = svg.getBoundingClientRect();
    const px = ((e.clientX - r.left) / r.width) * W;
    const t = x0 + ((px - m.l) / (W - m.l - m.r)) * (x1 - x0);
    if (px < m.l || px > W - m.r) return;
    const lines = [];
    let shown = null;
    for (const s of parsed) {
      if (!s.pts.length) continue;
      const p = nearest(s.pts, t);
      shown = shown ?? p[0];
      lines.push(`<span style="color:${s.color}">●</span> ${s.name}: <b>${fmt(p[1])}</b>`);
    }
    cursor.setAttribute("x1", X(shown)); cursor.setAttribute("x2", X(shown));
    cursor.setAttribute("visibility", "visible");
    tip.innerHTML = `<div>${new Date(shown).toISOString().slice(0, 10)}</div>` + lines.join("<br>");
    tip.style.display = "block";
    const left = (X(shown) / W) * r.width;
    tip.style.left = Math.min(left + 12, r.width - tip.offsetWidth - 4) + "px";
    tip.style.top = "8px";
  });
  svg.addEventListener("mouseleave", () => {
    tip.style.display = "none"; cursor.setAttribute("visibility", "hidden");
  });
}

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url}: ${res.status} ${await res.text()}`);
  return res.json();
}

function badge(status) {
  const s = status || "none";
  return `<span class="badge ${s}">${status || "not published"}</span>`;
}

function esc(v) {
  return String(v ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
}
