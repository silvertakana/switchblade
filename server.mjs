// switchblade server.mjs
// Zero-dependency Node 25 router that replaces homelab LiteLLM for the
// deepseek-v4-flash family: two OpenCode Go accounts load-balanced by
// session-affinity hashing, with a DeepSeek direct fallback, per-backend
// health tracking and exponential backoff. Routing goes through three layers:
// backends -> models -> presets. Backends are raw upstream accounts (no model
// binding; a legacy `model` field remains legal as a default-upstream source).
// Models bind a logical id to one or more providers (backend + upstream +
// optional per-provider dialect). Presets are routing policies over models
// (affinity / failover / weighted strategies). Legacy configs (old `models`
// with `backends`, and DESIGN-PRESETS-era presets with `members`) are
// synthesized into the new shape at load. Manual cools are sticky: they never
// participate in the all-cooling fallback and a success never clears them.

import http from "node:http";
import { readFileSync, statSync, watch as watchF, appendFileSync, renameSync, openSync, readSync, closeSync } from "node:fs";
import { readFile as readFileP, rename as renameP, writeFile as writeFileP } from "node:fs/promises";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import crypto from "node:crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = process.env.ROUTER_CONFIG || join(__dirname, "config.json");
const ENV_PATH = process.env.ROUTER_ENV || join(__dirname, ".env");

// ---- config + env loading ----------------------------------------------------

function parseEnv(text) {
  const out = {};
  for (const line of text.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const eq = t.indexOf("=");
    if (eq < 0) continue;
    const k = t.slice(0, eq).trim();
    let v = t.slice(eq + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    if (k) out[k] = v;
  }
  return out;
}

function loadEnv() {
  try {
    const raw = readFileSync(ENV_PATH, "utf8");
    const parsed = parseEnv(raw);
    // Only set env vars not already present so a real shell env wins.
    for (const [k, v] of Object.entries(parsed)) {
      if (process.env[k] === undefined) process.env[k] = v;
    }
  } catch {
    // no .env file -> rely on real environment
  }
}

function mask(key) {
  if (!key) return "";
  return key.length > 14 ? key.slice(0, 4) + "..." + key.slice(-4) : "(secret)";
}

// ---- health registry ---------------------------------------------------------

const health = new Map(); // backendId -> {state, nextAvailableAt, fails, backoffMs, lastError}

function backoffFor(kind, cfg, fails) {
  const bp = cfg.backoff;
  const base = bp[kind + "BaseMs"] ?? 30000;
  const max = bp[kind + "MaxMs"] ?? 300000;
  return Math.min(base * 2 ** (fails - 1), max);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function markHealthy(id, origin) {
  // Observe the transition OUT of cooling for the dashboard cooling timeline
  // (success / uncool / reset). A backend that was never cooling emits nothing.
  const prev = health.get(id);
  if (prev && prev.state === "cooling") {
    recordCoolingEvent({ backend: id, action: origin || "success", kind: prev.lastError ? prev.lastError.kind : null, manual: !!(prev.lastError && prev.lastError.kind === "manual") });
    ANALYTICS.expiredNotified.delete(id);
  }
  health.set(id, { state: "healthy", nextAvailableAt: 0, fails: 0, backoffMs: 0, lastError: null });
}

function ensureBackends(cfg) {
  for (const b of cfg.backends) {
    if (!health.has(b.id)) markHealthy(b.id);
  }
}

// Classify an upstream failure into a backoff "kind".
function classify(status, bodyText) {
  if (status === 429) {
    if (/GoUsageLimitError/i.test(bodyText || "")) return "weekly";
    return "rate";
  }
  if (status === 401 || status === 403) return "auth";
  if (status >= 500) return "server";
  if (status === 0) return "server"; // network error
  if (status === 400 || status === 422) {
    // Billing failures (insufficient credits/quota) are a backend-resource
    // problem, not a payload bug: they must fail over to the next provider
    // instead of aborting the whole request, and they cool the dry account.
    if (/insufficient (credits|quota)|insufficient_quota|out of credits|billing error/i.test(bodyText || "")) return "billing";
    return "client"; // our payload bug, not backend health
  }
  return "server";
}

// Whether a failure is safe to RETRY on the same provider. Distinct from the
// cooling classification: some failures are retryable (transient overload,
// 5xx, network blip) without being a reason to abandon the provider for
// everyone, and some cooling failures (429 weekly, auth) must never be retried
// because retrying cannot succeed and only wastes the retry budget.
function isRetryableError(status, bodyText) {
  const kind = classify(status, bodyText);
  if (kind === "weekly" || kind === "auth" || kind === "client") return false;
  if (status === 429) return /overload/i.test(bodyText || "");
  if (status === 502 || status === 503 || status === 504) return true;
  if (status === 0) return true; // network error
  return false;
}

// ---- empty-completion detection --------------------------------------------

// Whether a streaming result is "empty": status 200 but no usable assistant
// output was produced. A legitimately tiny reply ("OK") has content and is
// NOT empty; a tool_calls-only response is NOT empty.
function isEmptyStreamResult({ hasContent, hasReasoning, hasToolCalls, completionTokens }) {
  if (hasContent || hasReasoning || hasToolCalls) return false;
  const ct = completionTokens == null ? 0 : Number(completionTokens) || 0;
  return ct <= 1;
}

function hasUsableContentNonStream(parsed) {
  if (!parsed || typeof parsed !== 'object') return false;
  const choices = parsed.choices;
  if (!Array.isArray(choices) || choices.length === 0) return false;
  for (const ch of choices) {
    const msg = (ch && (ch.message || ch.delta)) || {};
    if (msg.content != null && String(msg.content).trim() !== '') return true;
    if (msg.reasoning != null && String(msg.reasoning).trim() !== '') return true;
    if (msg.reasoning_content != null && String(msg.reasoning_content).trim() !== '') return true;
    if (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) return true;
    // also check choice-level tool_calls alias
    if (Array.isArray(ch.tool_calls) && ch.tool_calls.length > 0) return true;
  }
  return false;
}

function isEmptyNonStreamResult(parsed, completionTokens, isJson) {
  if (!isJson) return false;
  if (hasUsableContentNonStream(parsed)) return false;
  const ct = completionTokens == null ? 0 : Number(completionTokens) || 0;
  return ct <= 1;
}

function markFailure(cfg, id, status, bodyText) {
  const h = health.get(id) || { state: "healthy", fails: 0, backoffMs: 0 };
  const kind = classify(status, bodyText);
  if (kind === "client") {
    // Client-side payload error: the backend is fine; do not cool it down.
    // Record the last error for visibility but keep the backend healthy.
    h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
    health.set(id, h);
    return;
  }
  h.fails = (h.fails || 0) + 1;
  // Only a transition INTO cooling lands on the dashboard timeline; a re-cool
  // of an already-active cooling (the all-cooling fallback retrying the
  // soonest backend) is not a new failure episode.
  const wasCoolingActive = h.state === "cooling" && h.nextAvailableAt > Date.now();
  let nextMs;
  if (kind === "weekly") {
    // Parse "Resets in N days" if present, else full week.
    const m = /resets in (\d+)\s+day/i.exec(bodyText || "");
    const days = m ? parseInt(m[1], 10) : 7;
    nextMs = cfg.backoff.weeklyDefaultMs || days * 24 * 60 * 60 * 1000;
    h.fails = 100; // effectively pinned until reset
    fireAlert(cfg, "weekly_limit", { backend: id, days: m ? parseInt(m[1], 10) : null });
  } else {
    h.backoffMs = backoffFor(kind, cfg, h.fails);
    nextMs = h.backoffMs;
  }
  h.state = "cooling";
  h.nextAvailableAt = Date.now() + nextMs;
  h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
  health.set(id, h);
  if (!wasCoolingActive) recordCoolingEvent({ backend: id, action: "cool", kind, manual: false });
}

function markSuccess(id) {
  const h = health.get(id);
  // A manual cool is a user decision: a success must NEVER clear it. Only the
  // Uncool button (or expiry of a timed manual cool) restores the backend.
  if (h && h.state === "cooling" && h.lastError && h.lastError.kind === "manual") return;
  markHealthy(id);
}

function resetAllHealth() {
  for (const id of health.keys()) markHealthy(id, "reset");
}

// ---- cooling-event ring (dashboard timeline) ---------------------------------
//
// Observes health transitions ONLY - cooling semantics are untouched. The
// dashboard needs to explain WHY a backend was down: failure cool (with the
// classified kind), lazy expiry at request time, success clearing, admin
// uncool, and reset-health. Newest first, capped at 500 rows.

const COOLING_EVENTS = []; // newest at head: {t, backend, action, kind, manual}
const COOLING_EVENT_CAP = 500;

function recordCoolingEvent({ backend, action, kind, manual }) {
  COOLING_EVENTS.unshift({ t: Date.now(), backend, action, kind: kind || null, manual: !!manual });
  if (COOLING_EVENTS.length > COOLING_EVENT_CAP) COOLING_EVENTS.length = COOLING_EVENT_CAP;
}

// Request-time usability check that doubles as the lazy-expiry observer: a
// cooling backend whose timer elapsed is immediately usable again, and that
// elapsed moment lands on the cooling timeline once per expiry (later success /
// uncool / reset events cover the rest of the lifecycle).
function isUsableCurrently(id, now) {
  const h = health.get(id);
  if (!h || h.state !== "cooling") return true;
  if (h.nextAvailableAt <= now) {
    if (!ANALYTICS.expiredNotified.has(id)) {
      ANALYTICS.expiredNotified.add(id);
      recordCoolingEvent({ backend: id, action: "expire", kind: h.lastError ? h.lastError.kind : null, manual: !!(h.lastError && h.lastError.kind === "manual") });
    }
    return true;
  }
  return false;
}

// ---- history + stats ----------------------------------------------------------

const HISTORY_CAP = 500;
const HISTORY = []; // ring buffer (newest at tail): {t, callId, model, backend, stream, status, latencyMs, ttftMs, promptTokens, completionTokens, cacheHit, genMs, session}
// Collision-safe counter + entropy mix so two calls in the same ms still
// produce distinct ids; the `c` prefix keeps the id grep-able in logs.
let callCounter = 0;
function newCallId() {
  callCounter = (callCounter + 1) % 1296; // 36^2, wraps harmlessly
  const entropy = crypto.randomBytes(5).toString("base64url").slice(0, 8);
  return "c" + callCounter.toString(36).padStart(2, "0") + entropy;
}
const STATS = {
  since: Date.now(),
  total: 0,
  byBackend: {}, // backendId -> {requests, ok, errors, latencySumMs, latencyMaxMs}
  byModel: {}, // modelId -> count
};
const startedAt = Date.now();
// Dedup set for preset-of-presets cycle warnings: warn once per cyclic path
// (a repeated cycle on a hot path must not spam the router log).
const cycleWarned = new Set();
// JSONL history lands next to the config so tests write into their temp dir.
const historyPath = process.env.ROUTER_HISTORY || join(dirname(CONFIG_PATH), "router-history.jsonl");

function recordHistory(entry) {
  HISTORY.push(entry);
  if (HISTORY.length > HISTORY_CAP) HISTORY.shift();
  try {
    appendFileSync(historyPath, JSON.stringify(entry) + "\n");
  } catch {
    // Logging must never crash the router.
  }
}

// ---- bounded cache-miss payload capture (persistent) -------------------------
//
// OPTIONAL. Controlled by the top-level `missCapture` config block; when the
// block is absent or `enabled` is false the whole path is a no-op so existing
// behavior is untouched. A cache MISS means the upstream had to re-process
// (and the user got billed for) the full prompt; capturing the exact payload
// that missed makes a bad night recoverable for forensics. Storage is bounded:
// `boundedPayload()` clips content/tools, and the file rotates to `<file>.old`
// once it exceeds `maxFileBytes`.
const MISS_CAPTURE = {
  enabled: false,
  maxMissPct: 50,
  minMissTokens: 1000,
  file: "router-misses.jsonl",
  maxFileBytes: 25000000,
};

function missCapturePath(cfg) {
  const name = (cfg.missCapture && cfg.missCapture.file) || MISS_CAPTURE.file;
  return join(dirname(CONFIG_PATH), name);
}

// Append one bounded payload row for a low-cache-hit request. Must NEVER crash
// the router, so the whole body is guarded. The payload never contains keys:
// key values live only in env / request headers and are never copied.
function captureMiss(cfg, entry, payload) {
  const mc = cfg.missCapture;
  if (!mc || mc.enabled === false) return;
  const pct = entry.cacheHitPct;
  if (pct == null || pct >= (mc.maxMissPct ?? MISS_CAPTURE.maxMissPct)) return;
  let missTokens = Math.max(0, Math.round((entry.promptTokens || 0) * (1 - pct / 100)));
  if (missTokens < (mc.minMissTokens ?? MISS_CAPTURE.minMissTokens)) return;
  const file = missCapturePath(cfg);
  const row = {
    t: entry.t,
    callId: entry.callId,
    backend: entry.backend,
    model: entry.model,
    session: entry.session,
    cacheHitPct: pct,
    promptTokens: entry.promptTokens,
    cacheMissTokens: missTokens,
    payload: boundedPayload(payload),
  };
  try {
    // Rotate before append so the active file never grows unbounded.
    try {
      if (statSync(file).size > (mc.maxFileBytes ?? MISS_CAPTURE.maxFileBytes)) {
        renameSync(file, file + ".old");
      }
    } catch {
      // First write (no file yet) or a transient stat failure is fine.
    }
    appendFileSync(file, JSON.stringify(row) + "\n");
  } catch {
    // Capture must never crash the router.
  }
}

// ---- ntfy push alerts (OPTIONAL) ---------------------------------------------
//
// OPTIONAL. Controlled by the top-level `alerts.ntfy` block ({baseUrl, topic,
// minMissPct, minMissTokens, cooldownMs, events?}). When it is absent, or the
// kind's event is explicitly disabled (`events[kind] === false`), nothing is
// sent. Alerts are fire-and-forget: they must never block the hot request path
// or crash the router if ntfy is unreachable. Cooldown per kind suppresses an
// alert storm. Messages carry NO secrets (no keys).
const alertCooldowns = new Map();
const NTFY_DEFAULT_BASE = "https://ntfy.sh";
const ALERT_DEFS = {
  cache_miss: { title: "Cache break on switchblade", tag: "warning", priority: 3 },
  weekly_limit: { title: "Backend weekly usage limit reached", tag: "rotating_light", priority: 4 },
  all_cooling: { title: "All backends cooling", tag: "sos", priority: 5 },
  request_exhausted: { title: "Request failed - every provider down", tag: "exclamation", priority: 4 },
  relay_cut: { title: "Stream cut mid-relay", tag: "warning", priority: 3 },
};

function alertMessage(kind, f) {
  switch (kind) {
    case "cache_miss":
      return `Cache break on ${f.backend}: ${f.cacheHitPct}% hit, ~${f.missTokens} miss tokens (prompt ${f.promptTokens}, model ${f.model}, session ${f.session})`;
    case "weekly_limit":
      return `${f.backend} hit its weekly usage limit (resets in ${f.days ?? "?"} days); other providers may still serve`;
    case "all_cooling":
      return `All backends are cooling (${f.manualCooled} manually cooled); requests fail with 503`;
    case "request_exhausted":
      return `Request ${f.callId} (model ${f.model}) failed on every provider: ${f.error || "unknown"}`;
    case "relay_cut":
      return `${f.backend} cut mid-relay on ${f.model}: ${f.error}`;
    default:
      return null;
  }
}

function buildAlert(cfg, kind, fields) {
  const n = cfg.alerts && cfg.alerts.ntfy;
  if (!n || !n.topic || !ALERT_DEFS[kind]) return null;
  const events = n.events || {};
  if (events[kind] === false) return null;
  const message = alertMessage(kind, fields);
  if (!message) return null;
  const last = alertCooldowns.get(kind) || 0;
  const cooldownMs = n.cooldownMs ?? 600000;
  if (Date.now() - last < cooldownMs) return null;
  alertCooldowns.set(kind, Date.now());
  return {
    url: (n.baseUrl || NTFY_DEFAULT_BASE).replace(/\/$/, ""),
    body: {
      topic: n.topic,
      title: ALERT_DEFS[kind].title,
      message,
      tags: [ALERT_DEFS[kind].tag],
      priority: ALERT_DEFS[kind].priority,
    },
  };
}

function fireAlert(cfg, kind, fields) {
  const a = buildAlert(cfg, kind, fields);
  if (!a) return;
  // Fire-and-forget: a slow/unreachable ntfy must not stall or crash the router.
  fetch(a.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(a.body),
    signal: AbortSignal.timeout(5000),
  }).catch(() => {});
}

function resetAlertCooldowns() {
  alertCooldowns.clear();
}

// ---- request detail store (deep-dive side panel) -----------------------------
//
// Full per-request debug records live IN MEMORY ONLY, capped and size-guarded,
// so the deep dive never touches disk and cannot grow unboundedly:
// - DETAIL_CAP: at most 100 detail records (ring buffer, oldest dropped).
// - Size guard: requests whose raw body exceeds DETAIL_MAX_BODY_BYTES (100 KB)
//   are skipped - a detail record is only worth the memory when the payload is
//   small enough to inspect comfortably anyway.
// - The JSONL history file (router-history.jsonl) keeps ONLY the summary
//   entries; detail records are never appended to disk. Persistent capture is a
//   future change.
// - Detail records are keyed by the summary entry's `t` timestamp, which is
//   unique enough within the ring buffer (Date.now() per request).
const DETAIL_CAP = 100;
const DETAIL_MAX_BODY_BYTES = 100000;
const DETAILS = []; // ring buffer (newest at tail): {t, callId, payload, attempts, ...}

// Bounded copy of the outgoing payload for the deep-dive store. Requests whose
// raw body exceeds DETAIL_MAX_BODY_BYTES would otherwise be skipped entirely
// (no detail record at all); instead we keep a truncated copy so every request
// stays inspectable without holding multi-hundred-KB payloads in memory:
// message content is clipped per message, and tool schemas are reduced to the
// first few with the true count preserved.
function boundedPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const out = { ...payload };
  if (Array.isArray(out.messages)) {
    out.messages = out.messages.map((m) => {
      const c = { ...m };
      if (typeof c.content === "string" && c.content.length > 400) c.content = c.content.slice(0, 400) + "…[truncated]";
      return c;
    });
  }
  if (Array.isArray(out.tools)) {
    out._truncated = { tools: out.tools.length, kept: Math.min(out.tools.length, 5) };
    out.tools = out.tools.slice(0, 5);
  }
  return out;
}

// Record (or update) the deep-dive detail record for the request whose summary
// entry timestamp is `t` (and whose call id is `callId`). Pass the outgoing
// request payload (what forward() sent, never API keys - keys live only in
// env) and an append-style attempt record. Retries may already exist on the
// detail record (a parallel thread can call this per attempt), so this
// function merges into attempts[] by appending.
function recordDetail(t, callId, payload, attempt) {
  if (!t || !payload || typeof payload !== "object") return;
  let rec = DETAILS.find((d) => d.t === t);
  if (!rec) {
    rec = { t, callId: callId || null, payload, attempts: [] };
    DETAILS.push(rec);
    if (DETAILS.length > DETAIL_CAP) DETAILS.shift();
  }
  if (attempt) rec.attempts.push(attempt);
}

// Finalize a detail record once the attempt loop has run its course (success or
// exhausted failure): attach the response summary. No-op when the request was
// never captured (payload too large / no attempts) or already finalized.
function finalizeDetail(t, summary) {
  const rec = DETAILS.find((d) => d.t === t);
  if (!rec || rec.finalized) return;
  rec.finalized = true;
  if (summary) rec.response = summary;
}

// Get a detail record by summary timestamp, or by call id. Returns the record
// or null.
function getDetail(t) {
  return DETAILS.find((d) => d.t === t) || null;
}

function getDetailByCall(callId) {
  if (!callId) return null;
  return DETAILS.find((d) => d.callId === callId) || null;
}

// Canonical history entry shape with metric defaults, so every row (success
// or failure) carries the same fields for the UI. Backward compatible: the
// original fields (t, model, backend, stream, status, latencyMs, session)
// keep their exact semantics.
function newHistoryEntry(f) {
  return {
    t: f.t ?? Date.now(),
    // Stable per-request id for cross-referencing ("that call c-…"). Legacy
    // entries (pre-callId JSONL lines or rows built without one) get a
    // deterministic id derived from their timestamp so every row is
    // referenceable, not just new ones.
    callId: f.callId ?? "t" + (f.t ?? Date.now()).toString(36),
    model: f.model,
    backend: f.backend,
    stream: !!f.stream,
    status: f.status,
    latencyMs: f.latencyMs ?? 0,
    ttftMs: f.ttftMs ?? 0,                       // time to first upstream response (== latencyMs on success)
    promptTokens: f.promptTokens ?? 0,           // prompt usage tokens (0 if upstream did not report)
    completionTokens: f.completionTokens ?? 0,   // completion usage tokens (0 if upstream did not report)
    reasoningTokens: f.reasoningTokens ?? 0,     // reasoning-only completion tokens (usage.completion_tokens_details.reasoning_tokens; 0 if not reported - "0" and "not reported" are indistinguishable)
    cacheHit: f.cacheHit,                          // tri-state: true (header/usage hit), false (miss reported), null (unknown - no signal)
    cacheHitPct: f.cacheHitPct ?? null,            // 0..100 share of prompt tokens served from cache (hit/(hit+miss)); low % = early cache break; null = no measurable signal
    genMs: f.genMs ?? 0,                         // generation time: stream duration, or total - ttft for non-stream
    tps: f.tps ?? 0,                             // completion tokens per second (derived)
    session: f.session,
    routedModel: f.routedModel ?? null,          // resolved model id (success: serving attempt; failure: first candidate; null: none)
    wireParams: f.wireParams ?? null,            // redacted summary of the params actually sent upstream (for explaining reasoning on/off per row)
    retries: f.retries ?? 0,                     // number of retry attempts performed on the serving provider (0 = none)
    retryWaitedMs: f.retryWaitedMs ?? 0,         // total ms spent waiting in backoff across retries
    relayError: f.relayError ?? null,            // relay cut mid-body on a 200 row (idle timeout / upstream drop) - null = clean
    promptCacheHitTokens: f.promptCacheHitTokens ?? 0, // raw prompt tokens served from cache (0 = upstream did not report a split)
    promptCacheMissTokens: f.promptCacheMissTokens != null ? f.promptCacheMissTokens : Math.max(0, (f.promptTokens ?? 0) - (f.promptCacheHitTokens ?? 0)), // raw miss tokens; derived from prompt - hit when the upstream only reports hits
    errorKind: f.errorKind ?? null,              // classified upstream failure kind (weekly/rate/auth/server/client); null = no upstream failure
    cost: f.cost ?? null,                        // request cost in USD (see computeCost); null = model has no meta.pricing
    cacheSavings: f.cacheSavings ?? null,        // USD saved by cache hits vs full-price miss (peak rates)
    offPeakSavings: f.offPeakSavings ?? null,    // USD saved by the off-peak discount (0 when it did not apply)
    offPeak: f.offPeak ?? null,                  // whether off-peak pricing applied to this request
  };
}

// Redacted wire-params summary for history: captures which reasoning-relevant
// keys were actually sent upstream and their values (thinking type,
// reasoning_effort, max_tokens), WITHOUT copying messages, tools, or any
// key values. Used to explain per-request reasoning behavior.
function extractWireParams(payload) {
  if (!payload || typeof payload !== "object") return null;
  const p = payload;
  const out = {};
  if (p.thinking && typeof p.thinking === "object") out.thinking = String(p.thinking.type || "?");
  if (p.reasoning_effort != null) out.reasoning_effort = String(p.reasoning_effort);
  if (p.max_tokens != null) out.max_tokens = p.max_tokens;
  if (p.temperature != null) out.temperature = p.temperature;
  if (Array.isArray(p.tools) && p.tools.length) out.tools = p.tools.length;
  return Object.keys(out).length ? out : null;
}

function recordStats(entry) {
  STATS.total++;
  const b = STATS.byBackend[entry.backend] || {
    requests: 0, ok: 0, errors: 0, latencySumMs: 0, latencyMaxMs: 0,
    promptTokensSum: 0, completionTokensSum: 0, cacheHits: 0, cacheHitPctSum: 0, cachePctKnown: 0, ttftSumMs: 0,
  };
  b.requests++;
  if (entry.status >= 200 && entry.status < 400) b.ok++;
  else b.errors++;
  b.latencySumMs += entry.latencyMs || 0;
  if (entry.latencyMs > b.latencyMaxMs) b.latencyMaxMs = entry.latencyMs;
  if (entry.ttftMs != null) b.ttftSumMs += entry.ttftMs;
  if (entry.promptTokens) b.promptTokensSum += entry.promptTokens;
  if (entry.completionTokens) b.completionTokensSum += entry.completionTokens;
  if (entry.cacheHit) b.cacheHits++;
  if (typeof entry.cacheHitPct === "number") { b.cacheHitPctSum = (b.cacheHitPctSum || 0) + entry.cacheHitPct; b.cachePctKnown = (b.cachePctKnown || 0) + 1; }
  STATS.byBackend[entry.backend] = b;
  STATS.byModel[entry.model] = (STATS.byModel[entry.model] || 0) + 1;
}

// ---- analytics aggregates (dashboard) ----------------------------------------
//
// In-memory KPI + bucket store backing /api/dashboard. Fed at every
// history-entry finalization site via recordAnalytics() and seeded from the
// JSONL tail at startup so the charts survive restarts. Hour slots are
// hour-start epoch ms; day slots are UTC-midnight epoch ms. The payload keeps
// cost/cache/rate fields null until real data exists - null is the contract's
// "no data" signal, never a fake zero.

const ANALYTICS = {
  kpis: {
    requests: 0, ok: 0, errors: 0,
    promptTokens: 0, completionTokens: 0,
    cacheHitTokens: 0, cacheMissTokens: 0, cacheKnown: 0,
    priced: 0, cost: 0, cacheSavings: 0, offPeakSavings: 0, offPeakRequests: 0,
    models: new Set(), // distinct model ids seen (seeded rows too)
  },
  byBackend: new Map(), // backendId -> accumulator (shape below)
  byModel: new Map(),   // modelId -> accumulator (same shape)
  hourly: new Map(),    // hourSlot -> {requests, ok, errors, promptTokens, completionTokens, cacheHitTokens, cacheMissTokens, cost, priced, latencySumMs, latencyMaxMs}
  daily: new Map(),     // daySlot -> {requests, errors, promptTokens, completionTokens, cacheHitTokens, cacheMissTokens, cost, priced}
  expiredNotified: new Set(), // cooling backends whose expiry already hit the ring
};

const okStatus = (s) => s >= 200 && s <= 299;

function newAnalyticsAcc() {
  return { requests: 0, ok: 0, errors: 0, latencySumMs: 0, latencyMaxMs: 0, promptTokens: 0, completionTokens: 0, cost: 0, priced: 0, cacheHits: 0, cacheKnown: 0, cacheHitTokens: 0, cacheMissTokens: 0 };
}

// One accumulator update for a fully-shaped history entry (live or seeded).
// Cache denominators only count requests that actually carried a hit/miss
// split; a signal-less row (hit+miss == 0) must not drag the rate toward 0.
function recordAnalytics(entry) {
  const status = entry.status || 0;
  const promptTokens = entry.promptTokens || 0;
  const completionTokens = entry.completionTokens || 0;
  const hit = entry.promptCacheHitTokens || 0;
  const miss = entry.promptCacheMissTokens || 0;
  const cost = Number.isFinite(entry.cost) ? entry.cost : null;
  const k = ANALYTICS.kpis;
  k.requests++;
  if (okStatus(status)) k.ok++;
  else k.errors++;
  k.promptTokens += promptTokens;
  k.completionTokens += completionTokens;
  if (hit + miss > 0) {
    k.cacheHitTokens += hit;
    k.cacheMissTokens += miss;
    k.cacheKnown++;
  }
  if (cost != null) {
    k.priced++;
    k.cost += cost;
    if (entry.cacheSavings != null) k.cacheSavings += entry.cacheSavings;
    if (entry.offPeakSavings != null) k.offPeakSavings += entry.offPeakSavings;
  }
  if (entry.offPeak === true) k.offPeakRequests++;
  if (entry.model != null) k.models.add(entry.model);

  const backend = entry.backend || "none";
  let b = ANALYTICS.byBackend.get(backend);
  if (!b) {
    b = newAnalyticsAcc();
    ANALYTICS.byBackend.set(backend, b);
  }
  b.requests++;
  if (okStatus(status)) b.ok++;
  else b.errors++;
  b.latencySumMs += entry.latencyMs || 0;
  if ((entry.latencyMs || 0) > b.latencyMaxMs) b.latencyMaxMs = entry.latencyMs || 0;
  b.promptTokens += promptTokens;
  b.completionTokens += completionTokens;
  if (cost != null) { b.priced++; b.cost += cost; }
  if (entry.cacheHit === true) b.cacheHits++;
  if (entry.cacheHit === true || entry.cacheHit === false) b.cacheKnown++;
  if (hit + miss > 0) {
    b.cacheHitTokens += hit;
    b.cacheMissTokens += miss;
  }

  if (entry.model != null) {
    let m = ANALYTICS.byModel.get(entry.model);
    if (!m) {
      m = newAnalyticsAcc();
      ANALYTICS.byModel.set(entry.model, m);
    }
    m.requests++;
    if (okStatus(status)) m.ok++;
    else m.errors++;
    m.latencySumMs += entry.latencyMs || 0;
    if ((entry.latencyMs || 0) > m.latencyMaxMs) m.latencyMaxMs = entry.latencyMs || 0;
    m.promptTokens += promptTokens;
    m.completionTokens += completionTokens;
    if (cost != null) { m.priced++; m.cost += cost; }
    if (hit + miss > 0) {
      m.cacheHitTokens += hit;
      m.cacheMissTokens += miss;
    }
  }

  const hSlot = Math.floor(entry.t / 3600000) * 3600000;
  let hb = ANALYTICS.hourly.get(hSlot);
  if (!hb) {
    hb = { requests: 0, ok: 0, errors: 0, promptTokens: 0, completionTokens: 0, cacheHitTokens: 0, cacheMissTokens: 0, cost: 0, priced: 0, latencySumMs: 0, latencyMaxMs: 0 };
    ANALYTICS.hourly.set(hSlot, hb);
  }
  hb.requests++;
  if (okStatus(status)) hb.ok++;
  else hb.errors++;
  hb.promptTokens += promptTokens;
  hb.completionTokens += completionTokens;
  if (hit + miss > 0) {
    hb.cacheHitTokens += hit;
    hb.cacheMissTokens += miss;
  }
  if (cost != null) { hb.priced++; hb.cost += cost; }
  hb.latencySumMs += entry.latencyMs || 0;
  if ((entry.latencyMs || 0) > hb.latencyMaxMs) hb.latencyMaxMs = entry.latencyMs || 0;

  const dSlot = Math.floor(entry.t / 86400000) * 86400000;
  let db = ANALYTICS.daily.get(dSlot);
  if (!db) {
    db = { requests: 0, errors: 0, promptTokens: 0, completionTokens: 0, cacheHitTokens: 0, cacheMissTokens: 0, cost: 0, priced: 0 };
    ANALYTICS.daily.set(dSlot, db);
  }
  db.requests++;
  if (!okStatus(status)) db.errors++;
  db.promptTokens += promptTokens;
  db.completionTokens += completionTokens;
  if (hit + miss > 0) {
    db.cacheHitTokens += hit;
    db.cacheMissTokens += miss;
  }
  if (cost != null) { db.priced++; db.cost += cost; }
}

function coolingCount(now) {
  let n = 0;
  for (const h of health.values()) {
    if (h.state === "cooling" && h.nextAvailableAt > now) n++;
  }
  return n;
}

// Build the exact /api/dashboard payload: windows are generated at read time
// (exactly 24 hour-slots oldest first, exactly 30 day-slots ending today), so
// empty slots materialize as zeroed rows without needing a store sweep.
function dashboardPayload(cfg) {
  const now = Date.now();
  const k = ANALYTICS.kpis;
  const priced = k.priced > 0;
  const kpis = {
    requests: k.requests,
    ok: k.ok,
    errors: k.errors,
    errorRate: k.requests > 0 ? k.errors / k.requests : null,
    promptTokens: k.promptTokens,
    completionTokens: k.completionTokens,
    cacheHitTokens: k.cacheHitTokens,
    cacheMissTokens: k.cacheMissTokens,
    cacheHitRate: k.cacheKnown > 0 ? k.cacheHitTokens / (k.cacheHitTokens + k.cacheMissTokens) : null,
    cost: priced ? k.cost : null,
    cacheSavings: priced ? k.cacheSavings : null,
    offPeakSavings: priced ? k.offPeakSavings : null,
    offPeakRequests: k.offPeakRequests,
    offPeakShare: k.requests > 0 ? k.offPeakRequests / k.requests : null,
    cooledBackends: coolingCount(now),
    activeModelCount: k.models.size,
  };

  const byBackend = cfg.backends.map((b) => {
    const acc = ANALYTICS.byBackend.get(b.id);
    const h = health.get(b.id) || {};
    const isCooling = h.state === "cooling" && h.nextAvailableAt > now;
    return {
      id: b.id,
      requests: acc ? acc.requests : 0,
      ok: acc ? acc.ok : 0,
      errors: acc ? acc.errors : 0,
      errorRate: acc && acc.requests > 0 ? acc.errors / acc.requests : null,
      latencyAvgMs: acc && acc.requests > 0 ? acc.latencySumMs / acc.requests : null,
      latencyMaxMs: acc ? acc.latencyMaxMs : 0,
      promptTokens: acc ? acc.promptTokens : 0,
      completionTokens: acc ? acc.completionTokens : 0,
      cost: acc && acc.priced > 0 ? acc.cost : null,
      cacheHits: acc ? acc.cacheHits : 0,
      cacheKnown: acc ? acc.cacheKnown : 0,
      cacheHitRate: acc && acc.cacheKnown > 0 ? acc.cacheHitTokens / (acc.cacheHitTokens + acc.cacheMissTokens) : null,
      state: isCooling ? "cooling" : "healthy",
      fails: h.fails || 0,
      lastErrorKind: (h.lastError && h.lastError.kind) || null,
      manual: !!(isCooling && h.lastError && h.lastError.kind === "manual"),
    };
  });

  const byModel = [];
  for (const [id, m] of ANALYTICS.byModel) {
    const mcfg = (cfg.models || {})[id];
    byModel.push({
      id,
      label: (mcfg && mcfg.meta && mcfg.meta.label) || null,
      requests: m.requests,
      ok: m.ok,
      errors: m.errors,
      errorRate: m.requests > 0 ? m.errors / m.requests : null,
      latencyAvgMs: m.requests > 0 ? m.latencySumMs / m.requests : null,
      latencyMaxMs: m.latencyMaxMs,
      promptTokens: m.promptTokens,
      completionTokens: m.completionTokens,
      cost: m.priced > 0 ? m.cost : null,
      cacheHitRate: m.cacheHitTokens + m.cacheMissTokens > 0 ? m.cacheHitTokens / (m.cacheHitTokens + m.cacheMissTokens) : null,
    });
  }

  const curHour = Math.floor(now / 3600000) * 3600000;
  const hourly = [];
  for (let i = 23; i >= 0; i--) {
    const slot = curHour - i * 3600000;
    const hb = ANALYTICS.hourly.get(slot);
    hourly.push({
      t: slot,
      requests: hb ? hb.requests : 0,
      ok: hb ? hb.ok : 0,
      errors: hb ? hb.errors : 0,
      promptTokens: hb ? hb.promptTokens : 0,
      completionTokens: hb ? hb.completionTokens : 0,
      cacheHitTokens: hb ? hb.cacheHitTokens : 0,
      cacheMissTokens: hb ? hb.cacheMissTokens : 0,
      cost: hb && hb.priced > 0 ? hb.cost : null,
      latencyAvgMs: hb && hb.requests > 0 ? hb.latencySumMs / hb.requests : null,
      latencyMaxMs: hb ? hb.latencyMaxMs : 0,
    });
  }

  const curDay = Math.floor(now / 86400000) * 86400000;
  const daily = [];
  for (let i = 29; i >= 0; i--) {
    const slot = curDay - i * 86400000;
    const db = ANALYTICS.daily.get(slot);
    daily.push({
      t: slot,
      requests: db ? db.requests : 0,
      errors: db ? db.errors : 0,
      cost: db && db.priced > 0 ? db.cost : null,
      tokens: db ? db.promptTokens + db.completionTokens : 0,
      cacheHitRate: db && db.cacheHitTokens + db.cacheMissTokens > 0 ? db.cacheHitTokens / (db.cacheHitTokens + db.cacheMissTokens) : null,
    });
  }

  const pr = cfg.pricing && typeof cfg.pricing === "object" ? cfg.pricing : {};
  const pricing = {
    offPeakMultiplier: typeof pr.offPeakMultiplier === "number" ? pr.offPeakMultiplier : 0.5,
    peakWindows: Array.isArray(pr.peakWindows) ? pr.peakWindows : DEFAULT_PEAK_WINDOWS,
    modelsWithOffPeak: Object.keys(cfg.models || {}).filter((id) => {
      const m = cfg.models[id];
      return !!(m && m.meta && m.meta.pricing && m.meta.pricing.offPeak === true);
    }),
  };

  return { since: startedAt, uptimeMs: now - startedAt, kpis, byBackend, byModel, hourly, daily, cooling: COOLING_EVENTS.slice(0, COOLING_EVENT_CAP), pricing };
}

// Seed the aggregate store from the JSONL tail at startup so the dashboard
// charts survive restarts. Reads at most the last 20 MB (a pathological
// history must not delay boot); a partial first line from a tail read is
// dropped. Malformed lines are skipped; legacy rows still count as requests
// with zero tokens, exactly like a live entry would.
function seedAnalytics(file) {
  let raw;
  try {
    const size = statSync(file).size;
    const MAX_SEED_BYTES = 20 * 1024 * 1024;
    if (size > MAX_SEED_BYTES) {
      const fd = openSync(file, "r");
      try {
        const buf = Buffer.alloc(MAX_SEED_BYTES);
        const n = readSync(fd, buf, 0, MAX_SEED_BYTES, size - MAX_SEED_BYTES);
        raw = buf.subarray(0, n).toString("utf8");
      } finally {
        closeSync(fd);
      }
      const nl = raw.indexOf("\n");
      if (nl >= 0) raw = raw.slice(nl + 1);
    } else {
      raw = readFileSync(file, "utf8");
    }
  } catch {
    return; // no history file yet - a fresh install
  }
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }
    if (!row || typeof row !== "object" || typeof row.t !== "number" || !Number.isFinite(row.t)) continue;
    recordAnalytics(row);
  }
}

// ---- config normalization ---------------------------------------------------

// Normalize the three-layer config at load (initial + hot-reload), in place.
// Detection is PER ENTRY so single-era configs and accidental mixes both work:
// - `models[id]` with `providers` (new shape) is validated.
// - `models[id]` with `backends` (pre-presets era, path A) is synthesized into
//   providers `{backend, upstream: <that backend's model>}`.
// - `presets[id]` with `models` (new shape) is validated.
// - `presets[id]` with `members` (DESIGN-PRESETS era, path B) synthesizes ONE
//   model per preset (providers = members in order with per-provider upstream)
//   and rewrites the preset to `{strategy, models: [{model, weight: 1}]}`.
// Validation is lenient (warn + drop, never crash the router). Derived
// read-only `preset.members` (flattened provider backends, first occurrence,
// deduped) keeps the UI/tests working across the phased rollout. The
// normalized form lives only in memory; never written back to disk.
const collisionWarned = new Set(); // model id == preset id warns once per process

function normalizeConfig(cfg) {
  if (!cfg || typeof cfg !== "object") cfg = {};
  const backendById = new Map((cfg.backends || []).map((b) => [b.id, b]));
  if (!cfg.models || typeof cfg.models !== "object") cfg.models = {};
  if (!cfg.presets || typeof cfg.presets !== "object") cfg.presets = {};

  const warnOnce = (set, key, msg) => {
    if (set.has(key)) return;
    set.add(key);
    console.warn(msg);
  };

  // ---- 1. models map -----------------------------------------------------
  const legacySynthesized = new Set(); // path A model ids (need presets later)
  for (const [id, m] of Object.entries(cfg.models)) {
    if (!m || typeof m !== "object") {
      console.warn(`config warning: model '${id}': invalid entry dropped`);
      delete cfg.models[id];
      continue;
    }
    if (Array.isArray(m.providers)) {
      // NEW shape: validate each provider; keep optional dialect fields.
      const valid = [];
      for (const pr of m.providers) {
        if (!pr || typeof pr !== "object" || typeof pr.backend !== "string") {
          console.warn(`config warning: model '${id}': provider without backend id dropped`);
          continue;
        }
        const b = backendById.get(pr.backend);
        if (!b) {
          console.warn(`config warning: model '${id}': provider references unknown backend '${pr.backend}', dropped`);
          continue;
        }
        const upstream = pr.upstream ?? b.model;
        if (!upstream) {
          console.warn(`config warning: model '${id}': provider '${pr.backend}' has no resolvable upstream, dropped`);
          continue;
        }
        valid.push({ ...pr, upstream });
      }
      m.providers = valid;
    } else if (Array.isArray(m.backends)) {
      // path A: pre-presets era, `{backends: [id...], affinityPool}`.
      const valid = [];
      for (const bid of m.backends) {
        const b = typeof bid === "string" ? backendById.get(bid) : null;
        if (!b) {
          console.warn(`config warning: model '${id}': unknown backend '${bid}' dropped`);
          continue;
        }
        if (!b.model) {
          console.warn(`config warning: model '${id}': backend '${bid}' has no model (upstream), dropped`);
          continue;
        }
        valid.push({ backend: bid, upstream: b.model });
      }
      m.providers = valid;
      delete m.backends;
      legacySynthesized.add(id);
    } else {
      console.warn(`config warning: model '${id}': no providers/backends, entry dropped`);
      delete cfg.models[id];
      continue;
    }
    if ("params" in m && (m.params === null || typeof m.params !== "object")) {
      console.warn(`config warning: model '${id}': params must be an object, ignored`);
      delete m.params;
    }
    if (m.retry !== undefined) {
      if (m.retry === null || typeof m.retry !== "object") {
        console.warn(`config warning: model '${id}': retry must be an object, ignored`);
        delete m.retry;
      } else {
        const r = m.retry;
        if (typeof r.maxRetries !== "number" || r.maxRetries < 0 || !Number.isInteger(r.maxRetries)) {
          console.warn(`config warning: model '${id}': retry.maxRetries must be a non-negative integer, defaulting to 0`);
          r.maxRetries = 0;
        }
        for (const k of ["baseMs", "maxMs", "totalMs"]) {
          if (r[k] !== undefined && (typeof r[k] !== "number" || r[k] < 0)) {
            console.warn(`config warning: model '${id}': retry.${k} must be a non-negative number, ignored`);
            delete r[k];
          }
        }
        if (r.multiplier !== undefined && (typeof r.multiplier !== "number" || r.multiplier < 1)) {
          console.warn(`config warning: model '${id}': retry.multiplier must be a number >= 1, defaulting to 2`);
          r.multiplier = 2;
        }
      }
    }
  }

  // ---- 2. presets map -----------------------------------------------------
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (!p || typeof p !== "object") {
      console.warn(`config warning: preset '${id}': invalid entry dropped`);
      delete cfg.presets[id];
      continue;
    }
    if (Array.isArray(p.models)) {
      // NEW shape: normalize model refs to {model, weight} objects. A ref may
      // name a MODEL id or another PRESET id (preset-of-presets): both
      // namespaces are legal, unknown ids are dropped.
      const refs = [];
      for (const ref of p.models) {
        let mref = null;
        let weight = 1;
        if (typeof ref === "string") {
          mref = ref;
        } else if (ref && typeof ref === "object" && typeof ref.model === "string") {
          mref = ref.model;
          weight = typeof ref.weight === "number" && ref.weight > 0 ? ref.weight : 1;
        } else {
          console.warn(`config warning: preset '${id}': invalid model entry dropped`);
          continue;
        }
        if (!cfg.models[mref] && !cfg.presets[mref]) {
          console.warn(`config warning: preset '${id}': dropping unknown model '${mref}'`);
          continue;
        }
        refs.push({ model: mref, weight });
      }
      p.models = refs;
      if (p.strategy !== "affinity" && p.strategy !== "failover" && p.strategy !== "weighted") {
        console.warn(`config warning: preset '${id}': unknown strategy '${p.strategy}', defaulting to affinity`);
        p.strategy = "affinity";
      }
      if ("params" in p && (p.params === null || typeof p.params !== "object")) {
        console.warn(`config warning: preset '${id}': params must be an object, ignored`);
        delete p.params;
      }
      continue;
    }
    if (Array.isArray(p.members)) {
      // path B: DESIGN-PRESETS era. ONE model per preset; providers keep their
      // own per-provider upstream (mixed upstreams are exactly the target shape).
      const valid = [];
      for (const mem of p.members) {
        const bid = typeof mem === "string" ? mem : mem && typeof mem.backend === "string" ? mem.backend : null;
        const b = bid ? backendById.get(bid) : null;
        if (!b) {
          console.warn(`config warning: preset '${id}': dropping unknown backend member '${bid || mem}'`);
          continue;
        }
        if (!b.model) {
          console.warn(`config warning: preset '${id}': backend '${bid}' has no model (upstream), dropped`);
          continue;
        }
        valid.push({ backend: bid, upstream: b.model });
      }
      // Model id = the preset id; reuse an existing model only if its provider
      // backend-id set equals the synthesized set (order-insensitive).
      let modelId = id;
      const existing = cfg.models[modelId];
      if (existing) {
        const existingSet = new Set((existing.providers || []).map((pr) => pr.backend));
        const synthSet = new Set(valid.map((v) => v.backend));
        const equal =
          existingSet.size === synthSet.size && [...synthSet].every((bid) => existingSet.has(bid));
        if (!equal) {
          modelId = `${id}-legacy`;
          console.warn(`config warning: preset '${id}': existing model '${id}' provider set differs; synthesized model '${modelId}'`);
        }
      }
      if (!cfg.models[modelId]) {
        const nm = { providers: valid };
        if (typeof p.affinityPool === "number") nm.affinityPool = p.affinityPool;
        cfg.models[modelId] = nm;
      }
      // Strategy: the original, EXCEPT legacy weighted -> affinity (weights over
      // backends are not representable at the model layer; declared order kept).
      let strategy = p.strategy || "affinity";
      if (strategy !== "affinity" && strategy !== "failover" && strategy !== "weighted") {
        console.warn(`config warning: preset '${id}': unknown strategy '${strategy}', defaulting to affinity`);
        strategy = "affinity";
      }
      if (strategy === "weighted") {
        console.warn(`config warning: preset '${id}': legacy weighted preset collapsed to affinity (weights not representable at the model layer)`);
        strategy = "affinity";
      }
      const np = { strategy, models: [{ model: modelId, weight: 1 }] };
      if (p.params && typeof p.params === "object") np.params = p.params;
      if (p.meta && typeof p.meta === "object") np.meta = p.meta;
      cfg.presets[id] = np;
      if ("params" in p && (p.params === null || typeof p.params !== "object")) {
        console.warn(`config warning: preset '${id}': params must be an object, ignored`);
      }
      continue;
    }
    console.warn(`config warning: preset '${id}': no models/members, entry dropped`);
    delete cfg.presets[id];
  }

  // ---- 3. path A models with no preset get an identity preset ------------
  for (const id of legacySynthesized) {
    if (!cfg.presets[id]) {
      cfg.presets[id] = { strategy: "affinity", models: [{ model: id, weight: 1 }] };
    }
  }

  // ---- 4. derived read-only preset members --------------------------------
  // members = flattened provider backends of the preset's (recursively
  // expanded) model list, first-occurrence order, deduped. Nested preset refs
  // contribute their models' providers in place.
  const flattenPresetModels = (pid, seenPresets) => {
    const out = [];
    const p = cfg.presets[pid];
    if (!p || seenPresets.has(pid)) return out;
    const nextSeen = new Set(seenPresets).add(pid);
    for (const ref of p.models || []) {
      // Identity case (path B): models: [{model: <same id>}] where the id is
      // BOTH preset and model -> the ref is a MODEL, not a nested preset.
      if (ref.model === pid && cfg.models[pid]) {
        out.push(pid);
        continue;
      }
      if (cfg.presets[ref.model]) {
        out.push(...flattenPresetModels(ref.model, nextSeen));
      } else {
        out.push(ref.model);
      }
    }
    return out;
  };
  for (const [pid, p] of Object.entries(cfg.presets)) {
    const members = [];
    const seen = new Set();
    for (const mid of flattenPresetModels(pid, new Set())) {
      const m = cfg.models[mid];
      if (!m) continue;
      for (const pr of m.providers || []) {
        if (seen.has(pr.backend)) continue;
        seen.add(pr.backend);
        members.push({ backend: pr.backend, weight: 1 });
      }
    }
    p.members = members;
  }

  // ---- 5. collision + params validation warnings --------------------------
  for (const mid of Object.keys(cfg.models)) {
    if (cfg.presets[mid]) {
      warnOnce(collisionWarned, mid, `config warning: model id '${mid}' collides with a preset id; the preset wins at request time`);
    }
  }
  const warnModelInParams = (layer, where) => {
    if (layer && typeof layer === "object" && "model" in layer) {
      console.warn(`config warning: params layer ${where} contains reserved 'model' key, ignored`);
    }
  };
  warnModelInParams(cfg.params, "global");
  for (const b of cfg.backends || []) warnModelInParams(b.params, `backend ${b.id}`);
  for (const [mid, m] of Object.entries(cfg.models)) {
    warnModelInParams(m.params, `model ${mid}`);
    for (const pr of m.providers || []) warnModelInParams(pr.params, `provider ${mid}/${pr.backend}`);
  }
  for (const [pid, p] of Object.entries(cfg.presets)) warnModelInParams(p.params, `preset ${pid}`);
  // dropParams union warnings: every dropped public/request key is a silent
  // no-op for requests that use it.
  const warnedDrop = new Set();
  for (const [pid, p] of Object.entries(cfg.presets)) {
    for (const ref of p.models || []) {
      const m = cfg.models[ref.model];
      if (!m) continue;
      for (const pr of m.providers || []) {
        const b = backendById.get(pr.backend);
        if (!b) continue;
        const drop = new Set([...(b.dropParams || []), ...(pr.dropParams || [])]);
        for (const k of drop) {
          warnOnce(
            warnedDrop,
            `${pid}|${ref.model}|${pr.backend}|${k}`,
            `config warning: preset ${pid} model ${ref.model} provider ${pr.backend} drops param ${k} - requests using it will not receive it`
          );
        }
      }
    }
  }
  return cfg;
}

// ---- routing ---------------------------------------------------------------

function sessionKeyOf(req, body) {
  const h = req.headers;
  return (
    h["x-session-affinity"] ||
    h["x-session-id"] ||
    (body && (body.session_id || body.sessionId || body.user)) ||
    "no-session"
  );
}

function hashStr(s) {
  return crypto.createHash("sha256").update(String(s)).digest().readUInt32BE(0);
}

// Recursively expand a preset's models list into a flat model-id list.
// - A preset's models array may contain MODEL ids and/or PRESET ids
//   (preset-of-presets). Preset refs are expanded in place, preserving
//   declared order. A nested preset contributes ONLY its model list (flattened
//   recursively); its own strategy/affinityPool/weights are ignored for
//   ordering - the TOP-LEVEL preset's strategy governs the expanded list.
// - Weights: the top-level ref carries the weight (nested preset ids use the
//   weight of the position they were referenced from). Expansion itself is
//   weight-less; weights only matter when the top-level strategy is weighted.
// - Cycle handling: a visited set per expansion. On a cycle (a preset already
//   in the current expansion path is referenced again), console.warn ONCE and
//   stop expanding that path; the id that caused the cycle is kept as a MODEL
//   id if it resolves to a model, else dropped.
// - Unknown refs (neither preset nor model) are dropped (they are already
//   warned about at normalizeConfig time; this is a defensive re-check).
// Returns {modelIds, modelRefs} where modelRefs are the TOP-LEVEL refs
// (weights kept, expanded ordering).
function expandPresetModels(cfg, topPreset, topId) {
  const presets = cfg.presets || {};
  const models = cfg.models || {};
  const modelIds = [];
  const modelRefs = [];

  const expand = (ref, visited) => {
    const id = typeof ref === "string" ? ref : ref && ref.model;
    if (!id) return;
    const isPreset = !!presets[id];
    const isModel = !!models[id];
    if (!isPreset && !isModel) return; // unknown id (defensive; normalizeConfig warns)
    if (isPreset) {
      if (visited.has(id)) {
        const pathKey = [...visited, id].join("->");
        if (!cycleWarned.has(pathKey)) {
          cycleWarned.add(pathKey);
          console.warn(
            `config warning: preset-of-presets cycle detected at '${id}' (path: ${[...visited, id].join(" -> ")}); keeping it as a model id if it resolves, else dropping`
          );
        }
        if (isModel) modelIds.push(id); // keep as model if it resolves, else drop
        return;
      }
      const sub = presets[id];
      const subRefs = (sub.models || []).map((m) => (typeof m === "string" ? { model: m, weight: 1 } : m));
      for (const sr of subRefs) expand(sr, new Set(visited).add(id));
    } else {
      modelIds.push(id);
      modelRefs.push({ model: id, weight: ref && typeof ref === "object" ? ref.weight || 1 : 1 });
    }
  };

  for (const ref of topPreset.models || []) {
    const m = typeof ref === "string" ? { model: ref, weight: 1 } : ref;
    const id = m.model;
    // Identity case: a preset whose models list references its OWN colliding
    // id (path B synthesis produces {models: [{model: <same id>}]} where the
    // id is BOTH preset and model). That is a MODEL ref, not a nested preset:
    // treat it as a model without recursion or cycle warning.
    if (id === topId && models[id]) {
      modelIds.push(id);
      modelRefs.push({ model: id, weight: m.weight || 1 });
      continue;
    }
    expand(m, new Set([topId]));
  }
  return { modelIds, modelRefs };
}

// Build the flattened, backend-deduped ordered attempt list for a request:
// - preset-or-implicit-model resolution: a preset id uses its strategy; a bare
//   model id acts as an implicit single-model preset
//   {strategy: "affinity", models: [id], affinityPool: 1}.
// - preset level: order the candidate MODELS per strategy. affinity picks
//   models[hash(sessionKey) % pool]; failover keeps declared order; weighted
//   picks weighted-random (rng injectable for tests), rest sorted by weight
//   desc (stable sort keeps declared-order tiebreak).
// - model level: each model orders its PROVIDERS by affinity (pool =
//   min(model.affinityPool || 1, providers.length); primary =
//   providers[hash(sessionKey) % pool]; rest in declared order).
// - flatten in model order into {model, backend, upstream} attempts, deduped
//   by backend id (first occurrence wins, keeping that occurrence's model +
//   upstream) so a shared account is never retried twice in one request.
// hashStr and the modulo arithmetic are UNCHANGED from the preset-only era so
// affinity routing stays byte-identical for identical effective configs.
// Returns null for unknown ids, else {kind, presetId, preset, models, primary,
// ordered} where preset is the normalized preset object or null (implicit
// model), primary is ordered[0] or null, ordered is the attempt list. A preset
// with zero valid models returns ordered: [] (not null) so handleChat can
// produce the specific 404 message.
// Preset-of-presets: when a preset's models list contains another preset id,
// that nested preset's model list is EXPANDED in place (recursively, with
// cycle detection). The TOP-LEVEL preset's strategy governs ordering over the
// expanded list; a nested preset's own strategy is ignored (only its model
// list is taken). See expandPresetModels() above.
function candidates(cfg, requestedId, sessionKey, rng = Math.random) {
  const preset = (cfg.presets || {})[requestedId] || null;
  let kind;
  let modelIds;
  let modelRefs;
  if (preset) {
    kind = "preset";
    // Expand nested preset refs to a flat model list (cycle-safe).
    const expanded = expandPresetModels(cfg, preset, requestedId);
    modelIds = expanded.modelIds;
    modelRefs = expanded.modelRefs;
  } else if ((cfg.models || {})[requestedId]) {
    kind = "model";
    modelIds = [requestedId];
    modelRefs = [{ model: requestedId, weight: 1 }];
  } else {
    return null;
  }

  // Preset-level model ordering over the (possibly expanded) model list.
  let orderedModels;
  if (!preset) {
    orderedModels = modelIds; // implicit model: single candidate
  } else if (preset.strategy === "failover") {
    orderedModels = modelIds; // declared order
  } else if (preset.strategy === "weighted") {
    const weights = modelRefs.map((m) => m.weight || 1);
    const total = weights.reduce((s, w) => s + w, 0);
    const r = (typeof rng === "function" ? rng() : Math.random()) * total;
    let pickIdx = modelIds.length - 1;
    let acc = 0;
    for (let i = 0; i < modelIds.length; i++) {
      acc += weights[i];
      if (r < acc) {
        pickIdx = i;
        break;
      }
    }
    const rest = modelRefs
      .map((m, i) => ({ id: modelIds[i], weight: m.weight || 1 }))
      .filter((_, i) => i !== pickIdx)
      .sort((a, b) => (b.weight || 1) - (a.weight || 1)); // stable -> declared-order tiebreak
    orderedModels = [modelIds[pickIdx], ...rest.map((x) => x.id)];
  } else {
    // affinity (default)
    const pool = Math.min(preset.affinityPool || 1, modelIds.length);
    const primaryIdx = hashStr(sessionKey) % pool;
    orderedModels = [modelIds[primaryIdx], ...modelIds.filter((_, i) => i !== primaryIdx)];
  }

  // Model-level provider ordering + flatten + backend dedup.
  const ordered = [];
  const seenBackends = new Set();
  for (const mid of orderedModels) {
    const m = (cfg.models || {})[mid];
    if (!m) continue;
    const providers = m.providers || [];
    if (providers.length === 0) continue;
    const pool = Math.min(m.affinityPool || 1, providers.length);
    const primaryIdx = hashStr(sessionKey) % pool;
    const providerOrder = [providers[primaryIdx], ...providers.filter((_, i) => i !== primaryIdx)];
    for (const pr of providerOrder) {
      if (seenBackends.has(pr.backend)) continue;
      seenBackends.add(pr.backend);
      ordered.push({ model: mid, backend: pr.backend, upstream: pr.upstream });
    }
  }

  return {
    kind,
    presetId: requestedId,
    preset,
    models: modelIds,
    primary: ordered.length ? ordered[0] : null,
    ordered,
  };
}

function newError(status, body) {
  return { status, body };
}

// ---- layered parameters ----------------------------------------------------

// Merge request parameters across the six layers, highest priority last:
// global < backend.params < provider.params < model.params < preset.params <
// request body. `model` is RESERVED: config-layer model keys are ignored and
// the forwarded `model` is ALWAYS provider.upstream. Dialect handling runs
// AFTER the merge, on the PUBLIC/request key space: dropParams (union of
// backend + provider, legacy backend field still honored) deletes keys first,
// then provider.paramMap renames the survivors, then the developer-role
// translation (backend field, special-cased). Pure: never mutates `body`.
function buildPayload(body, cfg, preset, model, provider, backend) {
  const out = {};
  Object.assign(out, cfg.params || {}); // lowest layer
  Object.assign(out, backend.params || {});
  Object.assign(out, provider.params || {});
  Object.assign(out, model.params || {});
  if (preset) Object.assign(out, preset.params || {});
  delete out.model; // reserved in config layers
  Object.assign(out, body); // request body wins
  out.model = provider.upstream; // always
  // Dialect: drop FIRST (public/request key space), then rename survivors.
  const drop = new Set([...(backend.dropParams || []), ...(provider.dropParams || [])]);
  for (const k of drop) delete out[k];
  for (const [from, to] of Object.entries(provider.paramMap || {})) {
    if (from in out) {
      out[to] = out[from];
      delete out[from];
    }
  }
  // Role translation last (unchanged field-flag behavior).
  if (backend.translateDeveloperRole && Array.isArray(out.messages)) {
    out.messages = out.messages.map((m) => (m && m.role === "developer" ? { ...m, role: "system" } : m));
  }
  return out;
}

// ---- request forwarding ----------------------------------------------------

async function forward(req, cfg, backend, payload, incomingHeaders, stream) {
  const key = process.env[backend.apiKeyEnv];
  const upstream = backend.baseURL.replace(/\/$/, "") + "/chat/completions";

  const outHeaders = {
    "content-type": "application/json",
    authorization: `Bearer ${key}`,
    accept: stream ? "text/event-stream" : "application/json",
  };
  // Forward prompt-cache headers verbatim.
  for (const h of ["x-cache-key", "cachekey", "set-cache-key"]) {
    if (incomingHeaders[h]) outHeaders[h] = incomingHeaders[h];
  }

  const payloadText = JSON.stringify(payload);

  // Debug wire format: log the exact outgoing request body (redacted - never
  // print key values; the payload holds messages + params only) so operators
  // can verify what the upstream actually receives.
  if (process.env.ROUTER_DEBUG_WIRE === "1") {
    try {
      const p = JSON.parse(payloadText);
      const dbg = { ...p };
      if (Array.isArray(dbg.messages)) dbg.messages = dbg.messages.map((m) => ({ role: m.role, content: String(m.content || "").slice(0, 80) }));
      console.error(`[${new Date().toISOString()}] [wire] ${backend.id} -> ${dbg.model} params=${JSON.stringify(Object.keys(dbg).filter((k) => !["messages", "model", "stream"].includes(k)))} ${JSON.stringify(Object.fromEntries(Object.entries(dbg).filter(([k]) => !["messages", "model", "stream"].includes(k))))}`);
    } catch {}
  }

  // Connect/header timeout only: bound how long we wait for the upstream to
  // send response HEADERS (TTFT), NOT the whole body. A wall-clock signal on
  // fetch() is also tied to the response body in undici, so a slow-but-alive
  // SSE relay used to be aborted mid-stream exactly at timeoutMs (see router.log
  // "relay error: The operation was aborted due to timeout" on long thinking
  // generations). The body is instead protected by an idle/stall watchdog in
  // relay() (idleTimeoutMs): it only fires when actual silence exceeds the
  // bound, so healthy long streams run to completion while stuck ones still
  // fail.
  const connectTimeoutMs = backend.connectTimeoutMs ?? backend.timeoutMs ?? cfg.connectTimeoutMs ?? cfg.timeoutMs ?? 45000;
  const connectController = new AbortController();
  const connectTimer = setTimeout(() => connectController.abort(), connectTimeoutMs);
  let upstreamRes;
  try {
    upstreamRes = await fetch(upstream, {
      method: "POST",
      headers: outHeaders,
      body: payloadText,
      signal: connectController.signal,
    });
  } catch (e) {
    clearTimeout(connectTimer);
    return { err: newError(0, String(e && e.message)) };
  }
  clearTimeout(connectTimer); // headers arrived: the wall clock no longer applies to the body

  if (!upstreamRes.ok) {
    let text = "";
    try {
      text = await upstreamRes.text();
    } catch {}
    return {
      err: newError(upstreamRes.status, text),
      statusFromUpstream: upstreamRes.status,
    };
  }
  return { res: upstreamRes };
}

// ---- HTTP handlers ---------------------------------------------------------

async function handleChat(req, res, cfg, bodyText) {
  const started = Date.now();
  let body;
  try {
    body = JSON.parse(bodyText);
  } catch {
    const e = newError(400, JSON.stringify({ error: { message: "invalid JSON body", type: "invalid_request_error" } }));
    return writeError(res, e);
  }
  const modelId = body && body.model;
  const stream = !!(body && body.stream);
  if (!modelId) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' not found`, type: "model_not_found" } }));
    return writeError(res, e);
  }

  const sessionKey = sessionKeyOf(req, body);
  // One call id per incoming request, shared by the history entry, the detail
  // record, and the x-router-call response header so any request can be
  // referenced by a single short id.
  const callId = newCallId();
  const c = candidates(cfg, modelId, sessionKey);
  if (!c) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' not found`, type: "model_not_found" } }));
    return writeError(res, e);
  }
  if (c.kind === "preset" && c.models.length === 0) {
    const e = newError(404, JSON.stringify({ error: { message: `preset '${modelId}' has no valid models`, type: "model_not_found" } }));
    return writeError(res, e);
  }
  if (c.kind === "model" && c.ordered.length === 0) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' has no valid providers`, type: "model_not_found" } }));
    return writeError(res, e);
  }

  // Prefer healthy candidates. If all cooling, fall back to system-cooled
  // backends only (soonest nextAvailableAt first); manually cooled backends
  // NEVER participate in the all-cooling fallback.
  const now = Date.now();
  const healthy = c.ordered.filter((a) => isUsableCurrently(a.backend, now));
  const manualCooled = new Set(
    c.ordered
      .filter((a) => {
        const h = health.get(a.backend);
        return !!h && h.state === "cooling" && h.lastError && h.lastError.kind === "manual";
      })
      .map((a) => a.backend)
  );
  let pool;
  if (healthy.length) {
    pool = healthy;
  } else {
    pool = c.ordered.filter((a) => !manualCooled.has(a.backend)).sort((a, b) => {
      return (health.get(a.backend)?.nextAvailableAt || 0) - (health.get(b.backend)?.nextAvailableAt || 0);
    });
    if (pool.length === 0) {
      const latencyMs = Date.now() - started;
      const detailT = Date.now();
      const entry = applyCost(newHistoryEntry({ t: detailT, callId, model: modelId, backend: "none", stream, status: 503, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null, errorKind: null }), cfg);
      recordHistory(entry);
      recordAnalytics(entry);
      fireAlert(cfg, "all_cooling", { manualCooled: manualCooled.size });
      recordStats({ backend: "none", model: modelId, status: 503, latencyMs });
      // Detail record even with no attempts: captures the payload + a "no
      // candidates were eligible" note so the empty-pool 503 is inspectable.
      // Always recorded; oversized bodies are stored bounded (see
      // boundedPayload) so the panel never silently 404s.
      recordDetail(detailT, callId, bodyText.length <= DETAIL_MAX_BODY_BYTES ? { ...body } : boundedPayload(body), null);
      finalizeDetail(detailT, {
        status: 503,
        ttftMs: 0,
        latencyMs,
        genMs: 0,
        promptTokens: 0,
        completionTokens: 0,
        reasoningTokens: 0,
        cacheHit: null,
        cacheHitPct: null,
        preview: null,
        errorBody: `all available backends are cooling (${manualCooled.size} manually cooled - uncool to restore)`,
        noEligibleBackends: true,
      });
      res.setHeader("x-router-backend", "none");
      res.setHeader("x-router-call", callId);
      const msg = JSON.stringify({
        error: {
          message: `all available backends are cooling (${manualCooled.size} manually cooled - uncool to restore)`,
          type: "router_unavailable",
        },
      });
      return writeError(res, newError(503, msg));
    }
  }

  let lastErr = null;
  let lastTried = null;
  const captureDetail = bodyText.length <= DETAIL_MAX_BODY_BYTES; // size guard: skip huge payloads
  const detailT = Date.now();
  for (const attempt of pool) {
    lastTried = attempt;
    const backend = cfg.backends.find((b) => b.id === attempt.backend);
    const model = (cfg.models || {})[attempt.model];
    const provider = (model && (model.providers || []).find((p) => p.backend === attempt.backend)) || null;
    if (!backend || !model || !provider) continue; // defensive: candidates built the attempt
    const payload = buildPayload(body, cfg, c.preset, model, provider, backend);
    // Per-model retry: on retryable failures, retry the SAME provider with
    // exponential backoff before falling through to the next candidate. This
    // preserves the session's prompt cache (a fallback backend has zero cached
    // context) and gives chronically-overloaded free models a chance to catch
    // a free slot. Retries NEVER re-cool the backend - markFailure runs once
    // after retries are exhausted, exactly as before.
    const rcfg = (model.retry && typeof model.retry === "object" ? model.retry : null) || { maxRetries: 0 };
    const maxRetries = Math.max(0, Math.floor(rcfg.maxRetries || 0));
    const retryBaseMs = rcfg.baseMs ?? 1000;
    const retryMaxMs = rcfg.maxMs ?? 8000;
    const retryMult = rcfg.multiplier ?? 2;
    const retryTotalMs = rcfg.totalMs ?? 15000;
    let retries = 0;
    let retryWaitedMs = 0;
    let result = null;
    const attemptStarted = Date.now();
    while (true) {
      result = await forward(req, cfg, backend, payload, req.headers, stream);
      if (result.err && isRetryableError(result.err.status, result.err.body) && retries < maxRetries) {
        const retryCount = retries + 1;
        const waitMs = Math.min(retryBaseMs * retryMult ** (retryCount - 1), retryMaxMs);
        const budgetExceeded = retryWaitedMs + waitMs > retryTotalMs;
        if (budgetExceeded) break;
        retries = retryCount;
        retryWaitedMs += waitMs;
        await sleep(waitMs);
        continue;
      }
      break;
    }
    const attemptRec = {
      backend: attempt.backend,
      upstream: attempt.upstream || provider.upstream || null,
      model: attempt.model,
      startedAt: attemptStarted,
      status: result.err ? (result.err.status || 0) : 200,
      latencyMs: Date.now() - attemptStarted,
      errorBody: result.err ? (result.err.body || null) : null,
      retry: typeof attempt.retry === "number" ? attempt.retry : (attempt.attemptIndex != null ? attempt.attemptIndex : null),
    };
    // Always record the attempt in the deep-dive store; oversized bodies are
    // stored bounded (see boundedPayload) so the panel never silently 404s.
    recordDetail(detailT, callId, captureDetail ? payload : boundedPayload(payload), attemptRec);
    if (result.err) {
      lastErr = result.err;
      markFailure(cfg, attempt.backend, result.err.status, result.err.body);
      // Client-side errors are the same for every backend: fail fast, no retry.
      if (classify(result.err.status, result.err.body) === "client") break;
      continue;
    }
    // Success: mark healthy, relay (ttft = time to first upstream response),
    // then record full metrics once the body has been forwarded to the client.
    markSuccess(attempt.backend);
    const latencyMs = Date.now() - started;
    let metrics;
    try {
      // Stall bound per backend (idleTimeoutMs) with a 120s default; this is
      // silence-based (see relay), never a wall-clock kill. modelId is threaded
      // through so relay-cut log lines are request-scoped.
      const idleTimeoutMs = backend.idleTimeoutMs ?? cfg.idleTimeoutMs ?? 120000;
      metrics = await relay(res, result.res, stream, attempt.backend, started, callId, idleTimeoutMs, modelId);
    } catch (e) {
      // Belt-and-suspenders: relay() already catches internally, but metric
      // collection must never crash the router.
      writeRelayLog({
        level: "relay-threw",
        time: new Date().toISOString(),
        callId: callId || null,
        backend: attempt.backend,
        model: modelId,
        stream: !!stream,
        elapsedMs: Date.now() - started,
        idleTimeoutMs: backend.idleTimeoutMs ?? cfg.idleTimeoutMs ?? 120000,
        idleAborted: false,
        error: String((e && e.message) || e),
      });
      metrics = { ttftMs: latencyMs, cacheHit: false, promptTokens: 0, completionTokens: 0, reasoningTokens: 0, genMs: 0, relayError: String((e && e.message) || e) };
    }
    const genSec = (metrics.genMs || 0) / 1000;
    const entry = applyCost(newHistoryEntry({
      t: detailT, callId, model: modelId, backend: attempt.backend, stream, status: 200, latencyMs,
      ttftMs: metrics.ttftMs ?? latencyMs,
      promptTokens: metrics.promptTokens ?? 0,
      completionTokens: metrics.completionTokens ?? 0,
      reasoningTokens: metrics.reasoningTokens ?? 0,
      cacheHit: metrics.cacheHit,
      cacheHitPct: metrics.cacheHitPct,
      promptCacheHitTokens: metrics.promptCacheHitTokens,
      promptCacheMissTokens: metrics.promptCacheMissTokens,
      genMs: metrics.genMs ?? 0,
      tps: genSec > 0 && (metrics.completionTokens || 0) > 0 ? Math.round((metrics.completionTokens / genSec) * 10) / 10 : 0,
      session: sessionKey,
      routedModel: attempt.model,
      wireParams: extractWireParams(payload),
      retries,
      retryWaitedMs,
      relayError: metrics.relayError || null, // relay cut (idle timeout / upstream drop) for a 200 row
      errorKind: null, // upstream returned a 2xx: no failure to classify
    }), cfg);
    recordHistory(entry);
    recordStats(entry);
    recordAnalytics(entry);
    // Persistent bounded capture of the payload behind a big cache miss, and a
    // push alert when the miss crosses the configured thresholds. Both must
    // never break the successful response path.
    captureMiss(cfg, entry, payload);
    if (typeof metrics.cacheHitPct === "number" && metrics.cacheHitPct < (cfg.alerts && cfg.alerts.ntfy ? (cfg.alerts.ntfy.minMissPct ?? 50) : 50)) {
      const missTokens = Math.max(0, Math.round((metrics.promptTokens || 0) * (1 - metrics.cacheHitPct / 100)));
      const amin = cfg.alerts && cfg.alerts.ntfy ? (cfg.alerts.ntfy.minMissTokens ?? 20000) : 20000;
      if (missTokens >= amin) fireAlert(cfg, "cache_miss", { backend: attempt.backend, cacheHitPct: metrics.cacheHitPct, missTokens, promptTokens: metrics.promptTokens || 0, model: modelId, session: sessionKey });
    }
    if (metrics.relayError) fireAlert(cfg, "relay_cut", { backend: attempt.backend, model: modelId, error: metrics.relayError });
    // Finalize the deep-dive detail record (attempt already recorded above) with
    // the response summary.
    finalizeDetail(detailT, {
      status: 200,
      ttftMs: metrics.ttftMs ?? latencyMs,
      latencyMs,
      genMs: metrics.genMs ?? 0,
      promptTokens: metrics.promptTokens ?? 0,
      completionTokens: metrics.completionTokens ?? 0,
      reasoningTokens: metrics.reasoningTokens ?? 0,
      cacheHit: metrics.cacheHit,
      cacheHitPct: metrics.cacheHitPct,
      preview: metrics.preview || null,
      relayError: metrics.relayError || null,
    });
    return;
  }

  // All attempts failed (or were skipped): record the exhausted failure with a
  // 503. The detail record keyed by detailT holds every attempt's outcome.
  const latencyMs = Date.now() - started;
  const failEntry = applyCost(newHistoryEntry({ t: detailT, callId, model: modelId, backend: lastTried ? lastTried.backend : "none", stream, status: 503, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null, errorKind: lastErr ? classify(lastErr.status, lastErr.body) : null }), cfg);
  recordHistory(failEntry);
  recordAnalytics(failEntry);
  fireAlert(cfg, "request_exhausted", { callId, model: modelId, error: lastErr ? String(lastErr.body || lastErr.status) : "all providers unavailable" });
  recordStats({ backend: lastTried ? lastTried.backend : "none", model: modelId, status: 503, latencyMs });
  // A detail record exists whenever at least one attempt ran (recorded per
  // attempt above); finalize it unconditionally.
  finalizeDetail(detailT, {
    status: 503,
    ttftMs: 0,
    latencyMs,
    genMs: 0,
    promptTokens: 0,
    completionTokens: 0,
    reasoningTokens: 0,
    cacheHit: null,
    cacheHitPct: null,
    preview: null,
    errorBody: lastErr ? lastErr.body : null,
  });
  res.setHeader("x-router-backend", lastTried ? lastTried.backend : "none");
  res.setHeader("x-router-call", callId);
  const msg = lastErr ? lastErr.body : "all backends unavailable";
  return writeError(res, newError(503, typeof msg === "string" && msg ? msg : JSON.stringify({ error: { message: "all backends cooling", type: "router_unavailable" } })));
}

// Cache detection: the router forwards x-cache-key/cachekey/set-cache-key as
// REQUEST headers; when the upstream response echoes any of them back it is
// signaling a cache hit (or a cache write) for this request.
function hasCacheHeader(upstreamRes) {
  const h = upstreamRes.headers;
  return !!(h && h.get && (h.get("x-cache-key") || h.get("cachekey") || h.get("set-cache-key")));
}

// Per-call cache-hit percentage: 0..100 share of prompt tokens served from
// cache, rounded to 1 decimal, or null when there is no measurable cache signal.
// A cache break at message N means everything before N hits and everything from
// N on misses, so a LOW percentage means the break happened EARLY in the prompt
// (0% = cold account / break at the very start; 95% = break near the end). This
// is the number that tells an operator WHERE the break happened, which the
// boolean cacheHit cannot.
function cacheHitPctOf(hit, miss, cached, promptTokens) {
  const hitN = Number(hit);
  const missN = Number(miss);
  const cachedN = Number(cached);
  if (Number.isFinite(hitN) && Number.isFinite(missN)) {
    const total = hitN + missN;
    if (total > 0) return Math.round((hitN / total) * 100 * 10) / 10;
    return null; // both zero = nothing measured
  }
  // Folded shapes: some providers only expose cached_tokens (folded into hit),
  // or hit alone; compare against promptTokens as the denominator.
  if (Number.isFinite(hitN)) {
    if (Number.isFinite(Number(promptTokens)) && Number(promptTokens) > 0) return Math.round((hitN / Number(promptTokens)) * 100 * 10) / 10;
    return null;
  }
  if (Number.isFinite(cachedN)) {
    if (Number.isFinite(Number(promptTokens)) && Number(promptTokens) > 0) return Math.round((cachedN / Number(promptTokens)) * 100 * 10) / 10;
    return null;
  }
  return null;
}

// ---- per-request cost engine --------------------------------------------------
//
// Billing mirrors DeepSeek's published price shape: models declare
// meta.pricing (USD per 1M tokens, optional cache-split rate) and OPT IN to
// off-peak pricing with meta.pricing.offPeak === true. Peak windows are
// weekday UTC hour ranges; everything else (weekends, nights, window gaps) is
// off-peak at the top-level pricing.offPeakMultiplier (default 0.5 = DeepSeek's
// half price).
const DEFAULT_PEAK_WINDOWS = [
  { days: "1-5", start: "01:00", end: "04:00" },
  { days: "1-5", start: "06:00", end: "10:00" },
];

// True when t (epoch ms) is OUTSIDE every peak window. Days are
// Date.getUTCDay() numbers where "1-5" means Mon-Fri; window times are UTC
// "HH:MM", start inclusive, end exclusive. Weekends have no windows at all.
function isOffPeak(t, cfg) {
  const d = new Date(Number(t) || 0);
  const day = d.getUTCDay();
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
  const windows = cfg && cfg.pricing && Array.isArray(cfg.pricing.peakWindows)
    ? cfg.pricing.peakWindows
    : DEFAULT_PEAK_WINDOWS;
  for (const w of windows) {
    if (!w || typeof w.days !== "string" || typeof w.start !== "string" || typeof w.end !== "string") continue;
    const dash = w.days.indexOf("-");
    const dayStart = Number(dash >= 0 ? w.days.slice(0, dash) : w.days);
    const dayEnd = Number(dash >= 0 ? w.days.slice(dash + 1) : w.days);
    if (!Number.isFinite(dayStart) || !Number.isFinite(dayEnd) || day < dayStart || day > dayEnd) continue;
    const [hs, ms] = w.start.split(":").map(Number);
    const [he, me] = w.end.split(":").map(Number);
    const startMins = hs * 60 + ms;
    const endMins = he * 60 + me;
    if (!Number.isFinite(startMins) || !Number.isFinite(endMins)) continue;
    if (mins >= startMins && mins < endMins) return false; // inside a peak window
  }
  return true; // outside every window (or none configured) -> off-peak
}

// Fixed precision for reported USD. Token-count * rate/1e6 products sit well
// below 1e-9 per token, so 10 decimals absorbs binary-fraction noise while
// keeping exact-number assertions stable.
const roundCost = (n) => Math.round(n * 1e10) / 1e10;

// Per-request cost in USD from entry + config. Returns null when the model has
// no meta.pricing (the UI renders "n/a"); otherwise { cost, cacheSavings,
// offPeakSavings, offPeak } with every number >= 0. cost is the billed amount
// at effective (possibly off-peak-scaled) rates; cacheSavings is what cache
// hits saved vs paying full input price, measured at PEAK rates so it does not
// depend on when the request ran; offPeakSavings is the portion of cost the
// off-peak discount removed.
function computeCost(entry, cfg) {
  const modelCfg = (cfg && cfg.models) ? cfg.models[entry.model] : null;
  const pricing = modelCfg && modelCfg.meta ? modelCfg.meta.pricing : null;
  if (!pricing || typeof pricing !== "object") return null;
  const inputPerM = Number(pricing.inputPerM);
  const outputPerM = Number(pricing.outputPerM);
  if (!Number.isFinite(inputPerM) || !Number.isFinite(outputPerM)) return null;
  const cacheHitInputPerM = pricing.cacheHitInputPerM != null ? Number(pricing.cacheHitInputPerM) : inputPerM;
  const promptTokens = Math.max(0, Number(entry.promptTokens) || 0);
  const completionTokens = Math.max(0, Number(entry.completionTokens) || 0);
  let hitTokens = entry.promptCacheHitTokens == null ? 0 : Number(entry.promptCacheHitTokens);
  if (!Number.isFinite(hitTokens) || hitTokens < 0) hitTokens = 0;
  if (hitTokens > promptTokens) hitTokens = promptTokens; // clamp: hits can never exceed the prompt
  let missTokens = entry.promptCacheMissTokens == null ? NaN : Number(entry.promptCacheMissTokens);
  if (!Number.isFinite(missTokens)) missTokens = Math.max(0, promptTokens - hitTokens); // miss not reported -> derive from prompt - hit
  else missTokens = Math.max(0, missTokens);
  const offPeakTime = isOffPeak(entry.t, cfg);
  const offPeak = offPeakTime && pricing.offPeak === true; // discount applies only to opted-in models
  let mult = 1;
  if (offPeak) {
    mult = cfg && cfg.pricing && typeof cfg.pricing.offPeakMultiplier === "number" ? cfg.pricing.offPeakMultiplier : 0.5;
    if (!(mult > 0)) mult = 0.5; // 0/negative multiplier is a config bug; fall back to the standard half price
  }
  const cost = roundCost(missTokens * ((inputPerM * mult) / 1e6) + hitTokens * ((cacheHitInputPerM * mult) / 1e6) + completionTokens * ((outputPerM * mult) / 1e6));
  const cacheSavings = roundCost(hitTokens * ((inputPerM - cacheHitInputPerM) / 1e6)); // peak rates: what the cache hit saved vs full-price miss
  const offPeakSavings = offPeak ? roundCost(cost * (1 / mult - 1)) : 0; // discount saved vs the peak price
  return { cost, cacheSavings, offPeakSavings, offPeak };
}

// Attach the computed cost fields to a history entry in place (all null when
// the model has no pricing). Returns the entry for chaining.
function applyCost(entry, cfg) {
  const c = computeCost(entry, cfg);
  entry.cost = c ? c.cost : null;
  entry.cacheSavings = c ? c.cacheSavings : null;
  entry.offPeakSavings = c ? c.offPeakSavings : null;
  entry.offPeak = c ? c.offPeak : null;
  return entry;
}

// Incremental SSE usage collector. Streaming chat completions normally place
// the cumulative `usage` object on the final chunk before [DONE], so the LAST
// usage object seen wins. Chunk boundaries are handled by keeping a partial
// trailing line between feeds.
function makeUsageCollector() {
  let leftover = "";
  let promptTokens = 0;
  let completionTokens = 0;
  let reasoningTokens = 0;
  let promptCacheHit = 0;
  let promptCacheMiss = 0;
  let promptCacheMissSeen = false; // miss field actually reported (0 default != "reported zero")
  let cacheFieldSeen = false;
  function consume(line) {
    const t = line.trim();
    if (!t.startsWith("data:")) return;
    const payload = t.slice(5).trim();
    if (!payload || payload === "[DONE]") return;
    try {
      const obj = JSON.parse(payload);
      if (obj && obj.usage) {
        promptTokens = Number(obj.usage.prompt_tokens) || 0;
        completionTokens = Number(obj.usage.completion_tokens) || 0;
        // Reasoning token split: reasoning-only completion tokens. Not every
        // upstream reports it; 0 means either "no reasoning" or "not reported".
        const rt = obj.usage.completion_tokens_details && obj.usage.completion_tokens_details.reasoning_tokens;
        reasoningTokens = Number(rt) || 0;
        // DeepSeek/OpenAI-style prompt-cache accounting. Some providers expose
        // prompt_cache_hit_tokens + prompt_cache_miss_tokens; others expose
        // prompt_tokens_details.cached_tokens. Either one means the upstream
        // reports cache state at all (cacheFieldSeen), which lets us tell a
        // real miss from "no signal".
        const hit = Number(obj.usage.prompt_cache_hit_tokens);
        const miss = Number(obj.usage.prompt_cache_miss_tokens);
        const cached = obj.usage.prompt_tokens_details && obj.usage.prompt_tokens_details.cached_tokens;
        const cachedN = Number(cached);
        if (!Number.isNaN(hit) || !Number.isNaN(miss) || !Number.isNaN(cachedN)) cacheFieldSeen = true;
        if (!Number.isNaN(hit)) promptCacheHit = hit;
        else if (!Number.isNaN(cachedN)) promptCacheHit = cachedN;
        if (!Number.isNaN(miss)) {
          promptCacheMiss = miss;
          promptCacheMissSeen = true;
        }
      }
    } catch {
      // Non-JSON SSE event (keepalive comment, etc.) — ignore.
    }
  }
  return {
    feed(chunk) {
      let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
      const lines = text.split("\n");
      leftover = lines.pop() || "";
      for (const line of lines) consume(line);
    },
    finalize() {
      if (leftover) consume(leftover);
      leftover = "";
      return { promptTokens, completionTokens, reasoningTokens, promptCacheHit, promptCacheMiss, promptCacheMissSeen, cacheFieldSeen };
    },
  };
}

// Pass-through transform that feeds every chunk to the usage collector.
// Metrics collection must never break the stream, so failures are swallowed.
function usageTransform(collector) {
  return new Transform({
    transform(chunk, _enc, cb) {
      try {
        collector.feed(chunk);
      } catch {
        /* ignore */
      }
      cb(null, chunk);
    },
  });
}

// Pass-through transform that captures the FIRST content fragment from a
// streaming SSE response (first delta.content or delta.reasoning_content,
// truncated to 200 chars) for the request-detail preview. Never breaks the
// stream; if nothing is captured the preview stays null.
function previewTransform() {
  let leftover = "";
  let captured = false;
  let preview = null;
  const cap = (s) => (typeof s === "string" ? (s.length > 200 ? s.slice(0, 200) + "…" : s) : s);
  const tr = new Transform({
    transform(chunk, _enc, cb) {
      try {
        if (!captured) {
          let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
          const lines = text.split("\n");
          leftover = lines.pop() || "";
          for (const line of lines) {
            const t = line.trim();
            if (t.startsWith("data:")) {
              const payload = t.slice(5).trim();
              if (payload && payload !== "[DONE]") {
                try {
                  const obj = JSON.parse(payload);
                  if (obj && obj.choices && obj.choices[0]) {
                    const delta = obj.choices[0].delta || obj.choices[0].message || {};
                    const frag = delta.content != null ? delta.content : (delta.reasoning_content != null ? delta.reasoning_content : null);
                    if (frag != null) {
                      preview = cap(frag);
                      captured = true;
                      break;
                    }
                  }
                } catch {
                  /* non-JSON data line - pass through unchanged */
                }
              }
            }
          }
        }
        cb(null, chunk);
      } catch (e) {
        // Never break the stream: fall back to passthrough.
        cb(null, chunk);
      }
    },
  });
  tr.getPreview = () => preview;
  return tr;
}

// Reasoning-key normalization. OpenCode's client parser only reads
// delta.reasoning_content (DeepSeek-native) while commandcode/OpenAI-Reasoning
// API style upstreams emit delta.reasoning (+ reasoning_details). Without this,
// reasoning generated through commandcode never renders in OpenCode. The router
// keeps the original key AND exposes the normalized one - the playground reads
// both, so nothing already working changes.
// Works on a parsed chunk (choices[0].delta or .message) in place; returns the
// object for convenience.
function normalizeReasoningKey(obj) {
  if (!obj || typeof obj !== "object") return obj;
  for (const holder of ["delta", "message"]) {
    const slot = obj[holder];
    if (!slot || typeof slot !== "object") continue;
    if (slot.reasoning != null && slot.reasoning_content == null) {
      slot.reasoning_content = slot.reasoning;
      // Merge the full reasoning_details text when present (commandcode sends
      // reasoning as short per-token strings with details carrying the full
      // block; OpenCode renders reasoning_content as one stream).
      if (Array.isArray(slot.reasoning_details)) {
        const detailText = slot.reasoning_details
          .map((d) => (d && typeof d.text === "string" ? d.text : ""))
          .join("");
        if (detailText && slot.reasoning_content !== detailText) {
          slot.reasoning_content = detailText;
        }
      }
    }
  }
  return obj;
}

// SSE transform that normalizes the reasoning key on every data payload while
// passing everything else through byte-for-byte. Line-boundary safe: it buffers
// partial lines between chunks, exactly like the usage collector does.
function reasoningNormalizeTransform() {
  let leftover = "";
  return new Transform({
    transform(chunk, _enc, cb) {
      try {
        let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
        const lines = text.split("\n");
        leftover = lines.pop() || "";
        let out = "";
        for (const line of lines) {
          const t = line.trim();
          if (t.startsWith("data:")) {
            const payload = t.slice(5).trim();
            if (payload && payload !== "[DONE]") {
              try {
                const obj = JSON.parse(payload);
                if (obj && obj.choices && obj.choices[0]) {
                  normalizeReasoningKey(obj.choices[0]);
                  // Re-serialize the normalized object back into the SSE line.
                  out += "data: " + JSON.stringify(obj) + "\n";
                  continue;
                }
              } catch {
                /* non-JSON data line - pass through unchanged */
              }
            }
          }
          out += line + "\n";
        }
        cb(null, out);
      } catch (e) {
        // Never break the stream: fall back to passthrough.
        cb(null, chunk);
      }
    },
    flush(cb) {
      if (leftover) cb(null, leftover);
      else cb();
    },
  });
}

// Timestamped, structured relay/log line. Unlike the old "relay error:"
// console.error (no timestamp, no request id), every entry carries an ISO
// timestamp + callId + backend + model so a relay cut can be traced back to
// the exact request via /api/history?call=<callId> or the JSONL detail rows.
// Emitted as a single line for easy grep.
function writeRelayLog(f) {
  const tags = [
    f.callId ? `call=${f.callId}` : "call=-",
    f.backend ? `backend=${f.backend}` : "backend=-",
    f.model ? `model=${f.model}` : "model=-",
    `stream=${!!f.stream}`,
    `elapsedMs=${f.elapsedMs ?? 0}`,
    f.idleAborted ? "cause=idle-timeout" : "cause=relay-error",
  ];
  console.error(`[${f.time || new Date().toISOString()}] ${f.level || "relay"} ${tags.join(" ")} ${f.error ? ("err=" + String(f.error)) : ""}`);
}

// Idle/stall watchdog for the streaming path: if NO upstream bytes arrive for
// idleTimeoutMs the relay is aborted (genuine stall), but a slow-but-alive
// stream that keeps emitting resets the timer every chunk and is never cut.
// Returns a Transform to insert into the relay pipeline plus the controller
// whose signal bounds the upstream body read (fromWeb honors it).
function makeIdleWatchdog(idleTimeoutMs) {
  const controller = new AbortController();
  let timer = null;
  const arm = () => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => controller.abort(), idleTimeoutMs);
  };
  const clear = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };
  arm();
  const reset = new Transform({
    transform(chunk, enc, cb) {
      arm();
      cb(null, chunk);
    },
    flush(cb) {
      clear();
      cb();
    },
    _destroy(err, cb) {
      clear(); // never leave a post-error timer armed
      cb(err);
    },
  });
  return { controller, reset };
}

// Non-stream body read with the same stall semantics as the streaming
// watchdog: reset on every chunk, abort (via reader.cancel) if idleTimeoutMs
// passes with no data. A slow non-stream response completes; a stuck one is
// cut. Returns a Buffer (the caller treats it as the response body bytes).
async function readBodyWithIdle(webBody, idleTimeoutMs) {
  if (!webBody) return Buffer.from(await webBody.arrayBuffer()); // throws like Response.arrayBuffer() on null body
  const reader = webBody.getReader();
  const chunks = [];
  let timer = null;
  const arm = () => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => reader.cancel("relay idle timeout"), idleTimeoutMs);
  };
  arm();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(Buffer.from(value));
      arm(); // any progress re-arms the stall bound
    }
    return Buffer.concat(chunks);
  } finally {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  }
}

// Relay the upstream response to the client while capturing per-request
// metrics and normalizing the reasoning key (delta.reasoning ->
// delta.reasoning_content) for OpenCode compatibility. Returns
// { ttftMs, cacheHit, promptTokens, completionTokens, genMs, preview } where
// preview is the first content fragment (stream: first content delta;
// non-stream: first choice's message content), truncated to 200 chars, plus
// relayError (string|null) when the upstream body was cut mid-relay so the
// history row can expose truncations that otherwise look like HTTP 200.
// Never throws: on any failure it closes the response and returns what it has.
// idleTimeoutMs bounds SILENCE (stall detection), never total wall time, so a
// long-but-alive stream survives; the old wall-clock kill is gone (see forward).
async function relay(res, upstreamRes, stream, backendId, started, callId, idleTimeoutMs = 120000, modelId = null) {
  const ttftMs = Date.now() - started;
  const headers = {
    "content-type": upstreamRes.headers.get("content-type") || "application/json",
    "x-router-backend": backendId || "unknown",
  };
  if (callId) headers["x-router-call"] = callId;
  let cacheHit = null;   // tri-state: true / false / null (unknown - no signal)
  let cacheHitPct = null; // 0..100 share of prompt tokens from cache; null = no signal
  let promptTokens = 0;
  let completionTokens = 0;
  let reasoningTokens = 0;
  let genMs = 0;
  let promptCacheHitTokens = 0;  // raw cache-hit prompt tokens (0 = not reported)
  let promptCacheMissTokens = null; // raw miss tokens; null = not reported -> history derives from prompt - hit
  let preview = null;    // first content fragment, truncated to 200 chars
  let relayError = null; // set when the upstream body was cut mid-relay
  const cap = (s) => (typeof s === "string" ? (s.length > 200 ? s.slice(0, 200) + "…" : s) : s);
  try {
    if (upstreamRes.headers.get("x-request-id")) headers["x-request-id"] = upstreamRes.headers.get("x-request-id");
    res.writeHead(200, headers);
    if (hasCacheHeader(upstreamRes)) cacheHit = true;

    if (stream && upstreamRes.body) {
      // Tee the stream: one branch goes to the client, the transform branch
      // accumulates usage data from the final SSE chunk.
      const collector = makeUsageCollector();
      const pv = previewTransform();
      const genStart = Date.now();
      // Idle/stall bound: aborts the upstream body read only when NO bytes
      // arrive for idleTimeoutMs. Alive streams (any chunk within the bound)
      // run to completion regardless of total duration.
      const idle = makeIdleWatchdog(idleTimeoutMs);
      try {
        await pipeline(
          Readable.fromWeb(upstreamRes.body, { signal: idle.controller.signal }),
          idle.reset,
          usageTransform(collector),
          pv,
          reasoningNormalizeTransform(),
          res
        );
      } catch (e) {
        // Distinguish a stall-watchdog cut from an upstream/network drop so
        // the history row + timestamped log attribute the right cause.
        relayError = idle.controller.signal.aborted
          ? `upstream idle timeout (no data for ${idleTimeoutMs}ms)`
          : String(e && e.message);
        throw e; // let the outer catch close the response + log once
      }
      genMs = Date.now() - genStart;
      const usage = collector.finalize();
      promptTokens = usage.promptTokens;
      completionTokens = usage.completionTokens;
      reasoningTokens = usage.reasoningTokens;
      preview = pv.getPreview() || null;
      if (cacheHit !== true) {
        cacheHit = usage.cacheFieldSeen ? usage.promptCacheHit > 0 : null;
      }
      // cached-only upstreams fold their tokens into promptCacheHit while the
      // miss field stays unset; passing the 0 default would make the ratio read
      // 100%, so pass NaN unless the miss was actually reported.
      cacheHitPct = usage.cacheFieldSeen
        ? cacheHitPctOf(usage.promptCacheHit, usage.promptCacheMissSeen ? usage.promptCacheMiss : NaN, NaN, usage.promptTokens)
        : null;
      promptCacheHitTokens = usage.promptCacheHit; // raw cache-hit prompt tokens (0 when the upstream does not report a split)
      promptCacheMissTokens = usage.promptCacheMissSeen ? usage.promptCacheMiss : null; // null = miss not reported -> history derives it
    } else {
      try {
        let buf = await readBodyWithIdle(upstreamRes.body, idleTimeoutMs);
        try {
          const parsed = JSON.parse(buf.toString("utf8"));
          const usage = parsed && parsed.usage;
          if (usage) {
            promptTokens = Number(usage.prompt_tokens) || 0;
            completionTokens = Number(usage.completion_tokens) || 0;
            const rt = usage.completion_tokens_details && usage.completion_tokens_details.reasoning_tokens;
            reasoningTokens = Number(rt) || 0;
            if (cacheHit !== true) {
              const hit = Number(usage.prompt_cache_hit_tokens);
              const miss = Number(usage.prompt_cache_miss_tokens);
              const cached = usage.prompt_tokens_details && usage.prompt_tokens_details.cached_tokens;
              const cachedN = Number(cached);
              const seen = !Number.isNaN(hit) || !Number.isNaN(miss) || !Number.isNaN(cachedN);
              if (seen) cacheHit = (!Number.isNaN(hit) ? hit : cachedN) > 0;
              // else keep null (no signal)
              cacheHitPct = cacheHitPctOf(hit, miss, cachedN, promptTokens);
              if (!Number.isNaN(hit)) promptCacheHitTokens = hit;
              else if (!Number.isNaN(cachedN) && cachedN > 0) promptCacheHitTokens = cachedN;
              if (!Number.isNaN(miss)) promptCacheMissTokens = miss;
            }
          }
          // Non-stream: normalize the reasoning key on the first choice message.
          if (parsed && Array.isArray(parsed.choices) && parsed.choices[0]) {
            normalizeReasoningKey(parsed.choices[0]);
            buf = Buffer.from(JSON.stringify(parsed));
            const firstMsg = parsed.choices[0].message || parsed.choices[0].delta || {};
            preview = cap(firstMsg.content != null ? firstMsg.content : (firstMsg.text != null ? firstMsg.text : null));
          }
        } catch {
          // Body not JSON (e.g. upstream proxy error) — keep token counts at 0.
        }
        res.end(buf);
        genMs = Math.max(0, Date.now() - started - ttftMs);
      } catch (e) {
        // Non-stream body stall/cut (readBodyWithIdle cancels on idle) or
        // body read error: attribute the cause and let the outer catch log.
        relayError = /idle timeout/.test(String(e && e.message))
          ? `upstream idle timeout (no data for ${idleTimeoutMs}ms)`
          : String(e && e.message);
        throw e;
      }
    }
  } catch (e) {
    // Relay must never crash the router: record what we have, log a
    // TIMESTAMPED, request-scoped line so relay cuts are traceable to a call
    // (callId cross-references /api/history and the JSONL detail rows), then
    // close cleanly. elapsedMs = wall time from request start; idleAborted =
    // the stall watchdog fired vs an upstream/network drop.
    const idleAborted = relayError && /idle timeout/.test(relayError);
    writeRelayLog({
      level: "relay-error",
      time: new Date().toISOString(),
      callId: callId || null,
      backend: backendId || null,
      model: modelId,
      stream: !!stream,
      elapsedMs: Date.now() - started,
      idleTimeoutMs,
      idleAborted,
      error: relayError || String((e && e.message) || e),
    });
    try {
      if (!res.writableEnded) res.end();
    } catch {
      /* ignore */
    }
  }
  return { ttftMs, cacheHit, cacheHitPct, promptCacheHitTokens, promptCacheMissTokens, promptTokens, completionTokens, reasoningTokens, genMs, preview, relayError };
}

function writeError(res, e) {
  const status = e.status || 500;
  let buf;
  try {
    buf = Buffer.from(e.body || "");
  } catch {
    buf = Buffer.from(JSON.stringify({ error: { message: "router error", type: "router_error" } }));
  }
  // If upstream returned an error body that is JSON-shaped, pass it through;
  // the status reflects it.
  res.writeHead(status, { "content-type": "application/json" });
  res.end(buf);
}

// ---- inbound bearer auth ---------------------------------------------------

// Constant-time string compare over a fixed-length digest so that
// crypto.timingSafeEqual's equal-length buffers requirement holds without
// revealing the raw secret length to a wire-side observer.
function safeEqual(a, b) {
  const ha = crypto.createHash("sha256").update(String(a)).digest();
  const hb = crypto.createHash("sha256").update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function hashSecret(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

// ---- issued API keys -------------------------------------------------------
// Chat-only keys, persisted as SHA-256 hashes in a gitignored api-keys.json
// (LMR_KEYS_FILE overrides the path). The raw key is shown exactly once at
// creation; everything stored or served later is hash-only.
const ISSUED_KEYS_PATH = process.env.LMR_KEYS_FILE || join(__dirname, "api-keys.json");
const ISSUED_KEYS_MAX_NAME = 64;
let issuedKeys = null; // { mtimeMs, keys: [...] }; null = not loaded yet
let issuedKeysWriteChain = Promise.resolve(); // serializes atomic writes

function loadIssuedKeys() {
  // mtime-aware cache: the keys file may be edited by the UI, so a fresh stat
  // decides whether the in-memory copy is still current (cheap: one stat).
  try {
    const st = statSync(ISSUED_KEYS_PATH);
    if (issuedKeys && issuedKeys.mtimeMs === st.mtimeMs) return issuedKeys;
    const raw = readFileSync(ISSUED_KEYS_PATH, "utf8");
    const parsed = JSON.parse(raw);
    issuedKeys = { mtimeMs: st.mtimeMs, keys: Array.isArray(parsed.keys) ? parsed.keys : [] };
  } catch {
    issuedKeys = { mtimeMs: 0, keys: [] };
  }
  return issuedKeys;
}

function persistIssuedKeys(keys) {
  // Chain writes so two rapid mutations (issue then revoke) cannot interleave
  // temp-file renames; write-then-rename keeps the file atomic for readers.
  issuedKeysWriteChain = issuedKeysWriteChain.then(async () => {
    const tmp = ISSUED_KEYS_PATH + ".tmp";
    await writeFileP(tmp, JSON.stringify({ keys }, null, 2) + "\n", "utf8");
    await renameP(tmp, ISSUED_KEYS_PATH);
  });
  return issuedKeysWriteChain;
}

function issuedKeyValid(token) {
  if (!token) return null;
  const { keys } = loadIssuedKeys();
  const want = hashSecret(token);
  for (const k of keys) {
    if (k.revoked) continue;
    if (safeEqual(k.hash, want)) return k;
  }
  return null;
}

async function issueKey(name) {
  const raw = "sk-lmr-" + crypto.randomBytes(24).toString("base64url");
  const rec = { id: crypto.randomBytes(6).toString("hex"), name, hash: hashSecret(raw), createdAt: Date.now(), revoked: false };
  const { keys } = loadIssuedKeys();
  keys.push(rec);
  await persistIssuedKeys(keys);
  return { id: rec.id, name: rec.name, raw, createdAt: rec.createdAt };
}

async function revokeKey(id) {
  const { keys } = loadIssuedKeys();
  const rec = keys.find((k) => k.id === id);
  if (!rec) return false;
  rec.revoked = true;
  await persistIssuedKeys(keys);
  return true;
}

// ---- UI session (dashboard login) ------------------------------------------
// In-memory session tokens; logout removes them, restart invalidates all. The
// cookie is HttpOnly + SameSite=Strict, no Secure so plain-http localhost/LAN
// works. Login throttled per client IP: 5 attempts per 10s window.
const UI_LOGIN_MAX_ATTEMPTS = 5;
const UI_LOGIN_WINDOW_MS = 10000;
const uiSessions = new Set();
const loginAttempts = new Map(); // ip -> [timestamps of failed attempts]
const UI_SESSION_COOKIE = "lmr_ui";

function uiPasswordValue(cfg) {
  const name = cfg.uiPasswordEnv;
  if (!name) return null; // unset -> UI open
  return process.env[name] || ""; // set but empty -> fail closed
}

function uiAuthed(cfg, req) {
  if (!cfg.uiPasswordEnv) return true;
  const m = /(?:^|;\s*)lmr_ui=([^;\s]+)/.exec(req.headers.cookie || "");
  return !!(m && uiSessions.has(m[1]));
}

function loginRateLimited(ip) {
  const now = Date.now();
  const hits = (loginAttempts.get(ip) || []).filter((t) => now - t < UI_LOGIN_WINDOW_MS);
  loginAttempts.set(ip, hits);
  return hits.length >= UI_LOGIN_MAX_ATTEMPTS;
}

function recordLoginAttempt(ip) {
  const now = Date.now();
  const hits = (loginAttempts.get(ip) || []).filter((t) => now - t < UI_LOGIN_WINDOW_MS);
  hits.push(now);
  loginAttempts.set(ip, hits);
}

// ---- inbound gate -----------------------------------------------------------
// Gated paths: `/v1/chat/completions`, `/admin/*`, `/api/keys`. `masterKeyEnv`
// names an env var whose value the client must echo as `Authorization: Bearer
// <v>` (scheme matched case-insensitively); chat additionally accepts valid
// issued keys. Both config keys are read from the LIVE cfg object at request
// time, exactly like cfg.backoff / cfg.prefix, so a config hot-reload toggles
// the gate without a restart. Unset masterKeyEnv keeps today's open posture
// for every endpoint. A set name whose env value is empty at request time
// FAILS CLOSED: the gated endpoints reject every request rather than silently
// treating a missing expected key as "no auth". Read-only endpoints (health,
// models, stats, history, config, UI) never pass through here, so they stay
// open. `/api/auth/*` are the login itself and are deliberately NOT gated.
function requireAuth(cfg, req) {
  const name = cfg.masterKeyEnv;
  if (!name) return null;
  const url = req.url.split("?")[0];
  const p = (cfg.prefix || "/v1").replace(/\/$/, "");
  const isChat = url === p + "/chat/completions";
  if (!isChat && !url.startsWith("/admin") && !url.startsWith("/api/keys")) return null;
  const expected = process.env[name];
  const unauthorized = newError(401, JSON.stringify({ error: { message: "authentication required", type: "authentication_error" } }));
  if (!expected) {
    console.error(`[${new Date().toISOString()}] masterKeyEnv '${name}' set but process.env['${name}'] is empty; rejecting request (fail closed)`);
    return unauthorized;
  }
  const m = /^Bearer\s+(.+)$/i.exec(req.headers["authorization"] || "");
  if (m && safeEqual(m[1].trim(), expected)) return null;
  // Chat only: a non-revoked issued key also unlocks /v1/chat/completions.
  // Admin and /api/keys master-only, EXCEPT a logged-in dashboard session
  // (the lmr_ui cookie, set by POST /api/auth/login) counts as the admin
  // credential too -- the password IS the admin identity. Only when a
  // dashboard password is actually configured (uiPasswordEnv set AND its env
  // value non-empty); otherwise a session cannot exist and the master gate
  // stays authoritative. Machines keep using the master key; humans use the
  // password they logged in with.
  if (isChat && m && issuedKeyValid(m[1].trim())) return null;
  if (!isChat && cfg.uiPasswordEnv && process.env[cfg.uiPasswordEnv] && uiAuthed(cfg, req)) return null;
  return unauthorized;
}

// ---- server ---------------------------------------------------------------

let cfg = null;

async function loadConfig() {
  const raw = await readFileP(CONFIG_PATH, "utf8");
  const parsed = JSON.parse(raw);
  // Normalize the three-layer shape (backends -> models -> presets) on every
  // load: validate new-shape entries, synthesize legacy configs, derive preset
  // members. Routing and /api/config only ever see the normalized form.
  normalizeConfig(parsed);
  // Reconcile: keep health registry keyed by backend id on reload.
  const prevIds = new Set(cfg ? cfg.backends.map((b) => b.id) : []);
  cfg = parsed;
  if (cfg.masterKeyEnv && !process.env[cfg.masterKeyEnv]) {
    console.error(
      `[${new Date().toISOString()}] config warning: masterKeyEnv '${cfg.masterKeyEnv}' set but process.env['${cfg.masterKeyEnv}'] is empty; auth-gated endpoints will reject all requests (fail closed)`
    );
  }
  if (cfg.uiPasswordEnv && !process.env[cfg.uiPasswordEnv]) {
    console.error(
      `[${new Date().toISOString()}] config warning: uiPasswordEnv '${cfg.uiPasswordEnv}' set but process.env['${cfg.uiPasswordEnv}'] is empty; dashboard login will reject all passwords (fail closed)`
    );
  }
  ensureBackends(cfg);
  return cfg;
}

function server() {
  return http.createServer(async (req, res) => {
    const url = req.url.split("?")[0];
    const prefix = cfg.prefix || "/v1";
    const p = prefix.replace(/\/$/, "");

    const authErr = requireAuth(cfg, req);
    if (authErr) return writeError(res, authErr);

    if (req.method === "GET" && url === "/health") {
      const now = Date.now();
      const backends = cfg.backends.map((b) => {
        const h = health.get(b.id) || {};
        const isCooling = h.state === "cooling" && h.nextAvailableAt > now;
        return {
          id: b.id,
          state: isCooling ? "cooling" : "healthy",
          nextAvailableAtMs: h.nextAvailableAt || 0,
          fails: h.fails || 0,
          lastError: h.lastError || null,
          manual: !!(isCooling && h.lastError && h.lastError.kind === "manual"),
        };
      });
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, port: cfg.port, backends }));
    }

    if (req.method === "POST" && url === "/admin/reset-health") {
      resetAllHealth();
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (req.method === "GET" && url === p + "/models") {
      // Preset ids first (config order), then model ids not already listed
      // (config order). Colliding ids appear exactly once (the preset owns it).
      const ids = [];
      for (const id of Object.keys(cfg.presets || {})) {
        if (!ids.includes(id)) ids.push(id);
      }
      for (const id of Object.keys(cfg.models || {})) {
        if (!ids.includes(id)) ids.push(id);
      }
      const data = ids.map((id) => ({ id, object: "model", owned_by: "local-router" }));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ object: "list", data }));
    }

    if (req.method === "POST" && url === p + "/chat/completions") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      return handleChat(req, res, cfg, bodyText);
    }

    if (req.method === "GET" && url === "/") {
      let html;
      try {
        html = readFileSync(join(__dirname, "index.html"), "utf8");
      } catch {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "ui not found", type: "not_found" } }));
      }
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
      return res.end(html);
    }

    if (req.method === "GET" && url === "/api/history") {
      const raw = new URL(req.url, "http://localhost").searchParams.get("limit");
      let limit = 100;
      if (raw) {
        const n = parseInt(raw, 10);
        if (!Number.isNaN(n)) limit = Math.max(1, Math.min(500, n));
      }
      const entries = [...HISTORY].reverse().slice(0, limit);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ entries, limit }));
    }

    if (req.method === "GET" && url === "/api/history/detail") {
      const params = new URL(req.url, "http://localhost").searchParams;
      // Look up by call id (preferred, from the "Call" column) or legacy t.
      const callId = params.get("call");
      const rawT = params.get("t");
      const t = rawT != null && rawT !== "" ? Number(rawT) : NaN;
      const detail = (callId ? getDetailByCall(callId) : null) || (!Number.isNaN(t) ? getDetail(t) : null);
      if (!detail) {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: "not found" }));
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ detail }));
    }

    if (req.method === "GET" && url === "/api/stats") {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({ since: STATS.since, uptimeMs: Date.now() - startedAt, total: STATS.total, byBackend: STATS.byBackend, byModel: STATS.byModel })
      );
    }

    if (req.method === "GET" && url === "/api/dashboard") {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify(dashboardPayload(cfg)));
    }

    if (req.method === "GET" && url === "/api/config") {
      // Safe: cfg.backends carry apiKeyEnv NAMES, never resolved key values.
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify(cfg));
    }

    if (req.method === "POST" && url === "/admin/backend") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "invalid JSON body", type: "invalid_request_error" } }));
      }
      const id = body && body.id;
      const action = body && body.action;
      if (!id || !cfg.backends.some((b) => b.id === id)) {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: `backend '${id}' not found`, type: "not_found" } }));
      }
      if (action !== "cool" && action !== "uncool") {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "action must be 'cool' or 'uncool'", type: "invalid_request_error" } }));
      }
      if (action === "cool") {
        const h = health.get(id) || { state: "healthy", fails: 0, backoffMs: 0 };
        // No forMs = STICKY manual cool: cooled until uncooled. MAX_SAFE_INTEGER
        // is set exactly so /health can render "until uncooled" instead of a
        // countdown. A positive forMs = timed manual cool.
        const forMs = typeof body.forMs === "number" && body.forMs > 0 ? body.forMs : null;
        h.state = "cooling";
        h.nextAvailableAt = forMs ? Date.now() + forMs : Number.MAX_SAFE_INTEGER;
        h.fails = h.fails || 0;
        h.backoffMs = h.backoffMs || 0;
        h.lastError = { status: 0, kind: "manual", body: "manually cooled via UI" };
        health.set(id, h);
      } else {
        markHealthy(id, "uncool");
      }
      const h = health.get(id);
      const state = h.state === "cooling" && h.nextAvailableAt > Date.now() ? "cooling" : "healthy";
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, id, state }));
    }

    // ---- dashboard login (never gated: these ARE the login) ----------------

    if (req.method === "GET" && url === "/api/auth/status") {
      // passwordSet reflects the config key (set = login required), NOT the
      // env value, so a password left empty still reports "required" and the
      // login endpoint fails closed. Never echoes the password.
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ passwordSet: !!cfg.uiPasswordEnv, uiAuthed: uiAuthed(cfg, req) }));
    }

    if (req.method === "POST" && url === "/api/auth/login") {
      if (!cfg.uiPasswordEnv) {
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true, uiAuthed: true }));
      }
      const ip = req.socket.remoteAddress || "unknown";
      if (loginRateLimited(ip)) {
        res.writeHead(429, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "too many login attempts, try again shortly", type: "rate_limit_error" } }));
      }
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        body = null;
      }
      const expected = uiPasswordValue(cfg);
      if (!expected) {
        // Fail closed: password configured but env value empty at request time.
        res.writeHead(401, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false }));
      }
      const given = body && typeof body.password === "string" ? body.password : "";
      if (!safeEqual(hashSecret(given), hashSecret(expected))) {
        recordLoginAttempt(ip);
        res.writeHead(401, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false }));
      }
      const token = crypto.randomBytes(24).toString("base64url");
      uiSessions.add(token);
      res.writeHead(200, {
        "content-type": "application/json",
        "set-cookie": `${UI_SESSION_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Strict`,
      });
      return res.end(JSON.stringify({ ok: true, uiAuthed: true }));
    }

    if (req.method === "POST" && url === "/api/auth/logout") {
      const m = /(?:^|;\s*)lmr_ui=([^;\s]+)/.exec(req.headers.cookie || "");
      if (m) uiSessions.delete(m[1]);
      res.writeHead(200, {
        "content-type": "application/json",
        "set-cookie": `${UI_SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`,
      });
      return res.end(JSON.stringify({ ok: true }));
    }

    // ---- issued-keys management (master-key gated by requireAuth) ----------

    if (req.method === "GET" && url === "/api/keys") {
      // Never expose hashes or raw values, only the listing metadata.
      const { keys } = loadIssuedKeys();
      const list = keys.map((k) => ({ id: k.id, name: k.name, createdAt: k.createdAt, revoked: !!k.revoked }));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ keys: list }));
    }

    if (req.method === "POST" && url === "/api/keys") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        body = null;
      }
      const name = body && typeof body.name === "string" ? body.name.trim() : "";
      if (!name || name.length > ISSUED_KEYS_MAX_NAME) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: `name must be a non-empty string of at most ${ISSUED_KEYS_MAX_NAME} chars`, type: "invalid_request_error" } }));
      }
      try {
        const key = await issueKey(name);
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ key }));
      } catch (e) {
        console.error(`[${new Date().toISOString()}] issueKey failed: ${e.message}`);
        return writeError(res, newError(500, JSON.stringify({ error: { message: "failed to persist keys", type: "router_error" } })));
      }
    }

    if (req.method === "DELETE" && url === "/api/keys") {
      const id = new URL(req.url, "http://localhost").searchParams.get("id") || "";
      try {
        const ok = await revokeKey(id);
        if (!ok) {
          res.writeHead(404, { "content-type": "application/json" });
          return res.end(JSON.stringify({ error: { message: `key '${id}' not found`, type: "not_found" } }));
        }
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        console.error(`[${new Date().toISOString()}] revokeKey failed: ${e.message}`);
        return writeError(res, newError(500, JSON.stringify({ error: { message: "failed to persist keys", type: "router_error" } })));
      }
    }

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `not found: ${req.method} ${url}`, type: "not_found" } }));
  });
}

// ---- startup ---------------------------------------------------------------

async function main() {
  loadEnv();
  await loadConfig();
  seedAnalytics(historyPath);
  watchF(CONFIG_PATH, () => {
    loadConfig().catch((e) => console.error("config reload failed:", e.message));
  });

  // Always listen on localhost (existing consumers: OpenCode, dsh, local UI).
  // When cfg.host is set (e.g. a Tailscale IP to expose the router on the
  // tailnet), listen on that address too. Two server instances share the same
  // request handler. A wildcard host (0.0.0.0 / ::) already covers loopback, so
  // bind ONLY the wildcard: binding both on one port is EADDRINUSE on Linux.
  const hosts = new Set(["127.0.0.1"]);
  if (cfg.host) hosts.add(String(cfg.host));
  if (cfg.host === "0.0.0.0" || cfg.host === "::") hosts.delete("127.0.0.1");
  for (const bindHost of hosts) {
    const srv = server();
    srv.listen(cfg.port, bindHost, () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : cfg.port;
      console.error(`[${new Date().toISOString()}] switchblade listening on http://${bindHost}:${port}${cfg.prefix || "/v1"}`);
    });
  }
  console.error(`[${new Date().toISOString()}] config: ${CONFIG_PATH}`);
  console.error(`[${new Date().toISOString()}] backends: ${cfg.backends.map((b) => `${b.id}(${mask(process.env[b.apiKeyEnv])})`).join(", ")}`);
}

// Running the file as a script starts the server; importing it (test.mjs
// imports candidates/normalizeConfig/buildPayload for unit tests) does not.
const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  main().catch((e) => {
    console.error("fatal:", e);
    process.exit(1);
  });
}

export { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost };
