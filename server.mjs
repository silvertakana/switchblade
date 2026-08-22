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
import { readFileSync, watch as watchF, appendFileSync } from "node:fs";
import { readFile as readFileP } from "node:fs/promises";
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

function markHealthy(id) {
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
  if (status === 400 || status === 422) return "client"; // our payload bug, not backend health
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
  let nextMs;
  if (kind === "weekly") {
    // Parse "Resets in N days" if present, else full week.
    const m = /resets in (\d+)\s+day/i.exec(bodyText || "");
    const days = m ? parseInt(m[1], 10) : 7;
    nextMs = cfg.backoff.weeklyDefaultMs || days * 24 * 60 * 60 * 1000;
    h.fails = 100; // effectively pinned until reset
  } else {
    h.backoffMs = backoffFor(kind, cfg, h.fails);
    nextMs = h.backoffMs;
  }
  h.state = "cooling";
  h.nextAvailableAt = Date.now() + nextMs;
  h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
  health.set(id, h);
}

function markSuccess(id) {
  const h = health.get(id);
  // A manual cool is a user decision: a success must NEVER clear it. Only the
  // Uncool button (or expiry of a timed manual cool) restores the backend.
  if (h && h.state === "cooling" && h.lastError && h.lastError.kind === "manual") return;
  markHealthy(id);
}

function resetAllHealth() {
  for (const id of health.keys()) markHealthy(id);
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
    genMs: f.genMs ?? 0,                         // generation time: stream duration, or total - ttft for non-stream
    tps: f.tps ?? 0,                             // completion tokens per second (derived)
    session: f.session,
    routedModel: f.routedModel ?? null,          // resolved model id (success: serving attempt; failure: first candidate; null: none)
    wireParams: f.wireParams ?? null,            // redacted summary of the params actually sent upstream (for explaining reasoning on/off per row)
    retries: f.retries ?? 0,                     // number of retry attempts performed on the serving provider (0 = none)
    retryWaitedMs: f.retryWaitedMs ?? 0,         // total ms spent waiting in backoff across retries
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
    promptTokensSum: 0, completionTokensSum: 0, cacheHits: 0, ttftSumMs: 0,
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
  STATS.byBackend[entry.backend] = b;
  STATS.byModel[entry.model] = (STATS.byModel[entry.model] || 0) + 1;
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
      console.error(`[wire] ${backend.id} -> ${dbg.model} params=${JSON.stringify(Object.keys(dbg).filter((k) => !["messages", "model", "stream"].includes(k)))} ${JSON.stringify(Object.fromEntries(Object.entries(dbg).filter(([k]) => !["messages", "model", "stream"].includes(k))))}`);
    } catch {}
  }

  let upstreamRes;
  try {
    upstreamRes = await fetch(upstream, {
      method: "POST",
      headers: outHeaders,
      body: payloadText,
      signal: AbortSignal.timeout(backend.timeoutMs || cfg.timeoutMs || 45000),
    });
  } catch (e) {
    return { err: newError(0, String(e && e.message)) };
  }

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
  const healthy = c.ordered.filter((a) => {
    const h = health.get(a.backend);
    return !h || h.state !== "cooling" || h.nextAvailableAt <= now;
  });
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
      const entry = newHistoryEntry({ t: detailT, callId, model: modelId, backend: "none", stream, status: 503, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null });
      recordHistory(entry);
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
      metrics = await relay(res, result.res, stream, attempt.backend, started, callId);
    } catch (e) {
      // Belt-and-suspenders: relay() already catches internally, but metric
      // collection must never crash the router.
      console.error("relay threw:", e && e.message);
      metrics = { ttftMs: latencyMs, cacheHit: false, promptTokens: 0, completionTokens: 0, reasoningTokens: 0, genMs: 0 };
    }
    const genSec = (metrics.genMs || 0) / 1000;
    const entry = newHistoryEntry({
      t: detailT, callId, model: modelId, backend: attempt.backend, stream, status: 200, latencyMs,
      ttftMs: metrics.ttftMs ?? latencyMs,
      promptTokens: metrics.promptTokens ?? 0,
      completionTokens: metrics.completionTokens ?? 0,
      reasoningTokens: metrics.reasoningTokens ?? 0,
      cacheHit: metrics.cacheHit,
      genMs: metrics.genMs ?? 0,
      tps: genSec > 0 && (metrics.completionTokens || 0) > 0 ? Math.round((metrics.completionTokens / genSec) * 10) / 10 : 0,
      session: sessionKey,
      routedModel: attempt.model,
      wireParams: extractWireParams(payload),
      retries,
      retryWaitedMs,
    });
    recordHistory(entry);
    recordStats(entry);
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
      preview: metrics.preview || null,
    });
    return;
  }

  // All attempts failed (or were skipped): record the exhausted failure with a
  // 503. The detail record keyed by detailT holds every attempt's outcome.
  const latencyMs = Date.now() - started;
  const failEntry = newHistoryEntry({ t: detailT, callId, model: modelId, backend: lastTried ? lastTried.backend : "none", stream, status: 503, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null });
  recordHistory(failEntry);
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
        if (!Number.isNaN(miss)) promptCacheMiss = miss;
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
      return { promptTokens, completionTokens, reasoningTokens, promptCacheHit, promptCacheMiss, cacheFieldSeen };
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

// Relay the upstream response to the client while capturing per-request
// metrics and normalizing the reasoning key (delta.reasoning ->
// delta.reasoning_content) for OpenCode compatibility. Returns
// { ttftMs, cacheHit, promptTokens, completionTokens, genMs, preview } where
// preview is the first content fragment (stream: first content delta;
// non-stream: first choice's message content), truncated to 200 chars.
// Never throws: on any failure it closes the response and returns what it has.
async function relay(res, upstreamRes, stream, backendId, started, callId) {
  const ttftMs = Date.now() - started;
  const headers = {
    "content-type": upstreamRes.headers.get("content-type") || "application/json",
    "x-router-backend": backendId || "unknown",
  };
  if (callId) headers["x-router-call"] = callId;
  let cacheHit = null;   // tri-state: true / false / null (unknown - no signal)
  let promptTokens = 0;
  let completionTokens = 0;
  let reasoningTokens = 0;
  let genMs = 0;
  let preview = null;    // first content fragment, truncated to 200 chars
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
      await pipeline(
        Readable.fromWeb(upstreamRes.body),
        usageTransform(collector),
        pv,
        reasoningNormalizeTransform(),
        res
      );
      genMs = Date.now() - genStart;
      const usage = collector.finalize();
      promptTokens = usage.promptTokens;
      completionTokens = usage.completionTokens;
      reasoningTokens = usage.reasoningTokens;
      preview = pv.getPreview() || null;
      if (cacheHit !== true) {
        cacheHit = usage.cacheFieldSeen ? usage.promptCacheHit > 0 : null;
      }
    } else {
      let buf = Buffer.from(await upstreamRes.arrayBuffer());
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
    }
  } catch (e) {
    // Relay must never crash the router: record what we have and close cleanly.
    console.error("relay error:", e && e.message);
    try {
      if (!res.writableEnded) res.end();
    } catch {
      /* ignore */
    }
  }
  return { ttftMs, cacheHit, promptTokens, completionTokens, reasoningTokens, genMs, preview };
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
  ensureBackends(cfg);
  return cfg;
}

function server() {
  return http.createServer(async (req, res) => {
    const url = req.url.split("?")[0];
    const prefix = cfg.prefix || "/v1";
    const p = prefix.replace(/\/$/, "");

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
        markHealthy(id);
      }
      const h = health.get(id);
      const state = h.state === "cooling" && h.nextAvailableAt > Date.now() ? "cooling" : "healthy";
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, id, state }));
    }

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `not found: ${req.method} ${url}`, type: "not_found" } }));
  });
}

// ---- startup ---------------------------------------------------------------

async function main() {
  loadEnv();
  await loadConfig();
  watchF(CONFIG_PATH, () => {
    loadConfig().catch((e) => console.error("config reload failed:", e.message));
  });

  // Always listen on localhost (existing consumers: OpenCode, dsh, local UI).
  // When cfg.host is set (e.g. a Tailscale IP to expose the router on the
  // tailnet), listen on that address too. Two server instances share the same
  // request handler.
  const hosts = new Set(["127.0.0.1"]);
  if (cfg.host) hosts.add(String(cfg.host));
  for (const bindHost of hosts) {
    const srv = server();
    srv.listen(cfg.port, bindHost, () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : cfg.port;
      console.error(`switchblade listening on http://${bindHost}:${port}${cfg.prefix || "/v1"}`);
    });
  }
  console.error(`config: ${CONFIG_PATH}`);
  console.error(`backends: ${cfg.backends.map((b) => `${b.id}(${mask(process.env[b.apiKeyEnv])})`).join(", ")}`);
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

export { candidates, normalizeConfig, buildPayload };
