# DESIGN-PRESETS.md

Design contract for the presets routing layer and the sticky manual-cool fix.
This document is the single source of truth for implementation and verification.
Implementation is split into two phases (server, then UI); a verifier checks the
result against section 9.

---

## 1. Overview

Today the router has two layers: raw accounts (`backends`) and model mappings
(`models`, a public id mapped onto a backend list with session-affinity
hashing). This design adds a third conceptual layer and renames the second:

- **Backends** stay raw upstream accounts (an OpenCode Go account, a DeepSeek
  API key, a Z.ai GLM account). Clients never address them directly.
- **Presets** (replaces `models`) are named routing policies exposed to clients
  as model ids. Whatever a client puts in `model` is a preset id.
- **Strategies** are how a preset picks its primary backend and orders its
  fallback chain: `affinity`, `failover`, or `weighted`.

Goals: multiple model families behind one local endpoint (DeepSeek, GLM, later
more), per-preset routing policy, per-presentation metadata (pricing, context
window, label) moved from hardcoded UI values into config.json, and a fix for
the reported bug where a manually cooled backend un-cools itself.

Non-goals for v1: session-sticky weighted balancing (affinity already covers
sticky LB), cross-family parameter translation (per-backend `dropParams` only),
authentication (localhost-only design stays).

---

## 2. Bug: sticky manual cool

### Root cause (current code)

1. The UI Cool button posts `/admin/backend {action:"cool"}` with no `forMs`,
   defaulting to a 10-minute cool.
2. When ALL backends are cooling, `handleChat` falls back to routing to the
   backend with the soonest `nextAvailableAt`. A 10-minute manual cool beats
   the GO accounts' multi-day weekly limits, so the router routes through the
   manually cooled backend.
3. The request succeeds, so `markSuccess` calls `markHealthy`, which clears
   the cooling state unconditionally. The user's manual cool is erased by the
   very request it was meant to block.

### New semantics

The distinction that matters is *user decision* vs *system reaction*:

- A **manual cool** (kind `"manual"`, sticky or timed) is a user decision. It
  NEVER participates in the all-cooling fallback, and a success NEVER clears
  it. Only the Uncool button (or expiry of a timed manual cool) restores the
  backend.
- A **system cool** (rate limit, server error, auth, weekly) is the router
  reacting to observed failures. It keeps today's fallback behavior: when
  everything is cooling, the router may still try the soonest system-cooled
  backend.

Sticky vs timed only changes when a manual cool auto-expires:

- `POST /admin/backend {id, action:"cool"}` (no forMs) = **sticky**:
  `nextAvailableAt = Number.MAX_SAFE_INTEGER`. Cooled until uncooled.
- `POST /admin/backend {id, action:"cool", forMs: N}` = **timed manual**:
  current behavior (`nextAvailableAt = now + N`), default N removed; callers
  must pass forMs explicitly to get a timed cool.

### Code changes

`markSuccess(id)`: if the backend is cooling and `lastError.kind === "manual"`,
return without clearing. Otherwise `markHealthy` as today.

All-cooling fallback in `handleChat`: build `manualCooled` = set of backend ids
that are cooling with `lastError.kind === "manual"`. The fallback pool is
`ordered.filter(id => !manualCooled.has(id)).sort(soonest nextAvailableAt)`.
If that pool is empty, return 503 with body
`{"error":{"message":"all available backends are cooling (N manually cooled - uncool to restore)","type":"router_unavailable"}}`
(N = count of manual-cooled), and record history/stats as today's 503 path does.

`/health`: each backend entry gains `"manual": true|false` (cooling AND
lastError.kind === "manual"), so the UI can show "until uncooled" instead of a
countdown for sticky cools. `nextAvailableAtMs` for sticky cools is
`9007199254740991` (MAX_SAFE_INTEGER); the UI must not render that as a
countdown.

---

## 3. Preset strategies

All strategies return an ordered candidate list; `handleChat` keeps its
existing healthy-filter + in-order retry over that list. Health, backoff,
weekly pinning, and metrics are per-backend and inherited by every strategy
for free. The preset layer never needs to know about backoff.

### affinity (today's behavior, generalized)

Pool = `min(affinityPool || 1, members.length)`. Primary =
`members[hash(sessionKey) % pool].backend`. Ordered = `[primary, ...remaining
members in declared order]`. Sessions stick to one backend (cache-friendly,
conversation-coherent) while spreading across the pool.

Default `affinityPool` is 1 (only the first member receives traffic unless
raised), matching today's `models` default exactly.

### failover

Ordered = declared member order, no hashing. The first member always serves
when healthy; later members are spares. Use case: "GLM quality first, DeepSeek
as backup".

### weighted

Primary = weighted-random pick over members, each member's chance proportional
to its weight. Ordered = `[pick, ...rest sorted by weight desc, declared order
tiebreak]`. Use case: per-request load balancing with unequal shares. This is
stateless random per request, NOT session-sticky; when sticky spreading is
wanted, affinity with `affinityPool = N` is the right tool. A sticky-weighted
variant (small session-to-backend LRU) is a future addition, deliberately out
of scope for v1.

Function signature for testability:
`candidates(cfg, presetId, sessionKey, rng = Math.random)`.

Unknown preset id keeps today's 404 `model_not_found` path.

---

## 4. Config schema

New top-level key `presets` replaces `models`. Backends gain an optional
`dropParams`. Everything else (port, prefix, masterKeyEnv, backoff) is
unchanged.

```jsonc
{
  "port": 8787,
  "prefix": "/v1",
  "masterKeyEnv": null,
  "backends": [
    { "id": "go-primary", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_KEY", "model": "deepseek-v4-flash" },
    { "id": "go-alt", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_ALT_KEY", "model": "deepseek-v4-flash" },
    { "id": "direct", "baseURL": "https://api.deepseek.com/v1",
      "apiKeyEnv": "DEEPSEEK_API_KEY", "model": "deepseek-v4-flash",
      "translateDeveloperRole": true },
    { "id": "zai-glm", "baseURL": "https://api.z.ai/v1",
      "apiKeyEnv": "ZAI_KEY", "model": "glm-5.3",
      "dropParams": ["reasoning_effort"] }
  ],
  "presets": {
    "deepseek-v4-flash": {
      "strategy": "affinity",
      "members": [
        { "backend": "go-primary", "weight": 1 },
        { "backend": "go-alt", "weight": 1 },
        { "backend": "direct", "weight": 1 }
      ],
      "affinityPool": 2,
      "meta": {
        "label": "DeepSeek V4 Flash",
        "contextWindow": 128000,
        "pricing": { "inputPerM": 0.18, "outputPerM": 0.87 }
      }
    },
    "glm-quality": {
      "strategy": "failover",
      "members": ["zai-glm", "direct"],
      "meta": {
        "label": "GLM 5.3 (DeepSeek fallback)",
        "contextWindow": 128000,
        "pricing": { "inputPerM": 0.8, "outputPerM": 2.4 }
      }
    },
    "spread": {
      "strategy": "weighted",
      "members": [
        { "backend": "zai-glm", "weight": 1 },
        { "backend": "direct", "weight": 3 }
      ]
    }
  ],
  "backoff": { "...": "unchanged" }
}
```

Members accept string shorthand (`"direct"` means weight 1) or object form.
`meta` is optional; when absent the UI falls back to sensible defaults
(pricing 0.18/0.87 per M, context 128000, label = preset id). `meta` is
surfaced to the UI through `/api/config` and is never used for routing.

`dropParams` (per backend, optional array of strings): in `forward()`, after
parsing the payload, delete those keys before re-stringifying. Purpose:
upstream dialects differ (`reasoning_effort` is DeepSeek/OpenAI-speak; GLM
rejects unknown params). This only strips; real translation is future work.

### Validation (lenient, never crash the router)

- Invalid/absent `strategy` defaults to `"affinity"` with a console.warn.
- Member referencing an unknown backend id is dropped with a console.warn.
- A preset left with zero valid members stays in the config but returns 404 at
  request time (`preset '<id>' has no valid members`).
- Normalization happens once at config load; routing and /api/config only ever
  see the normalized form (members as `{backend, weight}` objects).

---

## 5. Backward compatibility and migration

Synthesis at config load: if `presets` is absent or empty and `models` exists,
synthesize `presets` from `models` (strategy `"affinity"`, members from the
backend list, `affinityPool` copied, no meta). The 24 existing tests use old
schema fixtures and must pass unchanged through this path.

`/v1/models` lists `Object.keys(cfg.presets)` after synthesis, so old clients
see identical ids. `/api/config` returns the normalized config (presets always
present).

The live `config.json` migrates to the new schema preserving every existing
value verbatim (port, prefix, masterKeyEnv, backends, backoff):

- `deepseek-v4-flash`: affinity, members [go-primary, go-alt, direct],
  affinityPool 2, meta {label "DeepSeek V4 Flash", contextWindow 128000,
  pricing {inputPerM 0.18, outputPerM 0.87}}.
- `deepseek-v4-flash-alt`: same shape as above.
- `deepseek-v4-flash-direct`: failover, members [direct], same meta.

These three preset ids are load-bearing: OpenCode (opencode.jsonc), dsh
(settings.yaml + cordis.patch.yml), and opencode-mem reference them. They must
survive the migration byte-identical.

---

## 6. Server change summary (phase 1: server.mjs, test.mjs, config.json, README.md)

1. `normalizePresets(cfg)`: synthesis + member normalization + validation
   (section 4/5). Called from `loadConfig()`.
2. `candidates(cfg, presetId, sessionKey, rng)` rewritten per section 3;
   `handleChat` looks up `cfg.presets[modelId]`.
3. Sticky manual cool per section 2: `markSuccess` guard, fallback exclusion,
   503 message, `/health` manual flag, `/admin/backend` cool without forMs =
   sticky.
4. `/v1/models` from presets; `/api/config` returns normalized cfg.
5. `forward()`: strip `backend.dropParams` from the parsed payload.
6. Metrics, JSONL history, stream tee, config hot-reload, prefix handling,
   error paths, weekly pinning: NO regressions.
7. New tests (section 8) + all 24 existing tests green.
8. README: new schema documented with the GLM example, strategies explained,
   sticky-cool semantics, backward-compat note, dropParams.

---

## 7. UI change summary (phase 2: index.html only)

1. **Meta-driven pricing/context**: replace the hardcoded `PRICING` constant
   and the hardcoded 128000 context window. On config load, build
   `PRESET_META[presetId] = {label, contextWindow, pricing}` from
   `cfg.presets[id].meta`. `calcCost` and the context tracker use the current
   model's meta with fallbacks to the current defaults. Compare-mode cards use
   each compared model's own meta.
2. **Models table becomes Presets table**: columns Preset | Strategy |
   Members | Pool. Members render as ids, weights shown when not 1
   (`zai-glm (x1), direct (x3)`). Label from meta shown under or beside the
   preset id when present.
3. **Manual cool display**: backend cards read the new `manual` flag. Manual
   cools show a "Manual" badge and "until uncooled" instead of a countdown;
   the countdown ticker must never render MAX_SAFE_INTEGER as a duration.
   System cools keep the live countdown.
4. Everything else (chat, streaming, localStorage persistence, debug panel,
   compare mode, templates, shortcuts, stats) unchanged.

---

## 8. Test plan (test.mjs additions)

Existing 24 tests must pass unchanged. New tests:

1. **Synthesis**: a config using the old `models` schema serves `/v1/models`
   with the same ids, and an affinity request routes identically to before.
2. **Failover**: first member mock-fails 500, second member serves; response
   carries the second backend's `x-router-backend`.
3. **Weighted**: with an injected rng, the pick follows weights (rng stubbed
   to select each band), and the ordered list starts with the pick.
4. **Sticky manual cool, full loop**: cool the only non-cooling backend (no
   forMs) while another is weekly-pinned; a request returns 503 (not routed
   through the manual one); `/health` shows cooling + manual; a success on
   another backend does NOT clear it; uncool restores routing.
5. **Timed manual cool**: with forMs still expires (short forMs, clock
   advanced in test).
6. **Fallback exclusion**: all backends cooling, the soonest is manual, the
   later one is system-cooled: the router tries the system-cooled one, not
   the manual one.
7. **dropParams**: backend with `dropParams: ["reasoning_effort"]` forwards a
   payload without that key (mock asserts absence).
8. **meta passthrough**: `/api/config` contains presets with meta.

---

## 9. Verification checklist (super agent)

Fresh-context verification with NO knowledge of the implementation effort.
Read this document first, then the code, then verify each item. Report
PASS / FAIL / UNCLEAR per item with evidence; never guess a PASS.

1. `node test.mjs` passes: 24 pre-existing + 8 new tests.
2. `node --check server.mjs` clean; zero new npm dependencies.
3. Live router restarted on 127.0.0.1:8787; `GET /health`, `GET /v1/models`,
   `GET /api/config` all 200. `/v1/models` lists exactly the three
   load-bearing preset ids. `/api/config` contains normalized presets with
   meta and no api key values (apiKeyEnv names only).
4. Sticky cool, live: cool `direct` via admin (no forMs); `/health` shows
   manual; with both GO accounts also cooling, a chat request to
   `deepseek-v4-flash-direct` returns the documented 503 (NOT routed through
   direct); uncool restores service. No success path clears a manual cool.
5. Routing: a request to each preset id routes to a healthy member and carries
   the correct `x-router-backend` header; history entries still contain
   ttftMs/promptTokens/completionTokens/cacheHit/genMs/tps after a streamed
   request.
6. UI: `GET /` serves the updated page. In a real browser: presets table shows
   strategy/members/pool; a manual cool displays "until uncooled" (no
   absurd countdown); cost and context numbers derive from meta (temporarily
   edit a meta value in config.json, reload, see it change, revert).
7. Backward compat: temporarily swap in an old-schema config (models key, no
   presets), router hot-reloads, `/v1/models` unchanged, request routes;
   restore migrated config.
8. No regression scan: JSONL file still appends; config hot-reload works;
   prefix handling intact (`/v1/chat/completions`); old 404 paths unchanged.

---

## 10. Future work (explicitly out of scope for v1)

- Sticky weighted balancing (session-to-backend LRU).
- Cross-family parameter translation (reasoning_effort to GLM thinking
  toggles) beyond dropParams.
- Preset-level overrides of backoff parameters.
- Per-preset request budget / spend caps driven by meta.pricing.
