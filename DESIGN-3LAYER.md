# DESIGN-3LAYER.md

Design contract for the three-layer routing refactor: backends -> models -> presets,
with layered parameter configuration. This document is the single source of truth
for implementation and verification.

STATUS: FINAL. Reviewed against the real code (server.mjs, test.mjs @ 63 asserts,
live config.json, index.html) and against DESIGN-PRESETS.md. All draft ambiguities
are resolved; every section is implementable as written.

Supersedes the backends -> presets direct-binding design in DESIGN-PRESETS.md
(which remains valid for strategy semantics and sticky-cool semantics).
Implementation is split into two phases (server, then UI); a verifier checks the
result against section 11.

---

## 0. Changes from the reviewed draft (review verdict applied)

The draft was directionally sound but had one behavior-breaking flaw and several
under-specified contracts. These are the substantive changes; all are folded into
the sections below:

1. **Synthesis path B rewritten (the critical fix).** The draft split a preset
   whose member backends had mixed upstream model strings into one model per
   distinct upstream and forced the preset to `failover`. The LIVE config is
   exactly such a preset (commandcode's upstream is `deepseek/deepseek-v4-flash`,
   the others `deepseek-v4-flash`, with `affinityPool: 3`), so during the window
   between server deploy and config migration the router would have silently
   switched from session-spread-over-3-accounts to go-primary-until-it-fails.
   New rule (section 6): one model per preset, providers keep their own
   per-provider upstream, the legacy preset `affinityPool` moves to the model,
   and legacy `weighted` presets collapse to declared order with a warning.
   Synthesis B is now behavior-preserving for the live config by construction.
2. **Dialect order fixed: dropParams -> paramMap -> role translation.** The draft
   applied paramMap first, then dropParams, which makes
   `dropParams: ["reasoning_effort"]` a no-op on any provider that also maps that
   key, and contradicts how dropParams has always worked (public request keys,
   test x). dropParams now always refers to public/request key names; paramMap
   renames the survivors (section 4).
3. **backend.params example corrected.** The draft's schema showed
   `translateDeveloperRole` as a backend `params` key while section 7 kept it a
   special-cased field. Decision: `translateDeveloperRole` stays a backend FIELD
   (special-cased, unchanged); backend `params` carries only real request
   params. Legacy `backend.dropParams` (DESIGN-PRESETS era, test x) remains
   supported and unions with provider-level dropParams.
4. **candidates() contract specified exactly.** The draft renamed the export
   without defining the new shape. Section 8 now defines the return type: a
   flattened, backend-deduped ordered attempt list. Also exports
   `normalizeConfig` (replaces `normalizePresets`) and `buildPayload` (pure,
   unit-testable merge).
5. **`model` is a reserved param key.** No params layer, and not the request
   body, can override it; the forwarded `model` is always `provider.upstream`.
6. **History entries gain `routedModel`** (the resolved model id). `entry.model`
   stays the REQUESTED id so `STATS.byModel`, the UI history table, and tests
   m/o3 cannot regress. UI phase 2 reads `routedModel` for the new debug row.
7. **/api/config presets expose a derived read-only `members` array** so the
   phase-1-server / phase-2-UI window, test t1/t2, and the current presets
   table keep working unchanged (section 5).
8. **Preset/model id collision policy decided:** the preset wins, a config
   warning fires once, `/v1/models` lists the id once. Synthesis A intentionally
   creates colliding ids (model id = preset id = old model id); this is
   harmless because the preset resolves to the same-named model.
9. **Model-level provider ordering is affinity-only**, and affinity with pool 1
   degenerates to declared order (i.e. failover). Documented explicitly instead
   of implied; no `strategy` field on models.
10. **Test plan expanded** (section 10): dedup across models sharing a backend,
    routedModel history, reserved `model` key, /api/config normalized-shape
    assertions, weighted collapse shape, collision listing, empty-preset 404
    message.

---

## 1. Overview and goals

Today the router binds presets directly to backends: a preset's `members` point
at raw accounts, and each backend carries a single fixed upstream model id. That
cannot express "one account serves several models" (e.g. CommandCode serving both
deepseek-v4-flash and deepseek-v4-pro) and forces duplicated backend entries.

The new architecture adds a middle layer:

- **Backends** - raw upstream accounts. No model binding (the legacy `model`
  field remains legal as a default-upstream source for synthesis and old
  configs). Optionally carry `params` (request defaults for that account),
  `dropParams` (legacy dialect strip), and `translateDeveloperRole` (field,
  special-cased).
- **Models** - a logical model id, bound to one or more backends through
  `providers` (backend + upstream model string + optional per-provider dialect:
  `params`, `dropParams`, `paramMap`). Carries `affinityPool` (provider-level
  session spreading), optional `params` defaults, and `meta`.
- **Presets** - routing policies over MODELS. `strategy` (affinity / failover /
  weighted) picks among models; each selected model routes within its own
  providers. Clients see preset ids (plus bare model ids as implicit
  single-model presets).

Goals: one account can serve many models; presets reference models (add a
provider to a model and every preset using it inherits it); per-layer parameter
configuration with a well-defined merge order; model-dialect handling
(dropParams / paramMap) at the provider level; config-time validation warnings.

Non-goals for v1: response-key rewriting by the router (the UI already reads both
reasoning key shapes), cross-family parameter inference beyond explicit maps,
runtime permission/auth, dynamic model creation via API.

Design decisions worth stating plainly:

- Models have ONE ordering strategy: affinity over providers. Affinity with
  `affinityPool: 1` (the default) degenerates to declared order with the first
  provider as primary, which IS failover semantics. No model-level `strategy`
  field exists or is needed.
- The same backend may appear under several models (and under several presets).
  Within a single request the flattened attempt list is deduped by backend id
  (first occurrence wins), so a shared account is never retried twice in one
  request.
- `hashStr` and the modulo arithmetic are UNCHANGED so affinity routing is
  byte-identical for identical configs (verification relies on this).

---

## 2. New config schema

```jsonc
{
  "port": 8787,
  "prefix": "/v1",
  "masterKeyEnv": null,
  "params": {                         // OPTIONAL global defaults, lowest layer
    "temperature": 0.7
  },
  "backends": [
    { "id": "go-primary", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_KEY" },
    { "id": "go-alt", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_ALT_KEY" },
    { "id": "direct", "baseURL": "https://api.deepseek.com/v1",
      "apiKeyEnv": "DEEPSEEK_API_KEY",
      "translateDeveloperRole": true,          // FIELD, special-cased (not a param)
      "params": { "max_tokens": 8192 } },      // optional real request params
    { "id": "commandcode", "baseURL": "https://api.commandcode.ai/provider/v1",
      "apiKeyEnv": "COMMANDCODE_API_KEY",
      "translateDeveloperRole": true,
      "dropParams": ["reasoning_effort"] }     // legacy field, still honored
  ],
  "models": {
    "deepseek-v4-flash": {
      "providers": [
        { "backend": "go-primary", "upstream": "deepseek-v4-flash" },
        { "backend": "go-alt", "upstream": "deepseek-v4-flash" },
        { "backend": "commandcode", "upstream": "deepseek/deepseek-v4-flash" },
        { "backend": "direct", "upstream": "deepseek-v4-flash" }
      ],
      "affinityPool": 3,              // session affinity across providers; default 1
      "params": { "reasoning_effort": "high" },   // optional model-level defaults
      "meta": {
        "label": "DeepSeek V4 Flash",
        "contextWindow": 128000,
        "pricing": { "inputPerM": 0.18, "outputPerM": 0.87 }
      }
    },
    "deepseek-v4-flash-direct": {
      "providers": [ { "backend": "direct", "upstream": "deepseek-v4-flash" } ],
      "affinityPool": 1
    },
    "glm-5.3": {                      // dialect example
      "providers": [
        { "backend": "zai-glm", "upstream": "glm-5.3",
          "paramMap": { "reasoning_effort": "thinking" } },
        { "backend": "some-proxy", "upstream": "glm-5.3",
          "dropParams": ["reasoning_effort"] }
      ]
    }
  },
  "presets": {
    "deepseek-v4-flash": {
      "strategy": "affinity",
      "models": ["deepseek-v4-flash"],
      "affinityPool": 1               // preset-level pool over MODELS (default 1)
    },
    "deepseek-v4-flash-alt": {        // alias preset, same model
      "strategy": "affinity",
      "models": ["deepseek-v4-flash"]
    },
    "deepseek-v4-flash-direct": {
      "strategy": "failover",
      "models": ["deepseek-v4-flash-direct"]
    }
  },
  "backoff": { "...": "unchanged" }
}
```

### Field rules

- `backends[].id/baseURL/apiKeyEnv`: unchanged. `model` (legacy upstream
  default), `translateDeveloperRole`, `dropParams` remain legal backend fields.
  New optional `params` object (real request params only; `model` inside any
  params layer is ignored with a warning).
  New optional timeout pair (also legal at top level):
  `timeoutMs`/`connectTimeoutMs` bound only the HEADER wait (TTFT), default
  45s; `idleTimeoutMs` bounds SILENCE during the body (stall detection),
  default 120s. The body is never killed by a wall clock — a slow-but-alive
  stream runs to completion while a stuck upstream still fails after
  `idleTimeoutMs` (see section "Timeout semantics").
- `backends[].timeoutMs`: retained for compatibility; now means the connect/header
  bound (was: whole-request wall clock, which aborted long SSE relays mid-body).
  `connectTimeoutMs` overrides it when present.
- `models[id].providers[]`: `backend` MUST reference an existing backend id
  (else the provider is dropped with console.warn). `upstream` = the model
  string sent to that backend; resolution order: `provider.upstream` ->
  `backend.model` -> if neither, the provider is dropped with console.warn.
  Optional per-provider `params`, `dropParams` (array), `paramMap` (object).
- `models[id].affinityPool`: default 1. Number of providers sessions spread
  across (same arithmetic as the old preset-level affinityPool).
- `models[id].params`, `models[id].meta`: optional.
- `presets[id].models[]`: strings or `{model, weight}` objects (weight only
  matters for `weighted`; default 1, must be > 0). MUST reference existing
  model ids or existing preset ids (else dropped with console.warn). A ref to
  another preset id creates a PRESET OF PRESETS (tier-style nesting, section
  3a): that preset's model list is expanded in place at request time.
- `presets[id].strategy`: affinity | failover | weighted (default affinity,
  unknown values warn + default).
- `presets[id].affinityPool`: default 1, applies to the preset's model
  selection for affinity strategy.
- `presets[id].params`, `presets[id].meta`: optional. Meta resolution for the
  UI is field-level: preset.meta wins over model.meta, which wins over
  defaults.
- Top-level `params`: optional object, lowest-priority defaults.
- If a model id is also a preset id: the PRESET wins at request time, a config
  warning fires once at load, and `/v1/models` lists the id once.

---

## 3. Routing semantics (two-level)

Request flow:

1. Client asks for id X (`model` field). No/blank X -> 404 `model_not_found`
   (message `model '<X>' not found`, exactly today's path).
2. If X is a preset id, use it. Else if X is a model id, treat X as an implicit
   single-model preset: `{strategy: "affinity", models: [X], affinityPool: 1}`
   (the model's own `affinityPool` still governs its providers). Else 404
   `model_not_found`.
3. Preset level: build the ordered candidate MODEL list per strategy:
   - affinity: pool = min(affinityPool || 1, models.length); primary =
     models[hash(sessionKey) % pool]; ordered = [primary, ...rest declared].
   - failover: declared order.
   - weighted: weighted-random primary over models (rng injectable for tests);
     ordered = [pick, ...rest weight desc, declared tiebreak (stable sort)].
4. Model level: for each model in the preset's ordered list (in order), build
   the ordered PROVIDER list: pool = min(model.affinityPool || 1,
   providers.length); primary = providers[hash(sessionKey) % pool]; ordered =
   [primary, ...rest declared]. Flatten all models' provider lists in model
   order into one attempt list of `{model, backend, upstream}`.
5. Dedup the flattened list by backend id (first occurrence wins, keeping that
   occurrence's model+upstream).
6. Health filter + fallback (semantics identical to DESIGN-PRESETS section 2):
   healthy = attempts whose backend is not cooling (or whose cool expired).
   If any healthy exist, pool = healthy in order. Else pool = all attempts
   whose backend is NOT manually cooled, sorted by soonest
   `nextAvailableAt`. Manual cools NEVER participate in the fallback and are
   never success-cleared. If that pool is empty, 503 with the existing message
   `all available backends are cooling (N manually cooled - uncool to
   restore)` (N = count of distinct manual-cooled backend ids in the attempt
   list), recording history/stats on the 503 path exactly as today.
7. Attempt loop: for each attempt, build the payload (section 4) and forward.
   Failure -> markFailure, continue (client-classified errors break the loop,
   unchanged). Success -> markSuccess, relay, record history/stats, return.
   Exhausted -> 503 with the last upstream error body (unchanged shape).

Forwarding: resolve the key from `backend.apiKeyEnv`, URL =
`backend.baseURL` (trailing slash stripped) + `/chat/completions`,
`payload.model = provider.upstream`.

History/stats: `entry.model` = the REQUESTED id (preset or bare model);
`entry.routedModel` = the model id of the serving attempt on success, or the
first candidate model on failure (null when there were no candidates).
`entry.backend` = the attempt's backend id. `STATS.byModel` stays keyed by the
requested id. All other fields (ttftMs, tokens, cacheHit, genMs, tps, session)
unchanged.

Session-affinity hashing (`hashStr`, sha256 readUInt32BE), the health registry
(keyed by backend id), weekly pinning, backoff, metrics, stream tee, JSONL
history: UNCHANGED. This is a hard requirement: for an identical effective
config, the same session key must hash to the same primary backend as before.

Session key resolution (priority order): `x-session-affinity` header,
`x-session-id` header, then the request body's `session_id` / `sessionId` /
`user`, then the constant `no-session`. The two body session spellings let a
client that identifies its conversation in the chat-completions body (instead
of via headers) still get per-session affinity; the hash itself is unchanged.

---

## 3a. Preset-of-presets (tier-style nesting)

A preset's `models` array may reference MODEL ids AND/OR PRESET ids. A ref to
another preset is expanded, recursively, into that preset's model list at
request time (and at derived-`members` computation). This gives tier
abstraction: `lite`/`standard`/`pro`/`super` tiers, each an aggregation, where
a tier can reuse another tier's full definition.

```jsonc
"presets": {
  "glm":  { "strategy": "failover", "models": ["glm-5.3", "glm-5.3-lite"] },
  "os-alpha": { "strategy": "affinity", "models": ["deepseek-v4-flash", "mimo-v2.5-pro"] },
  "super": { "strategy": "failover", "models": ["glm", "os-alpha"] }
}
```

Requesting `super` expands `glm` -> `[glm-5.3, glm-5.3-lite]` and `os-alpha` ->
`[deepseek-v4-flash, mimo-v2.5-pro]`, producing the flat model list
`[glm-5.3, glm-5.3-lite, deepseek-v4-flash, mimo-v2.5-pro]` in declared order.

### Expansion semantics

- **In-place flattening, declared order preserved.** Each nested preset's model
  list is spliced into the parent list exactly where the preset ref appeared.
  Duplicates in the MODEL list are allowed (they are resolved by the existing
  backend-dedup pass: first occurrence wins, so a duplicate model contributes
  no extra attempts).
- **The top-level preset's strategy governs ordering** over the EXPANDED list
  (affinity/failover/weighted behave exactly as if the expanded models had
  been declared directly).
- **A nested preset's own strategy/affinityPool/weights are IGNORED for
  ordering.** Only its model list is taken. `os-alpha` above is `affinity`, but
  inside the `failover` `super` it contributes its two models in declared
  order, exactly as if `super` had listed the model ids directly.
- **Weights.** When the top-level strategy is `weighted`, each ref carries its
  declared weight. A preset ref's weight applies to the whole nested group's
  first model for the weighted-random pick; the rest of the group is ordered
  per the standard weighted rule (rest sorted by weight desc, declared-order
  tiebreak) and each leaf keeps its own weight 1 unless declared otherwise.

### Cycle detection

`A -> B -> A` (or self-reference `A -> A`) must not infinite-loop. Expansion
carries a visited set of preset ids per path:

- On a cycle, `console.warn` once per cyclic path (deduped per process), stop
  expanding that path, and:
  - if the id that closed the cycle also resolves as a MODEL id (id collision
    case), keep it as a model in the expanded list;
  - otherwise drop it.
- The request still succeeds if any valid model remains (see z20: a valid
  sibling of the cyclic ref routes normally).

### Collision rule (unchanged)

"Preset wins" at request time is INTACT: if an id is both a preset and a model,
a request for that id resolves the preset. The special identity case (a preset
whose `models` list references its own colliding id - produced by path B
synthesis, `{models: [{model: <same id>}]}`) is treated as a MODEL ref, not a
nested-preset self-cycle, so legacy synthesized presets keep working
byte-identically.

### Config validation

`normalizeConfig` now accepts a preset ref if the id exists in EITHER
namespace (`models[id]` or `presets[id]`). An id in neither is still dropped
with the existing `dropping unknown model '<id>'` warning. The derived
read-only `members` array (flattened provider backends) recursively expands
nested preset refs, so `/api/config` and the UI keep showing the full provider
chain of a tier preset.

### /v1/models

Unchanged: preset ids first (config order), then model ids not already listed.
A preset-of-presets does NOT change the id list - nested presets are already
listed as their own ids.

---

## 4. Layered parameters (merge order)

Priority, highest first (a key set at a higher layer overwrites lower layers;
each lower layer only fills keys not already set):

1. Request body (client) - highest priority except `model` (reserved).
2. Preset `params`.
3. Model `params`.
4. Provider `params`.
5. Backend `params`.
6. Global top-level `params`.

`model` is reserved: after merging, `payload.model` is ALWAYS set to
`provider.upstream`. A `model` key inside any params layer is ignored with a
config-time warning. (The request body's `model` is the routing key and is
overwritten by the upstream string, exactly as today.)

Exact merge algorithm (`buildPayload(body, cfg, preset, model, provider,
backend)`, pure, never mutates `body`):

```js
const out = {};
Object.assign(out, cfg.params || {});       // lowest
Object.assign(out, backend.params || {});
Object.assign(out, provider.params || {});
Object.assign(out, model.params || {});
if (preset) Object.assign(out, preset.params || {});
delete out.model;                           // reserved in config layers
Object.assign(out, body);                   // request body wins
out.model = provider.upstream;              // always
// Dialect: drop FIRST (public/request key space), then rename survivors.
const drop = new Set([...(backend.dropParams || []), ...(provider.dropParams || [])]);
for (const k of drop) delete out[k];
for (const [from, to] of Object.entries(provider.paramMap || {})) {
  if (from in out) { out[to] = out[from]; delete out[from]; }
}
// Role translation last (unchanged field-flag behavior).
if (backend.translateDeveloperRole && Array.isArray(out.messages)) {
  out.messages = out.messages.map((m) => (m && m.role === "developer" ? { ...m, role: "system" } : m));
}
return out;
```

Notes:

- dropParams entries refer to PUBLIC request keys (pre-map names). A provider
  that both maps `reasoning_effort -> thinking` and needs the key gone simply
  omits it from paramMap and lists it in dropParams. Renames overwrite the
  target key if present (defined behavior, last write wins).
- `backend.dropParams` (legacy) and `provider.dropParams` union. This keeps
  DESIGN-PRESETS-era configs and test x working unchanged.
- `stream` and `messages` are NOT reserved: a config layer may legitimately
  force `stream: false` (messages are only ever touched by role translation).

### Config-time validation warnings (console.warn, never refuse to load)

On every config load (initial + hot-reload), after normalization:

- For every preset P, model M (valid ref), provider PR: let keys = union of
  cfg.params + backend.params + provider.params + model.params + preset.params
  keys. For each key in (backend.dropParams + provider.dropParams): warn once
  `config warning: preset <P> model <M> provider <PR.backend> drops param <k>
  - requests using it will not receive it`.
- `model` present in any params layer -> warn (ignored).
- Unknown strategy -> warn + default affinity.
- Provider referencing an unknown backend id -> drop + warn.
- Provider with no resolvable upstream -> drop + warn.
- Preset referencing an unknown model id (an id that is neither a model nor a
  preset) -> drop + warn.
- Legacy weighted preset collapsed (section 6) -> warn.
- Model id colliding with a preset id -> warn once.
- A preset left with zero valid models, or a model with zero valid providers,
  stays in the config but 404s at request time (below).

Request-time 404 messages (type `model_not_found`, same shape as today):

- Unknown id: `model '<id>' not found` (unchanged).
- Preset with zero valid models: `preset '<id>' has no valid models`.
- Bare model id with zero valid providers: `model '<id>' has no valid
  providers`.

---

## 5. Endpoint changes

- `GET /v1/models`: lists preset ids (config order), then model ids not already
  listed (config order). Colliding ids appear exactly once (the preset owns the
  id). Entry shape unchanged: `{id, object: "model", owned_by: "local-router"}`.
  After live migration the output is IDENTICAL to today (both model ids collide
  with preset ids), which is the consumer-safety property: OpenCode (opencode
  .jsonc), dsh (settings.yaml + cordis.patch.yml), and opencode-mem see the
  same three ids in the same order.
- `GET /api/config`: returns the normalized config: backends (apiKeyEnv NAMES
  only, never values), models with normalized providers, presets with
  normalized `models` arrays PLUS a derived read-only `members` array
  `[{backend, weight: 1}]` (flattened provider backends of the preset's models
  - nested preset refs recursively expanded, first-occurrence order, deduped)
  kept for UI/test compatibility across the phased rollout, top-level `params`
  when present, backoff. Never key values.
- `/health`, `/api/stats`, `/api/history` (entries gain `routedModel`),
  `/v1/chat/completions`, `/admin/*`: unchanged shapes apart from `routedModel`.

---

## 6. Backward-compat synthesis

`normalizeConfig(cfg)` runs at every config load (initial + hot-reload), in
place, and always produces a fully normalized new-shape config. Routing and
/api/config only ever see the normalized form. Detection is PER ENTRY so
single-era configs (all test fixtures) and accidental mixes both work:

1. **models map** (default `{}`). For each `[id, m]` of `cfg.models`:
   - If `m.providers` is an array (NEW shape): validate each provider;
     `upstream = provider.upstream ?? backend.model`; drop invalid providers
     (unknown backend, or no resolvable upstream) with warns. Keep optional
     params/dropParams/paramMap/affinityPool/meta.
   - Else if `m.backends` is an array (pre-presets era, path A): synthesize
     `models[id].providers` = each backend id (strings) mapped to
     `{backend, upstream: <that backend's model>}` (unknown backend or missing
     upstream -> drop that provider + warn). Copy `affinityPool`. Remember id
     as legacy-synthesized. Drop the old `backends` key from the entry.
   - Else: warn + drop the entry.
2. **presets map** (default `{}`). For each `[id, p]` of `cfg.presets`:
   - If `p.models` is an array (NEW shape): normalize entries to
     `{model, weight}` (string shorthand allowed; weight default 1, must be
     > 0); drop refs to nonexistent models with warns; validate strategy
     (unknown -> affinity + warn); keep affinityPool/params/meta.
   - Else if `p.members` is an array (DESIGN-PRESETS era, path B): synthesize:
     - providers = members in declared order -> `{backend, upstream: <that
       backend's model>}` (unknown backend / missing upstream -> drop +
       warn). ONE model per preset regardless of mixed upstreams (providers
       carry their own upstream; this is exactly the target shape).
     - Model id = the preset id. If `models[presetId]` already exists: reuse
       it only if its provider backend-id set equals the synthesized set
       (order-insensitive); otherwise use id `<presetId>-legacy` and warn.
     - New model gets `affinityPool` = the legacy preset's affinityPool
       (number, else absent) and no meta.
     - The preset becomes `{strategy, models: [{model: <modelId>, weight: 1}],
       params?, meta?}` where strategy = the original, EXCEPT legacy
       `weighted`, which becomes `affinity` + warn (weights over backends are
       not representable at the model layer; declared order is preserved).
       Preset-level affinityPool is removed (the model owns it now).
   - Else: warn + drop the preset.
3. After step 2: for each legacy-synthesized model id (path A) that has no
   preset with that id, create `presets[id] = {strategy: "affinity",
   models: [{model: id, weight: 1}]}`.
4. **Derived members**: for each preset, `members` = []; walk its models in
   order, each model's providers in order; push `{backend, weight: 1}` for
   backends not already present.
5. Collision warns (model id also a preset id), params-shape validation
   (non-object params -> warn + drop key).

Behavior-preservation proofs (these are requirements, verified by tests):

- Path A: old `{id: {backends, affinityPool}}` -> preset(affinity, pool 1,
  [model id]) + model(providers = declared backends, affinityPool = old pool).
  The preset level is identity; the model level hashes with the same hashStr
  and modulo over the same ordered list -> identical primary and identical
  fallback order. Tests r1/r2, d, e keep passing unchanged.
- Path B: old preset(affinity, members, pool N) -> model(providers = members
  order, pool N) + preset(affinity, [that model]) -> identical. Old
  preset(failover, members) -> model pool defaults 1 -> primary = first
  provider, ordered = declared -> identical. The LIVE config (mixed upstreams,
  affinityPool 3) therefore routes identically during the deploy window,
  before config.json is migrated.
- Backend `dropParams`/`translateDeveloperRole` continue to work via the
  union/field rules (tests x and j keep passing unchanged).

---

## 7. Live config migration

Rewrite `config.json` to EXACTLY this target (behavior-identical to today by
the section 6 proofs; the three load-bearing preset ids survive byte-identical
and /v1/models output is unchanged):

```json
{
  "port": 8787,
  "prefix": "/v1",
  "masterKeyEnv": null,
  "backends": [
    { "id": "go-primary", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_KEY" },
    { "id": "go-alt", "baseURL": "https://opencode.ai/zen/go/v1",
      "apiKeyEnv": "OPENCODE_GO_ALT_KEY" },
    { "id": "direct", "baseURL": "https://api.deepseek.com/v1",
      "apiKeyEnv": "DEEPSEEK_API_KEY", "translateDeveloperRole": true },
    { "id": "commandcode", "baseURL": "https://api.commandcode.ai/provider/v1",
      "apiKeyEnv": "COMMANDCODE_API_KEY", "translateDeveloperRole": true }
  ],
  "models": {
    "deepseek-v4-flash": {
      "providers": [
        { "backend": "go-primary", "upstream": "deepseek-v4-flash" },
        { "backend": "go-alt", "upstream": "deepseek-v4-flash" },
        { "backend": "commandcode", "upstream": "deepseek/deepseek-v4-flash" },
        { "backend": "direct", "upstream": "deepseek-v4-flash" }
      ],
      "affinityPool": 3,
      "meta": {
        "label": "DeepSeek V4 Flash",
        "contextWindow": 128000,
        "pricing": { "inputPerM": 0.18, "outputPerM": 0.87 }
      }
    },
    "deepseek-v4-flash-direct": {
      "providers": [
        { "backend": "direct", "upstream": "deepseek-v4-flash" }
      ],
      "affinityPool": 1,
      "meta": {
        "label": "DeepSeek V4 Flash",
        "contextWindow": 128000,
        "pricing": { "inputPerM": 0.18, "outputPerM": 0.87 }
      }
    }
  },
  "presets": {
    "deepseek-v4-flash": {
      "strategy": "affinity",
      "models": ["deepseek-v4-flash"]
    },
    "deepseek-v4-flash-alt": {
      "strategy": "affinity",
      "models": ["deepseek-v4-flash"]
    },
    "deepseek-v4-flash-direct": {
      "strategy": "failover",
      "models": ["deepseek-v4-flash-direct"]
    }
  },
  "backoff": {
    "rateLimitBaseMs": 30000,
    "rateLimitMaxMs": 600000,
    "serverBaseMs": 30000,
    "serverMaxMs": 300000,
    "authBaseMs": 60000,
    "authMaxMs": 1800000,
    "weeklyDefaultMs": 604800000
  }
}
```

Notes:

- `model` fields move from backends into provider `upstream` strings
  (commandcode keeps `deepseek/deepseek-v4-flash`).
- `translateDeveloperRole` stays a backend FIELD (special-cased; not a param).
- Meta lives on the MODELS (the presets fall back to model meta in the UI, so
  `deepseek-v4-flash-alt` and `deepseek-v4-flash-direct` render identical
  labels/pricing to today with no duplication).
- `deepseek-v4-flash` model `affinityPool: 3` preserves today's preset pool
  over [go-primary, go-alt, commandcode] with direct as ordered fallback.
- No `params` keys anywhere in the migrated config (today's config has none;
  forwarding behavior is bit-identical apart from the internal rewrite).
- port, prefix, masterKeyEnv, backoff: verbatim.
- Keep a `config.json.pre-3layer` backup next to the file before overwriting.

---

## 8. Server change summary (phase 1: server.mjs, test.mjs, config.json, README.md)

1. `normalizeConfig(cfg)` per section 6 (replaces `normalizePresets`; delete
   the old function). Called from `loadConfig()` on every load. Idempotent on
   already-normalized shapes (hot-reload re-parses the raw file, so this is
   naturally satisfied; never write the normalized form back to disk).
2. `candidates(cfg, requestedId, sessionKey, rng = Math.random)` per section 3:
   - returns `null` when requestedId is neither a preset id nor a model id;
   - else `{ kind: "preset"|"model", presetId, preset, models: [modelId],
     primary, ordered }` where `preset` is the normalized preset object or
     null (implicit model), `primary` is `ordered[0]` or null, and `ordered`
     is the flattened, backend-deduped list of
     `{model, backend, upstream}` attempts;
   - a preset with zero valid models returns `{kind: "preset", ..., ordered:
     []}` (not null) so handleChat can produce the specific 404 message.
3. `buildPayload(body, cfg, preset, model, provider, backend)` per section 4
   (pure). `forward(req, cfg, backend, payload, incomingHeaders, stream)` now
   receives the built payload OBJECT and stringifies it; everything else in
   forward (headers, cache headers, timeout, error shapes) unchanged.
   **Timeout semantics change**: `timeoutMs`/`connectTimeoutMs` now bound only
   the header wait (TTFT), never the body. The body has an idle/stall watchdog
   in `relay()` (`idleTimeoutMs`, default 120s) that aborts only on silence.
   Motivation + evidence in the "Timeout semantics" section below.
4. `handleChat`: resolve via `candidates`, apply the health filter + manual
   exclusion + fallback over the flattened attempt list, attempt loop calls
   buildPayload per attempt. `newHistoryEntry` gains `routedModel: f.
   routedModel ?? null`; `entry.model` stays the requested id. 404/503 message
   paths per sections 3/4.
5. `/v1/models` per section 5; `/api/config` unchanged endpoint, new normalized
   body (with derived preset `members`).
6. Config-time validation warnings per section 4.
7. No regressions: health registry keyed by backend id, weekly pinning,
   backoff, metrics fields, JSONL history, stream tee, hot-reload, prefix,
   localhost binding, sticky manual cool (markSuccess guard, fallback
   exclusion, /health manual flag, sticky/timed admin cool).
8. Exports: `export { candidates, normalizeConfig, buildPayload }` (remove
   `normalizePresets`).
9. Existing 63 tests: all names/assertions pass; the y1..y9 unit block is
   REWRITTEN in place against the new exports (same count, equivalent
   coverage); t2 additionally asserts `models` arrays + derived `members`.
   New tests per section 10.
10. README: three-layer schema with the GLM dialect example (backend + model +
    preset), layered params precedence, dropParams/paramMap semantics and
    order, synthesis note (old configs keep working), updated "What it serves"
    table (preset -> models -> providers chain).

---

## 9. UI change summary (phase 2: index.html only)

1. `renderPresets()` (`#presetsBody`): columns Preset | Strategy | Models |
   Pool. Models column renders `p.models` entries: model id + `(xN)` when
   weight != 1. Pool column: preset `affinityPool` for affinity strategy,
   "-" otherwise. Keep tolerating string members (derived `members` remains
   available but is no longer the primary source).
2. NEW Models section in the dashboard, between Presets and the config viewer:
   `<section><h2 data-icon="box">Models</h2>` with a table Model | Providers |
   Affinity | Params, `<tbody id="modelsBody">`. New `renderModels()`: called
   from `loadConfig()`; guarded by `sectionChanged("models", cfg.models)`;
   Providers renders `backend (upstream)` pairs joined by ", "; Affinity =
   `model.affinityPool || 1`; Params = compact `JSON.stringify(model.params)`
   or "-". Empty state: "No models configured."
3. `presetMeta(id)` / `PRESET_META`: field-level fallback preset.meta ->
   `cfg.models[first model in preset.models].meta` -> defaults. Build in
   `loadConfig()` exactly as today (clear + refill `PRESET_META`), but resolve
   each field with the model fallback baked in. `calcCost`/`fmtContext`/
   compare cards unchanged (they already call `presetMeta`).
4. Playground model select (`#pgModel`) and `populateCompareModels()`: STILL
   populate from `Object.keys(state.cfg.presets)` only. Do not add bare model
   ids to the select; bare ids are an API-level convenience, the UI keeps
   steering users to presets. (Explicit no-op so the implementer does not
   "improve" this.)
5. Debug panel: new row directly after the Backend row:
   `<div class="debug-row"><span class="debug-label" data-icon="box">Model</span>
   <span class="debug-value" id="dbgModel">-</span></div>`.
   `applyServerDebugMetrics()` sets `setDebug("dbgModel", best.routedModel ||
   best.model || "-")`; `clearDebug()` resets it to "-".
6. Request-history table: UNCHANGED (`e.model` = requested id; optionally add
   `title` attr showing routedModel, not required).
7. Everything else (chat, streaming, localStorage, reasoning dual-key parsing,
   compare, templates, shortcuts, admin actions, stats, config viewer) is
   untouched.

---

## 10. Test plan (test.mjs)

Existing tests: all 63 must pass. In-place edits ONLY where noted:
- y1..y9 rewritten against `normalizeConfig`/`candidates`/`buildPayload`
  (equivalent coverage: unknown-backend member dropped; weighted rng bands +
  ordered-after-pick; multi-model weighted tiebreak; unknown id -> null;
  failover declared order; affinity hashing + same-session stickiness;
  implicit bare-model addressing returns single-model ordered list; provider
  missing upstream dropped).
- t2 extended: assert `presets["deepseek-v4-flash"].models` normalized AND
  derived `members` present as `{backend, weight}` objects.

New tests (z-series; use `startRouterCfg` with new-shape configs, mock
backends capture payloads):

1. z1 synB shape: presets-era config (backend members, MIXED upstreams: one
   member backend has a different `model` field) -> `/api/config` shows ONE
   model named `<presetId>` with providers in member order carrying
   per-provider upstreams; preset `models: [{model, weight: 1}]`; strategy
   preserved; derived `members` present.
2. z2 synB affinity preserved: same config; 3 requests with one session ->
   all served by the same backend; x-router-backend consistent.
3. z3 synB failover preserved: failover preset, first member mock 500 ->
   second serves; forwarded payload `model` = second backend's upstream
   string.
4. z4 synB weighted collapse: weighted members -> normalized strategy
   "affinity" (assert via /api/config), providers in declared order.
5. z5 implicit model: new-shape config with a model id that is NOT a preset
   id; request by bare model id -> 200, x-router-backend set, forwarded
   payload model = provider upstream.
6. z6 /v1/models dedup + order: config with presets [p1] and models [p1, m2]
   -> ids exactly ["p1", "m2"] (collision listed once, presets first).
7. z7 preset-over-models failover: preset failover [m1, m2]; m1's single
   provider mock always 500; request -> m2's provider serves; payload model =
   m2's upstream; x-router-backend = m2's backend.
8. z8 model-level affinity: model with 3 providers, affinityPool 3; 4 requests
   same session -> 1 distinct backend; (author the test with session names
   verified to spread so a second session asserts a different backend where
   the hash dictates it).
9. z9 layered params full stack: global {temperature}, backend {top_p},
   provider {frequency_penalty}, model {reasoning_effort}, preset {max_tokens:
   512}, body {max_tokens: 33} -> forwarded payload asserts each: temperature
   from global, top_p from backend, frequency_penalty from provider,
   reasoning_effort from model, max_tokens 33 (body wins over preset).
10. z10 same-key precedence + reserved model: preset {temperature: 0.2},
    model {temperature: 0.5} -> forwarded 0.2 (preset > model); body
    {temperature: 0.9} -> 0.9. Any params layer containing `model` -> ignored;
    forwarded model is the upstream string.
11. z11 dialect order: provider with dropParams ["reasoning_effort"] AND
    paramMap {"top_p": "topP"}; body sends reasoning_effort + top_p ->
    forwarded payload has neither reasoning_effort nor top_p, has topP.
    Backend-level dropParams (legacy) still strips (covered by existing x2;
    no duplicate needed).
12. z12 validation + empty 404: config with unknown strategy (defaults),
    preset referencing unknown model (dropped) leaving it empty -> request to
    that preset -> 404 with `preset '<id>' has no valid models`.
13. z13 routedModel history: preset id != model id; successful request ->
    newest /api/history entry has model == preset id AND routedModel == model
    id.
14. z14 shared-backend dedup: preset failover [m1, m2]; BOTH models' sole
    provider is the same backend with different upstreams; request -> mock
    records exactly 1 hit and payload model = m1's upstream (first occurrence
    wins; m2 never attempted).
15. z15 manual cool through the model layer: one model, two providers; sticky
    cool the session's primary backend; request -> served by the other
    provider; success does NOT clear the manual cool (/health manual still
    true); uncool restores. Complements u/w with the model layer in between.
16. z19 preset-of-presets expansion: preset `super` = failover [glm, os] where
    glm and os are themselves presets (failover/affinity) -> request routes to
    the first EXPANDED model; /v1/models unchanged (presets first, then
    models); derived `members` on `super` includes all four backends.
17. z20 cycle termination: presets a -> [b], b -> [a, m20b] (cycle A->B->A) ->
    request to `a` succeeds via m20b, no hang/crash; the cycle path warns once
    and is cut.
18. z21 ordering rules (unit): failover parent over nested presets preserves
    declared expansion order; weighted parent applies the top-level weight of
    the nested preset position; a nested preset's own strategy is ignored for
    ordering.
19. z22 unknown id in nesting: a preset whose models list contains an unknown
    id (dropped at normalize) inside a nested preset context; valid sibling
    models still route.

Expected total after preset-of-presets: 80 + 11 = 91 passing asserts (63
legacy names, y-block, z1..z18 = 83 baseline, plus z19..z22 = 8 more).

---

## 11. Verification checklist (super agent)

Fresh-context verification, NO knowledge of implementation effort. Read this
document first, then verify. Report PASS / FAIL / UNCLEAR per item with
evidence; never guess PASS. Never echo API key values;
apiKeyEnv names only.

1. Static: `node --check server.mjs` clean; `package.json` dependency fields
   untouched (zero new deps); `node test.mjs` exits 0 with 78 passes
   (63 legacy [y-block rewritten] + z1..z15), 0 failures.
2. Live boot (`node server.mjs` or start.cmd; it must bind 127.0.0.1:8787):
   `GET /health` 200 + backends array; `GET /v1/models` 200 listing EXACTLY
   ["deepseek-v4-flash", "deepseek-v4-flash-alt", "deepseek-v4-flash-direct"]
   in order (both model ids collide with preset ids and are deduped; any extra
   or missing id is a FAIL); `GET /api/config` 200 containing
   `models.deepseek-v4-flash.providers` (4 entries, commandcode upstream
   "deepseek/deepseek-v4-flash"), presets with `models` arrays + derived
   `members`, and NO api key values anywhere in the body.
3. Live routing: non-stream POST /v1/chat/completions {model:
   "deepseek-v4-flash"} -> 200; x-router-backend in {go-primary, go-alt,
   commandcode, direct}; newest /api/history entry: model ==
   "deepseek-v4-flash", routedModel == "deepseek-v4-flash", ttftMs/
   promptTokens/completionTokens/genMs/tps present (some may be 0 depending
   on upstream; fields must EXIST).
4. Implicit model + unknown id live: temporarily add a model
   "probe-model" {providers: [{backend: "direct", upstream:
   "deepseek-v4-flash"}]} to config.json, wait for hot-reload (or restart),
   request {model: "probe-model"} -> 200 via direct, history routedModel ==
   "probe-model"; /v1/models temporarily includes it after the three presets.
   Remove it, hot-reload, confirm /v1/models back to the three ids. Unknown id
   (e.g. "nope-1") -> 404 model_not_found with message `model 'nope-1' not
   found`.
5. Layered params: z9/z10/z11 unit tests are AUTHORITATIVE for merge order
   (upstream payloads cannot be inspected live without a logging backend; do
   NOT claim live payload verification). Live sanity only: temporarily add
   top-level `"params": {"temperature": 0.7}` to config.json, hot-reload,
   /api/config reflects it, one request still 200, revert.
6. Reasoning regression (live): streamed request with reasoning_effort high
   through a reasoning-capable provider yields reasoning deltas (delta.reasoning
   or delta.reasoning_content in the raw SSE). If every reasoning-capable
   backend is cooling at verification time, mark UNCLEAR-live and cite the
   mock-level evidence instead.
7. Manual cool regression (live): sticky-cool commandcode (no forMs);
   /health manual=true; request deepseek-v4-flash still 200 via another
   provider; /health manual STILL true after the success; uncool restores.
8. Backward compat: back up the migrated config; swap in the DESIGN-PRESETS
   era live shape (backends WITH model fields incl. commandcode
   "deepseek/deepseek-v4-flash", presets with backend members and
   affinityPool 3); hot-reload; /v1/models UNCHANGED (same three ids, same
   order); same-session request routes and sticks to one backend across 3
   calls; /api/config shows the synthesized model with 4 providers and
   per-provider upstreams. Restore the migrated config; byte-compare against
   the section 7 target (must be identical).
9. No-regression scan: router-history.jsonl line count grows with requests;
   prefix intact (POST /chat/completions without /v1 -> 404 not-found shape);
   rapid double config touch (hot-reload twice) does not crash; /admin/backend
   cool/uncool round-trip works; /admin/reset-health clears cooling.
10. UI (after phase 2): `GET /` serves HTML containing `id="modelsBody"` and
    `id="dbgModel"`. In a real browser (Playwright MCP if available): presets
    table shows the Models column; Models table renders 2 rows with
    "go-primary (deepseek-v4-flash)" style provider pairs; a playground send
    populates the debug Model row with the routed model; compare mode still
    runs; no console errors. If no browser is available, verify the served
    HTML contains the new ids and mark browser-only sub-checks UNCLEAR.

---

## 12. Out of scope

- Response-key rewriting by the router (UI handles both reasoning key shapes).
- Cross-family param inference beyond explicit provider paramMap.
- Backend-level paramMap (provider-level only).
- Dynamic model CRUD via API.
- Sticky-weighted provider balancing (affinity covers sticky LB).
- Per-model spend caps driven by meta.pricing.

---

## 13. Timeout semantics (connect vs idle)

The router applies TWO independent timeouts per backend, split at the response
boundary so a long-but-alive stream is never killed by a wall clock:

| Field | Bound | Default | Fires when |
|---|---|---|---|
| `timeoutMs` / `connectTimeoutMs` | header wait (TTFT) | 45s | the upstream has not sent response headers within the bound |
| `idleTimeoutMs` | body silence | 120s | no bytes arrive for that long during streaming/body read |

Resolution: `backend.connectTimeoutMs ?? backend.timeoutMs ?? cfg.connectTimeoutMs ?? cfg.timeoutMs ?? 45000`; `backend.idleTimeoutMs ?? cfg.idleTimeoutMs ?? 120000`.

- **forward()**: the fetch uses an AbortController armed only for the header
  wait. The moment headers arrive the timer is cleared, so the body is NOT
  bound to the connect wall clock. (Previously `AbortSignal.timeout(...)`
  covered the whole exchange; undici also aborts the body on that signal, so
  SSE relays were cut exactly at `timeoutMs`.)
- **relay()** (stream + non-stream): an idle watchdog re-arms on every received
  chunk/segment; if `idleTimeoutMs` of true silence elapses, the upstream body
  is aborted (relay error, client gets an end without `[DONE]`). A slow stream
  that keeps emitting resets the timer every chunk and runs to completion.
- Non-stream bodies use the same stall semantics via a reader-based read
  (`readBodyWithIdle`), preserving boundedness without a wall clock.

Motivating evidence (2026-08-22/23): `router.log` recorded 98 "relay error: The
operation was aborted due to timeout" + 34 "terminated" + 8 "Premature close".
History showed 37 go-alt2 SSE streams with ttft+gen clustered in [42s,47s]
(45s default) and 13 commandcode streams in [57s,62s] (its 60s timeout),
clustered overnight rather than scattered; these were the router's own
wall-clock cuts, distinct from genuine upstream `terminated` drops. Tests z23
(slow-alive stream completes past a tiny timeoutMs) and z24 (stalled stream cut
by idle watchdog) pin the new behavior.

---
