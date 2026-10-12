# Switchblade Config v3 — Models, Deployments, Presets

Date: 2026-09-19. What is being built: a redesign of Switchblade's conceptual config model — models own deployments, presets become ordered-fallback routing bundles with visibility control, cooling keys per deployment — implemented by evolving the existing zero-dependency codebase in place (path A), migrated through a bridge, and deployed to production with the served id set unchanged.

## Context and problem

Switchblade (`C:\dev\local-model-router`, zero-dependency Node ESM, `server.mjs` ~3,500 raw lines + `index.html` ~5,300 raw lines, behavior pinned by `test.mjs` at 374 passing tests) serves OpenAI-compatible chat from a three-layer config: backends (upstream accounts), models (providers per model), presets (routing bundles). The operator reports the model is unintuitive when adding things and does not read like the system behaves. Verified defects backing this (all sites re-verified by grep during the 2026-09-19 architecture assessment):

- Cooling is keyed by backend id only (`server.mjs:453`, `markFailure` :550-582, `markSuccess` :584-590): a rate-limited paid model cools every model sharing the account (free models go dark), and any model's success clears the whole account's cooling (rate-limited models re-arm and error again).
- 19 model/preset id collisions in the config produce "id 'X' is both a model and a preset; the preset wins" warnings on every save (`server.mjs:402`) — presets exist mainly to make models addressable.
- Save/load validation asymmetry: `validateConfig` (:305-444) is structural-only; `normalizeConfig` warn-drops mistyped `retry`/`params` (:1339-1364); `meta.pricing` is checked nowhere (`computeCost` null-guards at :2220-2226); a non-integer `affinityPool` passes save and crashes the process at request time (`candidates` :1723-1724).
- The structured editor rebuilds entries and silently drops fields it does not display (`index.html` cfgModalCollect ~:4900-5022; providers mapped to `{backend, upstream}` only at :4808).
- `/api/config/reset` bypasses the port/host guard and the lost-update guard (:3113-3145).

Constraints (fixed): zero npm dependencies; effectively two source files; the live production config is a single bind-mounted file written in place (`writeConfigAtomic` :278-295); config changes hot-reload via fs.watch, never restart; external consumers (dsh `settings.yaml`, OpenCode `opencode.jsonc`) pin served model ids; UI stays single-file, zero external assets, phone-friendly; every behavior change ships with a test in `test.mjs`.

Direction already decided with the operator: requirements baseline v2 approved; path A (evolve in place, not rewrite) chosen per the architecture assessment; the concept is named **preset** (not "chain") with an extensible `strategy` field; exposure control is required.

## Goals and non-goals

Goals:

- G1. One id is one thing: a model or a preset, never both — enforced by the schema, not by warnings.
- G2. A model is directly addressable and load-balances its deployments with unchanged session-affinity hashing; a deployment can be `backupOnly` (tried only after primaries fail).
- G3. Presets are ordered-fallback bundles by default (`strategy: "fallback"`, the only value in v3; the field exists so future strategies need no schema change). Presets-of-presets stay, with cycle detection preserved (`server.mjs:1601-1612`).
- G4. Visibility: every preset and model carries `exposed` (default `true`); `exposed: false` entries stay routable but are omitted from `/v1/models` and all UI pickers.
- G5. Cooling keyed per deployment `(backend, upstream)`; account-wide only for auth, billing, and weekly-limit classes; server/timeout/network failures skip the whole backend for the current request only; a success never clears another deployment's state.
- G6. Dialect settings have two obvious scopes: backend-level (whole platform) and deployment-level (one model on one account), with the documented union precedence kept and test-pinned.
- G7. One shared validation used by save, load, and reset, with typed checks and field-path error messages; a config that fails validation at load keeps the previous config running.
- G8. The editor preserves fields it does not display (merge, not rebuild), gains inputs for the new fields, and every UI model list derives from the same rule as `/v1/models`, honoring exposure.
- G9. Migration via bridge: both shapes readable during transition; the live config converts once; the served id set is identical across the switch; legacy synthesis code is then removed.

Non-goals: multi-tenancy; any change to the served API shape (`/v1/chat/completions`, `/v1/models` stay OpenAI-compatible); a curated model catalog or remote parameter feed; escalation ladders and probe-based cooldown recovery (deferred, noted for the future); engine rewrite beyond the cooling re-key; preset strategies beyond `fallback` (door kept, nothing implemented); renaming the `backends` JSON key; a history id map (no model ids are renamed by this design).

## Design

### Config shape (v3)

```json
{
  "backends": [
    { "id": "go-primary", "baseURL": "https://...", "apiKeyEnv": "GO_PRIMARY_KEY",
      "timeoutMs": 120000, "translateDeveloperRole": true,
      "params": {}, "dropParams": ["thinking"] }
  ],
  "models": {
    "deepseek-v4.1-flash": {
      "deployments": [
        { "backend": "go-primary", "upstream": "deepseek-v4.1" },
        { "backend": "go-alt", "upstream": "deepseek-v4.1", "weight": 2 },
        { "backend": "direct", "upstream": "deepseek-v4.1", "backupOnly": true,
          "dropParams": ["reasoning_effort"] }
      ],
      "params": {},
      "retry": { "maxRetries": 2, "baseMs": 500, "maxMs": 8000, "multiplier": 2, "totalMs": 20000 },
      "meta": { "label": "DeepSeek 4.1 Flash", "contextWindow": 163840,
                "pricing": { "inputPerM": 0.26, "outputPerM": 1.4, "cacheHitInputPerM": 0.02 } }
    }
  },
  "presets": {
    "glm-tier": { "strategy": "fallback", "models": ["glm-5.3", "glm-5.3-flash"],
                  "exposed": false },
    "super": { "strategy": "fallback", "models": ["glm-tier", "os-alpha"],
               "meta": { "tier": "super", "whenToUse": "hard tasks" } }
  }
}
```

`backends[]` — unchanged concept and JSON key: `id` (unique), `baseURL`, `apiKeyEnv`; optional `timeoutMs`, `translateDeveloperRole`, `params` (object), `dropParams` (string array, platform-wide). Legacy-only `model` field remains readable during the bridge era, removed at legacy cleanup.

`models{}` — key is the served model id. Value:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `deployments[]` | array | required, non-empty | The model as served by each account |
| `deployments[].backend` | string ref | required | Must reference an existing backend id |
| `deployments[].upstream` | string | required, non-empty | The upstream model string sent as `payload.model` |
| `deployments[].weight` | integer >= 1 | 1 | Relative pick weight among primaries |
| `deployments[].backupOnly` | boolean | false | Never a primary; tried after all primaries fail |
| `deployments[].dropParams` / `paramMap` / `params` | as today | absent | Dialect handling scoped to this account+model |
| `exposed` | boolean | true | false = internal: routable, hidden from `/v1/models` and UI pickers |
| `params` | object | absent | Model-level request defaults (merge layer kept) |
| `retry` | object | absent | Per-model retry budget; fields `maxRetries`, `baseMs`, `maxMs`, `multiplier`, `totalMs`, all numbers |
| `meta` | object | absent | UI-only: `label`, `contextWindow`, `pricing{inputPerM, outputPerM, cacheHitInputPerM, offPeak}` |

`presets{}` — key is the served preset id. Value:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `strategy` | string | `"fallback"` | Only `fallback` exists in v3; unknown values warn and fall back. Field kept so future strategies need no schema change |
| `models[]` | string refs | required | Ordered fallback: model ids or preset ids (nesting, cycle detection) |
| `exposed` | boolean | true | false = internal: routable, hidden from `/v1/models` and UI pickers |
| `meta` | object | absent | UI-only: `tier`, `whenToUse`, `label` |

Removed concepts: `affinityPool` (replaced by primaries vs `backupOnly`), preset `strategy` values `affinity`/`failover`/`weighted`, per-ref `{model, weight}` objects (weights live on deployments), preset-level `params` (presets route, they do not rewrite payloads), identity presets (a preset whose only ref is the same-named model — the model is directly addressable). Preset-level `params` removal collapses the merge chain to `global < backend.params < deployment.params < model.params < request body` (deployment params occupy provider params' old slot).

### Routing semantics

- Primaries = deployments with `backupOnly` false. Session affinity picks the primary: `hashStr(sessionKeyOf(req)) % primaries.length` — `sessionKeyOf` and `hashStr` (`server.mjs:1558-1570`) are unchanged. Order-compatibility is conditional: for a model whose live `affinityPool` is present and equals its provider count, v3's candidate order is byte-identical to today's for the same session key (pinned by existing tests); conversion of `affinityPool` N < provider count reproduces today's order by marking `providers[N..]` `backupOnly: true`; an absent `affinityPool` (today: strict declared order, pool 1) deliberately becomes load-balanced, and the converter emits a warning naming each such model (zero instances in the repo mirror — all 20 live models set the field explicitly).
- The weighted pick is sticky, not rotating: the primary index is derived from the same session hash mapped through the cumulative `weight` intervals of the primaries in config order, so a session key always resolves to the same primary; when every weight is 1 this is exactly `hash % primaries.length`. This replaces the preset-level `weighted` strategy.
- Attempt list per request: affinity-ordered primaries, then `backupOnly` deployments in config order, expanded through preset nesting as today.
- Dedup per request: each `(backend, upstream)` pair is tried at most once. After a 5xx, timeout, or transport failure from a backend, that backend's remaining deployments are skipped for the current request only (per-request backend skip — FreeLLMAPI's pattern), instead of burning a hop per deployment.
- `/v1/models`: exposed preset ids in config order, then exposed model ids in config order. Ids are unique across both namespaces (G1), so each served id appears exactly once. During the bridge era the legacy collision rule (`server.mjs:2951-2964`) still governs old-shape configs.

### Cooling model

Health state splits into two maps:

- Deployment health, keyed `"<backendId>::<upstream>"`: classes `server` (5xx/network/timeout, 30 s base / 5 min cap), `rate` (generic 429, 30 s base / 10 min cap), `client` (no cooling). Exponential backoff per class exactly as today (`backoffFor` :455-460, `markFailure` :550-582).
- Account health, keyed `backendId` (the existing map): classes `auth` (401/403, 1 min / 30 min), `billing` (402 and the billing-error body regex, 30 s / 5 min), `weekly` (GoUsageLimitError pin, `fails: 100`). Weekly parsing and the `weeklyDefaultMs` override keep current behavior pending the explicit keep/fix decision (Open question OQ2).
- A failure marks exactly one map entry. A success clears only the deployment that served it; it never clears account-wide state or a sibling deployment's state. Manual cools remain sticky and excluded from all-cooling fallback (`markSuccess` guard :584-590, exclusion :1902-1921).
- Attempt selection skips a deployment when its deployment health is cooling OR its account health is cooling. The all-cooling last resort keeps today's semantics exactly: only manual cools are excluded (`server.mjs:1902-1921`); automatic account-wide states still participate.
- **Recovery is authoritative over this draft, and it is scope-local.** Where the two
  disagree, the shipped provider-recovery policy wins; this section describes what
  v3 needs from cooling, not a second policy. Concretely, the recovery design has
  final say on: the failure classes and their backoffs; that a failure marks
  EXACTLY ONE scope and a success clears only its own; that a route cooldown never
  implies an account cooldown (account cooling needs two DISTINCT routes failing
  inside 60 s); that a manual cool is sticky, never auto-cleared, and never
  escalated into an account outage; that an opaque `400`/`422` is a 30 s
  session-scoped exclusion and NOT a cooldown at all (`upstream_rejection`, no
  evidence the payload was at fault); and that expiry admits exactly ONE probe -
  a lease is claimed synchronously before any network await, so a recovering route
  or account is discovered by one request rather than by every request at once.
  When no attempt is eligible the router returns the structured `503` naming the
  blocking scopes with a `Retry-After` for the nearest finite recovery, and an
  empty pool of manually cooled deployments carries no such header because nothing
  recovers on its own.

  Precedence when more than one scope blocks one candidate: manual cool outranks
  everything (it is an operator decision), then account cooling, then a route
  cooldown, then a probe another request already holds, then this session's
  exclusion. The reported blocker counts are per scope, so an empty pool names what
  actually held it instead of blaming cooling in general.

  **The successful-backup-pin exception.** A deployment that produced usable output
  becomes the first choice for its `(auth scope, session, resolved logical model)`
  group, and that pin SURVIVES the original provider recovering: a recovered primary
  does not reclaim a session that already failed over successfully. This is a
  deliberate exception to ordinary stickiness, and it is bounded in three ways - the
  pin only ever reorders within its own logical-model group, it is skipped the moment
  it is not eligible, and the map keeps at most 10,000 entries with LRU eviction. The
  pin is dropped on failure, on route removal, on an operator cool/uncool/reset (an
  explicit routing decision must never be overruled by learned preference), and on
  process restart, which loses the table. Callers with the shared `no-session` key
  get no pin at all, because one anonymous caller's experience must not steer every
  other caller. A pin never reorders models and never undoes backend dedup.
- `/health` gains a `deployments[]` array per backend entry (id, upstream, state, fails, lastError, manual). `POST /admin/backend` body extends to `{id, upstream?, action: "cool"|"uncool", forMs?}`: without `upstream` it targets account-wide state as today; with `upstream` it targets one deployment.
- History/detail attempt entries add `upstream` alongside the existing `backend` field (additive).

### Validation (one shared validator)

`validateConfig(cfg, current)` (:305-444) becomes the single validator used by save (:3067), reset (:3130), and load (after `JSON.parse`, before any normalize). Rules:

| Check | Class at save | Class at load |
|---|---|---|
| Model id collides with preset id | error (400, field path) | keep previous config, log |
| `deployments` missing/empty; `backend` ref unknown; `upstream` empty | error | keep previous config, log |
| `weight` not integer >= 1; `backupOnly`/`exposed` not boolean; `retry` not the documented object shape; `params`/`paramMap` not objects; `meta.pricing` fields not numbers | error | keep previous config, log |
| Preset `models[]` ref unknown | error | warn + drop ref (legacy-lenient) |
| `strategy` unknown | warning, treated as `fallback` | warning, treated as `fallback` |
| Unrecognized top-level or entry keys | warning (forward compatible) | warning |
| Port/host change | error (RESTART_ONLY check :315-319) | n/a |

Load semantics keep the non-destructive contract: any error-class finding leaves the previous config running and logs precise field paths. At boot there is no previous config to keep, so the ONLY findings that abort startup are those that make the config uninterpretable: an unreadable or unparseable file, a root that is not an object, or a missing `backends` array. Note the precision of that last item: `backends` is the ONLY required top-level key the loader does not synthesize (`normalizeConfig` creates `models` and `presets` itself, even from a non-object), so a missing or malformed `models`/`presets` is non-fatal by construction and pre-presets-era config files must keep booting. Every other error-class finding is logged as a warning and the router starts, exactly as the loader already behaves today for findings it repairs or ignores. The reason is that the validator's error set is the SAVE-side contract and is deliberately stricter than what the loader tolerates (today the loader repairs a non-object `retry`, a bad `maxRetries` and non-numeric retry fields, warns on an unknown strategy or an unknown reference, and leaves an unset `apiKeyEnv` to fail closed per backend); a save-side error the loader would have repaired must never be able to stop a running router. This is not theoretical: the live production config carries `models.glm-5.2.retry` as an empty string, which is a save-side error AND a loader repair, so aborting a boot on it would take the whole router down. At reload, when a previous config exists, ANY error-class finding rejects the reload and the running config is kept. Save returns 400 with the field-path list; warnings return 200 with the warnings array the UI already renders. Reset uses the same validator and, unlike today, the same RESTART_ONLY and baseRevision guards as save (the vacuous `validateConfig(restored, restored)` call is removed). Old-shape snapshots are converted by the bridge converter first; a snapshot that fails conversion is refused with a clear error, never partially applied.

### Editor and UI

- `cfgModalCollect` and `cfgApplyModal` are rewritten as merge-preserve: collection starts from the existing draft entry, applies only the fields the form edits, removes only explicitly cleared fields, and leaves unknown keys untouched. Merge-preserve covers the CHANNEL as well as the keys: a model entry opened with `providers` is stored back with `providers` (see "Shape on save"). The per-deployment form gains `weight`, `backupOnly` inputs and `dropParams`/`paramMap`/`params` textareas; presets gain `exposed` and a read-only `strategy` display ("fallback" in v3).
- Playground model selector and compare checkboxes derive from the same served-id rule as `/v1/models` (exposed presets then exposed models), honoring exposure; the deleted-preset stale-selection fallback posts no empty `model` value.
- After a successful config save the dashboard config state refreshes (Models/Presets tables, config viewer); after an env write, the backend "Key env" name list refreshes. These close the 2026-09-19 audit's UI staleness gaps as part of the editor rebuild.

### Shape on save (authoritative)

Two shapes are live during the bridge era, and a SAVE NEVER MIGRATES BETWEEN THEM.

- The load path is dual-shape and shape-preserving: an old-shape document is
  normalized in memory and served from that normalization, while the file on disk keeps
  whatever shape it was written with. Nothing converts a document behind the operator's
  back `structuralViewForValidation` builds a canonicalized COPY for the validator and
  leaves the document itself alone; `convertLegacyConfig` is a pure, exported function
  that runs only when a migration is asked for.
- The structured editor writes the shape it was GIVEN. A model entry opened with a
  `providers` array is stored back with `providers` (and keeps the `affinityPool` that
  shape owns); one opened with `deployments` is stored back with `deployments`. An entry
  that carries BOTH is treated as legacy and stored as `providers`, because that is the
  array the router reads - providers win over deployments in the converter, so writing
  `deployments` beside them would bury the operator's edit under an array nothing reads.
- A model entry therefore never comes back carrying both arrays, and a no-op save is
  byte-identical to its input apart from the form's own default-omission rule (a value
  equal to a control's default, such as `weight: 1` or `exposed: true`, is not written
  back as though the operator had typed it).
- A LEGACY row's `backupOnly` is DERIVED from the entry's `affinityPool`: rows past the
  pool are the ordered tail. It is display state, not a stored key, so a no-op save of a
  pooled legacy entry does not invent `backupOnly: true` on its tail rows.
- **Served-id stability is a consequence.** Converting one saved model to v3 makes the
  whole document v3 for the validator, and every id that a shadowing identity preset owns
  then collides - 17 of the mirror's ids do - so `/v1/models` changes for ids nobody
  edited. Keeping the document's shape keeps the served list in its order.

Migration is therefore a deliberate act (increment 4's explicit conversion), never a side
effect of opening a tab and pressing Save.

### Error handling

Covered above: precise field paths on every validation message; non-destructive load; convert-or-refuse snapshot restore; no partial writes (`writeConfigAtomic` untouched).

## Interfaces

- `validateConfig(cfg, current)` — single validator, exported for tests; error vs warning findings distinguishable programmatically.
- `convertLegacyConfig(raw)` → `{ config: <v3>, warnings: [string] }` — the bridge converter, exported for tests and used by load, save, and reset during the bridge era.
- Config v3 field names exactly as the tables above; `backends` key unchanged; served ids unchanged.
- `POST /admin/backend` body `{id, upstream?, action, forMs?}`; `GET /health` adds `deployments[]` per backend.
- `/v1/models`, `/v1/chat/completions`, `/api/config`, `/api/config/raw`: response shapes unchanged; `/api/config` returns the normalized v3 config.

## Testing and verification

Gate for every increment: `node --check server.mjs` clean and `node test.mjs` tail reads "N passed, 0 failed" (374 today). New tests in `test.mjs`:

- T1 Validation: `retry: ""`, `weight: "abc"`, non-numeric `meta.pricing` each rejected at save with field paths; at load the previous config keeps running.
- T2 XOR: a model and preset sharing an id fails save with 400 and keeps the previous config at load.
- T3 Exposure: `exposed: false` preset and model are routable (a chat request resolves) but absent from `/v1/models`.
- T4 Backup-only: never chosen as primary; tried after primaries exhaust; affinity candidate order unchanged for models whose pool equals the provider count (existing affinity tests assert this), and a bridge test asserts the `affinityPool` N < provider count → `backupOnly` conversion reproduces the same order.
- T5 Cooling granularity: a 429 on `(backendA, upstreamX)` cools only that pair; a success on `(backendA, upstreamY)` leaves X cooling; a 5xx on backendA skips backendA's remaining deployments for that request only; weekly/auth/billing remain account-wide; manual cools sticky; a weight case pins stickiness (a session key always resolves to the same primary) and rough weight-ratio distribution across distinct session keys.
- T6 Bridge golden test: a snapshot of the live homelab config captured at increment 4 (the repo mirror is a stand-in only; it held 20 models / 25 presets / 19 collisions, zero weighted presets, and a single per-ref weight of 1 — inside identity preset kimi-k3, dropped wholesale by conversion — as of 2026-09-19) converts with exactly the expected warnings — identity presets dropped, plus weighted-preset / ref-weight / absent-`affinityPool` / preset-`affinityPool` warnings only if the snapshot contains any; served id set of the converted config equals the original's `/v1/models` set.
- T7 Reset: an old-shape snapshot converts; a corrupt one is refused without touching the live file.

Editor verification (no unit harness exists for the single-file UI): live dashboard round-trip of `glm-5.3-flash` preserving all four providers' `dropParams`; Playground lists standalone models; phone-width visual check. Production verification per deploy: `/v1/models` id-set diff against pre-deploy, `/health` summary, one real chat call through the dsh route.

## Rollout and migration

Six increments, each one commit/PR, each shippable and revertible, suite green after each:

1. **Shared validation** — extend `validateConfig` to the full typed rule set; wire into save, reset (adding the RESTART_ONLY + baseRevision guards), and load. No schema change; old configs validate as today plus the new type checks. Kills the crash class and the asymmetry.
2. **Editor merge-preserve + UI lists** — rewrite `cfgModalCollect`/`cfgApplyModal`; Playground/compare lists from the served rule; envNames and post-save refreshes. No schema change.
3. **Schema v3 bridge** — add `deployments`/`weight`/`backupOnly`/`exposed`/`strategy: "fallback"` reading, `convertLegacyConfig`, dual-shape load (old shape normalizes in memory; a save writes back the shape it was given, see "Shape on save" — the bridge era does not migrate on save). The editor gains the v3 inputs in the same increment (weight, backupOnly, exposed, per-deployment dialect textareas). Deploy protocol: capture an old-shape config snapshot to the host BEFORE deploying, since any dashboard save from this point writes v3. Live config otherwise untouched.
4. **Convert the live config** — over SSH: refresh/verify the pre-bridge old-shape snapshot captured in increment 3, POST the converted config, verify the served id set is identical and both consumers still resolve their pinned ids; compute the T6 golden expectations from the captured snapshot.
5. **Cooling re-key** — deployment/account health split, per-request backend skip, `/health`/`/admin/backend` extensions. Depends on 3.
6. **Legacy removal** — after a soak period (set at increment-6 review): remove legacy synthesis (paths A/B :1316-1333 and :1408-1462, identity-preset synthesis :1469-1473, carve-outs :1485-1490 and :1629-1633), the old-shape reader, the legacy `providers` key handling, and preset-level `affinityPool` handling (test.mjs:691 pins today's semantics); update `DESIGN-3LAYER.md` (v3 contract), `AGENTS.md`, and the switchblade skill runbook.

Bridge conversion rules (`convertLegacyConfig`): `providers[]` map 1:1 to `deployments[]` in order; `affinityPool` N less than the provider count marks `providers[N..]` `backupOnly: true` (none if absent or equal); identity presets are dropped (the model remains directly addressable); any remaining model/preset id collision renames nothing — conversion refuses with a clear error naming the ids (manual resolution required); preset strategies map to `fallback` preserving ref order, with a warning listing every preset whose behavior changes from affinity-spread to ordered-failover; per-ref weights are dropped with a warning; preset-level `affinityPool` is dropped with a warning if present (zero live usage, `test.mjs:691` pins today's semantics); `exposed: true` everywhere. Check ordering is fixed for deterministic golden runs: identity-preset drop first, then per-ref-weight and preset-`affinityPool` warnings, then strategy mapping.

Load pipeline and untouched blocks: at load, old-shape input flows `convertLegacyConfig` → `validateConfig` → normalize (members derivation unchanged until increment 6); v3 input flows `validateConfig` → normalize. The converter and validator carry through, never warn about, and preserve verbatim the top-level blocks outside this redesign — `params`, `backoff`, `missCapture`, `pricing`, `alerts` — and the backend fields `idleTimeoutMs`/`connectTimeoutMs`; genuinely unknown keys still warn (forward compatibility).

Served-list guarantee: the id SET is identical across the switch. One-time order shift: ids freed by identity-preset collapse move from the presets block to the models block of `/v1/models`, so the order can change once at increment 4 (never again). Consumers resolve by id, not position; increment 4's verification step confirms dsh and OpenCode resolve every pinned id after the switch.

Rollback: increments 1-2 revert cleanly (config unaffected). Increment 3 reverts cleanly only if no config save happened while it was deployed — any dashboard save writes v3, which pre-bridge code cannot read (it drops every model entry lacking `providers`); after any v3 write, rollback follows the snapshot path: restore the old-shape snapshot captured in increment 3 + revert code to a pre-bridge revision. After increment 4 the same snapshot path applies. After increment 5, deployment-health state is disposable (rebuilds on the fly); rollback to pre-4 still uses the snapshot path.

## Open questions

- OQ1. Weighted-preset conversion (to ordered fallback with a warning) is decided in the design; the repo mirror contains zero weighted presets and one per-ref weight of 1 inside identity preset kimi-k3 as of 2026-09-19, so the behavior-change warning list is expected empty — confirm against the increment-4 live snapshot that the warning list matches operator intent.
- OQ2. `weeklyDefaultMs` overriding a parsed "Resets in N days" is a known bug (AGENTS.md): keep or fix is decided during increment 5, not silently inherited.
- OQ3. Soak-period length for increment 6 (legacy removal) is set when increment 5 ships.
