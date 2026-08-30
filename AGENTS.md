# AGENTS.md

## Project Overview

Switchblade (repo dir: `local-model-router`) is a **zero-dependency, single-process local model router** for OpenAI-compatible LLM APIs. It load-balances chat requests across multiple upstream accounts (OpenCode Go, DeepSeek, CommandCode, Z.ai, or any OpenAI-compatible base URL) with session-affinity hashing, per-backend health tracking, exponential backoff, and a web dashboard. It runs on `127.0.0.1:8787`, needs no database or container stack, and was designed to be maintained by AI agents: every behavior is pinned by a mock-based test suite that runs in seconds with no keys and no network.

**Two source files, zero npm dependencies.** Do not add packages without a strong reason (see Contributing in README).

### Architecture: three layers

- **backends** — raw upstream accounts: `{id, baseURL, apiKeyEnv, ...}`. No model binding.
- **models** — logical model ids bound to providers: `{backend, upstream}` pairs, an `affinityPool`, optional per-provider `params`/`dropParams`/`paramMap`, optional `meta` (label/contextWindow/pricing, UI-only).
- **presets** — named routing policies over models, exposed to clients as model ids. Clients never address backends or upstreams directly.

Routing is **two-level**: the preset strategy picks an ordered model list; each model then orders its providers by session affinity (`pool = min(affinityPool, providers.length)`, `primary = hash(sessionKey) % pool`). The lists flatten into one attempt list, deduped by backend id (a shared account is never retried twice in one request), and the router retries through healthy members in order.

Strategies (preset level): **affinity** (default, session-sticky spreading), **failover** (strict declared order), **weighted** (weighted-random primary). Presets of presets (tier nesting) work: a preset's `models` array may reference other preset ids, expanded recursively at request time with cycle detection; the top-level preset's strategy governs, nested strategies are ignored.

### Key files

| File | Role |
|---|---|
| `server.mjs` | The whole router (~1660 lines). Starts only when run as a script; exports the internals used by tests. |
| `test.mjs` | Mock test suite — **249 passing assertions**. Spawns real `server.mjs` children on temp configs against in-process mock backends. No keys, no network. |
| `config.json` | Live config; **hot-reloads via `fs.watch` (~300 ms)** — no restart for schema/backend/model/preset/backoff changes. Validate JSON before saving. |
| `.env` | API keys (gitignored). Read once at startup; a restart is needed after edits. Never commit. |
| `.env.example` | Placeholder key names (`KEY=` only — CI fails if any example has a value). |
| `index.html` | Web UI (Dashboard + Playground), single file, inline CSS/JS, zero external assets. |
| `start.cmd` | Idempotent Windows launcher (logon + scheduled-task keepalive). Skips if 8787 is already listening. |
| `DESIGN-3LAYER.md` | **Design contract — single source of truth** for the three-layer schema, two-level routing, layered params, and backward-compat synthesis. Read before changing config semantics. |
| `DESIGN-PRESETS.md` | Presets layer contract: strategy semantics, sticky manual-cool semantics. |
| `.github/workflows/ci.yml` | CI: syntax check + test suite on Node 22/24/25, plus a secret-scan job and a `.env.example`-placeholder check. |
| `docs/UI-PLAN.md`, `docs/VISUAL-PLAN.md` | UI design plans. |
| `router-history.jsonl` | Request history append log (gitignored; override path with `ROUTER_HISTORY`). |
| `router-misses.jsonl` | Bounded cache-miss payload capture (gitignored). Controlled by the `missCapture` config block; homelab path: `/data/history/router-misses.jsonl` (durable named volume); rotates to `.old` at `maxFileBytes`. Use for cache-break forensics (see the switchblade skill's Cache miss forensics section). |
| `api-keys.json` | Issued chat-only keys store (SHA-256 hashes only, gitignored; override path with `LMR_KEYS_FILE`). |

## Setup Commands

Requires Node.js 22+ (CI runs 22, 24, 25; developed on 25 — no Node-25-only APIs).

```bash
git clone <repo> && cd local-model-router
cp .env.example .env        # then fill in the keys you use
npm install                 # no-op: the repo is zero-dependency, keeps lockfile conventions
```

No build step exists. No lint or format tooling is configured (zero-dependency mandate — no eslint/prettier). The only quality gates are the syntax check and the test suite.

## Development Workflow

- **Start the router**: `npm start` or `node server.mjs`. Listens on `http://127.0.0.1:8787` (API prefix `/v1`); web UI at `/`.
- **Config hot-reload**: edit `config.json` — the router picks it up in ~300 ms. A broken JSON write can wedge routing (the reload fails and the previous config stays, but partial writes may be picked up mid-file). Validate before saving:
  ```bash
  node -e "JSON.parse(require('fs').readFileSync('config.json','utf8'))"
  ```
- **`.env` changes**: keys are read once at startup, not at request time; a restart is needed after edits. Keys are referenced from config by NAME only (`apiKeyEnv`); values live in `.env`.
- **Config reload is non-destructive to health state** (health is keyed by backend id and survives reloads). Manual cools survive reloads too.
- **Legacy configs keep working**: `normalizeConfig(cfg)` auto-synthesizes both pre-three-layer eras at every load. Old `models[id] = {backends, affinityPool}` and presets-with-`members` shapes normalize to the three-layer form byte-identically for identical effective configs.

## Testing Instructions

```bash
npm test                    # node test.mjs -> expect "249 passed, 0 failed"
node --check server.mjs     # syntax check (CI also checks test.mjs)
```

- The suite **spins up mock backends plus router instances on temp configs — no real keys, no network**. Safe to run anytime, even while the production router is live on 8787 (test children bind port 0).
- Coverage: health, non-stream + SSE streaming, session affinity, failover/weighted selection, sticky + timed manual cools, fallback exclusion, dialect handling (dropParams/paramMap/developer-role), synthesis from both legacy eras, layered-params merge order and precedence, preset-of-presets nesting (expansion, cycles, ordering), per-model retry/backoff budgets, cache tri-state, history `routedModel`, timeout failover, reasoning-key relay (`reasoning`/`reasoning_content`).
- **Every behavior change ships with a test in `test.mjs`** (project rule, README Contributing). The legacy (`y*`) and three-layer (`z*`) blocks import `server.mjs` internals; integration blocks spawn the real server as a child.
- Known environment quirk: the child-port banner capture can transiently crash with `TypeError: fetch failed ... bad port` (observed once 2026-08-22); a plain re-run passes 249/249. Do not treat a single crash as a regression — re-run first.

## Config Contract (load-bearing — read `DESIGN-3LAYER.md` before touching)

Top-level keys: `port`, `prefix`, `host` (bind host), `masterKeyEnv`, `uiPasswordEnv`, `backends[]`, `models{}`, `presets{}`, optional top-level `params`, `backoff{}`.

- **backends[]**: `id`, `baseURL`, `apiKeyEnv` (env NAME only). Optional: `translateDeveloperRole` (rewrites OpenAI `developer`-role messages to `system`), `params` (request defaults), `dropParams` (legacy dialect strip, unions with provider-level), `model` (legacy default-upstream source), `timeoutMs`.
- **models{}**: `providers[]` = `{backend, upstream}` (resolution: `provider.upstream` -> `backend.model` -> provider dropped with warn), optional per-provider `params`/`dropParams`/`paramMap`; `affinityPool` (default 1; pool 1 degenerates to declared order); optional `params`, `retry` (per-provider transient retry: `maxRetries`, `baseMs`, `maxMs`, `multiplier`, `totalMs`), `meta`.
- **presets{}**: `strategy` (`affinity`|`failover`|`weighted`), `models` (strings or `{model, weight}`; may reference other preset ids), optional `affinityPool`, `params`, `meta`.
- **backoff{}**: per-class base/max (rate limit, server, auth) + `weeklyDefaultMs`.

**Layered params merge** (highest priority last; lower layers only fill keys not already set): `global < backend.params < provider.params < model.params < preset.params < request body`. `model` is reserved — after merging, `payload.model` is ALWAYS the resolved `provider.upstream`. **Dialect order (exact)**: `dropParams` BEFORE `paramMap` BEFORE developer-role translation. Config validation is lenient — invalid strategy defaults to affinity, unknown references are dropped with a console warn, the router never refuses to load.

Known bug (do not "fix" casually): weekly-limit cooling parses `Resets in N days` from the error body but the configured `weeklyDefaultMs` overrides it, so a GO weekly cool always pins for the configured default (7 days). See DESIGN-3LAYER.md.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | health + routing state per backend (`state`, `fails`, `lastError`, `manual`) |
| GET | `/v1/models` | OpenAI-compatible id list: preset ids first, then non-colliding model ids |
| POST | `/v1/chat/completions` | chat completions (stream + non-stream) |
| GET | `/api/stats` | traffic stats (since, uptimeMs, per-backend/per-model) |
| GET | `/api/history?limit=N` | request history (default 100, clamp 1..500); entries carry `routedModel`, `retries`, `retryWaitedMs` |
| GET | `/api/history/detail?t=<timestamp>` or `?call=<callId>` | deep-dive detail for one request (payload, attempts, response summary); memory-only, capped at 100 |
| GET | `/api/config` | normalized config (env NAMES only, never key values); adds derived read-only `members` |
| GET | `/api/auth/status` | dashboard auth state (`passwordSet`, `uiAuthed`); never echoes the password |
| POST | `/api/auth/login` | `{password}` -> sets the UI session cookie; 401 wrong password, 429 throttled (5 attempts / 10s per IP) |
| POST | `/api/auth/logout` | clears the UI session cookie |
| GET | `/api/keys` | issued chat-only keys (`id`, `name`, `createdAt`, `revoked`; never hashes or raw values); master key required |
| POST | `/api/keys` | `{name}` -> issues a chat-only key, raw value returned once; master key required |
| DELETE | `/api/keys?id=<id>` | revokes an issued key; master key required |
| POST | `/admin/reset-health` | reset all cooling states, including manual |
| POST | `/admin/backend` | `{id, action: "cool"\|"uncool", forMs?}` manual cool/uncool |
| GET | `/` | web UI |

Every request gets a short stable `callId` returned in the `x-router-backend`/`x-router-call` headers. History lives in an in-memory ring buffer (500) and appends to `router-history.jsonl` (`ROUTER_HISTORY` overrides path).

## Routing & Health Semantics (quick reference)

- **Failure classes -> cooling**: 429 weekly limit (`GoUsageLimitError`) pins until weekly reset; 429 generic -> exponential backoff 30s base / 10 min cap; 5xx/network/timeout -> 30s / 5 min; 401/403 -> 1 min / 30 min. **400/422 client payload errors are returned immediately and NEVER cool the backend** — except billing failures (body matching `insufficient (credits|quota)|insufficient_quota|out of credits|billing error`), which classify as `billing`: they fail over to the next provider and cool the dry account (30s base / 5 min cap) instead of blocking the pool. Success resets backoff to base.
- **Manual cools are sticky**: `cool` without `forMs` cools until `uncool`; they NEVER participate in the all-cooling fallback and a successful request never clears them. `cool` with `forMs` expires on its own.
- **Empty pool** -> 503 `all available backends are cooling (N manually cooled - uncool to restore)`.
- **Upstream URL construction (exact)**: `backend.baseURL.replace(/\/$/, "") + "/chat/completions"` (trailing slash stripped).
- **`/v1/models` safety property**: the same ids must always appear in the same order; consumers (opencode.jsonc, dsh) depend on it.
- Session key resolution: `x-session-affinity` header -> `x-session-id` -> request `user` -> `no-session`.

## Code Style

- **Zero npm dependencies. ESM everywhere** (`"type": "module"`, `.mjs` files). No lint/format tooling — match surrounding style by hand.
- Plain Node built-ins only (`node:http`, `node:fs`, `node:path`), no frameworks. 2-space indent, single quotes, semicolons, template literals for string building.
- Comments are rare and explain *why*; the config contract and synthesis paths are the most commented regions for a reason.
- New endpoints/features must stay in `server.mjs` (or `test.mjs` for tests) unless the design docs say otherwise; `index.html` is the single-file UI.

## Deployment Topology

**Two routers run simultaneously**, as independent instances with separate configs, keys, and histories; changes to one never apply to the other.

- **Local (dev/test)**: `http://127.0.0.1:8787`, `node server.mjs` from this repo, launched by `start.cmd` (Windows logon shortcut) and the scheduled task "LocalModelRouter keepalive". Used for development and the mock test suite. Idle in daily operation.
- **Production (homelab)**: the Coolify container `a8dyidd8o8id2se9czgauem0-134810439826` on `192.168.68.69:8787`, managed via SSH/Coolify. Both dsh and OpenCode route to the HOMELAB instance (baseURL `http://192.168.68.69:8787/v1`), not the local one.

Production container details:

- Live config is `/app/config.server.json` (env `ROUTER_CONFIG`), a bind mount from `/data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json`. `/app/config.json` inside the container is UNUSED.
- Durable request history is the named volume `/data/history/router-history.jsonl` (env `ROUTER_HISTORY`).
- Keys: the container reads `/app/.env` at startup. That file lives in the container filesystem and is lost on a Coolify image rebuild; durable keys belong in the Coolify application env vars.

## Build and Deployment

No build step — `server.mjs` runs directly on the system Node (22+/25).

- **Local instance (dev/test)**: `start.cmd` launches `node server.mjs` at Windows logon (Startup shortcut) and a scheduled task "LocalModelRouter keepalive" restarts it if down. It is idempotent: exits 0 if 8787 is already listening. Restart pattern: kill the port owner (`netstat -ano | grep ":8787.*LISTENING"`, `taskkill /PID <pid> /F`), then `nohup node server.mjs > router.log 2>&1 < /dev/null &` from Git Bash. This instance is for development and the mock test suite only; it is idle in daily operation (production is the homelab container, see Deployment Topology).
- **Production instance (homelab)**: the Coolify container `a8dyidd8o8id2se9czgauem0-134810439826` on `192.168.68.69:8787`, managed via SSH/Coolify. Both dsh and OpenCode route here, not to the local machine. Container paths, history, and key handling are in Deployment Topology above.
- **Production deploy protocol (config changes)**: connect to the host -> backup the host config -> diff/merge the fetched host config (never overwrite it wholesale with the repo config; the host file carries `host: 0.0.0.0`) -> ship the LF-normalized file via scp -> install host-side (`docker cp` INTO the bind mount fails with "device or resource busy") -> md5-verify host vs in-container paths -> verify `/api/config` and `/health` after ~3 s. Config changes hot-reload in ~300 ms; `.env`, `host`, and code changes need `docker restart a8dyidd8o8id2se9czgauem0-134810439826`. Full step-by-step: the `switchblade` dsh skill runbook `C:\Users\silve\.dsh\skills\switchblade\references\add-provider.md`.
- **CI** (`.github/workflows/ci.yml`): `node --check server.mjs && node --check test.mjs`, then `node test.mjs` on Node 22/24/25; a separate secret-scan job greps tracked files and fails on secret-like patterns, and verifies `.env.example` contains placeholders only.
- **Consumers of this service**: OpenCode (`~/.config/opencode/opencode.jsonc` local-router provider + tier files) and dsh (`~/.dsh/settings.yaml` + `cordis.patch.yml`) both point at the PRODUCTION homelab instance via baseURL `http://192.168.68.69:8787/v1`, not the local dev instance. If you change model/preset ids or `/v1/models` ordering, verify those consumers still resolve.

## Security Considerations

- Keys live ONLY in `.env` (gitignored, `.env.*` ignored except `.env.example`). `config.json` references env NAMES (`apiKeyEnv`) — never key values.
- `/api/config` and the detail endpoint must never echo key values (this is enforced by tests).
- Do not commit `router-history.jsonl` (request payloads) or `*.log`.
- Keep `.env.example` placeholder-only: `KEY=` with no value (CI enforces this).
- Dashboard auth: `uiPasswordEnv` names the env var holding the UI password (`LMR_UI_PASSWORD`); unset keeps the UI open. Login is throttled (5 attempts / 10s per IP); the session cookie is `HttpOnly` + `SameSite=Strict`.
- `masterKeyEnv` gates `POST /v1/chat/completions`, `/admin/*`, and `/api/keys`; it fails closed if the named env var is empty. Issued keys are chat-only — they never unlock `/admin/*` (admin stays master-key-only).
- `api-keys.json` (issued-key store, SHA-256 hashes only) and `*.json.tmp` (atomic-write temp files) are gitignored.
- Read-only endpoints (`/health`, `/v1/models`, `/api/stats`, `/api/history`, `/api/config`, `/api/auth/*`, `/`) stay open.

## Pull Request Guidelines

- **Commit messages**: Conventional Commits — `feat:`, `fix:`, `refactor:`, `perf:` (see `git log`). One logical change per commit.
- **Before pushing**: `node test.mjs` must pass 249/249 and `node --check server.mjs` must pass. CI re-checks on Node 22/24/25 plus secret scan.
- **Project rules (README Contributing)**: keep it zero-dependency (no new npm packages without a strong reason); every behavior change ships with a test in `test.mjs`; the config contract and its synthesis paths are load-bearing — change them only with a documented design note (update `DESIGN-3LAYER.md`).

## Troubleshooting

- **Router won't start / port in use**: find and kill the owner (`port 8787`), then relaunch. Never `taskkill /F /IM node.exe` (kills every Node process).
- **Config edit did nothing**: JSON was probably invalid — the reload failed and the previous config stayed. Validate before saving.
- **Model 404s**: `model_not_found` means the id is not a configured preset/model. Check `/v1/models`. An empty preset returns 404 `preset '<id>' has no valid models`.
- **Backends all "cooling"**: check `/health` — weekly-limit pins (GO accounts, `fails: 100`) are normal and last the configured reset window; use `/admin/reset-health` only after verifying upstream limits. Manual cools require `uncool`.
- **MSYS/Git Bash gotchas**: use forward-slash paths (`C:/dev/...`); `netstat`/`taskkill` need `MSYS_NO_PATHCONV=1`; backgrounding a server needs all three redirects (`> log 2>&1 < /dev/null &`) or the shell hangs.
- **Stale docs**: README's assertion count (100) lags the actual suite (183). `SPEC.md` is referenced by README and DESIGN docs but does not exist in the repo — rely on `DESIGN-3LAYER.md` as the contract source of truth.

## Additional Notes

- This repo is the machine-local replacement for homelab LiteLLM for the deepseek-v4-flash family; GLM/vision/free historically stayed on LiteLLM, but the live config now also routes glm/qwen/mimo/muse/laguna through this router (7 backends: go-primary, go-alt, go-alt2, go-alt3, direct, commandcode, zai; 13 models, 14 presets as of 2026-08-26). Config evolves — never assume the model/preset list; read `config.json` or `/api/config`.
- **Cache forensics**: dashboard rows carry per-call `cacheHitPct`; miss payloads land in `router-misses.jsonl` (homelab: `/data/history/router-misses.jsonl`). Warm calls sit below 100% due to 256-token block granularity (ceiling = `floor(promptTokens/256)*256/promptTokens`); a cold first call reports null, not 0. Payload-diff method and known break patterns are documented in the switchblade skill's Cache miss forensics section.
- The companion operating playbook for this repo is the `local-model-router` skill (`~/.config/opencode/skills/local-model-router/`); the DESIGN docs pin the design contract.