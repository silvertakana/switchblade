# AGENTS.md

## Purpose & Role

This repo is **Switchblade**. The directory name `local-model-router` is the legacy name, kept for compatibility. In this project the agent's job is:

- **Diagnose and fix router and inference issues** — routing failures, upstream/provider problems, stream cuts, cache misses, model resolution, and anything that breaks how requests are routed or answered.
- **Develop on Switchblade** — backends / models / presets, `config.json` (the mirror of the live homelab config), the `index.html` UI, and new features in `server.mjs`.

**When the user pastes a bare router log id (e.g. `cm9Qz3V4oQ`), treat it as a `callId`.** Resolve it via `GET /api/history?limit=N` then `GET /api/history/detail?call=<callId>` (or grep the JSONL). Full method: the `switchblade` skill, `references/logs.md`.

Working rules:

- **Verify against the homelab production router** (`192.168.68.69:8787`) — it is the real deploy target; dsh and OpenCode route through it. The local instance (`127.0.0.1:8787`) is for dev and the mock test suite only.
- **Provider/config work happens on the homelab, over SSH.** For ANY request to add, remove, replace, or re-wire a provider, backend, account, API key, model, preset, or routing change, the target is the production router at `192.168.68.69:8787`, and the job is done when the change is LIVE AND VERIFIED there. Editing `C:\dev\local-model-router\config.json` alone is NOT a completed task and must never be reported as one: that file is a MIRROR of the homelab change, updated after (or alongside) it and labeled as a mirror.
- **Do NOT start the local instance and do NOT point the work at `127.0.0.1:8787`** unless the user explicitly says "local", "test locally", or "on my machine". If the intent is ambiguous, default to the homelab.
- **The two instances in one line each.** LOCAL (dev/test, never the target): `127.0.0.1:8787`, started with `node server.mjs` from this repo, config `C:\dev\local-model-router\config.json`, idle in daily operation. HOMELAB PRODUCTION (the real router): `192.168.68.69:8787`, the Coolify container, live config `/app/config.server.json`, and the only instance dsh and OpenCode route through. The repo `config.json` is a mirror of the homelab config, not the target.
- **Debug UI issues locally first**, then ship via a PR/push to GitHub — pushing auto-deploys to the homelab.
- **The API keys on the server are authorized for use** in testing and verification.
- **Recommended: replay/mock the request locally** to isolate variables before changing config or code (see the `switchblade` skill's diagnose references).
- Operational playbooks: the `switchblade` and `homelab` dsh skills — load them before acting on this repo.

## Project Overview

Switchblade (repo dir: `local-model-router`) is a **zero-dependency, single-process local model router** for OpenAI-compatible LLM APIs. It load-balances chat requests across multiple upstream accounts (OpenCode Go, DeepSeek, CommandCode, Z.ai, or any OpenAI-compatible base URL) with session-affinity hashing, per-backend health tracking, exponential backoff, and a web dashboard. The repo instance runs on `127.0.0.1:8787` for development and the mock test suite only; the router that actually serves dsh and OpenCode is the homelab production container (see Deployment Topology). It needs no database or container stack, and was designed to be maintained by AI agents: every behavior is pinned by a mock-based test suite that runs in seconds with no keys and no network.

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
| `test.mjs` | Mock test suite — **349 passing assertions**. Spawns real `server.mjs` children on temp configs against in-process mock backends. No keys, no network. |
| `config.json` | MIRROR of the live homelab config (`/app/config.server.json`) for local dev and reference; **hot-reloads via `fs.watch` (~300 ms)** — no restart for schema/backend/model/preset/backoff changes. Validate JSON before saving. Editing it is not a deploy: provider/config changes are made on the homelab over SSH. |
| `.env` | API keys (gitignored). Read once at startup; a restart is needed after edits. Never commit. Superseded per-name by the managed `secrets.json` (see Env editor). |
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
| `secrets.json` | Managed env-var store written by the Config tab's env editor (VALUES only, gitignored; override path with `ROUTER_SECRETS`). Hot-reloads via `fs.watch`; snapshots to `secrets.history/`. |

## Setup Commands

Requires Node.js 22+ (CI runs 22, 24, 25; developed on 25 — no Node-25-only APIs).

```bash
git clone <repo> && cd local-model-router
cp .env.example .env        # then fill in the keys you use
npm install                 # no-op: the repo is zero-dependency, keeps lockfile conventions
```

No build step exists. No lint or format tooling is configured (zero-dependency mandate — no eslint/prettier). The only quality gates are the syntax check and the test suite.

## Development Workflow

- **Start the router (LOCAL dev instance, never the deploy target)**: `npm start` or `node server.mjs`. Listens on `http://127.0.0.1:8787` (API prefix `/v1`); web UI at `/`.
- **Where a config change actually lands**: provider, backend, account, API key, model, preset, and routing changes are made on the homelab production router over SSH (live config `/app/config.server.json`; protocol in the `switchblade` skill's `references/deploy-config.md`). The repo `config.json` below is the MIRROR of that change, updated after (or alongside) the homelab ship and labeled as a mirror. Do not report a repo-only edit as done.
- **Config hot-reload**: edit `config.json` — the router picks it up in ~300 ms. A broken JSON write can wedge routing (the reload fails and the previous config stays, but partial writes may be picked up mid-file). Validate before saving:
  ```bash
  node -e "JSON.parse(require('fs').readFileSync('config.json','utf8'))"
  ```
- **`.env` changes**: keys are read once at startup, not at request time; a restart is needed after edits. Keys are referenced from config by NAME only (`apiKeyEnv`); values live in `.env`.
- **Env editor (Config tab)**: `GET/POST/DELETE /api/env` plus `/api/env/reveal` manage credential env vars in a hot-reloading `secrets.json` (`ROUTER_SECRETS`), snapshotted to `secrets.history/` before every write. Precedence at boot and at reload: real shell env > `secrets.json` > `.env`; a name the real environment owns is never overridden and is reported as `source: "shell"`. The list endpoint serves NAMES and state only, a value comes back solely from the per-name reveal call, and `masterKeyEnv`/`uiPasswordEnv` are refused on write server-side (they are the credentials that unlock the editor) and also refused by reveal (400): the dashboard session is gated by `uiPasswordEnv`, while the master key is a wider-scope machine credential, so revealing it would let a dashboard password escalate to the machine credential. Names in `STARTUP_ONLY_VARS` are writable but report `reloadable: false`.
- **Config reload is non-destructive to health state** (health is keyed by backend id and survives reloads). Manual cools survive reloads too.
- **Legacy configs keep working**: `normalizeConfig(cfg)` auto-synthesizes both pre-three-layer eras at every load. Old `models[id] = {backends, affinityPool}` and presets-with-`members` shapes normalize to the three-layer form byte-identically for identical effective configs.

## Testing Instructions

```bash
npm test                    # node test.mjs -> expect "349 passed, 0 failed"
node --check server.mjs     # syntax check (CI also checks test.mjs)
```

- The suite **spins up mock backends plus router instances on temp configs — no real keys, no network**. Safe to run anytime, even while the production router is live on 8787 (test children bind port 0).
- Coverage: health, non-stream + SSE streaming, session affinity, failover/weighted selection, sticky + timed manual cools, fallback exclusion, dialect handling (dropParams/paramMap/developer-role), synthesis from both legacy eras, layered-params merge order and precedence, preset-of-presets nesting (expansion, cycles, ordering), per-model retry/backoff budgets, cache tri-state, history `routedModel`, timeout failover, reasoning-key relay (`reasoning`/`reasoning_content`).
- **Every behavior change ships with a test in `test.mjs`** (project rule, README Contributing). The legacy (`y*`) and three-layer (`z*`) blocks import `server.mjs` internals; integration blocks spawn the real server as a child.
- Known environment quirk: the child-port banner capture can transiently crash with `TypeError: fetch failed ... bad port` (observed once 2026-08-22); a plain re-run passes 349/349. Do not treat a single crash as a regression — re-run first.
- Known hygiene quirk: the suite leaks its `lmr-test-*` temp dir behind each run (a child can still hold the directory when cleanup runs on Windows); they are tiny (tens of KB) and accumulate in `%TEMP%`. Cosmetic, not a failure.

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
| GET | `/api/env` | env var NAMES with `set`/`editable`/`reloadable`/`source`/`live`; never values; admin required |
| POST | `/api/env` | `{name, value, baseRevision}` -> writes the managed secrets store (`ROUTER_SECRETS`); 409 on a stale revision; admin required |
| POST | `/api/env/reveal` | `{name}` -> `{name, value}` for ONE variable; 400 for `masterKeyEnv`/`uiPasswordEnv` (auth credentials are never revealed); admin required |
| DELETE | `/api/env?name=X` | removes a managed variable; 409 for `.env`/shell-sourced names; admin required |
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

**Operating instruction (unambiguous):** provider, backend, account, API key, model, preset, and routing changes are made on the HOMELAB production container over SSH. The repo `config.json` is a MIRROR of the live homelab config: useful as a pre-flight or as the record of a change already shipped, never the target. Editing it alone is not a completed task, and the local instance below is never the deploy target unless the user explicitly says "local", "test locally", or "on my machine".

- **Local (dev/test, not a deploy target)**: `http://127.0.0.1:8787`, `node server.mjs` from this repo, launched by `start.cmd` (Windows logon shortcut) and the scheduled task "LocalModelRouter keepalive". Used for development and the mock test suite. Idle in daily operation. Config `C:\dev\local-model-router\config.json`.
- **Production (homelab, the real router)**: the Coolify container `a8dyidd8o8id2se9czgauem0-<suffix>` (`<CID>`) on `192.168.68.69:8787`, managed via SSH/Coolify. The container name suffix and id change on every Coolify redeploy; resolve it first: `ssh root@192.168.68.69 "docker ps --format '{{.ID}} {{.Names}}' | grep a8dyidd8o8id2se9czgauem0"`. Both dsh and OpenCode route to the HOMELAB instance (baseURL `http://192.168.68.69:8787/v1`), not the local one. Live config `/app/config.server.json`.

Production container details:

- Live config is `/app/config.server.json` (env `ROUTER_CONFIG`), a bind mount from `/data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json`. `/app/config.json` inside the container is UNUSED.
- Durable request history is the named volume `/data/history/router-history.jsonl` (env `ROUTER_HISTORY`).
- The managed env-var store is `/data/secrets.json` (env `ROUTER_SECRETS`, set as an image `ENV` in the `Dockerfile`), on that same durable `/data` volume, with snapshots at `/data/secrets.history/`. Without it the store would default to `/app/secrets.json` and be wiped by every image rebuild.
- Keys: the container reads `/app/.env` at startup. That file lives in the container filesystem and is lost on a Coolify image rebuild; durable keys belong in the Coolify application env vars.

## Build and Deployment

No build step — `server.mjs` runs directly on the system Node (22+/25).

- **Local instance (dev/test, never the deploy target)**: `start.cmd` launches `node server.mjs` at Windows logon (Startup shortcut) and a scheduled task "LocalModelRouter keepalive" restarts it if down. It is idempotent: exits 0 if 8787 is already listening. Restart pattern: kill the port owner (`netstat -ano | grep ":8787.*LISTENING"`, `taskkill /PID <pid> /F`), then `nohup node server.mjs > router.log 2>&1 < /dev/null &` from Git Bash. This instance is for development and the mock test suite only; it is idle in daily operation (production is the homelab container, see Deployment Topology). Do not start it, and do not point a provider/config change at it, unless the user explicitly says "local", "test locally", or "on my machine".
- **Production instance (homelab)**: the Coolify container `<CID>` (resolve first, see Deployment Topology; the name suffix and id change on every redeploy) on `192.168.68.69:8787`, managed via SSH/Coolify. Both dsh and OpenCode route here, not to the local machine. Container paths, history, and key handling are in Deployment Topology above.
- **Production deploy protocol (config changes)**: connect to the host -> backup the host config -> diff/merge the fetched host config (never overwrite it wholesale with the repo config; the host file carries `host: 0.0.0.0`) -> ship the LF-normalized file via scp -> install host-side (`docker cp` INTO the bind mount fails with "device or resource busy") -> md5-verify host vs in-container paths -> verify `/api/config` and `/health` after ~3 s. Config changes hot-reload in ~300 ms; `.env`, `host`, and code changes need `docker restart <CID>` (resolve the live id first; it changes on every redeploy). Full step-by-step: the `switchblade` dsh skill runbook `C:\Users\silve\.dsh\skills\switchblade\references\add-provider.md`.
- **Where provider/config changes are made**: on the homelab over SSH, never in the repo. The repo `config.json` is the MIRROR (pre-flight or record of the shipped change) and must be labeled as such when reported; an edit to it alone is not a completed change, because dsh and OpenCode only ever see the production container.
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
- The env editor's `secrets.json` holds key VALUES: gitignored, never echoed by `GET /api/env` (the list serves names and state only), reachable only one name at a time via `/api/env/reveal`, which is admin-gated like the rest of `/api/env*`. Reveal additionally refuses the protected auth names (`masterKeyEnv`, `uiPasswordEnv`), and `GET /api/env` is SCOPED to config-referenced names plus the managed store plus `.env`-declared names: it never walks the inherited process environment, which would list hundreds of unrelated system variables and operator secrets. `secrets.history/` is gitignored for the same reason. `/api/env` is in the same fail-closed auth gate as `/api/config*` and `/api/keys`.
- Read-only endpoints (`/health`, `/v1/models`, `/api/stats`, `/api/history`, `/api/config`, `/api/auth/*`, `/`) stay open.

## Pull Request Guidelines

- **Commit messages**: Conventional Commits — `feat:`, `fix:`, `refactor:`, `perf:` (see `git log`). One logical change per commit.
- **Before pushing**: `node test.mjs` must pass 349/349 and `node --check server.mjs` must pass. CI re-checks on Node 22/24/25 plus secret scan.
- **Project rules (README Contributing)**: keep it zero-dependency (no new npm packages without a strong reason); every behavior change ships with a test in `test.mjs`; the config contract and its synthesis paths are load-bearing — change them only with a documented design note (update `DESIGN-3LAYER.md`).

## Troubleshooting

- **Router won't start / port in use**: find and kill the owner (`port 8787`), then relaunch. Never `taskkill /F /IM node.exe` (kills every Node process).
- **Config edit did nothing**: JSON was probably invalid — the reload failed and the previous config stayed. Validate before saving.
- **Model 404s**: `model_not_found` means the id is not a configured preset/model. Check `/v1/models`. An empty preset returns 404 `preset '<id>' has no valid models`.
- **Backends all "cooling"**: check `/health` — weekly-limit pins (GO accounts, `fails: 100`) are normal and last the configured reset window; use `/admin/reset-health` only after verifying upstream limits. Manual cools require `uncool`.
- **MSYS/Git Bash gotchas**: use forward-slash paths (`C:/dev/...`); `netstat`/`taskkill` need `MSYS_NO_PATHCONV=1`; backgrounding a server needs all three redirects (`> log 2>&1 < /dev/null &`) or the shell hangs.
- **Stale docs**: `SPEC.md` is referenced by README and DESIGN docs but does not exist in the repo — rely on `DESIGN-3LAYER.md` as the contract source of truth.

## Additional Notes

- This repo is the machine-local replacement for homelab LiteLLM for the DeepSeek family (canonical model deepseek-v4.1-flash, with the pre-rename deepseek-v4-flash ids still served as alias presets); GLM/vision/free historically stayed on LiteLLM, but the live config now also routes glm/qwen/mimo/muse/laguna through this router (12 backends: go-primary, go-alt, go-alt2, go-alt3, go-alt4, direct, commandcode, commandcode-alt, zai, commandcode-alt2, commandcode-alt3, freellm; 20 models, 25 presets, 26 served ids; read from the homelab production router on 2026-09-11). Config evolves — never assume the model/preset list; read `config.json` or `/api/config`.
- **Cache forensics**: dashboard rows carry per-call `cacheHitPct`; miss payloads land in `router-misses.jsonl` (homelab: `/data/history/router-misses.jsonl`). Warm calls sit below 100% due to 256-token block granularity (ceiling = `floor(promptTokens/256)*256/promptTokens`); a cold first call reports null, not 0. Payload-diff method and known break patterns are documented in the switchblade skill's Cache miss forensics section.
- The operating playbook for this repo is the `switchblade` dsh skill (`~/.dsh/skills/switchblade/`); the DESIGN docs pin the design contract. The older `local-model-router` skill (`~/.config/opencode/skills/local-model-router/`) is now a POINTER to that skill, not a second playbook - open it for the entry point and the two-instance rule, then read the `switchblade` skill for procedure.