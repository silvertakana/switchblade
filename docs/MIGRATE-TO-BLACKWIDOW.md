# Migrate Switchblade (local-model-router) to Blackwidow

> Status: PLAN (not yet approved). Prepared 2026-08-23 from live investigation of the repo and the server.

## Goal

Move Switchblade, the zero-dependency local model router, from this desktop (127.0.0.1:8787) onto the homelab server Blackwidow (192.168.68.69, `blackwidow` on the tailnet), hosted as a Coolify container from the public GitHub source, so dsh and OpenCode consume `http://blackwidow:8787/v1` instead of localhost. Keep a working rollback path throughout.

## Why container + Coolify

- Blackwidow is already a Coolify-managed box (LiteLLM is a Coolify app there). A Coolify app gets us: pull-from-source deploys, env-var management, app logs, health checks, restarts under Coolify's watch. That is the homelab's existing operating surface.
- Alternative (systemd unit on the host) would work (node 24 is present on the server) but invents a second management surface outside Coolify. Not better.
- Recommended build route: `app create public` + `--build-pack dockerfile`, with a small committed Dockerfile, so the image is ours and sourced from git (matches LiteLLM's pattern, except LiteLLM smuggles its Dockerfile in as base64; we version it in the repo).

## Facts the plan is built on (verified 2026-08-23)

- Repo: zero-dep Node ESM (`server.mjs` single file), test suite `node test.mjs` = 110 passed, no network/keys. CI: GitHub Actions matrix node 22/24/25, syntax check + tests + secret scan + `.env.example` placeholder check. Present on origin/main.
- Remote `origin/main` HEAD is `83bcdd8`; local is ahead by 2 commits (incl. `84e9f80` timeout/relay-telemetry fix) and has an untracked `AGENTS.md`. Deploy-from-source would miss the latest telemetry fix until pushed.
- Port and bind host come ONLY from `config.json` (no PORT/HOST env). `127.0.0.1` is always bound; `cfg.host` adds a second bind. Current config pins desktop Tailscale IP `100.65.175.106` (machine-specific; must not ship to the server).
- Keys: 6 env names used (`OPENCODE_GO_KEY`, `OPENCODE_GO_ALT_KEY`, `OPENCODE_GO_ALT2_KEY`, `DEEPSEEK_API_KEY`, `COMMANDCODE_API_KEY`, `ZAI_API_KEY`). `.env.example` is MISSING `OPENCODE_GO_ALT2_KEY` (must fix, or a fresh deploy silently lacks the go-alt2 backend).
- Env loading (server.mjs:47-58): keys come from ROUTER_ENV dir or real environment; **container env vars win** (only unset vars get filled from the key file). So the server can hold keys purely as Coolify env vars; no secret file in the image or as a mount.
- Config: hot-reloads via fs.watch (~300 ms) in the repo layout. No filesystem paths inside config.json (portable). History JSONL defaults to an ABSOLUTE path beside the config; in a container it dies with the container unless `ROUTER_HISTORY` points at a mounted volume. In-memory ring buffer (500) is unaffected.
- index.html is static, single-file, all relative fetches. Works behind the container as-is.
- Server: Coolify runs on the box itself (server uuid `z10act9f327rontblqz02980`); Homelab project uuid `mvvq6xm2nfnea4h9lwx6xwpe`; node 24.16.0 / npm / git present on host; ports 8787 and 8788 FREE; GitHub reachable and `silvertakana/switchblade` is public (verified via `git ls-remote` from the server); LiteLLM app is the reference pattern (file-mounted config + env vars + port mapping).
- SECURITY (flagged): server.mjs has NO inbound authentication. No bearer/token check on API or admin endpoints; `masterKeyEnv` is documented in the config contract but not enforced by the code. At loopback this is invisible. On a LAN/tailnet-facing host with 0.0.0.0 bind, anyone reachable can relay requests through the user's paid accounts (Go weekly quotas, DeepSeek balance). Must be decided before cutover.

## Decisions / answers to the planning questions

1. **Container?** Yes. Coolify app from git, `dockerfile` build pack. (systemd is the fallback if container friction appears; not the recommendation.)
2. **Pull from source and host?** Yes. `app create public` pointing at `https://github.com/silvertakana/switchblade.git`, branch `main`. Redeploys pull latest source and rebuild.
3. **What about new changes?** Split by kind:
   - Code (`server.mjs`, `index.html`, Dockerfile, tests): local dev -> `node test.mjs` -> commit -> push -> CI (GitHub Actions) -> Coolify webhook deploy on push to main.
   - Routing config (backends/models/presets/backoff): live-edit the mounted config file on the server -> fs.watch hot-reloads in ~300 ms, no redeploy, no downtime (same workflow as today on the desktop).
   - Keys: change Coolify env vars for the app -> rebuild/restart (keys load at startup), no image change.
4. **CI/CD?** Keep the existing GitHub Actions (tests + secret scan) as the quality gate. Add: (a) Dockerfile in repo; (b) Coolify deploy webhook on push to main (start manual / webhook once stable); (c) optional CI job that runs `docker build` to catch Dockerfile drift.
5. **Local development and shipping?** Unchanged repo workflow on the desktop (edit, `node test.mjs`, commit, push). Local instance on a dev port via `ROUTER_CONFIG` pointing at a dev config (port field distinguishes it); shipping is push -> CI -> webhook deploy. After cutover the desktop router is demoted to dev/test or stopped.
6. **How do we trial new features?** Three lanes:
   - Mock suite (`node test.mjs`) for code-level behavior.
   - Local dev instance on a second port (`ROUTER_CONFIG`) for config/preset experiments against real backends.
   - Staging Coolify app `switchblade-staging` on port 8788 (free today): isolated container with its own mounted config + env subset, to trial container builds and configs against real traffic without touching the 8787 production instance.

## Security decision (required before cutover)

Options, recommended first:
- **A (recommended): implement inbound auth on the router** - enforce the already-documented `masterKeyEnv`: when set, `/v1/chat/completions` and `/admin/*` require a matching Bearer token; `/health` and read-only endpoints (`/v1/models`, `/api/stats`, `/api/history`, `/api/config`) stay open (they expose routing state and env NAMES, never key values - tests already enforce that). When `masterKeyEnv` is unset, behavior is unchanged (today's open posture) so local/dev instances are unaffected. Change ships with tests per repo rule. dsh ALREADY sends a bearer token (value of `LITELLM_API_KEY`) and OpenCode is wired similarly, so consumers likely need only that the configured master key matches what clients already send. Verify per-consumer before enabling.
- **B: firewall-scope the port** - allow port 8787 only from the tailnet (e.g. `ufw allow from 100.64.0.0/10 to any port 8787`). Keeps the router code untouched; adds a host-side rule that survives rebuilds but is outside Coolify's view.
- **C: accept LAN exposure** like LiteLLM (`4003` is likewise LAN/tailnet HTTP). Do NOT accept for this router: it forwards to PAID upstreams with no auth, unlike LiteLLM which has a master key. Only acceptable as a temporary state during cutover with immediate follow-up.

Decide A/B/C before the server config can be finalized (`masterKeyEnv` in the server-side config if A).

## Phases

### Phase 0 - Repo hygiene (desktop, git)

- [ ] Fix `.env.example`: add `OPENCODE_GO_ALT2_KEY=`.
- [ ] Remove the desktop Tailscale-IP `host` field from `config.json` (falls back to localhost-only default; also avoids the EADDRNOTAVAIL crash class on stale Tailscale IPs). Requires a local router restart to take effect (host doesn't hot-reload).
- [ ] Add `config.server.json`: copy of current config with `host: "0.0.0.0"` (required so the container's published port works) and, if decision A, `masterKeyEnv`.
- [ ] Add `Dockerfile` + `.dockerignore`.

Dockerfile outline:

```dockerfile
FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production
ENV ROUTER_CONFIG=/app/config.server.json
COPY package.json server.mjs index.html config.json config.server.json ./
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8787/health || exit 1
CMD ["node", "server.mjs"]
```

`.dockerignore`: `.env*`, `*.log`, `router-history.jsonl`, `.git`, `test.mjs` (image doesn't need it; suite still runs in CI), `docs/`.

- [ ] Commit + push everything (including the 2 existing unpushed commits and the repo AGENTS.md) so origin/main is the true source.
- [ ] CI: add a `docker build` smoke job (optional but cheap) so Dockerfile drift fails on push, not at deploy.

### Phase 1 - Provision on Blackwidow (read-only checks done; now create)

- [ ] Confirm context/project/server UUIDs: `coolify project list`, `coolify server list` (expected server `z10act9f327rontblqz02980`, project Homelab `mvvq6xm2nfnea4h9lwx6xwpe`).
- [ ] Create app:

```
coolify app create public --server-uuid z10act9f327rontblqz02980 \
  --project-uuid mvvq6xm2nfnea4h9lwx6xwpe --environment-name production \
  --git-repository https://github.com/silvertakana/switchblade.git --git-branch main \
  --build-pack dockerfile --ports-exposes 8787 --ports-mappings "8787:8787" --name switchblade
```

- [ ] Set env vars (6 key names from current `.env`) + `ROUTER_HISTORY=/data/history/router-history.jsonl` via `coolify app env sync` / `coolify app env create`. Never paste key values into this doc.
- [ ] File-mount the live config: `coolify app storage create <uuid> --type file --mount-path /app/config.server.json --content "<server config JSON>"` (seeds the container's server config; the mount path matches `ROUTER_CONFIG`).
- [ ] Persistent volume for history: `coolify app storage create <uuid> --type persistent --name switchblade-history --mount-path /data`.
- [ ] Deploy: `coolify deploy uuid <app_uuid>`.

### Phase 2 - Smoke (from this desktop, over the tailnet)

- [ ] `curl http://blackwidow:8787/health` -> backends listed, ok:true.
- [ ] `curl http://blackwidow:8787/v1/models` -> id list matches the desktop's ordering (consumer-safety property: same ids, same order).
- [ ] Non-stream chat + SSE stream through `http://blackwidow:8787/v1/chat/completions` with a valid bearer; assert `x-router-backend` / `x-router-call` headers and a real completion.
- [ ] Check `coolify app logs <uuid>` for clean startup (no config warnings).

### Phase 3 - Cutover consumers (backup first; keep local router running)

- [ ] `cp -f` backups of each file before editing.
- [ ] dsh: `~/.dsh/settings.yaml` and `cordis.patch.yml` local-router provider baseURL `http://127.0.0.1:8787/v1` -> `http://blackwidow:8787/v1` (keep BOTH in sync; the default-model field lives in both files).
- [ ] OpenCode: `~/.config/opencode/opencode.jsonc` local-router provider baseURL -> `http://blackwidow:8787/v1`.
- [ ] Verify a live dsh session (deepseek-v4-flash default) and an OpenCode completion.
- [ ] Rollback path: flip both back to 127.0.0.1:8787 (desktop router still running during soak).

### Phase 4 - Soak, then retire the desktop instance

- [ ] Soak a few days on Blackwidow. Confirm health over time, session affinity still honored (dsh sends x-session-id; distance doesn't affect hashing).
- [ ] After soak: stop desktop router, remove Startup shortcut, disable/remove scheduled task "LocalModelRouter keepalive".
- [ ] Keep `~/.dsh` + `opencode.jsonc` pointing at `blackwidow:8787`.

### Phase 5 - Staging / trial lane (optional, on demand)

- [ ] Second Coolify app `switchblade-staging`, port mapping `8788:8787`, same source, its own storage-mount config (can subset backends) + env subset -> trial container builds and config changes at `http://blackwidow:8788` without touching production.
- [ ] Local dev instance anytime: `ROUTER_CONFIG=<dev config with its own port> node server.mjs`.

## Explicitly out of scope (this plan)

- Retiring LiteLLM on Blackwidow. The router now routes what LiteLLM used to serve (glm/qwen/mimo/laguna), but OpenClaw still consumes LiteLLM; folding that in is a separate decision.
- The unhealthy `minecraft-server` app and exited `glitchtip` service spotted on the server (peripheral finding; unrelated to this move).
- Coolify CLI remediation for `coolify app get --format json` leaking webhook secret values (peripheral finding; treat that output as sensitive).

## Open questions to resolve during execution

- Auth decision A/B/C (needs your call now; prevents finalizing the server config).
- Per-consumer bearer semantics under decision A (confirm dsh + OpenCode tokens match the configured master key before enabling).
- Exact Coolify flag spellings/limits confirmed against the live CLI before the create call (read-only `--help` first).

## Rollback summary

- Before any cutover: desktop router untouched -> flip consumers back = one-line baseURL edit per consumer.
- If Blackwidow goes down during soak: switch dsh/OpenCode to LiteLLM (`litellm` provider already wired in dsh) as the emergency route.