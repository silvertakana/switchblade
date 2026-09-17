# Plan: secure env editor (managed secrets store)

Status: approved 2026-09-17. Supersedes the earlier ".env editor" design after
live evidence showed the homelab container's `/app/.env` is image-baked and
regenerated on every Coolify redeploy (only `config.server.json` and the
history volume are bind-mounted). Hot-reloading `.env` there would be wiped by
the next deploy, invisibly. The design therefore moved to a managed secrets
file stored like the config, plus `.env` kept as a boot-time fallback.

## Goal

Let the operator add and change credential env vars from the web UI, applied in
place with no restart, stored durably across redeploys, and never exposing a
secret value except through an explicit per-var reveal.

## Design: three layers, one managed store

| Layer | Hot reload | Survives redeploy | Managed by |
|---|---|---|---|
| Real process env (shell / platform) | n/a | yes | platform |
| `secrets.json` (new, managed) | yes | yes (bind mount like config) | this UI |
| `.env` (existing) | boot only | no (container) / yes (bare metal) | hand-edited |

Precedence, highest first: **real shell env > secrets.json > .env**.

- Boot: `loadEnv()` keeps today's fill-only `.env` behavior, then
  `loadSecrets()` applies secrets.json on top, skipping names the real shell
  already owned (recorded in a startup snapshot of `Object.keys(process.env)`
  taken before `loadEnv()` runs).
- Runtime: the UI writes secrets.json; the watcher reloads it and pushes values
  into `process.env` (skipping shell-owned names). Every read site in the router
  (`masterKey()`, backend key resolution) already reads `process.env` fresh per
  request, so no restart is needed for anything reloadable.
- `.env` becomes purely a bootstrapping fallback. The UI never writes it, which
  deletes the comment-preservation risk entirely.

## File: `secrets.json`

- Path: `process.env.ROUTER_SECRETS || join(dirname(CONFIG_PATH), "secrets.json")`.
  Beside `config.json` so the homelab needs one more bind mount, same pattern.
- Format: flat JSON object `{ "NAME": "value", ... }`. JSON because the file is
  UI-managed: no comment-preservation machinery, and the config-editor's atomic
  write / snapshot / revision-guard patterns apply unchanged.
- Gitignored, `chmod 600`-equivalent handled by default umask; never logged.
- Snapshots to `secrets.history/`, 20 deep, before every write (mirrors
  `snapshotConfig()`).
- Lost-update guard: `baseRevision` = secrets.json `mtimeMs`; mismatch is 409.

## Security model (unchanged from the approved direction)

- Values are never returned by the list endpoint. No masked prefixes either.
- Reveal is a separate explicit call for exactly one name, admin-gated (the
  existing master-key / dashboard-session gate already covers all `/api/env`).
- Protected names are rejected on write server-side (UI hiding is not a
  boundary): `masterKeyEnv` and `uiPasswordEnv` from the live config.
- Reveal is ALSO refused for protected names (400). An earlier revision allowed
  it on the reasoning that anyone reaching reveal is already the admin those
  vars define, but that is wrong for `masterKeyEnv`: the dashboard session is
  gated by `uiPasswordEnv`, while the master key is a wider-scope machine
  credential that dsh and OpenCode hold. Revealing it would let a dashboard
  password escalate to the machine credential. The list still reports
  `set: true` for these names, so nothing operationally useful is lost.
- Names must match `[A-Z_][A-Z0-9_]*`; values must be strings, max 8 KiB.
- Responses never echo a value back after a write.

## API

| Method | Path | Body | Returns |
|---|---|---|---|
| GET | `/api/env` | - | `{vars: [{name, set, editable, reloadable, source, live}], restartRequired}` |
| POST | `/api/env/reveal` | `{name}` | `{name, value}` |
| POST | `/api/env` | `{name, value, baseRevision}` | `{ok, name, action, applied, live}` |
| DELETE | `/api/env?name=X` | - | `{ok, name, action: "removed"}` or 409 for non-managed names |

- `source`: `"shell"` (real env at boot), `"secrets"` (managed file), `"env"`
  (.env fallback), or `"unset"`.
- The list is SCOPED to names the router cares about: names the live config
  references (`masterKeyEnv`, `uiPasswordEnv`, every `apiKeyEnv`), names in the
  managed store, and names declared in `.env`. It deliberately does NOT walk the
  inherited process environment: doing so returned 76 rows on a real machine, 74
  of them noise (`PATH`, `ComSpec`, `ALLUSERSPROFILE`) plus unrelated operator
  secrets, burying the handful that matter. A shell-owned name still reports
  `source: "shell"` when it is listed.
- `live`: the running `process.env` value equals what this layer would serve.
  False only when a watcher reload failed or a shell shadows the file; the UI
  shows a restart/stale warning only for these, not as a blanket rule.
- `editable`: false for protected names and for names the real process env
  owns (`source: "shell"`).
- `reloadable`: false for startup-only names (`ROUTER_CONFIG`, `ROUTER_ENV`,
  `ROUTER_HISTORY`, `ROUTER_KEYS_FILE`, `NODE_OPTIONS`). Still writable (the
  file is the next boot's truth); flagged so the UI can say "applies on next
  start".
- DELETE only removes a secrets-managed entry. A var sourced from `.env` or the
  shell returns 409 with an explanatory message (the UI shows `source`, so this
  is visible before clicking).

## server.mjs changes

1. `SECRETS_PATH`, `SECRETS_HISTORY_DIR`, `MAX_ENV_SNAPSHOTS = 20`,
   `STARTUP_ONLY_VARS`, `SHELL_ENV_NAMES` (snapshot before `loadEnv()`).
2. Helpers: `readSecrets()`, `secretsRevision()`, `snapshotSecrets()`,
   `writeSecretsAtomic()`, `loadSecrets(apply)` (apply pushes into
   `process.env`, skipping shell-owned names), `envSource(name)`.
3. `fs.watch(SECRETS_PATH)` next to the existing `CONFIG_PATH` watcher;
   reload is idempotent (editors can fire several events per save).
4. Gate: add `/api/env` to the admin allowlist chain.
5. Routes as tabled above.

## test.mjs coverage (zSE block)

Spawned-child integration against temp `ROUTER_SECRETS` + temp config:

1. Unauthenticated `GET /api/env` -> 401; master key -> 200.
2. List contains a known `apiKeyEnv` name with `set: true` and `source`; the
   serialized response contains no secret value.
3. POST adds a var: response `{ok, applied: true}`; GET shows it `live`,
   `source: "secrets"`; reveal returns the written value.
4. POST on a protected name -> 400; file unchanged.
5. Reveal without auth -> 401.
6. Second write preserves the first var (object merge, no clobber).
7. Stale `baseRevision` -> 409 with `current`.
8. Snapshot file appears in `secrets.history/`.
9. DELETE removes exactly that entry; GET no longer lists it as secrets-sourced.
10. Invalid name (lowercase, dash, empty) -> 400.
11. Shell-shadowed name: child spawned with a real env var that secrets.json
    also defines -> `source: "shell"`, and the reveal returns the shell value.
12. `ROUTER_HISTORY` reports `reloadable: false`.
13. Startup behavior: secrets.json value wins over an `.env` value for the same
    name (boot precedence), shell wins over both.

## index.html changes

An "Environment" card in the Config tab, matching the existing card language:

- Table: name, source badge (`shell` / `managed` / `.env`), state badge
  (`live` / `stale` / `next start`), actions (reveal, edit, delete where
  managed).
- Reveal: per-var button, shows the value inline with a copy affordance, hidden
  again on click; never rendered on page load.
- Edit: inline input + save; protected names show a lock instead of edit.
- Add form: name + value, with the same validation mirrored client-side.
- Stale/restart warning banner only when some var reports `live: false`.

## README / AGENTS.md

Add the three `/api/env*` rows to both endpoint tables and a one-paragraph
note on the three-layer precedence and `ROUTER_SECRETS`.

## Homelab migration (verified read-only against production, 2026-09-18)

Facts read from the live container (id was `bd91dc0181ea`; resolve it fresh, it
changes on every redeploy, and only ENV NAMES were read, never values):

- Only two durable paths exist: the named volume
  `a8dyidd8o8id2se9czgauem0-switchblade-history` mounted at `/data`, and the
  config bind mount at `/app/config.server.json`. Everything else under `/app`
  is image filesystem and is WIPED by every redeploy.
- `ROUTER_SECRETS` is unset, so the default path resolves to `/app/secrets.json`
  (dirname of `ROUTER_CONFIG`), which is NOT durable. As deployed today, every
  redeploy would silently wipe the managed store.
- **Fix: `ROUTER_SECRETS=/data/secrets.json`, set in the `Dockerfile` as an
  image `ENV`** (the same pattern already used for `ROUTER_CONFIG`), so the path
  is version-controlled and needs no Coolify change. `/data` is already durable,
  so NO new bind mount is required; this corrects the earlier note in this doc,
  which assumed one was needed. Snapshots land at `/data/secrets.history/` and
  are durable too.
- `LMR_UI_PASSWORD` and `LITELLM_API_KEY` are both present in the container's
  process env, so the dashboard login works and the admin gate is satisfied.
- `/app/.env` exists (119 bytes) and holds exactly one name,
  `COMMANDCODE_ALT3_API_KEY`. It is container filesystem, lost on rebuild.
- 14 credential names arrive as real process env from Coolify app env vars.

### The consequence that decides the migration

Precedence is shell > secrets > `.env`, and a name the real process env owns is
never overridden: it reports `source: "shell"` and `editable: false`. On
production 14 of 15 credential names are Coolify-injected process env, so
**deployed as-is the editor would show nearly every key locked**. It could only
edit `COMMANDCODE_ALT3_API_KEY`, and even that would be wiped next redeploy.

So the open question is about key ownership, not code:

| Option | Effect |
|---|---|
| A. Move provider keys into the managed store | Editor becomes genuinely useful and durable. Per key: add to store, verify live, remove from Coolify app env, restart, re-verify routing. |
| B. Leave keys in Coolify | Editor is effectively read-only in production. Safest, near-zero value. |
| C. Hybrid | Move only the keys you want to rotate from the UI; keep the rest in Coolify. |

`LITELLM_API_KEY`, `LMR_UI_PASSWORD`, and `LMR_KEYS_FILE` must stay in Coolify
regardless (protected names, and startup-only). Moving
`COMMANDCODE_ALT3_API_KEY` out of `/app/.env` into the store is a straight
improvement: it currently dies on every rebuild.

### Security consequence to accept deliberately

`POST /api/env/reveal` is admin-gated, and on production a logged-in dashboard
session counts as admin because `uiPasswordEnv` is set. So BOTH the dashboard
password and the master key can read every credential, one name at a time. The
master key used to mean "can spend my quota"; after this it also means "can read
every provider key". If that is too wide, the options are: leave it, restrict
reveal to master-key callers only, or restrict reveal to names the store itself
manages.

### Checklist

1. Commit and push. Push auto-deploys (webhook rebuilds the image). Code only:
   the live config is a bind mount and a push does not touch it.
2. Confirm `ROUTER_SECRETS=/data/secrets.json` reaches the container (it is set
   as an image `ENV` in the `Dockerfile`, so it ships with the code and needs no
   Coolify change): check the container's env NAMES, never values.
3. Deploy, then verify: `/api/env` returns 401 unauthenticated; the
   authenticated list shows the right `source` per name; an add/delete
   round-trip works; the store lands at `/data/secrets.json`; `/v1/models` order
   is unchanged; `/health` states are as expected.
4. Manual cools reset on restart: re-cool the dead `commandcode` backend.
5. Then choose A/B/C and, for A or C, migrate keys one at a time with a routing
   smoke test between each.

Rollback: revert the commit and push. The config bind mount and the `/data`
volume are untouched by a code deploy.

## Verification

- `node --check server.mjs`; `node test.mjs` (count rises above 304).
- Live local check: add a var via the UI, confirm `applied: true`, `live: true`,
  reveal round-trips, secrets.json keeps valid JSON, snapshots appear, and no
  value appears in any GET. No restart of the local instance.
- Restart NOT performed; the local instance keeps running for the UI work.

## Risks

- **Two secret stores** is a real conceptual cost; mitigated by `.env` being
  documented as boot-only fallback and the UI showing `source` per var.
- **Watcher idempotency**: multiple events per save; reload is a pure re-read
  and re-apply, safe to run repeatedly.
- **Shell-shadow confusion**: a shell var silently wins over the UI's file.
  Mitigated by the `source: "shell"` badge and a test pinning the precedence.
- **Snapshot growth**: capped at 20, pruned on write, same as config.
