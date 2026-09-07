# Dashboard Auth + API Keys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:dispatching-subagents (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a password-gated dashboard and an API-keys management tab to the Switchblade local model router, and enable the existing `masterKeyEnv` gate end-to-end, without adding any npm dependencies.

**Architecture:** Thin, additive auth on top of the existing single-file router. `LMR_UI_PASSWORD` gates the dashboard; a SHA-256-hashed, gitignored `api-keys.json` stores issued chat-only keys; the existing `requireAuth` gate is extended to accept issued keys on chat, and the UI attaches the login credential to admin calls. When `LMR_UI_PASSWORD` is unset the UI stays open (today's behavior).

**Tech Stack:** Plain Node built-ins only (`node:http`, `node:crypto`, `node:fs`). No frameworks, no npm deps. Vanilla single-file `index.html` UI.

---

### Task 1: Server — extend auth + add keys endpoints

**Files:**
- Modify: `C:\dev\local-model-router\server.mjs`
- Test: `C:\dev\local-model-router\test.mjs`

**Context (the implementer starts cold — read these first):**
- `server.mjs` is the whole router (~1900 lines). It already has an inbound bearer gate `requireAuth(cfg, req)` at lines ~1651-1687: when `cfg.masterKeyEnv` names an env var, `/v1/chat/completions` and `/admin/*` require `Authorization: Bearer <value>`; read-only endpoints (`/health`, `/v1/models`, `/api/stats`, `/api/history`, `/api/config`, `/`) are NOT gated. It fails closed if the env var is set but empty. `safeEqual(a,b)` is a constant-time SHA-256 compare at lines ~1656-1660. `writeError(res, e)` at ~1637 writes an `{error:{...}}` JSON response.
- The server request handler is `http.createServer(async (req,res) => {...})` starting ~1712. It calls `const authErr = requireAuth(cfg, req); if (authErr) return writeError(res, authErr);` at ~1718-1719. Routes: `GET /health`, `POST /admin/reset-health`, `GET <p>/models`, `POST <p>/chat/completions`, `GET /`, `GET /api/history`, `GET /api/history/detail`, `GET /api/stats`, `GET /api/config`, `POST /admin/backend` (~1721-1838+). A fallthrough 404 at the end.
- `readFileP`/`writeFileP` promise wrappers exist. `crypto` is imported (used by `safeEqual`).
- The router runs as a single process; `start.cmd` + a scheduled task keep it alive. Editing `server.mjs` requires a restart to take effect (the config hot-reloads, the code does not).
- `.env` is read at request time via `process.env`; no reload needed for env changes. Never log key/password values.

**Design (implement exactly):**

1. `requireAuth(cfg, req)` must now also accept issued chat keys on `/v1/chat/completions`. After the existing master-key check passes or fails:
   - If the URL is `p + "/chat/completions"` and the master check returned unauthorized, also check the presented bearer token against `issuedKeyValid(token)` (see step 2). If a valid issued key matches, return `null` (allow).
   - Keep admin endpoints master-only (issued keys never unlock `/admin/*`).
   - Behavior with no `masterKeyEnv` AND no `uiPasswordEnv` configured stays open (all current tests rely on this).
2. Add an issued-keys store module-level state:
   - `const ISSUED_KEYS_PATH = process.env.LMR_KEYS_FILE || join(__dirname, "api-keys.json");`
   - `let issuedKeys = null;` lazily loaded via a `loadIssuedKeys()` that reads+parses the JSON if present (shape `{ keys: [ { id, name, hash, createdAt, revoked } ] }`), tolerating a missing file (`[]`). Re-load on each `requireAuth`/endpoint call is fine (cheap, ~1 tiny file) OR cache with a mtime check — pick the simplest correct approach; the file may be edited by the UI, so do NOT cache forever without invalidation.
   - `hashSecret(s)` = sha256 hex of the secret (reuse the existing crypto import; you may reuse or wrap `safeEqual`'s hashing).
   - `issuedKeyValid(token)`: look up a non-revoked key whose `hash` matches `hashSecret(token)` (constant-time compare against the stored hash to avoid timing leaks). Return the key record or null.
   - `issueKey(name)`: generate `crypto.randomBytes(24).toString("base64url")` as the raw key; store `{ id: randomBytes(6).toString("hex"), name, hash: hashSecret(raw), createdAt: Date.now(), revoked: false }`; persist the array atomically (write temp file + rename, or `writeFileP` — match existing style); return `{ id, name, raw, createdAt }` (raw only ONCE).
   - `revokeKey(id)`: set `revoked: true` and persist; return boolean.
3. Add a top-level config key `uiPasswordEnv` (exactly mirroring `masterKeyEnv`): it names an env var (recommended: `LMR_UI_PASSWORD`) whose value is the dashboard password. When unset, the UI stays open. When set with a non-empty env value, the UI-auth endpoints enforce the password (fail closed if the env value is empty at request time, same posture as `requireAuth`). Add UI-auth endpoints (never gated by `requireAuth` — they ARE the login):
   - `GET /api/auth/status` — always 200 JSON `{ passwordSet: !!uiPasswordEnv, uiAuthed: bool }`. When unset, `passwordSet:false` and the UI stays open. When set, `uiAuthed` is true iff the request carries the UI session cookie (below). NEVER echo the password.
   - `POST /api/auth/login` — body `{ password }`. If `uiPasswordEnv` is not configured, 200 `{ ok:true, uiAuthed:true }` (no-op). Otherwise: enforce a small brute-force throttle keyed by client IP (`req.socket.remoteAddress`): allow 5 attempts per 10s window; beyond that 429 with a generic message. Compare `hashSecret(body.password)` against `hashSecret(process.env[uiPasswordEnv])` with `safeEqual`/constant-time; on match set a cookie (below) and return `{ ok:true, uiAuthed:true }`; on mismatch 401 `{ ok:false }`.
   - `POST /api/auth/logout` — clear the cookie, 200 `{ ok:true }`.
   - Cookie: `lmr_ui=<token>`; `HttpOnly`, `SameSite=Strict` (omit `Secure` so plain-http localhost/LAN works). Token = `hashSecret(process.env[uiPasswordEnv] + ":" + <server-random-salt>)` or a random value stored in a module-level in-memory set of valid session tokens — prefer the simplest correct in-memory session-token set (survives until restart; logout removes it). `uiAuthed(req)` checks the cookie against that set.
   - These endpoints are gated by the UI password only; they must remain reachable without the master key (the UI login happens before any master-key usage). They are NOT part of `requireAuth`'s gated set.
4. In the request handler, wire it together:
   - After `requireAuth` (which now also accepts issued chat keys), when serving `GET /` (the UI): if `uiPasswordEnv` is configured and `!uiAuthed(req)`, serve the same `index.html` — the UI itself decides to show the login screen based on `/api/auth/status` (see Task 3). Server does not need to block `/`.
   - Add the three `/api/auth/*` routes and the keys routes below to the handler.
5. Add issued-keys management endpoints (admin-only, master-key gated by `requireAuth`):
   - `GET /api/keys` — 200 `{ keys: [ { id, name, createdAt, revoked } ] }` (NEVER hashes or raw values).
   - `POST /api/keys` — body `{ name }` (validate: non-empty string, max ~64 chars); 200 `{ key: { id, name, raw, createdAt } }` with `raw` only here.
   - `DELETE /api/keys?<id>` (or `POST /api/keys/revoke` with `{ id }` — pick one, match REST-ish style already present) — revoke; 200 `{ ok:true }` or 404.
   - All under the existing master-key gate (they start with `/api/keys`, which is NOT currently in `requireAuth`'s gated set — EXTEND `requireAuth`'s gate to include `/api/keys` so they're master-only. Chat remains the only issued-key path.)
6. Wire the UI's admin calls: the UI sends the master key as `Authorization: Bearer <masterKey>` on `/admin/*` calls (see Task 3). No server change needed beyond `requireAuth` (already gates `/admin/*`).

**Tests (append to `test.mjs`):**
- `zK` block — UI auth:
  - `zK1` no `uiPasswordEnv` -> `/api/auth/status` is `{ passwordSet:false, uiAuthed:true }`, `/` serves 200.
  - `zK2` with `uiPasswordEnv` + matching env value -> status `passwordSet:true, uiAuthed:false` before login; `POST /api/auth/login` with correct password sets cookie and returns `uiAuthed:true`; subsequent `/api/auth/status` with that cookie returns `uiAuthed:true`.
  - `zK3` wrong password -> 401, no cookie set.
  - `zK4` throttle: 6 rapid wrong attempts -> 429 on the 6th.
  - `zK5` logout clears the session -> status back to `uiAuthed:false`.
- `zL` block — issued keys:
  - `zL1` `POST /api/keys` without master bearer -> 401 (gated).
  - `zL2` with master bearer, `POST /api/keys {name}` -> 200 with `raw` starting `sk-lmr-` (use a sensible prefix), and the raw key works on `/v1/chat/completions` (backend hit).
  - `zL3` `GET /api/keys` -> 200, never contains `hash` or `raw`.
  - `zL4` revoke -> `DELETE` -> 200; the raw key now 401s on chat; master key still works.
  - `zL5` chat with a valid issued key while `masterKeyEnv` is set -> 200 (issued keys accepted).
  - `zL6` admin endpoint (`/admin/reset-health`) with an issued key -> 401 (master-only).
  - `zL7` missing `api-keys.json` file -> endpoints still work (empty list).
- Use the existing test harness patterns (temp configs, in-process mock backends, `bringUp` style). Temp `api-keys.json` path via `LMR_KEYS_FILE` env override to avoid touching the real file; clean up temp files.

**Constraints:**
- Zero npm dependencies. Plain Node built-ins only.
- Match existing style: 2-space indent, single quotes, semicolons, template literals, `crypto`/`fs` promises, rare comments explaining *why*.
- Never log raw keys or the password. Never return hashes or raw values from `GET /api/keys`.
- Do not change routing logic, config synthesis, health, or history semantics. All existing tests must still pass.
- Do not add an `Other`-style open question — make best judgment on minor naming, match the file's existing idioms.

**RETURN FORMAT:**
Report as a bullet list: (1) exact functions added/changed with line ranges, (2) the full list of new/changed endpoints and their auth requirements, (3) test block names + pass/fail counts, (4) any deviations from this brief, (5) HURDLES encountered (dead ends, wrong assumptions, long investigations).

**VERIFICATION:**
`node --check server.mjs && node --check test.mjs && node test.mjs` must pass (expect "N passed, 0 failed", N = 105 + new). Run it yourself and paste the final line.

**EXIT CONDITIONS:**
Stop and report if: the test suite fails in a way you cannot fix within the task scope; or you find the existing `requireAuth`/handler structure makes a listed requirement impossible without a redesign (report the redesign needed). Stopping with an honest report is success; continuing past an exit condition is failure.

---

### Task 2: UI — login gate, keys tab, auth plumbing

**Files:**
- Modify: `C:\dev\local-model-router\index.html`

**Context (implementer starts cold — read these first):**
- `index.html` is a single self-contained file (~2260 lines): inline CSS + JS, no external assets, no build. It has three tabs driven by URL hash: `#dashboard`, `#history`, `#playground` (tab bar at ~453-456; `activateView` at ~2184-2204; views are `<div id="view-...">` at ~460, ~497, ~512).
- There's a playground API-key field `pgKey` (line ~536, `pgAuthHeader()` at ~768-772) that attaches `authorization: Bearer <key>` to chat requests only.
- Admin calls go through `doAdmin(path, opts)` at ~1236-1241: `fetch(path, Object.assign({ method: "POST" }, opts))` with NO auth header today. Callers: `#resetHealth` (~1243-1246), `[data-reset-all]`, `[data-cool]`, `[data-uncool]` delegated clicks (~1248-1262).
- Polling: `poll()` (references at ~815, ~1182, ~1240) fetches `/health`, `/api/stats`, `/api/history`, `/api/config` on an interval. The UI must not call `/api/keys` or the auth endpoints in a tight loop.
- `$("id")` is a helper (likely `document.getElementById`). `esc()` exists for HTML-escaping user content (used in history rendering ~1090). Look for existing `esc` and reuse it for user-provided key names.

**Design (implement exactly):**

1. Add a **login screen**:
   - A full-screen overlay `<div id="loginOverlay" hidden>` with a password input (`#loginPass`), a submit button, an error line, and a small "Locked" note. When shown, hide the app (`#app` or the main container) or overlay it (overlay is simpler and non-destructive).
   - On load, `GET /api/auth/status`. If `passwordSet && !uiAuthed`: show the overlay, hide the tabs/app, and skip the normal poll loop until authed. If `!passwordSet`: never show it (open UI, exactly today). If `passwordSet && uiAuthed`: show normally (and the master key is expected in the stored config).
   - On submit: `POST /api/auth/login` with `{ password }`. On `{ ok:true }`: hide overlay, show app, start polling, refresh status. On 401: show "Wrong password". On 429: show "Too many attempts, wait a moment". Include a Logout control in the header that calls `/api/auth/logout` then reloads.
2. Add an **API Keys tab**:
   - New tab button `#tab-keys` (`data-view="keys"`, icon e.g. `key`) after Playground (~456), and a `<div id="view-keys">` panel with: a "New key" form (`#keyName` input + create button), a list of existing keys (name, id, created date, revoked badge, Revoke button), and a one-time "copy your key" notice shown right after creation (the raw value is only shown once; store it in a JS variable, not in the list).
   - `activateView`/tab wiring must include the new view (the existing hash-driven tab logic enumerates `.tab` elements — check and extend so `#keys` works like the others).
   - Visibility: the Keys tab is hidden until `uiAuthed` (or until master key is known). When the UI is open (no password set) the tab should still show but admin/master-gated calls will 401 until a master key is provided — simplest correct behavior: show the tab always, but show a "needs master key" hint and surface 401s from `/api/keys` as "master key required" in the UI.
3. **Wire auth into admin + keys calls:**
   - Extend `doAdmin` to attach `authorization: "Bearer " + masterKey()` where `masterKey()` reads from a stored value: keep a `state.masterKey` (in-memory), loaded from the login password by default (the same password can serve as the master key when they match, but do NOT assume it — better: the UI sends the login password as the master key on admin calls ONLY IF the server config uses the same env for both; simplest robust approach: after login, call `/api/config` and check whether `cfg.masterKeyEnv` is set and equals `cfg.uiPasswordEnv` name; if masterKeyEnv is set but different, show a small "Master key" input in the Keys tab/header that the user fills once and is kept in-memory + localStorage as a convenience, never sent anywhere except the `Authorization` header).
     - Keep it simple and match the project's lean philosophy: the login password IS the master key when both env vars point at the same value (which is the recommended `.env` setup — see Task 3); otherwise the user enters the master key once in a small field. Do not over-engineer.
   - Add a `keysApiAuth()` helper used by the Keys tab that attaches the same `Authorization` header to `/api/keys` calls.
4. **Playground:** keep `pgAuthHeader()` as-is (per-request key field). No change needed except it already works with issued keys.

**Constraints:**
- No external assets/CDNs/fonts. Inline everything. Match the existing visual style (CSS vars, `--panel`, `--border`, `.tab`, `.card`, button styles).
- Escape all user-provided names with `esc()` before injecting into HTML.
- Do not break the existing hash-tab behavior, history rendering, or polling.
- Never store the raw login password in localStorage. The master key may be stored in localStorage only as a convenience with a clear "local only" note (match how `pgKey` is persisted, ~1592).

**RETURN FORMAT:**
Report as a bullet list: (1) exact elements/functions added with line ranges, (2) how the login gate interacts with the poll loop, (3) how admin/keys calls now attach auth, (4) any deviations, (5) HURDLES.

**VERIFICATION:**
There is no build step and no UI test harness; the file must remain valid HTML/JS. Run `node --check index.html` is NOT valid (HTML). Instead: after editing, open the file in a headless check — extract the inline `<script>` block and run `node --check` on it (a quick way: a tiny node script that reads the file, pulls the script contents, and `new Function(code)`-compiles it; or use `node -e` with a regex). Paste the check result. Do NOT start a browser.

**EXIT CONDITIONS:**
Stop and report if: the inline JS does not compile, or a required UI element cannot be added without breaking an existing tab/view (report the conflict). Honest stop is success.

---

### Task 3: Config, env, gitignore, docs

**Files:**
- Modify: `C:\dev\local-model-router\config.json`
- Modify: `C:\dev\local-model-router\.env`
- Modify: `C:\dev\local-model-router\.env.example`
- Modify: `C:\dev\local-model-router\.gitignore`
- Modify: `C:\dev\local-model-router\README.md`
- Modify: `C:\dev\local-model-router\DESIGN-3LAYER.md`
- Modify: `C:\dev\local-model-router\AGENTS.md`
- Do NOT modify: `config.server.json` (already correct: `masterKeyEnv: "LITELLM_API_KEY"`)

**Context:**
- `config.json` (live, local) has `"masterKeyEnv": null` at line 4. `config.server.json` (server/Coolify) already sets `"masterKeyEnv": "LITELLM_API_KEY"` — leave it alone.
- `.env` currently has `LITELLM_API_KEY=<value>` (used as the master key value). `.env.example` has a stale comment: line 19-20 "Master key for the server-side config (config.server.json) - gates chat/admin" then `LITELLM_API_KEY=`. The master-key gate is implemented and tested but currently OFF locally.
- `.gitignore` must be checked; add `api-keys.json` if not present.
- README has a "Top-level keys" line (~170) and a config-contract table; DESIGN-3LAYER.md has the schema contract (masterKeyEnv mentioned ~127, ~554, ~630); AGENTS.md has a config-contract table (line 73). These are the authoritative docs — update them to reflect the new keys and endpoints, keeping the "verbatim" language style.

**Changes:**

1. `config.json`: set `"masterKeyEnv": "LITELLM_API_KEY"` (line 4).
2. `.env`: add (or confirm present, with real values):
   ```
   LMR_UI_PASSWORD=<the dashboard password you choose>
   LMR_MASTER_KEY=<same value as LITELLM_API_KEY today>
   ```
   Keep `LITELLM_API_KEY` as-is (it is the value dsh/OpenCode already send). `LMR_MASTER_KEY` is optional; if you add it, note the relationship. The code reads `masterKeyEnv` -> the env NAME in `config.json`, so `LITELLM_API_KEY` remains the master key value. Set `LMR_UI_PASSWORD` to a strong password you choose and record it for the user in the final report (do NOT paste it in this doc).
3. `.env.example`: add placeholder lines (values empty only — CI enforces placeholder-only):
   ```
   # Dashboard password - when set, the web UI requires this password to log in
   LMR_UI_PASSWORD=
   # Optional: name of an env var holding a separate master key for chat/admin
   # (defaults to LITELLM_API_KEY when unset)
   LMR_MASTER_KEY=
   ```
   Fix the stale `LITELLM_API_KEY` comment to say it is the master key for chat/admin.
4. `.gitignore`: add `api-keys.json` (and `*.json.tmp` if the atomic-write temp pattern needs it).
5. `README.md`:
   - Update the config-contract table/keys list to include `uiPasswordEnv` (or the env-var-only design if no config key was added) and the new endpoints (`/api/auth/*`, `/api/keys`).
   - Add a short "Security" section: how to enable the dashboard password, how issued keys work (chat-only), the throttle, and the note that read-only endpoints stay open.
6. `DESIGN-3LAYER.md`: add the new config/env keys + endpoints to the contract, matching the doc's existing style (verbatim key lists, endpoint table if present).
7. `AGENTS.md`: update the config-contract table (line ~73) and the Endpoints table to include `/api/auth/*` and `/api/keys`, and the Security Considerations section (issued keys are chat-only; master key stays admin-only; `api-keys.json` is gitignored).

**Constraints:**
- Do NOT put real secret VALUES in `.env.example`, README, DESIGN docs, or AGENTS.md (CI secret-scan fails). `.env` keeps real values and stays gitignored.
- Do not touch `config.server.json`.
- Keep docs in the existing tone/style; no invented structure.

**RETURN FORMAT:**
Bullet list of each file changed + the exact lines/keys added or changed. Confirm `config.json` now has `masterKeyEnv` set. Paste the final `.env.example` block and the `.gitignore` diff.

**VERIFICATION:**
`git -C C:\dev\local-model-router diff --stat` and `git -C C:\dev\local-model-router status --short`; confirm no real secrets are in any diff (run `git -C C:\dev\local-model-router diff | Select-String -Pattern 'sk-|password|KEY='` and confirm only `.env` (ignored) holds values).

**EXIT CONDITIONS:**
Stop and report if a real secret would land in a tracked file, or `config.server.json` is at risk of being touched. Honest stop is success.

---

### Task 4: Full verification

**Files:**
- Run (no edits): `C:\dev\local-model-router\test.mjs`

**Steps:**

- [ ] **Step 1: Syntax checks**
  Run: `node --check C:\dev\local-model-router\server.mjs` and `node --check C:\dev\local-model-router\test.mjs`
  Expected: no output, exit 0.

- [ ] **Step 2: Full test suite**
  Run: `node C:\dev\local-model-router\test.mjs` (from the repo dir)
  Expected: `N passed, 0 failed` where N >= 105 (the suite spawns real children on temp configs; safe to run while the live router is up).

- [ ] **Step 3: Secret scan**
  Run: `git -C C:\dev\local-model-router diff | Select-String -Pattern 'sk-|LMR_UI_PASSWORD=.+'`
  Expected: no matches (only `.env`, which is ignored, holds values).

- [ ] **Step 4: Report**
  Report the exact final test line, the diff stat, and flag anything that failed.

**EXIT CONDITIONS:**
Any red failure -> stop and report the exact error. Do not fix in this task; a fresh fix task will take it.

---

## Self-Review

**Spec coverage:**
- Password-gated dashboard: Task 1 (auth endpoints + session) + Task 2 (overlay) + Task 3 (env). Covered.
- API Keys tab with issue/revoke: Task 1 (`/api/keys` + store) + Task 2 (tab) + Task 3 (gitignore). Covered.
- Issued keys chat-only, admin master-only: Task 1 `requireAuth` extension + test `zL6`. Covered.
- Enable existing gate end-to-end: Task 3 (`config.json` `masterKeyEnv`). Covered.
- Brute-force throttle: Task 1 step 3 + test `zK4`. Covered.
- Stay-open-when-unset: Task 1 (`passwordSet:false`) + test `zK1` + Task 2 overlay logic. Covered.
- Zero deps / lean: no npm packages anywhere; single-file UI. Covered.
- Every behavior change ships a test: `zK*`/`zL*` blocks. Covered.
- Docs updated: Task 3. Covered.

**Placeholder scan:** No TBD/TODO; all steps carry exact code, commands, expected output, and file paths.

**Type consistency:** `uiPasswordEnv` env name, `LMR_UI_PASSWORD` value, `/api/auth/*` paths, `api-keys.json` path, `/api/keys` endpoints, `masterKeyEnv` gate, `requireAuth` extension, `zK`/`zL` test tags — consistent across all four tasks. `doAdmin` auth wiring and `pgAuthHeader` naming preserved from the existing file.
