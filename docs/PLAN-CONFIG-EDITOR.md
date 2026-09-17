# Plan: In-browser Config Editor (structured forms + raw JSON)

Status: approved by user 2026-09-11 (option: "Structured forms with an advanced raw-JSON fallback").
Target: the LOCAL dev instance (`127.0.0.1:8787`) for development and browser testing. Production
write-back is explicitly OUT OF SCOPE for this plan (see Scope Boundary).

## Goal

Let the user add and change Switchblade configuration (backends, models, presets, pricing, alerts,
missCapture, backoff) from the web UI instead of hand-editing `config.json`. Saving writes the file
that the router already watches; `fs.watch` on `CONFIG_PATH` picks the change up in roughly 300 ms,
so no restart and no reload endpoint is needed for any schema-level change.

## Constraints discovered while reading the code (these shape the design)

1. **`GET /api/config` returns the NORMALIZED config, not the file.** `loadConfig()` runs
   `normalizeConfig(parsed)` before serving it, and normalization rewrites legacy shapes and derives
   preset `members`. Saving that object back would silently rewrite the user's file and drop any
   top-level key normalization ignores. Observed live config top-level keys: `port, prefix,
   masterKeyEnv, uiPasswordEnv, backends, models, pricing, presets, backoff, missCapture, alerts`.
   Of these, `pricing`, `missCapture` and `alerts` are not part of the normalized three-layer schema,
   so a save round-tripped from `/api/config` would destroy cache-miss capture and ntfy alerts.
   **Therefore the editor must load raw from disk, never from `/api/config`.**
2. **`port` and `host` are server-owned.** They are read at startup to call `srv.listen(...)`.
   Editing them in the browser does nothing until a process restart, so the editor must reject them
   rather than silently accept a change that appears to work and does not.
3. **Auth already exists and is reusable.** `requireAuth(cfg, req)` gates `/admin/*` and `/api/keys`
   behind the master key, and additionally accepts a logged-in dashboard session when
   `uiPasswordEnv` is set. New write routes under `/api/config...` must be added to that same gate
   by extending the path check, so a human can use the password they already log in with.
4. **The file is live routing state.** A bad write can wedge routing for every consumer (dsh,
   OpenCode). Every save therefore takes a snapshot first, validates before writing, writes
   atomically, and the UI offers one-click revert.
5. **Historical reality check**: the local history has exactly 1 request in the last 7 days
   (backend `freellm`), so there is no real local traffic to disrupt while developing.

## Server work (`server.mjs`)

### New helpers

- `rawConfigPath()` -> resolves snapshot directory next to `CONFIG_PATH` (`config.history/`).
- `readRawConfig()` -> reads and JSON-parses the file as-is. Throws a typed error on parse failure.
- `snapshotConfig()` -> copies the current file into `config.history/<ISO-timestamp>.json`.
  Prunes to the newest `MAX_SNAPSHOTS = 20` by filename sort. Returns the snapshot filename.
- `writeConfigAtomic(obj)` -> serializes with 2-space indent, writes to `config.json.tmp` then
  `renameSync` over the target (same pattern already used for the keys store). `*.json.tmp` is
  already gitignored.
- `validateConfig(cfg, { env })` -> returns `{ fatal: [...], warnings: [...] }`.

### Validation rules

Fatal (reject the save, HTTP 400, change nothing):

- Body is not an object, or contains no `backends` / `models` / `presets` keys at all.
- `JSON.stringify` of the candidate fails (cycles).
- `port` or `host` present in the request body and differing from the on-disk value.
- Duplicate backend `id`.
- A backend missing `id`, `baseURL`, or `apiKeyEnv`.
- `apiKeyEnv` naming an env variable that is not set in `process.env` (reuses the server's own
  key-presence check; catches the "add a backend but forget the key" case at write time rather
  than at first request).
- A model provider referencing a backend id that does not exist.
- A model provider missing both `upstream` and a resolvable `backend.model`.
- A preset `models` entry referencing an id that is neither a model nor a preset (a preset of
  presets is legal, so both are accepted; the server expands recursively with cycle detection).
- Duplicate model ids, duplicate preset ids.
- `missCapture.file` containing a path separator or `..` (`configPathFor()` joins it to the config
  directory, so traversal would let the editor write outside the intended location).

Warnings (accepted, logged, surfaced in the UI, overridable by the user):

- Unknown top-level keys relative to the known set.
- A model or preset with an empty `providers` / `models` array (routes nowhere, 404 at request
  time).
- `affinityPool` greater than provider count (pools clamp, so this is harmless but usually a
  mistake).
- A preset id that shadows a model id (the preset wins in `/v1/models`, which is legal but
  confusing).
- `meta.pricing` missing on a model that other models in the same preset have.

Return shape: `{ ok: true, warnings: [...] }` on success; `{ ok: false, errors: [...],
warnings: [...] }` on rejection, where each entry is `{ path, message }` so the UI can point at a
field.

### New routes (all under the existing admin gate)

- `GET /api/config/raw` -> `{ config, revision, snapshots: [...] }` where `revision` is the mtimeMs
  of `config.json` (the merge token) and `snapshots` is the newest-first list of snapshot names.
- `POST /api/config` -> body `{ config, baseRevision, force }`.
  - 400 on JSON that fails `validateConfig` fatal rules.
  - **409 when `baseRevision` is present and does not match the current file mtime and the request
    is not `force: true`**, with `{ error: { message, type: "conflict" }, current: <raw config> }`
    so the UI can show "the file changed on disk, reload?". This is the lost-update guard: two
    browser tabs, or the user editing the file in VS Code, must not silently clobber each other.
  - On success: snapshot, atomic write, hot-reload via the existing watcher, respond
    `{ ok: true, revision, warnings, snapshot }`.
  - The write happens ONLY after validation passes; a fatal error leaves the file byte-identical.
- `POST /api/config/reset` -> body `{ snapshot }`. Validates the snapshot name is a bare basename
  from the snapshot directory (no separators, no `..`), snapshots the current state first, restores
  the named snapshot, responds with the same shape as a save. This is the undo button.

### Secret handling

`validateConfig` and every response path must never echo resolved key VALUES. `apiKeyEnv` names are
safe (already served by `/api/config` today). The tests assert that no value of any `apiKeyEnv`
appears anywhere in a save response or in `/api/config/raw`.

## UI work (`index.html`)

New tab `Config` (after API Keys), `VIEWS` extended, `activateView` loads raw config on entry.

Layout, single view:

1. **Save bar** (sticky): unsaved-change count, validation status line, `Validate`, `Save`,
   `Reload from disk`. Save is disabled while there are no changes or while validation reports a
   fatal error.
2. **Collection editors** as sub-sections with tables:
   - **Backends**: id, baseURL, apiKeyEnv (datalist of env names known to be set, from a new
     read-only field on the raw response), translateDeveloperRole, timeoutMs, params. Add / edit /
     duplicate / delete. Editing an `id` offers to rewrite the references in `models[].providers[]`.
   - **Models**: id, providers (rows of backend + upstream selects), affinityPool, params, meta
     (label, contextWindow, pricing.inputPerM / outputPerM / cacheHitInputPerM, offPeak opt-in).
     Provider backend select is populated from the backend list; a dangling reference is impossible
     to create from the form.
   - **Presets**: id, strategy (affinity / failover / weighted), models (list of picks from model +
     preset ids, with optional weight), affinityPool, params, meta.
   - **Settings**: pricing (offPeakMultiplier, peakWindows), missCapture, alerts, backoff, prefix,
     masterKeyEnv, uiPasswordEnv.
3. **Raw JSON mode**: toggle. A `<textarea>` holding pretty-printed JSON, a `Format` button, and a
   `Validate` button that calls the same validation through a dry-run save (`force: false` plus a
   flag that skips the write). Errors render with `path: message` lines. Warnings render in an
   amber panel with the "save anyway" acknowledgement.
4. **History**: newest-first snapshot list with timestamps, and a `Restore` button per row behind a
   confirm. Shows what a restore would replace.

Interaction rules: every edit mutates an in-memory draft object only; nothing touches the server
until Save. `beforeunload` warns when the draft is dirty. The poll loop must not overwrite a dirty
draft (the existing `loadConfig()` already avoids clobbering the playground; the editor gets the
same treatment).

## Tests (`test.mjs`)

New block, using the existing `startRouterCfg` harness (temp config, mock backends, no keys, no
network). Assertions:

1. `GET /api/config/raw` returns `pricing`, `missCapture` and `alerts` unchanged (the keys
   `/api/config` drops) - this is the regression guard for constraint 1.
2. Save with a valid new backend -> 200, file on disk contains it, warnings array present.
3. Save with a backend whose `apiKeyEnv` is unset -> 400, file on disk byte-identical.
4. Save with a provider referencing a missing backend -> 400.
5. Save with a preset referencing a missing model and preset -> 400.
6. Save with `missCapture.file = "../escape.jsonl"` -> 400.
7. Save with a changed `port` -> 400.
8. Stale `baseRevision` -> 409 and the file is unchanged; the same save with `force: true` -> 200.
9. Snapshot created on every successful save; `POST /api/config/reset` restores the previous bytes.
10. Reset with `snapshot: "../config.json"` -> 400.
11. Auth: with `masterKeyEnv` set, an unauthenticated save -> 401 and the file is unchanged.
12. Secret hygiene: no `apiKeyEnv` value appears in any response body.

Every new behavior ships with its assertion (project rule).

## Verification

- `node --check server.mjs && node --check test.mjs`
- `node test.mjs` -> must be green (256 existing + the new block).
- Live check on the running local instance: `GET /api/config/raw`, then a no-op save at
  `force: false` with the correct revision, then confirm `config.json` content and that
  `/api/config` reflects it and `/health` still answers.
- Browser: open `http://127.0.0.1:8787`, log in, walk the flow. Screenshot captured for the user
  to confirm visually (no vision model in this session, so the user is the reviewer).
- Restore `config.json` from `config.history/` at the end of manual testing.

## Scope boundary (explicit)

This writes the LOCAL `config.json` only. Production is the homelab container, whose live config is
`/app/config.server.json` (a Coolify bind mount). A production write-back would need a different
mechanism (the container rewriting its own bind mount, or a host-side agent), plus the whole
backup/merge/md5-verify protocol. That is a separate decision and is NOT part of this plan. The
repo `config.json` stays the mirror; nothing here changes the production-first deploy rule.

## Files touched

- `server.mjs` (helpers, validation, 3 routes, gate extension)
- `index.html` (Config tab, editors, raw mode, history)
- `test.mjs` (new test block)
- `docs/PLAN-CONFIG-EDITOR.md` (this file)
- `AGENTS.md` / `README.md` (endpoint table + UI section, only if the feature ships)
