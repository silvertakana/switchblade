# Switchblade Config v3 Implementation Plan
> **For agentic workers:** REQUIRED SUB-SKILL: Before executing this plan task-by-task, read `C:\Users\silve\.dsh\skills\dsh-subagent-usage\SKILL.md` and its `references\parallel-sdd.md`, which carry the subagent-driven execution loop (fresh implementer per task, then spec review, then code quality review). That hub is model-invocable, so load it with the `skill` tool by name (`dsh-subagent-usage`); the path above is the fallback for reading it directly with the `read` tool. Steps use checkbox (`- [ ]`) syntax for tracking.
**Goal:** Evolve Switchblade in place to the v3 config model (models own deployments, fallback presets, per-deployment cooling) with the served id set unchanged.
**Architecture:** Path A in-place evolution behind a bridge: shared validator first, editor merge-preserve second, dual-shape v3 bridge third, one SSH live conversion fourth, cooling re-key fifth, legacy removal sixth. Each increment ends shippable and revertible with the suite green.
**Tech Stack:** Zero-dependency Node ESM (.mjs), plain node:http/node:fs, single-file index.html UI, mock-based test.mjs suite.
**Spec:** docs/superpowers/specs/2026-09-19-switchblade-config-v3-spec.md
## Global Constraints
- Zero npm dependencies (no new packages, ever).
- ESM .mjs only, 2-space indent, single quotes, semicolons.
- Two source files (server.mjs, index.html) — new code lives there unless the spec says otherwise.
- Every behavior change ships with a test in test.mjs.
- Gate per task = `node --check server.mjs` clean AND `node test.mjs` ends "N passed, 0 failed" (no per-test filter exists in this suite; the full suite is the gate).
- Config carries env NAMES only — never write, log, or commit a raw key value.
- Conventional Commits (`feat:`, `fix:`, `refactor:`, `docs:`), one logical change per commit.
- The repo config.json mirror is never committed (it carries foreign uncommitted changes and is only a mirror).
- Production config changes go over SSH per the switchblade runbook at `C:\Users\silve\.dsh\skills\switchblade\references\deploy-config.md`, never by editing the repo mirror and calling it deployed.
- Commands run under pwsh on Windows.
- Baseline at planning time: Node v25.9.0, `node --check server.mjs` and `node --check test.mjs` clean, `node test.mjs` ends "374 passed, 0 failed".
---
## Increment 1 — Shared validation (no schema change)

### Task 1: Typed model checks on the old shape (T1 save-side, old-shape half)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:349-384` (models loop inside validateConfig)
- Test: `C:\dev\local-model-router\test.mjs` (new `v31)` block after the `y)` block, near line 742)
**Interfaces:**
- Consumes: `validateConfig(candidate, current) -> { errors: [{path, message}], warnings: [{path, message}] }` (existing, server.mjs:305)
- Produces: typed error paths `models.<id>.retry`, `models.<id>.meta.pricing.<field>`, `models.<id>.affinityPool`, `models.<id>.params`, `models.<id>.providers[<i>].params`, `models.<id>.providers[<i>].paramMap` for Tasks 2-5 (save/load/reset wiring)
- [ ] **Step 1: Write the failing test**
```js
// test.mjs — first, extend the static import on line 12 to include validateConfig:
// import { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, validateConfig } from "./server.mjs";
// (server.mjs export list is extended in Step 3 of this same task, so add both
// together before running; the import change alone is not a behavior change.)
// Then, inside main(), after the y) block:
{
  const base = {
    backends: [{ id: 'b1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_B1' }],
    models: {
      'm1': {
        providers: [{ backend: 'b1', upstream: 'u1' }],
        retry: '',
        affinityPool: 1.5,
        params: 'nope',
        meta: { pricing: { inputPerM: 'cheap' } }
      }
    },
    presets: { 'p1': { strategy: 'affinity', models: ['m1'] } }
  };
  process.env.KEY_B1 = 'x';
  const v = validateConfig(base, base);
  const paths = v.errors.map((e) => e.path);
  assert(paths.includes('models.m1.retry'), 'v31a) retry "" is an error (' + JSON.stringify(paths) + ')');
  assert(paths.includes('models.m1.affinityPool'), 'v31b) non-integer affinityPool is an error');
  assert(paths.includes('models.m1.params'), 'v31c) non-object params is an error');
  assert(paths.includes('models.m1.meta.pricing.inputPerM'), 'v31d) non-numeric pricing is an error');
  delete process.env.KEY_B1;
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL lines `v31a)`, `v31b)`, `v31c)`, `v31d)` (today validateConfig has no retry/pricing/affinityPool-integer checks, so `errors` is empty); tail reads "N passed, M failed" with M > 0.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — inside validateConfig, models loop, after the providers.forEach block:
if (m.affinityPool !== undefined && (!Number.isInteger(m.affinityPool) || m.affinityPool < 1)) {
  err(at + '.affinityPool', 'affinityPool must be an integer >= 1');
}
if (m.retry !== undefined) {
  if (!m.retry || typeof m.retry !== 'object' || Array.isArray(m.retry)) {
    err(at + '.retry', 'retry must be an object {maxRetries, baseMs, maxMs, multiplier, totalMs}');
  } else {
    if (!Number.isInteger(m.retry.maxRetries) || m.retry.maxRetries < 0) err(at + '.retry.maxRetries', 'retry.maxRetries must be a non-negative integer');
    for (const k of ['baseMs', 'maxMs', 'multiplier', 'totalMs']) {
      if (m.retry[k] !== undefined && (typeof m.retry[k] !== 'number' || !(m.retry[k] >= 0))) err(at + '.retry.' + k, 'retry.' + k + ' must be a number');
    }
  }
}
if (m.params !== undefined && (!m.params || typeof m.params !== 'object' || Array.isArray(m.params))) {
  err(at + '.params', 'params must be an object');
}
providers.forEach((pr, i) => {
  const pat = at + '.providers[' + i + ']';
  if (pr && pr.params !== undefined && (!pr.params || typeof pr.params !== 'object' || Array.isArray(pr.params))) err(pat + '.params', 'params must be an object');
  if (pr && pr.paramMap !== undefined && (!pr.paramMap || typeof pr.paramMap !== 'object' || Array.isArray(pr.paramMap))) err(pat + '.paramMap', 'paramMap must be an object');
});
if (m.meta !== undefined && m.meta.pricing !== undefined) {
  const prc = m.meta.pricing;
  if (!prc || typeof prc !== 'object' || Array.isArray(prc)) {
    err(at + '.meta.pricing', 'meta.pricing must be an object');
  } else {
    for (const k of ['inputPerM', 'outputPerM', 'cacheHitInputPerM']) {
      if (prc[k] !== undefined && typeof prc[k] !== 'number') err(at + '.meta.pricing.' + k, 'meta.pricing.' + k + ' must be a number');
    }
  }
}
```
// server.mjs:3504 — export the validator for tests (same task, same commit):
```js
export { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, validateConfig };
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `node --check` clean; `v31a)` through `v31d)` PASS; tail reads "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: typed model validation (retry, affinityPool, params, pricing)'
```

### Task 2: Collision stays warning-class for old-shape input (T2 characterization)
**Files:**
- Modify: none (characterization — the `server.mjs:402` warn IS the implementation; old configs validate exactly as today per the spec)
- Test: `C:\dev\local-model-router\test.mjs` (new `v32)` block after Task 1 block)
**Interfaces:**
- Consumes: typed error paths from Task 1
- Produces: pinned rule "old-shape collision = warn, save succeeds" for Task 4 (load must not reject colliding old configs) and Task 9 (round-trip stays green); the v3-shape XOR error is carried by Task 10b (the v3 branch of `validateConfig`), and this task's Step 3 comment records that the old-shape warn at `server.mjs:402` stays verbatim (there is no separate "Task 12b")
- [ ] **Step 1: Write the characterization test (uses the Task 1 static `validateConfig` import — no fresh import)**
```js
{
  process.env.KEY_B1 = 'x';
  const cfg = {
    backends: [{ id: 'b1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_B1' }],
    models: { 'same': { providers: [{ backend: 'b1', upstream: 'u1' }] } },
    presets: { 'same': { strategy: 'affinity', models: ['same'] } }
  };
  const v = validateConfig(cfg, cfg);
  assert(v.errors.length === 0, 'v32a) old-shape collision is NOT an error (' + JSON.stringify(v.errors) + ')');
  assert(v.warnings.some((w) => w.path === 'presets.same'), 'v32b) old-shape collision warns at presets.same (' + JSON.stringify(v.warnings) + ')');
  delete process.env.KEY_B1;
}
```
- [ ] **Step 2: Run test to confirm current behavior**
Run: `node --check server.mjs; node test.mjs`
Expected: `v32a)` and `v32b)` already PASS (this documents the spec's "old configs validate as today" rule; no FAIL phase — characterization, not a behavior change).
- [ ] **Step 3: No implementation change (record the deferral)**
```js
// server.mjs:402 stays verbatim:
// if (modelIds.has(id)) warn(at, `id '${id}' is both a model and a preset; the preset wins in /v1/models`);
// The error-class XOR lands in Task 10b and applies ONLY to v3-shaped input
// (old-shape input converts first; conversion drops identity presets, so no
// collision can remain). After increment 4 the production config is v3 and the
// error path governs.
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v32a)` and `v32b)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit (test-only)**
```pwsh
git add test.mjs
git commit -m 'test: pin collision warning-class for old-shape configs'
```

### Task 3: Unknown-ref error + strategy/unknown-key characterization (save/load split prep)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:386-444` (presets loop + known-keys tail of validateConfig) — ONLY the save-class/load-class split; the unknown-ref error and the strategy/unknown-key behaviors below already exist and are characterized, not re-implemented
- Test: `C:\dev\local-model-router\test.mjs` (new `v33)` block after Task 2 block)
**Interfaces:**
- Consumes: Task 1-2 error paths
- Produces: signature `validateConfig(candidate, current, opts) -> opts.mode 'save'|'load'` with "unknown preset ref = error at save, warn+drop at load" for Task 4 (load rejects error-class only) and Task 10 (converter output validates at save); strategy warn + unknown-key warn pinned as-is
- [ ] **Step 1: Write the characterization test (uses the Task 1 static `validateConfig` import — no fresh import)**
```js
{
  process.env.KEY_B1 = 'x';
  const cfg = {
    backends: [{ id: 'b1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_B1' }],
    models: { 'm1': { providers: [{ backend: 'b1', upstream: 'u1' }] } },
    presets: {
      'p1': { strategy: 'affinity', models: ['ghost'] },
      'p2': { strategy: 'sideways', models: ['m1'] }
    },
    extraFuture: { keep: true }
  };
  const v = validateConfig(cfg, cfg);
  assert(v.errors.some((e) => e.path === 'presets.p1.models[0]'), 'v33a) unknown preset ref is an error (' + JSON.stringify(v.errors) + ')');
  assert(v.warnings.some((w) => w.path === 'presets.p2.strategy'), 'v33b) unknown strategy warns and falls back (' + JSON.stringify(v.warnings) + ')');
  assert(v.warnings.some((w) => w.path === 'extraFuture'), 'v33c) unknown top-level key warns');
  // Save/load split: load tolerates the dangling ref (warn+drop), save refuses it.
  const vl = validateConfig(cfg, cfg, { mode: 'load' });
  assert(vl.errors.every((e) => e.path !== 'presets.p1.models[0]'), 'v33d) load mode downgrades the dangling ref to warn (' + JSON.stringify(vl.errors) + ')');
  assert(vl.warnings.some((w) => w.path === 'presets.p1.models[0]'), 'v33e) load warn carries the ref path');
  delete process.env.KEY_B1;
}
```
- [ ] **Step 2: Run test to verify the partial fail**
Run: `node --check server.mjs; node test.mjs`
Expected: `v33a)` through `v33c)` already PASS (they pin existing behavior at server.mjs:414-416 for the error, :399-400 for the strategy warn, :440 for the unknown-key warn — characterization, no FAIL phase for those); `v33d)`/`v33e)` FAIL (no `opts.mode` exists yet).
- [ ] **Step 3: Write minimal implementation (mode split only — the three characterized branches stay verbatim)**
```js
// server.mjs — validateConfig signature gains opts; presets loop, refs forEach:
// (the unknown-ref err at :414-416, the strategy warn at :399-400, and the
// unknown-key warn at :440 are UNCHANGED; only this branch is new):
function validateConfig(candidate, current, opts) {
  const mode = (opts && opts.mode) || 'save';
  // ...
  // inside the refs forEach, replacing the unconditional err:
  if (!modelIds.has(ref) && !presetIds.has(ref)) {
    if (mode === 'load') {
      warn(sat, 'unknown model or preset \'' + ref + '\'; ref dropped at load');
    } else {
      err(sat, 'unknown model or preset \'' + ref + '\'');
    }
  }
}
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v33a)` through `v33e)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: save-load validation split for dangling preset refs'
```

### Task 4: Load wiring — validate before normalize, non-destructive
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:2894-2916` (loadConfig)
- Test: `C:\dev\local-model-router\test.mjs` (new `v34)` block: child router keeps serving the previous config after a broken on-disk write)
**Interfaces:**
- Consumes: Tasks 1-3 error paths (load mode: dangling refs are warn+drop, never a rejection)
- Produces: load guarantee "error-class finding leaves the previous config running; boot aborts with field paths" for Tasks 5, 11, 13
- [ ] **Step 1: Write the failing test**
```js
{
  const dir = await mkdtemp(join(tmpdir(), 'lmr-v34-'));
  const cfgPath = join(dir, 'config.json');
  const envPath = join(dir, '.env');
  const good = {
    port: 0, prefix: '/v1', masterKeyEnv: null,
    backends: [{ id: 'v34', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_V34' }],
    models: { 'v34m': { providers: [{ backend: 'v34', upstream: 'u' }], meta: { label: 'good' } } },
    presets: {}
  };
  await writeFile(cfgPath, JSON.stringify(good, null, 2));
  await writeFile(envPath, 'KEY_V34=k\n');
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: join(import.meta.dirname),
    env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let base = null;
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('no start')), 8000);
    child.stderr.on('data', (d) => {
      const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
      if (m) { base = 'http://127.0.0.1:' + parseInt(m[1], 10); clearTimeout(t); resolve(); }
    });
  });
  try {
    await new Promise((r) => setTimeout(r, 300));
    // meta.label SURVIVES normalizeConfig verbatim (normalize never touches
    // meta), so flipping it proves whether the broken reload was rejected or
    // loaded: retry:'' would be silently dropped by normalize and is useless
    // as a probe here.
    const bad = JSON.parse(JSON.stringify(good));
    bad.models.v34m.retry = '';
    bad.models.v34m.meta.label = 'broken-write';
    await writeFile(cfgPath, JSON.stringify(bad, null, 2));
    await new Promise((r) => setTimeout(r, 900));
    const live = await (await fetch(base + '/api/config')).json();
    assert(live.models.v34m && live.models.v34m.meta.label === 'good', 'v34a) bad reload keeps the previous config running (label=' + (live.models.v34m && live.models.v34m.meta.label) + ')');
  } finally {
    child.kill();
    await rm(dir, { recursive: true, force: true });
  }
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v34a)` (today loadConfig calls normalizeConfig with no validateConfig gate, so the broken retry shape loads and the label flips to 'broken-write').
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — inside loadConfig, after JSON.parse, before normalizeConfig(parsed):
// Load mode: dangling preset refs are warn+drop (Task 3 split), so a stale ref
// on disk never wedges the running router; every other error-class finding
// rejects the reload and keeps the previous config.
{
  const verdict = validateConfig(parsed, cfg, { mode: 'load' });
  if (verdict.errors.length && !cfg) {
    throw new Error('config invalid: ' + verdict.errors.map((e) => (e.path || '(root)') + ': ' + e.message).join('; '));
  }
  if (verdict.errors.length && cfg) {
    console.error('[' + new Date().toISOString() + '] config reload rejected: ' + verdict.errors.map((e) => (e.path || '(root)') + ': ' + e.message).join('; '));
    return cfg;
  }
  for (const w of verdict.warnings) console.warn('config warning: ' + w.path + ': ' + w.message);
}
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v34a)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: validating config load keeps previous config on error'
```

### Task 5: Reset hardening — same validator, RESTART_ONLY, baseRevision (T7 reset half)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:3113-3145` (POST /api/config/reset)
- Test: `C:\dev\local-model-router\test.mjs` (new `v35)` block in the config-editor region near line 3473)
**Interfaces:**
- Consumes: Tasks 1-4 validator + load guarantee
- Produces: reset guarantee "RESTART_ONLY + baseRevision enforced; corrupt snapshot refused without touching the live file" for Task 13
- [ ] **Step 1: Write the failing test**
```js
{
  const r = await post('/api/config/reset', { snapshot: targetSnap, baseRevision: 1 });
  assert(r.status === 409, 'v35a) stale baseRevision on reset -> 409 (got ' + r.status + ')');
  const r2 = await post('/api/config/reset', { snapshot: targetSnap, baseRevision: 1, force: true });
  assert(r2.status === 200, 'v35b) forced reset with guards passes (got ' + r2.status + ')');
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v35a)` because today reset ignores baseRevision and calls validateConfig(restored, restored), which can never trip RESTART_ONLY.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — inside POST /api/config/reset, replacing validateConfig(restored, restored):
let currentRaw;
try {
  currentRaw = readRawConfig();
} catch (e) {
  return writeError(res, newError(500, JSON.stringify({ error: { message: 'cannot read the current config: ' + e.message, type: 'router_error' } })));
}
const verdict = validateConfig(restored, currentRaw);
if (verdict.errors.length) {
  res.writeHead(400, { 'content-type': 'application/json' });
  return res.end(JSON.stringify({ ok: false, errors: verdict.errors, warnings: verdict.warnings }));
}
if (input.baseRevision != null && Number(input.baseRevision) !== configRevision() && input.force !== true) {
  res.writeHead(409, { 'content-type': 'application/json' });
  return res.end(JSON.stringify({ ok: false, error: { message: 'the config file changed on disk since this page loaded it', type: 'conflict' }, warnings: verdict.warnings, revision: configRevision() }));
}
// CAUTION: keep the revision guard CONDITIONAL on `input.baseRevision != null`.
// A reset request that omits baseRevision must still be accepted: the existing
// CE reset test (test.mjs:3473) is exactly that shape, so an unconditional
// `Number(input.baseRevision) !== configRevision()` would 409 it and regress.
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v35a)` and `v35b)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'fix: reset uses shared validator plus restart and revision guards'
```
---
## Increment 2 — Editor merge-preserve + UI lists (no schema change)

### Task 6: Editor merge-preserve (cfgModalCollect/cfgApplyModal)
**Files:**
- Modify: `C:\dev\local-model-router\index.html:4900-5023` (cfgModalCollect + cfgApplyModal)
- Test: `C:\dev\local-model-router\test.mjs` — none (no unit harness exists for the single-file UI; verification is the live round-trip in Task 9 plus the API suite gate)
**Interfaces:**
- Consumes: none (UI-only; reads cfg.draft entry shape from the raw config)
- Produces: merge-preserve collect/apply contract for Task 14 (v3 inputs build on it)
- [ ] **Step 1: Write the failing live check (no unit harness exists for the single-file UI)**
```js
// Manual live check recorded as the task test: open the dashboard Config tab,
// edit model glm-5.3-flash notes only, save, then GET /api/config/raw and
// confirm all four providers still carry their dropParams entries.
```
- [ ] **Step 2: Run check to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: the round-trip FAILS today (cfgModalCollect rebuilds providers as `{backend, upstream}` only at index.html:4922-4924, dropping per-provider dropParams/params/paramMap); the API suite itself still ends "N passed, 0 failed".
- [ ] **Step 3: Write minimal implementation**
```js
// index.html — models branch of cfgModalCollect: start from the draft entry,
// apply only edited fields, keep unknown keys:
if (kind === 'models') {
  const prev = (cfg.draft.models && cfg.draft.models[cfg.modal.key]) || {};
  const m = Object.assign({}, prev);
  m.providers = (cfg.modal.provs || [])
    .filter((p) => p.backend)
    .map((p) => {
      const out = Object.assign({}, p.prev || {});
      out.backend = p.backend;
      if (p.upstream) out.upstream = p.upstream; else delete out.upstream;
      return out;
    });
  // ...apply affinityPool/meta/params/retry only when the field is non-empty,
  // delete the key when explicitly cleared, leave every other key untouched...
  return m;
}
```
```js
// index.html — cfg.modal.provs rows now stash the previous provider object:
cfg.modal.provs = (Array.isArray(data.providers) ? data.providers : []).map((p) => ({ backend: p.backend || '', upstream: p.upstream || '', prev: p }));
```
- [ ] **Step 4: Run check to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: round-trip PASSES (dropParams survive); suite ends "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add index.html
git commit -m 'fix: config editor merges instead of rebuilding entries'
```

### Task 7: Served-id UI lists + stale-selection fallback
**Files:**
- Modify: `C:\dev\local-model-router\index.html:2309-2333` (pgModel populate), compare populate near `populateCompareModels`, `3167-3179` (loadPlaygroundState restore)
- Test: `C:\dev\local-model-router\test.mjs` — none (no unit harness exists for the single-file UI; verification is live below plus the API suite gate)
**Interfaces:**
- Consumes: none (reads state.cfg served ids; server rule is presets-then-models from Task 12)
- Produces: `servedIds()` UI helper (exposed presets then exposed models) for Task 14
- [ ] **Step 1: Write the failing live check**
```js
// Live check: with an exposed:false model present, open Playground and Compare;
// both pickers must omit it, and restoring a deleted preset selection must not
// post an empty model value.
function servedIds(cfg) {
  const out = [];
  for (const id of Object.keys(cfg.presets || {})) if (cfg.presets[id].exposed !== false && !out.includes(id)) out.push(id);
  for (const id of Object.keys(cfg.models || {})) if (cfg.models[id].exposed !== false && !out.includes(id)) out.push(id);
  return out;
}
```
- [ ] **Step 2: Run check to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: FAIL today (populate uses presets-only list at index.html:2313, compare lists presets, deleted-selection fallback posts the stale value).
- [ ] **Step 3: Write minimal implementation**
```js
// index.html — single shared helper + both pickers use it:
function servedIds(cfg) {
  const out = [];
  for (const id of Object.keys((cfg && cfg.presets) || {})) {
    if (cfg.presets[id] && cfg.presets[id].exposed === false) continue;
    if (!out.includes(id)) out.push(id);
  }
  for (const id of Object.keys((cfg && cfg.models) || {})) {
    if (cfg.models[id] && cfg.models[id].exposed === false) continue;
    if (!out.includes(id)) out.push(id);
  }
  return out;
}
// populate: const models = servedIds(state.cfg);
// stale restore: if (!models.includes(saved.model)) { sel.value = models[0] || ''; }
// send path: const modelId = $('pgModel').value; if (!modelId) return showError('pick a model');
```
- [ ] **Step 4: Run check to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: hidden ids absent from both pickers; stale selection falls back to the first served id and never posts empty; suite ends "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add index.html
git commit -m 'feat: playground and compare lists follow the served-id rule'
```

### Task 8: Post-save and post-env refreshes
**Files:**
- Modify: `C:\dev\local-model-router\index.html:3956-4010` (cfgSave success path, after `refreshConfigFromDisk(true)`), `C:\dev\local-model-router\index.html:4489-4526` (envAdd success path), `C:\dev\local-model-router\index.html:4591-4623` (envSaveEdit success path), new `renderEnvPickers` + `refreshCfgEnvNames` helpers next to `loadEnvVars` (`C:\dev\local-model-router\index.html:4312`)
- Test: `C:\dev\local-model-router\test.mjs` — none (no unit harness exists for the single-file UI; verification is live below plus the API suite gate)
**Interfaces:**
- Consumes: Task 6 merge-preserve draft
- Produces: refresh contract "save -> Models/Presets tables + config viewer refresh; env write -> Key env list refresh" for Task 9 verification
- [ ] **Step 1: Write the failing live check**
```js
// Live check: edit a model label, Save, and without reloading confirm the
// Models table and config viewer show the new label; write an env var and
// confirm the backend Key env picker lists the new name.
```
- [ ] **Step 2: Run check to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: FAIL today (refreshConfigFromDisk re-renders the tables via renderConfig, but the `#configView` viewer is only written by the dashboard loader at index.html:2323, `cfg.envNames` is only set by applyRawPayload at :3887, and `renderEnvPickers` does not exist at all — the backend Key env select is built once by cfgField at :4781 — so the viewer and the Key env picker stay stale until manual reload).
- [ ] **Step 3: Write minimal implementation**
```js
// index.html — cfgSave success path, after await refreshConfigFromDisk(true):
// (tables already re-render via renderConfig; sync the viewer too)
const cv = $('configView');
if (cv) cv.innerHTML = cfg.draft ? highlightJSON(cfg.draft) : 'Config unavailable.';
```
```js
// index.html — renderEnvPickers does NOT exist today: the backend "Key env"
// <select> is built ONCE by cfgField (index.html:4781, options from
// cfgEnvNames()) and nothing re-renders it, so a variable written from the env
// editor never appears in an open form until a page reload. This task CREATES
// the function next to loadEnvVars and makes it the single re-render path for
// every env-name picker:
function renderEnvPickers() {
  const names = cfgEnvNames();
  const sel = $('f_apiKeyEnv');
  if (sel) {
    const cur = sel.value;
    const opts = ['<option value="">(choose an environment variable)</option>']
      .concat((names.includes(cur) || !cur ? names : names.concat([cur])).map((n) => "<option value='" + esc(n) + "'" + (n === cur ? ' selected' : '') + '>' + esc(n) + '</option>'));
    sel.innerHTML = opts.join('');
  }
  renderCfgBackends(); // the table's "not set in this environment" badge reads the same list
}
```
```js
// index.html — new helper next to loadEnvVars: re-read the env NAME list so
// a newly added variable appears in the backend Key env picker (which reads
// cfgEnvNames(), index.html:3850) without a page reload:
async function refreshCfgEnvNames() {
  try {
    const r = await fetch('/api/env', { headers: keysApiAuth() || {}, cache: 'no-store' });
    const p = await r.json();
    const names = Array.isArray(p.vars) ? p.vars.map((v) => v.name).filter(Boolean) : null;
    if (names) { cfg.envNames = names; renderConfig(); renderEnvPickers(); }
  } catch {}
}
// envAdd success path (after await loadEnvVars({ keepIssue: true })):
// envSaveEdit success path (after await loadEnvVars({ keepIssue: true })):
await refreshCfgEnvNames();
```
- [ ] **Step 4: Run check to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: tables, viewer, and Key env picker update with no manual reload; suite ends "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add index.html
git commit -m 'fix: dashboard refreshes config state after save and env writes'
```

### Task 9: Editor live round-trip + phone-width verification
**Files:**
- Modify: none (verification-only task; fixes go to Tasks 6-8 if this fails)
- Test: `C:\dev\local-model-router\test.mjs` — API suite gate only (no unit harness exists for the single-file UI)
**Interfaces:**
- Consumes: Tasks 6-8 refresh and merge contracts
- Produces: none (gate before Increment 3 ships)
- [ ] **Step 1: Write the live verification script**
```js
// 1) GET /api/config/raw, record glm-5.3-flash providers[].dropParams.
// 2) Dashboard Config tab: open glm-5.3-flash, change only the label, Save.
// 3) GET /api/config/raw again: all four providers keep identical dropParams.
// 4) Playground picker lists standalone models; Compare lists served ids.
// 5) Phone width (360px): Config modal and Playground send usable, no overlap.
```
- [ ] **Step 2: Run verification to confirm the starting state**
Run: `node --check server.mjs; node test.mjs`
Expected: suite ends "N passed, 0 failed"; if any live check in Step 1 fails, fix in Tasks 6-8, not here.
- [ ] **Step 3: Record the passing observations**
```pwsh
# round-trip evidence (run from the dev machine against the LOCAL dev instance only):
# 1) before/after diff of the glm-5.3-flash entry shows label-only change
# 2) Playground + Compare screenshots at 360px attached to the ship PR
```
- [ ] **Step 4: Run the gate once more**
Run: `node --check server.mjs; node test.mjs`
Expected: PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Ship Increment 2 (code increments ship via PR/push per AGENTS.md)**
```pwsh
git log --oneline -4
git push origin main
```
---
## Increment 3 — Schema v3 bridge (dual-shape read, v3 writes)

### Task 10: convertLegacyConfig + export (bridge rules, fixed order)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:1275` (new convertLegacyConfig above normalizeConfig), `C:\dev\local-model-router\server.mjs:3504` (export list)
- Test: `C:\dev\local-model-router\test.mjs` (new `v36)` converter unit block after Task 5 blocks)
**Interfaces:**
- Consumes: Tasks 1-3 error paths (converter output must pass the validator)
- Produces: `convertLegacyConfig(raw) -> { config: <v3>, warnings: [string] }` for Tasks 11-13 and 18
- [ ] **Step 1: Write the failing test**
```js
// test.mjs — first extend the static import on line 12 to include
// convertLegacyConfig (same task, same commit as the Step 3 export change).
// Then, inside main(), after the Task 5 blocks:
{
  const old = {
    backends: [
      { id: 'b1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_B1' },
      { id: 'b2', baseURL: 'http://127.0.0.1:2', apiKeyEnv: 'KEY_B2' },
      { id: 'b3', baseURL: 'http://127.0.0.1:3', apiKeyEnv: 'KEY_B3' }
    ],
    models: {
      'm1': { providers: [{ backend: 'b1', upstream: 'u' }, { backend: 'b2', upstream: 'u' }, { backend: 'b3', upstream: 'u' }], affinityPool: 2 },
      'kimi-k3': { providers: [{ backend: 'b1', upstream: 'u' }] }
    },
    presets: {
      'kimi-k3': { strategy: 'affinity', models: [{ model: 'kimi-k3', weight: 1 }] },
      'w': { strategy: 'weighted', models: ['m1'] },
      'tier': { strategy: 'failover', models: ['m1', 'kimi-k3'] },
      'amulti': { strategy: 'affinity', models: ['m1', 'kimi-k3'] }
    },
    params: { temperature: 0.2 },
    backoff: { serverBaseMs: 50 }
  };
  const r = convertLegacyConfig(old);
  assert(r.config.models.m1.deployments.length === 3, 'v36a) providers map 1:1 to deployments');
  assert(r.config.models.m1.deployments[2].backupOnly === true, 'v36b) affinityPool 2 of 3 marks providers[2..] backupOnly');
  assert(!r.config.presets['kimi-k3'], 'v36c) identity preset dropped, model stays addressable');
  assert(r.config.presets.w.strategy === 'fallback', 'v36d) weighted preset maps to fallback silently (order preserved; per-ref weights warned separately)');
  assert(r.warnings.some((w) => w.includes('kimi-k3')), 'v36e) identity drop warned first');
  assert(r.config.params.temperature === 0.2 && r.config.backoff.serverBaseMs === 50, 'v36f) untouched blocks carried through');
  // Converter warns ONLY on behavior changes: an affinity preset with MORE THAN
  // ONE ref loses its session-spread under ordered fallback. The live mirror's
  // five affinity presets (deepseek-v4-flash, deepseek-v4-flash-alt,
  // deepseek-v4-pro, mimo-v2.5-pro, deepseek-v4.1-flash) are each SINGLE-ref, so
  // none of them is a behavior change: three of the five (deepseek-v4-pro,
  // mimo-v2.5-pro, deepseek-v4.1-flash) are identity presets dropped by step 1
  // with an identity warning, and the other two (deepseek-v4-flash,
  // deepseek-v4-flash-alt) are alias presets whose one ref is a different model
  // id, so mapped to fallback they behave exactly as before.
  // failover->fallback and weighted->fallback keep declared order = NOT a
  // behavior change = NO warning. The multi-ref case is pinned by the fixture's
  // 'amulti' below.
  assert(r.warnings.some((w) => w.includes('amulti')), 'v36g) affinity multi-model preset warns (session-spread lost)');
  assert(!r.warnings.some((w) => w.includes("'tier'")), 'v36h) failover->fallback does not warn (order preserved)');
  assert(!r.warnings.some((w) => w.includes("'w'") && w.includes('strategy')), 'v36i) weighted->fallback does not warn (order preserved)');
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL (`convertLegacyConfig is not a function` / `v36a)` fails) because the converter does not exist yet.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — above normalizeConfig:
function convertLegacyConfig(raw) {
  const warnings = [];
  const cfg = JSON.parse(JSON.stringify(raw));
  cfg.models = cfg.models || {};
  cfg.presets = cfg.presets || {};
  // 1) identity-preset drop first (fixed order for deterministic golden runs).
  for (const [id, p] of Object.entries(cfg.presets)) {
    const refs = p && Array.isArray(p.models) ? p.models : null;
    if (refs && refs.length === 1) {
      const only = typeof refs[0] === 'string' ? refs[0] : refs[0] && refs[0].model;
      if (only === id && cfg.models[id]) {
        delete cfg.presets[id];
        warnings.push('preset \'' + id + '\' is an identity preset and was dropped; the model stays directly addressable');
      }
    }
  }
  // 2) per-ref-weight + preset-affinityPool: warn ONLY when routing-relevant.
  // Per-ref weights on a weighted-strategy preset WERE honored at request time
  // (weighted-random over models), so dropping them changes behavior. Weights
  // on affinity/failover presets were ignored by candidates() (affinity picks
  // by session hash, failover by declared order) — dropping those is silent.
  // preset-affinityPool always warns: pool>1 widened the session spread on any
  // strategy, and the v3 fallback has no pool.
  for (const [id, p] of Object.entries(cfg.presets)) {
    for (const e of p.models || []) {
      if (e && typeof e === 'object' && e.weight != null && p.strategy === 'weighted') warnings.push('preset \'' + id + '\' per-ref weight dropped; weights live on deployments');
    }
    if (p.affinityPool !== undefined) warnings.push('preset \'' + id + '\' affinityPool dropped');
  }
  // 3) models: providers -> deployments, affinityPool N < count -> backupOnly.
  for (const [id, m] of Object.entries(cfg.models)) {
    if (!m || typeof m !== 'object' || !Array.isArray(m.providers)) continue;
    const n = m.providers.length;
    const pool = m.affinityPool;
    m.deployments = m.providers.map((pr, i) => {
      const d = { backend: pr.backend, upstream: pr.upstream };
      if (pr.weight !== undefined && pr.weight !== 1) d.weight = pr.weight;
      if (typeof pool === 'number' && i >= pool) d.backupOnly = true;
      for (const k of ['params', 'dropParams', 'paramMap']) if (pr[k] !== undefined) d[k] = pr[k];
      return d;
    });
    delete m.providers;
    delete m.affinityPool;
    if (m.exposed === undefined) m.exposed = true;
  }
  // 4) presets: strategies map to fallback preserving ref order, string refs only.
  // Warn ONLY on behavior changes: affinity multi-model presets lose their
  // session-spread under fallback (ordered) semantics. failover->fallback and
  // weighted->fallback keep declared order and stay silent.
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (p.strategy !== 'fallback') {
      if (p.strategy && p.strategy !== 'affinity' && p.strategy !== 'failover' && p.strategy !== 'weighted') warnings.push('preset \'' + id + '\' unknown strategy treated as fallback');
      else if (p.strategy === 'affinity' && (p.models || []).length > 1) warnings.push('preset \'' + id + '\' affinity spread mapped to fallback (ordered failover); session-spread behavior changes');
      p.strategy = 'fallback';
    }
    p.models = (p.models || []).map((e) => (typeof e === 'string' ? e : e.model));
    delete p.affinityPool;
    delete p.params;
    // A preset read back through /api/config carries the derived `members` key
    // (normalizeConfig adds it). Drop it here: nobody reads it in v3, and a
    // mixed-era snapshot would otherwise convert into v3 carrying a stale key.
    delete p.members;
    if (p.exposed === undefined) p.exposed = true;
  }
  // 5) residual model/preset collision refuses (manual resolution required).
  for (const id of Object.keys(cfg.models)) {
    if (cfg.presets[id]) throw new Error('conversion refused: id \'' + id + '\' is both a model and a preset');
  }
  return { config: cfg, warnings };
}
```
```js
// server.mjs:3504 export list — add convertLegacyConfig and validateConfig:
export { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, convertLegacyConfig, validateConfig };
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v36a)` through `v36i)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: legacy to v3 bridge converter'
```

### Task 10b: v3 typed checks in validateConfig (deployments/weight/strategy/whitelist + XOR error)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:305-444` (validateConfig v3 branch — ADDED alongside the old-shape path; v3 detection: any model with `deployments`)
- Test: `C:\dev\local-model-router\test.mjs` (new `v36xa)`-`v36xk)` block: typed failures pin exact error paths; uses the Task 1 static `validateConfig` import — no fresh import)
**Interfaces:**
- Consumes: Tasks 1-3 error/warn paths (v3 branch shares the same `{errors, warnings}` verdict shape)
- Produces: v3 guarantees "missing/empty deployments = error; unknown deployment backend = error; empty upstream = error; non-integer weight = error; non-boolean backupOnly/exposed = error; v3 strategy must be 'fallback' (warn otherwise); v3-shaped collision = error" for Task 11 (pipeline rejects bad v3 at load) and Task 13 (save rejects bad v3)
- [ ] **Step 1: Write the failing test**
```js
{
  process.env.KEY_B1 = 'x';
  const base = () => ({
    backends: [{ id: 'b1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_B1', idleTimeoutMs: 5000, connectTimeoutMs: 2000, model: 'bridge-era-upstream' }],
    models: { 'm1': { deployments: [{ backend: 'b1', upstream: 'u' }], exposed: true } },
    presets: { 'p1': { strategy: 'fallback', models: ['m1'], exposed: true } }
  });
  const bad1 = base(); bad1.models.m1.deployments = [];
  assert(validateConfig(bad1, bad1).errors.some((e) => e.path === 'models.m1.deployments'), 'v36xa) empty deployments is an error');
  const bad2 = base(); bad2.models.m1.deployments = [{ backend: 'ghost', upstream: 'u' }];
  assert(validateConfig(bad2, bad2).errors.some((e) => e.path === 'models.m1.deployments[0].backend'), 'v36xb) unknown deployment backend ref is an error');
  const bad3 = base(); bad3.models.m1.deployments = [{ backend: 'b1', upstream: '' }];
  assert(validateConfig(bad3, bad3).errors.some((e) => e.path === 'models.m1.deployments[0].upstream'), 'v36xc) empty upstream is an error');
  const bad4 = base(); bad4.models.m1.deployments = [{ backend: 'b1', upstream: 'u', weight: 'abc' }];
  assert(validateConfig(bad4, bad4).errors.some((e) => e.path === 'models.m1.deployments[0].weight'), 'v36xd) non-integer weight is an error');
  const bad5 = base(); bad5.models.m1.deployments = [{ backend: 'b1', upstream: 'u', backupOnly: 'yes' }];
  assert(validateConfig(bad5, bad5).errors.some((e) => e.path === 'models.m1.deployments[0].backupOnly'), 'v36xe) non-boolean backupOnly is an error');
  const bad5e = base(); bad5e.models.m1.exposed = 'yes';
  assert(validateConfig(bad5e, bad5e).errors.some((e) => e.path === 'models.m1.exposed'), 'v36xk) non-boolean model exposed is an error');
  const bad6 = base(); bad6.presets.p1.strategy = 'affinity';
  const v6 = validateConfig(bad6, bad6);
  assert(v6.errors.length === 0 && v6.warnings.some((w) => w.path === 'presets.p1.strategy'), 'v36xf) v3 non-fallback strategy warns only (bridge already mapped old shapes)');
  const good = base();
  const vg = validateConfig(good, good);
  assert(vg.errors.length === 0 && vg.warnings.length === 0, 'v36xg) known backend fields (idleTimeoutMs/connectTimeoutMs + bridge-era model) never warn (' + JSON.stringify(vg.warnings) + ')');
  const coll = base(); coll.presets.m1 = { strategy: 'fallback', models: ['m1'], exposed: true };
  assert(validateConfig(coll, coll).errors.some((e) => e.path === 'presets.m1'), 'v36xh) v3-shaped collision is an error (G1 XOR)');
  // Old-shape input must NOT hit the v3 branch: affinity strategy stays silent
  // here (the bridge maps it with its own behavior-change warning).
  const old = { backends: base().backends, models: { 'm1': { providers: [{ backend: 'b1', upstream: 'u' }] } }, presets: { 'p1': { strategy: 'affinity', models: ['m1'] } } };
  assert(!validateConfig(old, old).warnings.some((w) => w.path === 'presets.p1.strategy'), 'v36xi) old-shape strategy untouched by the v3 warn set');
  // Warn-set narrowing, other half: old-shape input keeps ALL FOUR legacy
  // strategy values silent (affinity/failover/weighted/fallback); only the v3
  // branch narrows validity to 'fallback' (v36xf).
  const legacy4 = {
    backends: base().backends,
    models: { 'm1': { providers: [{ backend: 'b1', upstream: 'u' }] } },
    presets: {
      'la': { strategy: 'affinity', models: ['m1'] },
      'lf': { strategy: 'failover', models: ['m1'] },
      'lw': { strategy: 'weighted', models: ['m1'] },
      'lb': { strategy: 'fallback', models: ['m1'] }
    }
  };
  assert(!validateConfig(legacy4, legacy4).warnings.some((w) => /\.strategy$/.test(w.path)), 'v36xj) old-shape keeps all four legacy strategies silent (' + JSON.stringify(validateConfig(legacy4, legacy4).warnings.map((w) => w.path)) + ')');
  delete process.env.KEY_B1;
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v36xa)` onward (no v3 branch exists yet); the old-shape assertions `v36xi)`/`v36xj)` already PASS (characterization of the four legacy values).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — validateConfig, after the existing model/preset collection,
// ONLY when at least one model carries deployments (pure v3-shape signal;
// old-shape configs never enter this branch):
// - models.<id>.deployments missing/empty -> err('deployments must be a non-empty array')
// - deployments[i].backend unknown -> err at models.<id>.deployments[i].backend
// - deployments[i].upstream missing/empty -> err at models.<id>.deployments[i].upstream
// - weight present and (!Number.isInteger || < 1) -> err at ...deployments[i].weight
// - backupOnly/exposed present and typeof !== 'boolean' -> err at their paths
// - backends: known keys = id, baseURL, apiKeyEnv, params, dropParams,
//   connectTimeoutMs, idleTimeoutMs, translateDeveloperRole, timeoutMs,
//   model (bridge-era legacy, never warn); anything else warns (never errors)
// - presets: strategy !== 'fallback' -> warn at presets.<id>.strategy
//   (NO legacy allowlist here: old shapes never reach this branch, and the
//   old-shape path keeps its four legacy values affinity/failover/weighted/
//   fallback silent - pinned by v36xi)/v36xj))
// - model/preset id collision on v3-shaped input -> err at presets.<id>
//   (the old-shape warn at :402 stays for old-shape input per Task 2)
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v36xa)` through `v36xk)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: v3 typed validation for deployments and strategy'
```

### Task 11: Dual-shape load pipeline + dual-shape normalize (convert -> validate -> normalize)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:2894-2916` (loadConfig pipeline), `C:\dev\local-model-router\server.mjs:1275-1365` (normalizeConfig models loop: teach the deployments shape)
- Test: `C:\dev\local-model-router\test.mjs` (new `v37)` child-router test: old-shape temp config loads as v3 via /api/config; `v37c)`/`v37d)` unit assertions inside the same block: v3 models and presets survive normalize)
**Interfaces:**
- Consumes: `convertLegacyConfig` (Task 10), validator (Tasks 1-4), v3 typed checks (Task 10b)
- Produces: pipeline guarantee "old-shape converts in memory; v3 validates directly; dual-shape normalize keeps both readable" for Tasks 12-13
- normalizeConfig becomes DUAL-SHAPE in this task and stays dual-shape for the whole bridge era: it learns the v3 `deployments` branch HERE (Increment 3) while the legacy `providers` branch and the pre-presets `backends` synthesis stay; those legacy branches are removed only in Task 25 (Increment 6), never before.
- [ ] **Step 1: Write the failing test**
```js
{
  const dir = await mkdtemp(join(tmpdir(), 'lmr-v37-'));
  const cfgPath = join(dir, 'config.json');
  const envPath = join(dir, '.env');
  const old = {
    port: 0, prefix: '/v1', masterKeyEnv: null,
    backends: [{ id: 'v37', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_V37' }],
    models: { 'v37m': { providers: [{ backend: 'v37', upstream: 'u' }] } },
    presets: { 'v37p': { strategy: 'affinity', models: ['v37m'] } }
  };
  await writeFile(cfgPath, JSON.stringify(old, null, 2));
  await writeFile(envPath, 'KEY_V37=k\n');
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: join(import.meta.dirname),
    env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let base = null;
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('no start')), 8000);
    child.stderr.on('data', (d) => {
      const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
      if (m) { base = 'http://127.0.0.1:' + parseInt(m[1], 10); clearTimeout(t); resolve(); }
    });
  });
  try {
    const live = await (await fetch(base + '/api/config')).json();
    assert(live.models.v37m.deployments && live.models.v37m.deployments[0].backend === 'v37', 'v37a) old-shape loads through the bridge as v3 (' + JSON.stringify(live.models.v37m) + ')');
    assert(live.presets.v37p.strategy === 'fallback', 'v37b) preset strategy mapped to fallback');
  } finally {
    child.kill();
    await rm(dir, { recursive: true, force: true });
  }
}
// v37c)/v37d) dual-shape normalize: a v3-shaped config passes through
// normalizeConfig with its deployments + fallback strategy intact (the legacy
// providers/backends synthesis runs only for legacy-shaped entries; both
// branches coexist until Task 25).
{
  const v3 = {
    backends: [{ id: 'v37' }],
    models: { 'v37m': { deployments: [{ backend: 'v37', upstream: 'u', weight: 2 }], exposed: false } },
    presets: { 'v37p': { strategy: 'fallback', models: ['v37m'], exposed: true } }
  };
  normalizeConfig(v3);
  assert(v3.models.v37m.deployments.length === 1 && v3.models.v37m.deployments[0].weight === 2 && v3.models.v37m.exposed === false, 'v37c) v3 model shape survives normalize (dual-shape read)');
  assert(v3.presets.v37p.strategy === 'fallback' && Array.isArray(v3.presets.v37p.models) && v3.presets.v37p.models[0] === 'v37m', 'v37d) v3 preset shape survives normalize');
}
```
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — inside loadConfig, replacing the bare normalizeConfig(parsed) call.
// This SUPERSEDES Task 4's validate-on-raw block at this same call site:
// validation runs on the converted `effective` ONLY. Validating the parsed raw
// input here would validate old-shape input twice (once as old shape, again as
// v3 after conversion), and an old-shape-only finding could reject a config that
// converts cleanly.
function isOldShape(raw) {
  return Object.values((raw && raw.models) || {}).some((m) => m && typeof m === 'object' && Array.isArray(m.providers));
}
let effective = parsed;
if (isOldShape(parsed)) {
  let converted;
  try {
    converted = convertLegacyConfig(parsed);
  } catch (e) {
    console.error('[' + new Date().toISOString() + '] config reload rejected: conversion failed: ' + e.message);
    return cfg;
  }
  for (const w of converted.warnings) console.warn('config warning: bridge: ' + w);
  effective = converted.config;
}
{
  const verdict = validateConfig(effective, cfg, { mode: 'load' });
  if (verdict.errors.length && !cfg) throw new Error('config invalid: ' + verdict.errors.map((e) => (e.path || '(root)') + ': ' + e.message).join('; '));
  if (verdict.errors.length && cfg) {
    console.error('[' + new Date().toISOString() + '] config reload rejected: ' + verdict.errors.map((e) => (e.path || '(root)') + ': ' + e.message).join('; '));
    return cfg;
  }
}
normalizeConfig(effective);
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v37a)` through `v37d)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: dual-shape config load pipeline'
```

### Task 12: v3 routing read — deployments, sticky weights, fallback, exposure (T3/T4)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:1665-1741` (candidates), `C:\dev\local-model-router\server.mjs:1757-1787` (buildPayload layers), `C:\dev\local-model-router\server.mjs:2951-2964` (/v1/models), `C:\dev\local-model-router\server.mjs:1504-1512` (normalizeConfig members derivation: read deployments as well as providers)
- Test: `C:\dev\local-model-router\test.mjs` (new `v38)` unit + child tests: T3 exposure routable-but-hidden, T4 backup-only order + affinity compat)
**Interfaces:**
- Consumes: `convertLegacyConfig` (Task 10), pipeline (Task 11)
- Produces: candidate shape `{ model, backend, upstream }` with backupOnly/weight/exposed semantics for Tasks 19-20 (cooling reads the same attempt list)
- normalizeConfig stays DUAL-SHAPE here: the members derivation reads BOTH `deployments` (v3) and `providers` (legacy), so a bridged old-shape config and a native v3 config behave identically; the legacy `providers`/`backends` branches are removed only in Task 25.
- [ ] **Step 1: Write the failing test**
```js
// Uses the static line-12 imports (candidates, normalizeConfig) — no fresh import.
{
  const cfg = {
    backends: [{ id: 'b1' }, { id: 'b2' }, { id: 'b3' }],
    models: {
      'm1': { deployments: [{ backend: 'b1', upstream: 'u' }, { backend: 'b2', upstream: 'u', weight: 2 }, { backend: 'b3', upstream: 'u', backupOnly: true }], exposed: true },
      'hid': { deployments: [{ backend: 'b1', upstream: 'uh' }], exposed: false }
    },
    presets: {
      'tier': { strategy: 'fallback', models: ['m1', 'hid'] },
      'secret': { strategy: 'fallback', models: ['m1'], exposed: false }
    }
  };
  normalizeConfig(cfg);
  const c = candidates(cfg, 'm1', 'sess-a');
  assert(c.ordered[0].backend !== 'b3', 'v38a) backupOnly never primary (' + JSON.stringify(c.ordered) + ')');
  assert(c.ordered[c.ordered.length - 1].backend === 'b3', 'v38b) backupOnly tried last');
  assert(candidates(cfg, 'm1', 'sess-a').ordered[0].backend === candidates(cfg, 'm1', 'sess-a').ordered[0].backend, 'v38c) sticky primary per session key');
  const t = candidates(cfg, 'tier', 'sess-a');
  assert(t.models.includes('hid'), 'v38d) hidden model still routable through preset');
}
// T3 omission half — the exposed:false ids above stay ROUTABLE but must not be
// LISTED. A unit assertion cannot prove that (exposure is a listing rule, not a
// candidates() rule), so this child router carries the listing fixture with its
// own exposed:false entries.
{
  const hid = mockBackend('hidB', {});
  hid.port = await listen(hid.srv);
  const hcfg = {
    port: 0, prefix: '/v1', masterKeyEnv: null,
    backends: [{ id: 'hidB', baseURL: 'http://127.0.0.1:' + hid.port, apiKeyEnv: 'KEY_HID' }],
    models: {
      'shown-m': { deployments: [{ backend: 'hidB', upstream: 'u' }], exposed: true },
      'hidden-m': { deployments: [{ backend: 'hidB', upstream: 'u' }], exposed: false }
    },
    presets: {
      'shown-p': { strategy: 'fallback', models: ['shown-m'], exposed: true },
      'hidden-p': { strategy: 'fallback', models: ['hidden-m'], exposed: false }
    }
  };
  const { child: hidChild, base: hidBase, dir: hidDir } = await startRouterCfg(hcfg, 'KEY_HID=k\n');
  try {
    const served = (await (await api(hidBase, '/v1/models', { method: 'GET' })).json()).data.map((x) => x.id);
    assert(served.join(',') === 'shown-p,shown-m', 'v38e) exposed:false model and preset ids are ABSENT from /v1/models (exposed presets first, then exposed models; got ' + JSON.stringify(served) + ')');
    const rh = await api(hidBase, '/v1/chat/completions', { body: { model: 'hidden-p', messages: [{ role: 'user', content: 'hi' }] } });
    const rm = await api(hidBase, '/v1/chat/completions', { body: { model: 'hidden-m', messages: [{ role: 'user', content: 'hi' }] } });
    assert(rh.status === 200 && rm.status === 200, 'v38f) an exposed:false preset and an exposed:false model stay routable (preset=' + rh.status + ', model=' + rm.status + ')');
  } finally {
    hidChild.kill(); hid.srv.close();
    await rm(hidDir, { recursive: true, force: true });
  }
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v38a)`/`v38b)` (today candidates reads providers/affinityPool and ignores deployments/backupOnly), FAIL `v38e)` (today /v1/models applies the collision rule, which has no exposure filter, so `hidden-p` and `hidden-m` are listed) and FAIL `v38f)` (candidates does not read `deployments` yet, so neither hidden id has an attempt list to route through).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — candidates model-level ordering (v3 branch; old-shape already
// converted in memory by Task 11, so read deployments here):
const deployments = m.deployments || [];
const primaries = deployments.filter((d) => !d.backupOnly);
const backups = deployments.filter((d) => d.backupOnly);
const weights = primaries.map((d) => (Number.isInteger(d.weight) && d.weight >= 1 ? d.weight : 1));
const total = weights.reduce((s, w) => s + w, 0);
let acc = 0;
const h = hashStr(sessionKey) % total;
let pick = 0;
for (let i = 0; i < weights.length; i++) { acc += weights[i]; if (h < acc) { pick = i; break; } }
const deploymentOrder = primaries.length ? [primaries[pick], ...primaries.filter((_, i) => i !== pick), ...backups] : [...backups];
for (const d of deploymentOrder) {
  const key = d.backend + '::' + d.upstream;
  if (seenPairs.has(key)) continue;
  seenPairs.add(key);
  ordered.push({ model: mid, backend: d.backend, upstream: d.upstream });
}
```
```js
// server.mjs — /v1/models (replaces the collision rule; G1 guarantees unique ids):
const ids = [];
for (const id of Object.keys(cfg.presets || {})) {
  if (cfg.presets[id] && cfg.presets[id].exposed === false) continue;
  if (!ids.includes(id)) ids.push(id);
}
for (const id of Object.keys(cfg.models || {})) {
  if (cfg.models[id] && cfg.models[id].exposed === false) continue;
  if (!ids.includes(id)) ids.push(id);
}
```
```js
// server.mjs — buildPayload merge (preset.params layer removed; deployment slot kept):
Object.assign(out, cfg.params || {});
Object.assign(out, backend.params || {});
Object.assign(out, deployment.params || {});
Object.assign(out, model.params || {});
delete out.model;
Object.assign(out, body);
out.model = deployment.upstream;
const drop = new Set([...(backend.dropParams || []), ...(deployment.dropParams || [])]);
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v38a)` through `v38f)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: v3 routing read with backup-only sticky weights and exposure'
```

### Task 13: Save/reset bridge — convert-or-refuse, writes v3 (T7 convert half)

> **SUPERSEDED 2026-10-12.** The "any save writes v3" guarantee below was withdrawn in
> the combined release work: a save writes back the shape it was given, and migration is
> a deliberate act. See `DESIGN-3LAYER.md` section 8b and the config-v3 spec's "Shape on
> save". The reset path still converts-or-refuses a snapshot, which is what this task's
> convert-or-refuse half is about; only the "save converts the live config" half changed.
> Evidence: `u9s1`/`u9s1b` in `test.mjs`.
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:3053-3111` (POST /api/config), `C:\dev\local-model-router\server.mjs:3113-3145` (reset convert step)
- Test: `C:\dev\local-model-router\test.mjs` (`v39)` cases inside the existing config-editor block, after CE11: `v39a)`/`v39b)` old-shape save converts; `v39c)`/`v39d)` a corrupt old-shape snapshot is refused with the live file byte-identical)
**Interfaces:**
- Consumes: `convertLegacyConfig` (Task 10), reset guards (Task 5)
- Produces: "any save writes v3" guarantee for Task 15 (snapshot timing) and Task 18 (rollback path)
- [ ] **Step 1: Write the failing test**
```js
{
  const draft = JSON.parse(JSON.stringify(((await (await fetch(baseCE + '/api/config/raw')).json()).config)));
  draft.models['bridge-probe'] = { providers: [{ backend: Object.keys(draft.models)[0] ? draft.models[Object.keys(draft.models)[0]].providers?.[0]?.backend || 'x' : 'x', upstream: 'u-probe' }] };
  const r = await post('/api/config', { config: draft, force: true });
  assert(r.status === 200, 'v39a) old-shape save accepted via bridge (got ' + r.status + ')');
  const raw = await (await fetch(baseCE + '/api/config/raw')).json();
  assert(raw.config.models['bridge-probe'].deployments, 'v39b) saved file is v3 (deployments present)');
  // v39c)/v39d) CORRUPT old-shape snapshot: the bridge converter refuses it (a
  // non-identity preset whose id collides with a model id cannot be dropped),
  // so reset must return 400 and leave the live config file BYTE-IDENTICAL -
  // never partially applied. The snapshot file is written straight into the
  // server's snapshot dir (SNAPSHOT_DIR = join(dirname(CONFIG_PATH),
  // 'config.history'), server.mjs:74/:241-250); the dir already exists because
  // earlier saves in this block took snapshots.
  const before13 = await rawOnDisk();
  const corruptOld = {
    backends: [{ id: 'ce-a', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_CE_A' }],
    models: { 'ce-model': { providers: [{ backend: 'ce-a', upstream: 'u1' }] } },
    presets: { 'ce-model': { strategy: 'affinity', models: ['ce-model', 'ce-preset'] } }
  };
  await writeFile(join(dirCE, 'config.history', 'corrupt-old.json'), JSON.stringify(corruptOld, null, 2));
  const rc13 = await post('/api/config/reset', { snapshot: 'corrupt-old.json', force: true });
  const bc13 = await rc13.json();
  assert(rc13.status === 400, 'v39c) corrupt old-shape snapshot is refused by conversion (got ' + rc13.status + ', ' + JSON.stringify(bc13) + ')');
  assert((await rawOnDisk()) === before13, 'v39d) refused snapshot restore left the live file byte-identical');
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v39b)` (today save writes the candidate verbatim, so providers persist; today reset has no conversion step either, so `v39c)`/`v39d)` FAIL because the corrupt snapshot is written through).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — POST /api/config, after reading candidate, before validateConfig:
let toSave = candidate;
{
  const looksOld = Object.values((candidate.models) || {}).some((m) => m && typeof m === 'object' && Array.isArray(m.providers));
  if (looksOld) {
    let converted;
    try {
      converted = convertLegacyConfig(candidate);
    } catch (e) {
      res.writeHead(400, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, errors: [{ path: '', message: String(e.message) }], warnings: [] }));
    }
    for (const w of converted.warnings) console.warn('[' + new Date().toISOString() + '] config save bridge: ' + w);
    toSave = converted.config;
  }
}
// ...validate toSave, write toSave (not candidate)...
writeConfigAtomic(toSave);
```
```js
// server.mjs — POST /api/config/reset, after parsing the snapshot file:
{
  const looksOld = Object.values((restored.models) || {}).some((m) => m && typeof m === 'object' && Array.isArray(m.providers));
  if (looksOld) {
    try {
      const converted = convertLegacyConfig(restored);
      restored = converted.config;
    } catch (e) {
      return writeError(res, newError(400, JSON.stringify({ error: { message: String(e.message), type: 'invalid_request_error' } })));
    }
  }
}
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v39a)` through `v39d)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: config save and reset bridge old shape to v3'
```

### Task 14: Editor v3 inputs
**Files:**
- Modify: `C:\dev\local-model-router\index.html:4790-4889` (model/preset modal form), `C:\dev\local-model-router\index.html:4900-4965` (collect), Task 7 `servedIds()` helper
- Test: `C:\dev\local-model-router\test.mjs` — none (no unit harness exists for the single-file UI; verification is the live save below plus the API suite gate)
**Interfaces:**
- Consumes: Task 6 merge-preserve contract, Task 7 servedIds helper
- Produces: v3 draft shape (deployments/weight/backupOnly/exposed/dialect) for Task 15 snapshot timing
- [ ] **Step 1: Write the failing live check**
```js
// Live check: open a model, set deployment 2 weight=2 + backupOnly on
// deployment 3 + exposed=false, add a per-deployment dropParams entry; Save;
// GET /api/config/raw shows deployments[] with those exact values.
```
- [ ] **Step 2: Run check to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: FAIL today (the form has providers/affinityPool inputs, no weight/backupOnly/exposed/dialect fields).
- [ ] **Step 3: Write minimal implementation**
```js
// index.html — model modal: providers become deployments with weight/backupOnly
// + per-deployment dialect textareas; preset modal: exposed checkbox + read-only
// strategy display 'fallback (v3)'.
cfg.modal.provs = (Array.isArray(data.deployments || data.providers) ? (data.deployments || data.providers) : []).map((p) => ({
  backend: p.backend || '', upstream: p.upstream || '',
  weight: p.weight == null ? 1 : p.weight, backupOnly: !!p.backupOnly,
  dropParams: p.dropParams, paramMap: p.paramMap, params: p.params, prev: p
}));
```
```js
// index.html — collect writes deployments (never providers), keeps exposed:
m.deployments = (cfg.modal.provs || []).filter((p) => p.backend).map((p) => {
  const out = Object.assign({}, p.prev || {});
  out.backend = p.backend;
  out.upstream = p.upstream;
  out.weight = Number(p.weight) >= 1 ? Math.floor(Number(p.weight)) : 1;
  if (p.backupOnly) out.backupOnly = true; else delete out.backupOnly;
  return out;
});
m.exposed = bool('f_exposed');
```
- [ ] **Step 4: Run check to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Also run the live check described in Step 1 (there is no unit harness for the single-file UI).
Expected: live save round-trips the v3 values; suite ends "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add index.html
git commit -m 'feat: config editor v3 deployment inputs'
```

### Task 15: Pre-bridge old-shape snapshot capture (SSH backup before bridge ships)
**Files:**
- Modify: none (SSH-only operational task; no repo files change)
- Test: snapshot file exists on the operator machine with the expected shape
**Interfaces:**
- Consumes: Task 13 "any save writes v3" guarantee (this is why the snapshot must predate the bridge deploy)
- Produces: `switchblade-prebridge-<date>.json` snapshot + sha for Task 18 rollback
- [ ] **Step 1: Resolve the live container and back up per the runbook**
```pwsh
ssh root@192.168.68.69 "docker ps --format '{{.ID}} {{.Names}}' | grep a8dyidd8o8id2se9czgauem0"
ssh root@192.168.68.69 "cp /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json.prebridge-2026-09-19"
```
Full protocol is the switchblade runbook at `C:\Users\silve\.dsh\skills\switchblade\references\deploy-config.md` (steps: resolve CID, backup, fetch + LF-normalize, never overwrite wholesale).
- [ ] **Step 2: Fetch and verify the snapshot locally**
Run: `scp root@192.168.68.69:/data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json.prebridge-2026-09-19 C:/dev/local-model-router/.prebridge-snapshot.json; node -e "const c=JSON.parse(require('fs').readFileSync('C:/dev/local-model-router/.prebridge-snapshot.json','utf8')); console.log(Object.keys(c.models).length + ' models, ' + Object.keys(c.presets).length + ' presets')"`
Expected: prints the live model/preset counts; the file is old-shape (models carry `providers`, presets carry strategies other than fallback).
- [ ] **Step 3: Record the snapshot hash (rollback anchor)**
```pwsh
ssh root@192.168.68.69 "md5sum /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json.prebridge-2026-09-19"
```
- [ ] **Step 4: Confirm no v3 write can precede the bridge deploy**
Run: `node test.mjs`
Expected: tail "N passed, 0 failed" (dashboard saves after the bridge deploy write v3, which pre-bridge code cannot read — hence this snapshot).
- [ ] **Step 5: Ship Increment 3 (code increments ship via PR/push per AGENTS.md; snapshot file itself is never committed)**
```pwsh
git status --short
git push origin main
```
---
## Increment 4 — Convert the live config (SSH-only, no code)

### Task 16: POST the converted config + served id-set diff + /health
**Files:**
- Modify: none (SSH-only; the repo mirror is never edited as a deploy)
- Test: live verification commands below (snapshot freshness re-verified before the POST, id-set identical, /health healthy)
**Interfaces:**
- Consumes: Task 15 pre-bridge snapshot, Task 10 converter
- Produces: live v3 config + id-set diff evidence for Task 17
- [ ] **Step 1: Capture the pre-deploy served id set**
```pwsh
curl -s http://192.168.68.69:8787/v1/models | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.data.map(x=>x.id).join('\n'))})" | Sort-Object | Set-Content C:/dev/local-model-router/.models-before.txt
```
- [ ] **Step 2: Re-verify snapshot freshness immediately before the POST (spec: refresh/verify the increment-3 snapshot)**
```pwsh
curl -s -H "Authorization: Bearer $env:LITELLM_API_KEY" http://192.168.68.69:8787/api/config/raw | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);const raw=JSON.parse(require('fs').readFileSync('C:/dev/local-model-router/.prebridge-snapshot.json','utf8'));const pick=(c)=>JSON.stringify({b:c.backends,m:c.models,p:c.presets});console.log(pick(raw)===pick(j.config)?'snapshot is FRESH (live config still matches the pre-bridge capture)':'snapshot is STALE - re-run the Task 15 capture before converting')})"
```
Expected: prints `snapshot is FRESH`. If it prints STALE the live config changed since the capture (e.g. another dashboard save): stop, re-run Task 15's capture, and restart this task at Step 1.
- [ ] **Step 3: Convert the snapshot locally (real command; env NAMES only, never a key value)**
Run: `node -e "import('./server.mjs').then(({convertLegacyConfig})=>{const fs=require('node:fs');const raw=JSON.parse(fs.readFileSync('C:/dev/local-model-router/.prebridge-snapshot.json','utf8'));const r=convertLegacyConfig(raw);fs.writeFileSync('C:/dev/local-model-router/.converted-v3.json',JSON.stringify(r.config,null,2));console.log('models='+Object.keys(r.config.models).length+' presets='+Object.keys(r.config.presets).length+' warnings='+r.warnings.length)})"`
Expected: prints the converted model/preset counts and the warning count; `.converted-v3.json` is v3 (every model carries `deployments`, every preset carries `strategy: 'fallback'`).
- [ ] **Step 4: Install the converted file host-side and verify it landed**
```pwsh
# In-place write into the single-file bind mount (NEVER replace the inode: a
# rename/cp onto the mount point fails EBUSY and pins the container's fs.watch
# to the old inode). Full protocol: C:\Users\silve\.dsh\skills\switchblade\references\deploy-config.md
scp C:/dev/local-model-router/.converted-v3.json root@192.168.68.69:/data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/.converted-v3.json
ssh root@192.168.68.69 "cat /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/.converted-v3.json > /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json"
ssh root@192.168.68.69 "md5sum /data/coolify/applications/a8dyidd8o8id2se9czgauem0/app/config.server.json"
ssh root@192.168.68.69 "docker exec \$(docker ps -qf name=a8dyidd8o8id2se9czgauem0) md5sum /app/config.server.json"
```
Expected: the two md5 sums are identical (host file == in-container `/app/config.server.json`); no key value appears in any output.
- [ ] **Step 5: Diff the served id SET (order may shift once, set must be identical)**
```pwsh
curl -s http://192.168.68.69:8787/v1/models | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.data.map(x=>x.id).join('\n'))})" | Sort-Object | Set-Content C:/dev/local-model-router/.models-after.txt
Compare-Object (Get-Content C:/dev/local-model-router/.models-before.txt) (Get-Content C:/dev/local-model-router/.models-after.txt)
```
Expected: no differences (empty Compare-Object output). One-time ORDER shift is allowed (identity-preset ids move from the presets block to the models block); the SET must be identical.
- [ ] **Step 6: Check /health**
Run: `curl -s http://192.168.68.69:8787/health | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.backends.map(b=>b.id+':'+b.state).join(' '))})"`
Expected: all backends healthy (or only previously-known cools), no new cooling.
- [ ] **Step 7: Record (no commit — SSH-only task, do not touch git)**
```pwsh
Get-Content C:/dev/local-model-router/.models-after.txt
```

### Task 17: Real chat call + consumer pin checks
**Files:**
- Modify: none (SSH-only verification)
- Test: live chat + local consumer resolution
**Interfaces:**
- Consumes: Task 16 live v3 config
- Produces: consumer evidence (dsh + OpenCode resolve every pinned id) for Task 18
- [ ] **Step 1: One real chat call through the dsh route**
```pwsh
# pwsh quoting: the JSON body goes into a SINGLE-quoted variable (nothing inside
# needs expansion), while the Authorization header is DOUBLE-quoted so
# $env:LMR_DSH_KEY expands. A single-quoted header would send the literal text
# '$env:LMR_DSH_KEY' (401), and '\"' escapes inside a single-quoted body would be
# transmitted verbatim (invalid JSON -> 400).
$chatBody = '{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"say ok"}],"max_tokens":8}'
curl -s http://192.168.68.69:8787/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer $env:LMR_DSH_KEY" -d $chatBody | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(s.slice(0,300)))"
```
Expected: HTTP 200 with assistant content (never a key value in output).
- [ ] **Step 2: Check the dsh consumer pins**
Run: `node -e "const ids=new Set(require('fs').readFileSync('C:/dev/local-model-router/.models-after.txt','utf8').split(/\r?\n/).filter(Boolean)); const y=require('fs').readFileSync('C:/Users/silve/.dsh/settings.yaml','utf8'); for (const m of [...y.matchAll(/model:\s*(\S+)/g)]) console.log(m[1] + ' ' + (ids.has(m[1]) ? 'OK' : 'MISSING'))"`
Expected: every pinned id prints OK.
- [ ] **Step 3: Check the OpenCode consumer pins**
Run: `node -e "const ids=new Set(require('fs').readFileSync('C:/dev/local-model-router/.models-after.txt','utf8').split(/\r?\n/).filter(Boolean)); const j=require('fs').readFileSync('C:/Users/silve/.config/opencode/opencode.jsonc','utf8'); for (const m of [...j.matchAll(/\"model\":\s*\"([^\"]+)\"/g)]) console.log(m[1] + ' ' + (ids.has(m[1]) ? 'OK' : 'MISSING'))"`
Expected: every pinned id prints OK.
- [ ] **Step 4: Confirm exposure rule live**
Run: `curl -s http://192.168.68.69:8787/v1/models | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s); console.log('served: ' + j.data.length)})"`
Expected: count equals the Task 16 after-set count.
- [ ] **Step 5: Record (no commit — SSH-only task)**
```pwsh
echo 'consumer checks recorded in the ship log'
```

### Task 18: T6 golden test + golden expectations + rollback path
**Files:**
- Modify: `C:\dev\local-model-router\test.mjs` (T6 golden block, embedded old-shape fixture — the only code change in this increment; the deploy itself is Task 16)
- Test: `C:\dev\local-model-router\test.mjs` golden block + the live captured-snapshot verification steps below
**Interfaces:**
- Consumes: Task 10 converter, Tasks 15-17 captured snapshot and served-id diff
- Produces: pinned T6 golden (warning set + served-id-set equality) and the rollback procedure for Increment 5-6 work
- [ ] **Step 1: Write the golden test (real test.mjs block, embedded fixture — no live files)**
```js
// T6 golden test (spec). The embedded representative OLD-SHAPE fixture stands in
// for the captured live snapshot; the live steps below re-run the same two
// assertions against C:/dev/local-model-router/.prebridge-snapshot.json.
{
  const fixture = {
    backends: [
      { id: 'gb1', baseURL: 'http://127.0.0.1:1', apiKeyEnv: 'KEY_GB1' },
      { id: 'gb2', baseURL: 'http://127.0.0.1:2', apiKeyEnv: 'KEY_GB2' }
    ],
    models: {
      'kimi-k3': { providers: [{ backend: 'gb1', upstream: 'kimi-k3' }] },
      'glm-5.3': { providers: [{ backend: 'gb1', upstream: 'glm-5.3' }] },
      'deepseek-v4.1-flash': { providers: [{ backend: 'gb1', upstream: 'deepseek-v4.1' }, { backend: 'gb2', upstream: 'deepseek-v4.1' }], affinityPool: 2 }
    },
    presets: {
      // identity preset carrying the mirror's single per-ref weight of 1
      'kimi-k3': { strategy: 'affinity', models: [{ model: 'kimi-k3', weight: 1 }] },
      // a second, weightless identity preset (failover)
      'glm-5.3': { strategy: 'failover', models: ['glm-5.3'] },
      // non-identity single-ref affinity alias preset: survives as a fallback
      // preset and changes no behavior (no session-spread to lose)
      'deepseek-v4-flash': { strategy: 'affinity', models: ['deepseek-v4.1-flash'] },
      // multi-ref affinity preset: THE behavior change (session-spread -> ordered)
      'amulti': { strategy: 'affinity', models: ['glm-5.3', 'deepseek-v4.1-flash'] },
      // multi-ref failover preset: order preserved -> silent
      'tier': { strategy: 'failover', models: ['glm-5.3', 'deepseek-v4.1-flash'] }
    },
    params: { temperature: 0.2 },
    backoff: { rateLimitBaseMs: 30000, rateLimitMaxMs: 600000 }
  };
  const r = convertLegacyConfig(fixture);
  const identity = r.warnings.filter((w) => w.includes('identity preset'));
  const behavior = r.warnings.filter((w) => w.includes('behavior changes'));
  const weights = r.warnings.filter((w) => w.includes('per-ref weight'));
  assert(identity.length === 2 && identity[0].includes("'kimi-k3'") && identity[1].includes("'glm-5.3'"), 'v45a) golden: identity presets dropped first, in config order (' + JSON.stringify(r.warnings) + ')');
  assert(behavior.length === 1 && behavior[0].includes("'amulti'"), 'v45b) golden: the only behavior-change warning is the multi-ref affinity preset (' + JSON.stringify(r.warnings) + ')');
  assert(weights.length === 0, 'v45c) golden: a per-ref weight of 1 inside a dropped identity preset warns nothing');
  const servedOld = (c) => {
    const out = [];
    for (const id of Object.keys(c.presets || {})) if (!out.includes(id)) out.push(id);
    for (const id of Object.keys(c.models || {})) {
      if (Object.prototype.hasOwnProperty.call(c.presets || {}, id)) continue; // legacy collision rule
      if (!out.includes(id)) out.push(id);
    }
    return out.sort();
  };
  const servedV3 = (c) => {
    const out = [];
    for (const id of Object.keys(c.presets || {})) { if (c.presets[id].exposed === false) continue; if (!out.includes(id)) out.push(id); }
    for (const id of Object.keys(c.models || {})) { if (c.models[id].exposed === false) continue; if (!out.includes(id)) out.push(id); }
    return out.sort();
  };
  const before = servedOld(fixture);
  const after = servedV3(r.config);
  assert(JSON.stringify(before) === JSON.stringify(after), 'v45d) golden: served id SET identical after conversion (' + JSON.stringify(before) + ' vs ' + JSON.stringify(after) + ')');
  assert(r.config.params.temperature === 0.2 && r.config.backoff.rateLimitMaxMs === 600000 && !r.config.presets['kimi-k3'], 'v45e) golden: untouched blocks carried through and identity presets gone');
}
```
- [ ] **Step 2: Run the golden test (golden pin, not a new behavior — the converter shipped in Task 10)**
Run: `node --check test.mjs; node test.mjs`
Expected: `v45a)` through `v45e)` PASS on the first run; tail reads "N passed, 0 failed". If `v45b)` fails, the converter warns on failover→fallback or on single-ref affinity presets, which is NOT a behavior change — fix the converter (Task 10), not the test.
- [ ] **Step 3: Compute the live golden warnings from the captured snapshot**
Run: `node -e "import('./server.mjs').then(({convertLegacyConfig})=>{const fs=require('node:fs');const raw=JSON.parse(fs.readFileSync('C:/dev/local-model-router/.prebridge-snapshot.json','utf8'));const r=convertLegacyConfig(raw);for (const w of r.warnings) console.log(w);console.log('TOTAL='+r.warnings.length)})"`
Expected: identity-preset drops first (19 for the 2026-09-19 mirror), then any per-ref-weight / preset-affinityPool lines the snapshot contains. The BEHAVIOR-CHANGE list is expected EMPTY for the mirror: its five affinity presets (deepseek-v4-flash, deepseek-v4-flash-alt, deepseek-v4-pro, mimo-v2.5-pro, deepseek-v4.1-flash) are each single-ref, so none of them may appear in a `behavior changes` line — three of them (deepseek-v4-pro, mimo-v2.5-pro, deepseek-v4.1-flash) appear as identity drops instead. A `behavior changes` line appears only for a MULTI-ref affinity preset in the snapshot; every such line must be confirmed against operator intent (OQ1). The per-ref weight of 1 inside identity preset kimi-k3 is dropped wholesale, so it warns nothing.
- [ ] **Step 4: Confirm served id-set equality converted-vs-live**
Run: `node -e "const fs=require('fs');const conv=JSON.parse(fs.readFileSync('C:/dev/local-model-router/.converted-v3.json','utf8'));const ids=(c)=>{const o=[];for(const id of Object.keys(c.presets||{})){if(c.presets[id].exposed!==false&&!o.includes(id))o.push(id)}for(const id of Object.keys(c.models||{})){if(c.models[id].exposed!==false&&!o.includes(id))o.push(id)}return o};const a=ids(conv).sort();const b=fs.readFileSync('C:/dev/local-model-router/.models-after.txt','utf8').split(/\r?\n/).filter(Boolean).sort();const miss=a.filter((x)=>!b.includes(x));const extra=b.filter((x)=>!a.includes(x));console.log(JSON.stringify(a)===JSON.stringify(b)?('SET IDENTICAL ('+a.length+' ids)'):('SET DIFF missing='+JSON.stringify(miss)+' unexpected='+JSON.stringify(extra)))"`
Expected: prints `SET IDENTICAL (N ids)` where N is the Task 16 after-set count.
- [ ] **Step 5: Write down the rollback path (spec rollback paragraph)**
```pwsh
echo 'rollback: restore C:/dev/local-model-router/.prebridge-snapshot.json host-side per the runbook, md5-verify, confirm /v1/models set, then revert code to pre-bridge only if no v3 write happened; after any v3 write the snapshot path is mandatory'
```
- [ ] **Step 6: Re-run the local gate and commit the golden test**
```pwsh
node --check server.mjs; node --check test.mjs; node test.mjs
git add test.mjs
git commit -m 'test: T6 bridge golden conversion (warnings + served-id-set equality)'
git push origin main
```
Expected: tail "N passed, 0 failed"; the push carries the test-only commit (the v3 config itself is already live from Task 16).
- [ ] **Step 7: Record (nothing else to commit)**
```pwsh
Get-Content C:/dev/local-model-router/.models-before.txt | Measure-Object -Line
Get-Content C:/dev/local-model-router/.models-after.txt | Measure-Object -Line
```
---
## Increment 5 — Cooling re-key (depends on Increment 3)

### Task 19: Deployment/account health split + mark paths (T5 core)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:451-594` (health registry, markFailure, markSuccess), `C:\dev\local-model-router\server.mjs:2927-2943` (/health: add the minimal `deployments[]` projection that makes the split observable; Task 22 extends each entry with `manual`), `C:\dev\local-model-router\server.mjs:2017,2024` (the mark call sites, updated in this same task so the suite stays green)
- Test: `C:\dev\local-model-router\test.mjs` (new `v46)` cooling block with mock backends: 429 on (A,X) cools only that pair while Y still serves; the configured rateLimit base/max are live; weekly/auth/billing stay account-wide)
**Interfaces:**
- Consumes: Task 12 attempt shape `{ model, backend, upstream }`, Task 11 pipeline
- Produces: `deploymentHealth` (`<backend>::<upstream>`; server/rate/client classes) + account `health` (auth/billing/weekly) + `markFailure(cfg, backendId, upstream, status, bodyText)` / `markSuccess(backendId, upstream)` for Tasks 20-22
- Test-fixture note: `mockBackend` (test.mjs:29-55) gains two lookup abilities in this task — a spec keyed `'upstream:<model>'` (the string the router actually sends as `payload.model`) and a LIST of specs consumed in order. That is what makes "one-shot 429 on upstream X, healthy 200 on upstream Y" possible on ONE backend; a blanket per-backend 429 would make the core assertion unpassable.
- [ ] **Step 1: Write the failing test**
```js
// Fixture: ONE backend (coolA) serves two upstreams of the same model, so
// per-deployment granularity is under test; coolB carries the account-wide
// classes. X is the primary and Y is backupOnly, so the failover reaches Y
// deterministically (no session-hash dependence).
{
  // mockBackend extension (test.mjs:29-55), applied in this same commit. Two
  // changes:
  //   (a) a `responses` entry may be keyed 'upstream:<model>' and may be an
  //       ARRAY consumed in order (the last spec sticks);
  //   (b) the returned object exposes the LIVE responses holder the handler
  //       reads at request time, so a test can swap specs mid-run with
  //       `a.responses = {...}`. A plain copied property would not do: the
  //       handler would keep reading the construction-time argument and every
  //       such swap would be silently dead (a no-op that lets the test pass for
  //       the wrong reason).
  // Concretely: add `let specs = responses;` beside `const hits = [];`, read the
  // spec through `specs` AT REQUEST TIME (replacing the single spec lookup at
  // test.mjs:43), and change the return at test.mjs:54 to:
  //   const model = (() => { try { return JSON.parse(body).model; } catch { return null; } })();
  //   const specList = (model && specs["upstream:" + model]) || specs[req.url] || specs["default"] ||
  //     { status: 200, body: { id: "mock", object: "chat.completion", choices: [{ message: { role: "assistant", content: `mock:${id}` } }] } };
  //   const spec = Array.isArray(specList) ? (specList.length > 1 ? specList.shift() : specList[0]) : specList;
  //   return { id, srv, hits, get responses() { return specs; }, set responses(v) { specs = v; } };
  const a = mockBackend('coolA', {
    // X's 429 is ONE-SHOT and the spec queued behind it is a 500, never a 200:
    // after r1 consumes the 429, no X request can succeed on its own. That is
    // what keeps Task 20's v47a honest - with a leftover 200 queued for X, a
    // later X request would return 200 and the per-request backend skip would
    // never be exercised (the hit count would be 1 for the wrong reason).
    'upstream:X': [
      { status: 429, body: 'rate limited' },
      { status: 500, body: 'boom' }
    ],
    'upstream:Y': { status: 200, body: { choices: [{ message: { role: 'assistant', content: 'ok-y' } }] } }
  });
  const b = mockBackend('coolB', {});
  const pa = await listen(a.srv); a.port = pa;
  const pb = await listen(b.srv); b.port = pb;
  const cfg = {
    port: 0, prefix: '/v1', masterKeyEnv: null,
    backends: [
      { id: 'coolA', baseURL: 'http://127.0.0.1:' + pa, apiKeyEnv: 'KEY_CA' },
      { id: 'coolB', baseURL: 'http://127.0.0.1:' + pb, apiKeyEnv: 'KEY_CB' }
    ],
    models: {
      'cm': { deployments: [{ backend: 'coolA', upstream: 'X' }, { backend: 'coolA', upstream: 'Y', backupOnly: true }], exposed: true },
      'cw': { deployments: [{ backend: 'coolB', upstream: 'W' }], exposed: true }
    },
    presets: {},
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 }
  };
  const { child, base, dir } = await startRouterCfg(cfg, 'KEY_CA=k1\nKEY_CB=k2\n');
  const deps = async (id) => (await (await fetch(base + '/health')).json()).backends.find((x) => x.id === id);
  try {
    // 1) A 429 on (coolA, X) cools ONLY that pair; the request still succeeds
    //    because the failover reaches Y. A blanket 429 on the whole backend
    //    (the pre-fix fixture) made the healthy half unpassable.
    const r1 = await api(base, '/v1/chat/completions', { body: { model: 'cm', messages: [{ role: 'user', content: 'hi' }] } });
    const h1 = await deps('coolA');
    const depX = h1.deployments.find((d) => d.upstream === 'X');
    const depY = h1.deployments.find((d) => d.upstream === 'Y');
    assert(r1.status === 200, 'v46a) failover reaches the sibling deployment on the same backend (got ' + r1.status + ')');
    assert(depX.state === 'cooling' && depY.state === 'healthy', 'v46b) a 429 cools only the (backend, upstream) pair (' + JSON.stringify([depX, depY]) + ')');
    // 2) DELIBERATE BEHAVIOR CHANGE, pinned. Today the live config keys
    //    rateLimitBaseMs/rateLimitMaxMs are DEAD: backoffFor (server.mjs:455-460)
    //    reads bp[kind + 'BaseMs'], and the rate class used to be passed as
    //    'rate', so it looked up rateBaseMs/rateMaxMs - keys no config has - and
    //    every live 429 backed off for the 30s/5min defaults instead of the
    //    configured 30s/10min (config.json backoff: rateLimitBaseMs 30000,
    //    rateLimitMaxMs 600000). This task passes 'rateLimit', making the
    //    configured keys live, exactly as the spec always said (rate = 30 s base
    //    / 10 min cap). The 50/200 backoff above proves it: with the dead key the
    //    pair would still be cooling 300 ms later (30 s default), with the live
    //    key it is healthy again after its 50 ms window.
    await new Promise((r) => setTimeout(r, 300));
    const depX2 = (await deps('coolA')).deployments.find((d) => d.upstream === 'X');
    assert(depX2.state === 'healthy', 'v46c) 429 cooling honors the configured rateLimitBaseMs/rateLimitMaxMs (not the dead rateBaseMs key)');
    // 3) weekly / auth / billing stay ACCOUNT-wide
    b.responses = { 'upstream:W': { status: 429, body: 'GoUsageLimitError Resets in 2 days' } };
    await api(base, '/v1/chat/completions', { body: { model: 'cw', messages: [{ role: 'user', content: 'hi' }] } });
    const accW = await deps('coolB');
    assert(accW.state === 'cooling' && accW.fails === 100, 'v46d) weekly limit pins the ACCOUNT (fails=100) (' + JSON.stringify({ state: accW.state, fails: accW.fails }) + ')');
    await api(base, '/admin/reset-health', {});
    b.responses = { 'upstream:W': { status: 401, body: 'unauthorized' } };
    await api(base, '/v1/chat/completions', { body: { model: 'cw', messages: [{ role: 'user', content: 'hi' }] } });
    const accA = await deps('coolB');
    assert(accA.state === 'cooling' && accA.lastError.kind === 'auth', 'v46e) an auth failure (401) cools the ACCOUNT (' + JSON.stringify(accA.lastError) + ')');
    await api(base, '/admin/reset-health', {});
    b.responses = { 'upstream:W': { status: 402, body: 'insufficient credits' } };
    await api(base, '/v1/chat/completions', { body: { model: 'cw', messages: [{ role: 'user', content: 'hi' }] } });
    const accB = await deps('coolB');
    assert(accB.state === 'cooling' && accB.lastError.kind === 'billing', 'v46f) a billing failure (402 + insufficient credits) cools the ACCOUNT (' + JSON.stringify(accB.lastError) + ')');
    // leave both registries clean for the Task 20 additions that continue in
    // this same block
    await api(base, '/admin/reset-health', {});
  } finally {
    child.kill(); a.srv.close(); b.srv.close();
    await rm(dir, { recursive: true, force: true });
  }
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v46b)` (today /health has no `deployments[]` array at all, so the `.find(...)` throws) and FAIL `v46c)` (a 429 cools the whole backend for the configured-or-default window).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — new deployment map next to health:
const deploymentHealth = new Map(); // '<backend>::<upstream>' -> {state, nextAvailableAt, fails, backoffMs, lastError}
function depKey(backendId, upstream) { return backendId + '::' + upstream; }
// markAccountFailure is today's markFailure body for the account-wide classes,
// semantics unchanged (weekly parse + weeklyDefaultMs pin at :566-572, the
// fireAlert call, recordCoolingEvent at :581, and the client lastError-only
// path at :553-559):
function markAccountFailure(cfg, backendId, status, bodyText, kind) {
  const h = health.get(backendId) || { state: 'healthy', fails: 0, backoffMs: 0 };
  h.fails = (h.fails || 0) + 1;
  const wasCoolingActive = h.state === 'cooling' && h.nextAvailableAt > Date.now();
  let nextMs;
  if (kind === 'weekly') {
    const m = /resets in (\d+)\s+day/i.exec(bodyText || '');
    const days = m ? parseInt(m[1], 10) : 7;
    nextMs = cfg.backoff.weeklyDefaultMs || days * 24 * 60 * 60 * 1000;
    h.fails = 100; // effectively pinned until reset
    fireAlert(cfg, 'weekly_limit', { backend: backendId, days: m ? parseInt(m[1], 10) : null });
  } else {
    h.backoffMs = backoffFor(kind, cfg, h.fails); // auth -> authBaseMs; billing -> 30s/5min defaults
    nextMs = h.backoffMs;
  }
  h.state = 'cooling';
  h.nextAvailableAt = Date.now() + nextMs;
  h.lastError = { status, kind, body: (bodyText || '').slice(0, 300) };
  health.set(backendId, h);
  if (!wasCoolingActive) recordCoolingEvent({ backend: backendId, action: 'cool', kind, manual: false });
}
function markFailure(cfg, backendId, upstream, status, bodyText) {
  const kind = classify(status, bodyText);
  if (kind === 'client') {
    // Client-side payload error: the account is fine; keep the lastError
    // visibility of :553-559 and cool nothing.
    const ah = health.get(backendId) || { state: 'healthy', fails: 0, backoffMs: 0 };
    ah.lastError = { status, kind, body: (bodyText || '').slice(0, 300) };
    health.set(backendId, ah);
    return;
  }
  if (kind === 'auth' || kind === 'billing' || kind === 'weekly') return markAccountFailure(cfg, backendId, status, bodyText, kind);
  const key = depKey(backendId, upstream);
  const h = deploymentHealth.get(key) || { state: 'healthy', fails: 0, backoffMs: 0 };
  h.fails = (h.fails || 0) + 1;
  // DELIBERATE BEHAVIOR CHANGE (pinned by v46c in Step 1): pass the CONFIG key
  // prefix 'rateLimit', not the class name 'rate'. backoffFor (:455-460) reads
  // bp[kind + 'BaseMs'], so 'rate' looked up the dead rateBaseMs/rateMaxMs and
  // every live 429 fell back to the 30s/5min defaults instead of the configured
  // rateLimitBaseMs/rateLimitMaxMs (config.json: 30000 / 600000 = 30s/10min).
  // The server class is unchanged (serverBaseMs / serverMaxMs).
  h.backoffMs = backoffFor(kind === 'rate' ? 'rateLimit' : 'server', cfg, h.fails);
  h.state = 'cooling';
  h.nextAvailableAt = Date.now() + h.backoffMs;
  h.lastError = { status, kind, body: (bodyText || '').slice(0, 300) };
  deploymentHealth.set(key, h);
}
function markSuccess(backendId, upstream) {
  // A success clears ONLY the deployment that served it. Account-wide cooling
  // (auth/billing/weekly) is never cleared by a success; manual cools stay
  // sticky; no sibling deployment is touched.
  const key = depKey(backendId, upstream);
  const h = deploymentHealth.get(key);
  if (h && h.state === 'cooling' && h.lastError && h.lastError.kind === 'manual') return;
  deploymentHealth.set(key, { state: 'healthy', nextAvailableAt: 0, fails: 0, backoffMs: 0, lastError: null });
}
resetAllHealth(); // reset-health must clear BOTH maps (Task 24 pins the manual half)
```
```js
// server.mjs — attempt loop call sites (:2017, :2024), same commit so the suite
// stays green after this task (Task 20 adds the per-request skip line beside them):
markFailure(cfg, attempt.backend, attempt.upstream, result.err.status, result.err.body);
markSuccess(attempt.backend, attempt.upstream);
```
```js
// server.mjs — /health (the existing cfg.backends.map((b) => {...}) at :2929)
// gains the minimal deployments[] projection that makes the split observable;
// Task 22 extends each entry with `manual` and adds /admin/backend upstream:
const now = Date.now();
const deployments = [];
for (const m of Object.values(cfg.models || {})) {
  for (const d of m.deployments || []) {
    if (d.backend !== b.id) continue;
    const dh = deploymentHealth.get(depKey(b.id, d.upstream)) || {};
    const cooling = dh.state === 'cooling' && dh.nextAvailableAt > now;
    deployments.push({ id: b.id, upstream: d.upstream, state: cooling ? 'cooling' : 'healthy', nextAvailableAtMs: dh.nextAvailableAt || 0, fails: dh.fails || 0, lastError: dh.lastError || null });
  }
}
return { id: b.id, state: isCooling ? 'cooling' : 'healthy', nextAvailableAtMs: h.nextAvailableAt || 0, fails: h.fails || 0, lastError: h.lastError || null, manual: !!(isCooling && h.lastError && h.lastError.kind === 'manual'), deployments };
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v46a)` through `v46f)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: per-deployment cooling health split'
```

### Task 20: Attempt selection + per-request backend skip + all-cooling
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:1902-1960` (healthy-pool selection), `C:\dev\local-model-router\server.mjs:1965-2021` (attempt loop skip + mark call sites)
- Test: `C:\dev\local-model-router\test.mjs` (extend the Task 19 block, same fixture: `v47a)` a 5xx skips the backend's remaining deployments for that request only; `v47b)`/`v47c)` manual cools are sticky and excluded from the all-cooling fallback)
**Interfaces:**
- Consumes: Task 19 health maps + Task 12 attempt list
- Produces: per-request skip guarantee for Task 22 (/health + admin surface the same states)
- [ ] **Step 1: Write the failing test**
```js
{
  // INSERT THIS SECTION INSIDE the Task 19 `v46)` block, immediately before its
  // `finally` clause: it reuses that block's `a`, `b`, `base`, `deps`, `api`,
  // `child` and `dir`. It is shown re-opened here only for readability.
  // 4) a 5xx skips the backend's REMAINING deployments for this request only:
  //    X is asked once and Y is NEVER asked, while the next request must try the
  //    backend again (the skip set is per request, never persisted). The swap
  //    below is live because Task 19's mockBackend extension exposes the
  //    responses holder the handler reads at request time; and X's queued spec is
  //    a 500 (Task 19 fixture), never a free 200, so a single X hit can only be
  //    explained by the skip. Both upstreams sit on the SAME backend, so the
  //    recorded hits themselves name which upstream was asked.
  const hitsBefore = a.hits.length;
  a.responses = { default: { status: 500, body: 'boom' } };
  const r = await api(base, '/v1/chat/completions', { body: { model: 'cm', messages: [{ role: 'user', content: 'hi' }] } });
  const hitModels = a.hits.slice(hitsBefore).map((h) => { try { return JSON.parse(h.body).model; } catch { return null; } });
  assert(hitModels.length === 1 && hitModels[0] === 'X' && r.status >= 500, 'v47a) a 5xx skips the backends remaining deployments for this request only (asked=' + JSON.stringify(hitModels) + ', status=' + r.status + ')');
  // 5) manual cools are sticky AND excluded from the all-cooling fallback: with
  //    every candidate manually cooled the pool is EMPTY, and the 503 names the
  //    manual cools. Automatic states still participate in the fallback; manual
  //    never does.
  await api(base, '/admin/backend', { body: { id: 'coolA', action: 'cool' } });
  const rManual = await api(base, '/v1/chat/completions', { body: { model: 'cm', messages: [{ role: 'user', content: 'hi' }] } });
  const bManual = await rManual.json();
  const msgManual = JSON.stringify(bManual);
  assert(rManual.status === 503 && /manually cooled/.test(msgManual), 'v47b) manual cools are excluded from the all-cooling fallback (got ' + rManual.status + ' ' + msgManual.slice(0, 160) + ')');
  const accManual = await deps('coolA');
  assert(accManual.state === 'cooling' && accManual.manual === true, 'v47c) the manual cool is sticky on the account entry across the 503 (' + JSON.stringify({ state: accManual.state, manual: accManual.manual }) + ')');
  await api(base, '/admin/reset-health', {});
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v47a)` only. The real mechanism: with no per-request skip, X's 500 burns a hop and the loop moves on to Y on the SAME backend, so the request records two hits and the assertion fails (the swap above is live, so both upstreams return the 500). `v47b)` and `v47c)` are CHARACTERIZATION and PASS at this step: the all-cooling fallback already excludes manual cools today (server.mjs:1902-1921), and the `manual` flag on the account entry is supplied by Task 19's /health projection - this block pins that behavior, it does not introduce it.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — healthy pool uses both maps:
const skippedBackends = new Set();
const usable = (attempt, now) => {
  if (skippedBackends.has(attempt.backend)) return false;
  if (!isDeploymentUsable(attempt.backend, attempt.upstream, now)) return false;
  return isUsableCurrently(attempt.backend, now);
};
// The all-cooling fallback must ALSO honor `skippedBackends`: the skip is scoped
// to the CURRENT request, so a last-resort retry cannot resurrect a backend that
// already failed this request (v47a would otherwise record a second hit even
// though the healthy pool skipped it).
// The all-cooling fallback exclusion reads BOTH registries: an ACCOUNT manual
// cool (`/admin/backend` without upstream, today's surface) and a per-deployment
// manual cool (Task 22's surface) are equally excluded; automatic cooling still
// participates.
const isManualCooled = (attempt, now) => {
  const ah = health.get(attempt.backend);
  if (ah && ah.state === 'cooling' && ah.nextAvailableAt > now && ah.lastError && ah.lastError.kind === 'manual') return true;
  const dh = deploymentHealth.get(depKey(attempt.backend, attempt.upstream));
  return !!(dh && dh.state === 'cooling' && dh.nextAvailableAt > now && dh.lastError && dh.lastError.kind === 'manual');
};
// server.mjs — attempt loop, on failure:
markFailure(cfg, attempt.backend, attempt.upstream, result.err.status, result.err.body);
if (result.err.status >= 500 || result.err.status === 0) skippedBackends.add(attempt.backend);
// server.mjs — attempt loop, on success (changed in Task 19; shown for
// completeness so the loop is coherent after this task):
markSuccess(attempt.backend, attempt.upstream);
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v47a)` through `v47c)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: per-request backend skip on server failures'
```

### Task 21: Sticky weights + affinity order-compat + distribution (T5 weight half, T4 compat)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:1665-1741` (primary pick — already added in Task 12; this task pins it), `C:\dev\local-model-router\server.mjs:3504` (export hashStr)
- Test: `C:\dev\local-model-router\test.mjs` (new `v40)` block: `v40a)` same session key always resolves to the same primary; `v40b)` rough ratio across distinct keys; `v40c)` pool==count order byte-identical to the legacy affinity order)
**Interfaces:**
- Consumes: Task 12 pick algorithm
- Produces: stickiness guarantee for Task 22 observability
- [ ] **Step 1: Write the failing test**
```js
{
  const { candidates, normalizeConfig, convertLegacyConfig, hashStr } = await import('./server.mjs');
  const cfg = {
    backends: [{ id: 'b1' }, { id: 'b2' }],
    models: { 'w': { deployments: [{ backend: 'b1', upstream: 'u' }, { backend: 'b2', upstream: 'u', weight: 2 }] } },
    presets: {}
  };
  normalizeConfig(cfg);
  const first = candidates(cfg, 'w', 'sticky-session').ordered[0].backend;
  const second = candidates(cfg, 'w', 'sticky-session').ordered[0].backend;
  assert(first === second, 'v40a) sticky primary per session key (' + first + ')');
  const counts = { b1: 0, b2: 0 };
  for (let i = 0; i < 60; i++) counts[candidates(cfg, 'w', 'sess-' + i).ordered[0].backend]++;
  assert(counts.b2 > counts.b1, 'v40b) rough weight ratio favors b2 (' + JSON.stringify(counts) + ')');
  // T4 compat: affinityPool == provider count converts to a byte-identical
  // candidate order. Legacy rule (server.mjs:1723-1725): pool = min(affinityPool,
  // providers.length); primaryIdx = hashStr(sessionKey) % pool; order =
  // [providers[primaryIdx], ...providers.filter(i !== primaryIdx)]. With every
  // weight 1, v3 is the same formula over `total = sum(weights) = pool`, so the
  // two orders must match byte-for-byte for the same session key.
  const old = {
    backends: [{ id: 'b1' }, { id: 'b2' }, { id: 'b3' }],
    models: { 'c': { providers: [{ backend: 'b1', upstream: 'u' }, { backend: 'b2', upstream: 'u' }, { backend: 'b3', upstream: 'u' }], affinityPool: 3 } },
    presets: { 'cp': { strategy: 'affinity', models: ['c'] } }
  };
  const declared = ['b1', 'b2', 'b3'];
  const legacyIdx = hashStr('sess-order') % declared.length;
  const legacyOrder = [declared[legacyIdx], ...declared.filter((_, i) => i !== legacyIdx)];
  const v3 = convertLegacyConfig(old).config;
  normalizeConfig(v3);
  const compatOrder = candidates(v3, 'cp', 'sess-order').ordered.map((a) => a.backend);
  assert(JSON.stringify(compatOrder) === JSON.stringify(legacyOrder), 'v40c) affinityPool == provider count keeps the legacy byte-identical order (' + JSON.stringify(compatOrder) + ' vs ' + JSON.stringify(legacyOrder) + ')');
}
```
```js
// server.mjs:3504 — export hashStr (server.mjs:1568) for the order-compat
// assertion above; same task, same commit:
export { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, convertLegacyConfig, validateConfig, hashStr };
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v40a)` if the pick used Math.random instead of the session hash (non-deterministic primary), and `v40c)` if the v3 order is not the legacy rotation (that half additionally needs the hashStr export added in Step 3).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — primary pick MUST be hash-derived (no Math.random here):
const weights = primaries.map((d) => (Number.isInteger(d.weight) && d.weight >= 1 ? d.weight : 1));
const total = weights.reduce((s, w) => s + w, 0);
const h = hashStr(sessionKey) % total;
let acc = 0;
let pick = 0;
for (let i = 0; i < weights.length; i++) { acc += weights[i]; if (h < acc) { pick = i; break; } }
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v40a)` through `v40c)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: sticky weighted primary with session hash'
```

### Task 22: /health deployments[] + /admin/backend upstream + history upstream
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:2927-2943` (/health: extend the Task 19 `deployments[]` projection with the `manual` flag), `C:\dev\local-model-router\server.mjs:3284-3324` (/admin/backend), history attempt record `C:\dev\local-model-router\server.mjs:2002-2011` (already carries upstream; pin it)
- Test: `C:\dev\local-model-router\test.mjs` (extend the Task 19 block: health shows deployments; cool with upstream targets one deployment; cool without upstream targets account state)
**Interfaces:**
- Consumes: Tasks 19-20 health maps
- Produces: admin/health surface for Task 24 (reset) and dashboard cooling timeline
- [ ] **Step 1: Write the failing test**
```js
{
  const h = await (await fetch(base + '/health')).json();
  const entry = h.backends.find((x) => x.id === 'coolA');
  assert(Array.isArray(entry.deployments) && entry.deployments.length === 2, 'v41a) /health carries deployments[] (' + JSON.stringify(entry) + ')');
  const rc = await api(base, '/admin/backend', { body: { id: 'coolA', upstream: 'X', action: 'cool' } });
  assert(rc.status === 200, 'v41b) per-deployment cool accepted (got ' + rc.status + ')');
  const depX = (await (await fetch(base + '/health')).json()).backends.find((x) => x.id === 'coolA').deployments.find((d) => d.upstream === 'X');
  assert(depX.manual === true && depX.state === 'cooling', 'v41c) the per-deployment cool shows manual:true (' + JSON.stringify(depX) + ')');
  await api(base, '/admin/backend', { body: { id: 'coolA', upstream: 'X', action: 'uncool' } });
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: `v41a)` already PASSES (Task 19 added the minimal projection); FAIL `v41b)`/`v41c)` because today /admin/backend ignores `upstream`.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — /health per-backend entry extends the Task 19 deployments[]
// projection with the manual flag:
const deployments = (cfg.models ? Object.values(cfg.models) : [])
  .flatMap((m) => (m.deployments || []).filter((d) => d.backend === b.id))
  .map((d) => {
    const dh = deploymentHealth.get(depKey(b.id, d.upstream)) || {};
    const cooling = dh.state === 'cooling' && dh.nextAvailableAt > now;
    return { id: b.id, upstream: d.upstream, state: cooling ? 'cooling' : 'healthy', nextAvailableAtMs: dh.nextAvailableAt || 0, fails: dh.fails || 0, lastError: dh.lastError || null, manual: !!(cooling && dh.lastError && dh.lastError.kind === 'manual') };
  });
const isManual = !!(isCooling && h.lastError && h.lastError.kind === 'manual');
return { id: b.id, state: isCooling ? 'cooling' : 'healthy', nextAvailableAtMs: h.nextAvailableAt || 0, fails: h.fails || 0, lastError: h.lastError || null, manual: isManual, deployments };
```
```js
// server.mjs — POST /admin/backend: optional upstream targets one deployment:
const upstream = body && body.upstream;
if (upstream != null) {
  const key = depKey(id, String(upstream));
  if (action === 'cool') {
    const forMs = typeof body.forMs === 'number' && body.forMs > 0 ? body.forMs : null;
    deploymentHealth.set(key, { state: 'cooling', nextAvailableAt: forMs ? Date.now() + forMs : Number.MAX_SAFE_INTEGER, fails: 0, backoffMs: 0, lastError: { status: 0, kind: 'manual', body: 'manually cooled via UI' } });
  } else {
    deploymentHealth.set(key, { state: 'healthy', nextAvailableAt: 0, fails: 0, backoffMs: 0, lastError: null });
  }
  res.writeHead(200, { 'content-type': 'application/json' });
  return res.end(JSON.stringify({ ok: true, id, upstream: String(upstream), state: action === 'cool' ? 'cooling' : 'healthy' }));
}
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v41a)` through `v41c)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'feat: per-deployment health surface and admin cool'
```

### Task 23: OQ2 weeklyDefaultMs keep-or-fix decision
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:566-572` (weekly branch of markAccountFailure)
- Test: `C:\dev\local-model-router\test.mjs` (new `v42)` block pinning the decided behavior)
**Interfaces:**
- Consumes: Task 19 account failure path
- Produces: decided weekly semantics (no silent inheritance) for Task 24
- [ ] **Step 1: Write the failing test (pins the KEEP decision explicitly)**
```js
{
  const { classify } = await import('./server.mjs');
  assert(classify(429, 'GoUsageLimitError Resets in 2 days') === 'weekly', 'v42a) weekly class detected');
}
```
```js
// server.mjs:3504 — classify is NOT exported today (server.mjs:484 defines it);
// add it in this task, same commit, so the import above resolves:
export { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, convertLegacyConfig, validateConfig, hashStr, classify };
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v42a)` only if classify is not exported (it is not: Step 1 adds it to the export list); this task then records the operator decision (keep the weeklyDefaultMs override pending a separate fix) and the test pins current behavior so it cannot silently change.
- [ ] **Step 3: Write minimal implementation (decision record, no behavior change)**
```js
// server.mjs — weekly branch keeps current behavior with an explicit comment:
// OQ2 decided 2026-09-19: keep the weeklyDefaultMs override (do not silently
// inherit the parsed 'Resets in N days'); a fix is a separate spec change.
nextMs = cfg.backoff.weeklyDefaultMs || days * 24 * 60 * 60 * 1000;
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v42a)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'docs: pin weeklyDefaultMs override decision (OQ2 keep)'
```

### Task 24: Manual cools sticky + reset clears both maps
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:592-594` (resetAllHealth), markSuccess guard (Task 19)
- Test: `C:\dev\local-model-router\test.mjs` (new `v43)` block: sticky manual cool survives success; reset-health clears deployment + account maps)
**Interfaces:**
- Consumes: Tasks 19-22 health maps
- Produces: cooling regression pin for Increment 6 soak
- [ ] **Step 1: Write the failing test**
```js
{
  await api(base, '/admin/backend', { body: { id: 'coolA', upstream: 'X', action: 'cool' } });
  const ok = await api(base, '/v1/chat/completions', { body: { model: 'cm', messages: [{ role: 'user', content: 'hi' }] } });
  assert(ok.status === 200, 'v43a) request still succeeds via sibling deployment (got ' + ok.status + ')');
  const h = await (await fetch(base + '/health')).json();
  const depX = h.backends.find((x) => x.id === 'coolA').deployments.find((d) => d.upstream === 'X');
  assert(depX.state === 'cooling' && depX.manual === true, 'v43b) manual deployment cool is sticky across success');
  await api(base, '/admin/reset-health', {});
  const h2 = await (await fetch(base + '/health')).json();
  const depX2 = h2.backends.find((x) => x.id === 'coolA').deployments.find((d) => d.upstream === 'X');
  assert(depX2.state === 'healthy', 'v43c) reset-health clears deployment cools');
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: `v43a)` and `v43b)` are CHARACTERIZATION and PASS at this step - the per-deployment manual cool surface is Task 22's (`/admin/backend` with `upstream`) and the deployment-only `markSuccess` clear plus the sticky manual guard arrive with Task 19, so this block PINS that behavior instead of introducing it. The new behavior this task owns is `v43c)`: `resetAllHealth` (server.mjs:592-594) must clear the deployment map as well, which is what Step 3 implements.
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs:
function resetAllHealth() {
  for (const id of health.keys()) markHealthy(id, 'reset');
  for (const key of deploymentHealth.keys()) {
    deploymentHealth.set(key, { state: 'healthy', nextAvailableAt: 0, fails: 0, backoffMs: 0, lastError: null });
  }
  recordCoolingEvent({ backend: '*', action: 'reset', kind: null, manual: false });
}
```
- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v43a)` through `v43c)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'fix: sticky manual deployment cools and full reset'
```
- [ ] **Step 6: Ship Increment 5 and verify per deployment (same shape as Task 17)**
```pwsh
node --check server.mjs; node --check test.mjs; node test.mjs
git push origin main
# This increment changes CODE, not just config: hot-reload covers config files
# only, so restart the production container per the runbook (never hand-kill;
# resolve the live container id first, it changes on every redeploy) and confirm
# the listening banner before verifying below.
curl -s http://192.168.68.69:8787/v1/models | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.data.map(x=>x.id).join('\n'))})" | Sort-Object | Set-Content C:/dev/local-model-router/.models-increment5.txt
Compare-Object (Get-Content C:/dev/local-model-router/.models-after.txt) (Get-Content C:/dev/local-model-router/.models-increment5.txt)
curl -s http://192.168.68.69:8787/health | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);for (const b of j.backends) console.log(b.id + ' ' + b.state + ' deployments=' + (b.deployments||[]).map(d=>d.upstream+':'+d.state).join(','))})"
# pwsh quoting, same idiom as Task 17 Step 1: the JSON body sits in a
# SINGLE-quoted variable; the Authorization header is DOUBLE-quoted so
# $env:LMR_DSH_KEY expands instead of being sent literally (401).
$chatBody = '{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"say ok"}],"max_tokens":8}'
curl -s http://192.168.68.69:8787/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer $env:LMR_DSH_KEY" -d $chatBody | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(s.slice(0,300)))"
```
Expected: suite tail "N passed, 0 failed"; the served id-set diff is EMPTY (the cooling re-key must not move the served set); `/health` prints one line per backend with its per-deployment states (`deployments=upstream:state,...`) and no unexpected cooling; the chat call returns HTTP 200 with assistant content and never prints a key value.
---
## Increment 6 — Legacy removal + docs (after the soak period set at increment-6 review)

### Task 25: Remove legacy synthesis (providers/backends/members/identity/affinityPool)
**Files:**
- Modify: `C:\dev\local-model-router\server.mjs:1316-1333` (path A), `C:\dev\local-model-router\server.mjs:1408-1462` (path B), `C:\dev\local-model-router\server.mjs:1469-1473` (identity presets), `C:\dev\local-model-router\server.mjs:1485-1490` + `C:\dev\local-model-router\server.mjs:1629-1633` (carve-outs), and the old-shape reader/converter wiring retired here: `isOldShape` + `convertLegacyConfig` call sites in `loadConfig` (`server.mjs:2894-2916`), the `POST /api/config` save path (`server.mjs:3053-3111`), and the `/api/config/reset` restore path (`server.mjs:3113-3145`), preset affinityPool handling pinned by test.mjs:691
- Test: `C:\dev\local-model-router\test.mjs` (legacy y/z blocks that assert synthesis now assert refusal or are removed; affinity compat test from Task 21 stays)
**Interfaces:**
- Consumes: Tasks 10-13 bridge. Scope clarification (replaces the earlier, too-broad "bridge deleted here" note): ONLY `normalizeConfig`'s legacy synthesis branches (paths A/B, identity-preset synthesis, the carve-outs, preset-level `affinityPool`) are deleted in this task; the conversion wiring stays in place and is retired by this task, with test blocks `v37a` and `v39a)`/`v39b)` retired alongside it (the old-shape path stops being exercised, so those blocks go with it rather than being left green against dead code).
- Produces: v3-only normalize/candidates/buildPayload for Task 27 gate
- [ ] **Step 1: Write the failing test (v3-only refusal)**
```js
{
  const { normalizeConfig } = await import('./server.mjs');
  const legacy = {
    backends: [{ id: 'b1' }],
    models: { 'old': { backends: ['b1'], affinityPool: 1 } },
    presets: {}
  };
  normalizeConfig(legacy);
  assert(!legacy.models.old, 'v44a) pre-presets backends shape no longer synthesizes (removed)');
}
```
- [ ] **Step 2: Run test to verify it fails**
Run: `node --check server.mjs; node test.mjs`
Expected: FAIL `v44a)` (today path A still synthesizes providers).
- [ ] **Step 3: Write minimal implementation**
```js
// server.mjs — models loop, v3-only (replaces paths A/B + carve-outs):
for (const [id, m] of Object.entries(cfg.models)) {
  if (!m || typeof m !== 'object' || !Array.isArray(m.deployments) || !m.deployments.length) {
    console.warn('config warning: model \'' + id + '\': deployments must be a non-empty array, entry dropped');
    delete cfg.models[id];
    continue;
  }
  const valid = [];
  for (const d of m.deployments) {
    if (!d || typeof d !== 'object' || typeof d.backend !== 'string') continue;
    if (!backendById.has(d.backend)) continue;
    if (!d.upstream || typeof d.upstream !== 'string') continue;
    valid.push(d);
  }
  m.deployments = valid;
}
```

```js
// server.mjs — retire the old-shape reader/converter wiring (this task, same commit):
// remove isOldShape and convertLegacyConfig and their call sites in loadConfig,
// the POST /api/config save path, and the /api/config/reset restore path.
// After this commit the router is v3-only: old-shape input is no longer converted
// or synthesized — validateConfig (load mode) warns on the missing-deployments
// entries and the v3-only models loop above warn-drops them.
```

- [ ] **Step 4: Run test to verify it passes**
Run: `node --check server.mjs; node test.mjs`
Expected: `v44a)` PASS; tail "N passed, 0 failed".
- [ ] **Step 5: Commit**
```pwsh
git add server.mjs test.mjs
git commit -m 'refactor: remove legacy config synthesis paths'
```

### Task 26: Docs — DESIGN-3LAYER v3 contract, AGENTS.md, skill runbook pointer
**Files:**
- Modify: `C:\dev\local-model-router\DESIGN-3LAYER.md` (v3 contract), `C:\dev\local-model-router\AGENTS.md` (config contract + endpoint rows), switchblade skill runbook (reference only, not in this repo)
- Test: `C:\dev\local-model-router\test.mjs` — none (docs-only; gate is the suite staying green)
**Interfaces:**
- Consumes: Tasks 10-25 final semantics (deployments, fallback, exposure, cooling keys, merge chain)
- Produces: none (documentation matches code)
- [ ] **Step 0: Resolve the foreign uncommitted changes in AGENTS.md (BLOCKING)**
Run: `git status --short AGENTS.md; git diff --stat AGENTS.md`
Expected: AGENTS.md carries foreign uncommitted changes; resolve them with the operator (commit or discard with explicit consent) before this task stages AGENTS.md; if unresolved, commit only DESIGN-3LAYER.md and leave the AGENTS.md updates pending in the working tree (never fold a foreign change into this commit).
- [ ] **Step 1: Update DESIGN-3LAYER.md to the v3 contract**
```md
## DESIGN-3LAYER.md — the section must document exactly these items
- Layers: `backends[]` = raw upstream accounts (`id`, `baseURL`, `apiKeyEnv` NAME only, optional `params`, `dropParams`, `model`, `timeoutMs`, `idleTimeoutMs`, `connectTimeoutMs`, `translateDeveloperRole`); `models{}` = the addressable unit and the thing that OWNS its deployments.
- `models{}`: `deployments[]` entries are `{ backend, upstream, weight (integer >= 1, default 1), backupOnly (default false), params?, dropParams?, paramMap? }`; `deployments` must be a non-empty array, every `backend` must reference a declared backend, every `upstream` must be non-empty; `backupOnly` deployments are tried only after every primary is exhausted; optional model-level `params`, `retry`, `meta`; `exposed` (default `true`).
- `presets{}`: routing bundles over model (or nested preset) ids: `strategy` (`"fallback"` is the ONLY valid v3 value; the field stays so future strategies need no schema change), `models[]` = STRING refs only (model ids or nested preset ids) in ordered-fallback order, `meta`, `exposed` (default `true`); presets-of-presets stay with cycle detection, and a nested preset contributes only its model list (the top-level strategy governs, nested strategies are ignored). Per-ref `{ model, weight }` objects and preset-level `params` are REMOVED concepts: weights live on deployments (`deployments[].weight`), and presets route - they do not rewrite payloads.
- One id is one thing: a preset id must not equal a model id (error-class at save); identity presets no longer exist; a model id is directly addressable.
- Session affinity: within a model, primaries are chosen by `hash(sessionKey) % sum(weights)` with integer weights >= 1; a single-primary or unweighted model degenerates to declared order; `affinityPool` is gone (its old meaning — "pool N of M providers" — is expressed as `backupOnly` on the tail).
- Merge chain (highest priority last; a lower layer only fills keys not already set): global `params` < `backend.params` < `deployment.params` (the old provider layer) < `model.params` < request body. There is NO preset layer (preset-level `params` is a removed concept; presets route, they do not rewrite payloads). `model` is reserved: after merging, the forwarded `model` is ALWAYS the resolved deployment `upstream`. Dialect order is exact: `dropParams` (backend union deployment) BEFORE `paramMap` BEFORE developer-role translation.
- Cooling keys: deployment-class failures (server/network/timeout, generic 429 `rate`) cool ONLY `<backend>::<upstream>`; auth (401/403), billing (402 with `insufficient credits|quota`) and weekly (`GoUsageLimitError`) cool the whole `backendId`; a success clears only the deployment that served it and never account-wide state; a 5xx/network failure skips that backend's remaining deployments for the CURRENT request only; 400/422 client payload errors never cool anything; manual cools are sticky and never participate in the all-cooling fallback.
- Backoff: `rateLimitBaseMs`/`rateLimitMaxMs` (429 generic; 30 s / 10 min) are LIVE from this revision — the pre-v3 wiring looked up the dead `rateBaseMs`/`rateMaxMs` keys, so a configured 429 window only takes effect now; `serverBaseMs`/`serverMaxMs` (30 s / 5 min), `authBaseMs`/`authMaxMs` (1 min / 30 min), `weeklyDefaultMs` (weekly pin; still overrides a parsed "Resets in N days" — OQ2, unchanged).
- `/health`: per-backend `{ id, state, nextAvailableAtMs, fails, lastError, manual, deployments[] }`, each deployment entry `{ id, upstream, state, nextAvailableAtMs, fails, lastError, manual }`.
- `POST /admin/backend`: `{ id, upstream?, action: "cool"|"uncool", forMs? }` — with `upstream` it targets one deployment, without it the backend/account; history attempt records carry `backend` + `upstream`.
- `/v1/models` exposure rule: exposed presets first (config order), then exposed model ids not already served; `exposed: false` entries stay routable but appear in neither `/v1/models` nor any UI picker; the same ids always appear in the same order.
```
- [ ] **Step 2: Run the gate to confirm docs-only**
Run: `node --check server.mjs; node test.mjs`
Expected: tail "N passed, 0 failed" (no code changed).
- [ ] **Step 3: Update AGENTS.md contract rows**
```md
## AGENTS.md — Config Contract and Endpoints sections must document exactly these rows
- models{} row: replace "providers[] = {backend, upstream}" with "deployments[] = { backend, upstream, weight (integer >= 1), backupOnly, params?, dropParams?, paramMap? } — non-empty array, backend must exist, upstream non-empty"; delete `affinityPool`; add `exposed` (default true) to models{} and presets{}.
- presets{} row: `strategy` is `"fallback"` in v3 (affinity/failover/weighted are accepted only while the legacy bridge is alive); `models` = STRING ids only (model or nested preset ids, ordered fallback) - per-ref `{ model, weight }` objects and preset-level `params` are removed in v3, weights live on `deployments[].weight`; presets-of-presets with cycle detection; `exposed: false` is omitted from `/v1/models` and every UI picker.
- One-id-one-thing row: a preset id must not equal a model id; identity presets no longer exist; a model id is directly addressable.
- Cooling row: deployment-keyed `<backend>::<upstream>` for server/rate, account-keyed for auth/billing/weekly; 5xx/network skips the backend's remaining deployments for the current request only; a success clears only that deployment; manual cools are sticky and excluded from the all-cooling fallback (the empty-pool 503 names them).
- Backoff row: record that `rateLimitBaseMs`/`rateLimitMaxMs` became LIVE (the old dead `rateBaseMs`/`rateMaxMs` lookup) and that `weeklyDefaultMs` still overrides a parsed "Resets in N days".
- Endpoints table: `GET /health` -> per-backend `deployments[]` plus `manual`; `POST /admin/backend` -> `{ id, upstream?, action, forMs? }`; `GET /v1/models` -> exposed presets then exposed models, with the same-set-same-order guarantee kept.
- Key files table: update the `DESIGN-3LAYER.md` description to the v3 contract (models own deployments, fallback presets, per-deployment cooling).
```
- [ ] **Step 4: Run the gate again**
Run: `node --check server.mjs; node test.mjs`
Expected: tail "N passed, 0 failed".
- [ ] **Step 5: Commit (gated by Step 0)**
```pwsh
git status --short
git add DESIGN-3LAYER.md
git add AGENTS.md
git commit -m 'docs: v3 config contract and endpoint updates'
```
Expected: `DESIGN-3LAYER.md` is always staged. `AGENTS.md` is staged ONLY if Step 0 was resolved with the operator; if Step 0 is unresolved, drop the `git add AGENTS.md` line, commit `DESIGN-3LAYER.md` alone, and leave the AGENTS.md edits pending in the working tree.

### Task 27: Final soak gate + secret scan + served-id stability + ship
**Files:**
- Modify: none (verification + ship task)
- Test: full suite + CI secret scan + live served-id check
**Interfaces:**
- Consumes: all Tasks 1-26
- Produces: none (release gate)
- [ ] **Step 1: Run the full gate locally**
Run: `node --check server.mjs; node --check test.mjs; node test.mjs`
Expected: both checks clean; tail "N passed, 0 failed".
- [ ] **Step 2: Run the secret scan (CI parity)**
```pwsh
git ls-files | Select-String -Pattern '\.env$|secrets\.json$|api-keys\.json$|router-history\.jsonl$'
```
Expected: no tracked secret files (empty output); config carries env NAMES only.
- [ ] **Step 3: Confirm served-id stability post-soak**
```pwsh
curl -s http://192.168.68.69:8787/v1/models | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.data.map(x=>x.id).join('\n'))})" | Sort-Object | Compare-Object (Get-Content C:/dev/local-model-router/.models-after.txt)
```
Expected: no differences (empty output).
- [ ] **Step 4: Confirm the suite tail one last time**
Run: `node test.mjs`
Expected: tail "N passed, 0 failed".
- [ ] **Step 5: Ship (code increments ship via PR/push per AGENTS.md; never stage config.json)**
```pwsh
git status --short
git push origin main
```
