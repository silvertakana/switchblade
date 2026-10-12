// switchblade server.mjs
// Zero-dependency Node 25 router that replaces homelab LiteLLM for the
// deepseek-v4-flash family: two OpenCode Go accounts load-balanced by
// session-affinity hashing, with a DeepSeek direct fallback, per-backend
// health tracking and exponential backoff. Routing goes through three layers:
// backends -> models -> presets. Backends are raw upstream accounts (no model
// binding; a legacy `model` field remains legal as a default-upstream source).
// Models bind a logical id to one or more providers (backend + upstream +
// optional per-provider dialect). Presets are routing policies over models
// (affinity / failover / weighted strategies). Legacy configs (old `models`
// with `backends`, and DESIGN-PRESETS-era presets with `members`) are
// synthesized into the new shape at load. Manual cools are sticky: they never
// participate in the all-cooling fallback and a success never clears them.

import http from "node:http";
import { readFileSync, statSync, watch as watchF, appendFileSync, renameSync, openSync, readSync, closeSync, readdirSync, unlinkSync, mkdirSync, writeFileSync } from "node:fs";
import { readFile as readFileP, rename as renameP, writeFile as writeFileP } from "node:fs/promises";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import crypto from "node:crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = process.env.ROUTER_CONFIG || join(__dirname, "config.json");
const ENV_PATH = process.env.ROUTER_ENV || join(__dirname, ".env");

// ---- config + env loading ----------------------------------------------------

function parseEnv(text) {
  const out = {};
  for (const line of text.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const eq = t.indexOf("=");
    if (eq < 0) continue;
    const k = t.slice(0, eq).trim();
    let v = t.slice(eq + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    if (k) out[k] = v;
  }
  return out;
}

function loadEnv() {
  try {
    const raw = readFileSync(ENV_PATH, "utf8");
    const parsed = parseEnv(raw);
    // Only set env vars not already present so a real shell env wins.
    for (const [k, v] of Object.entries(parsed)) {
      if (process.env[k] === undefined) process.env[k] = v;
    }
  } catch {
    // no .env file -> rely on real environment
  }
}

// ---- config editing (web UI write path) --------------------------------------
//
// The editor writes CONFIG_PATH directly; the fs.watch below picks it up in
// ~300ms, so a save needs no restart and no explicit reload call. Two rules
// make that safe on a file that is live routing state:
//   1. Never round-trip through /api/config. That endpoint serves the
//      NORMALIZED config, which drops top-level keys normalization does not
//      model (pricing, missCapture, alerts) and rewrites derived fields.
//      Saving it back would silently destroy them. The editor loads raw.
//   2. Snapshot before every write, validate before every write, write
//      atomically (tmp + rename) so a partial write can never be observed.
// port and host are set at startup by srv.listen() and are therefore rejected
// here: accepting them would look like it worked and do nothing.

const SNAPSHOT_DIR = join(dirname(CONFIG_PATH), "config.history");
const MAX_SNAPSHOTS = 20;
const RESTART_ONLY_KEYS = ["port", "host"];

// ---- managed secrets store (web UI env editor) --------------------------------
//
// A UI-managed JSON store for credential env vars, separate from the
// hand-edited .env: it hot-reloads (the .env mechanism cannot, see loadEnv)
// and, unlike a container-baked .env, survives redeploys when bind-mounted
// like config.server.json. Precedence: real shell env > secrets.json > .env.

const SECRETS_PATH = process.env.ROUTER_SECRETS || join(dirname(CONFIG_PATH), "secrets.json");
const SECRETS_HISTORY_DIR = join(dirname(SECRETS_PATH), "secrets.history");
const MAX_ENV_SNAPSHOTS = 20;
// Names consumed once at module load; changing them changes which files the
// process reads, so the UI must say "next start" instead of "live".
const STARTUP_ONLY_VARS = new Set(["ROUTER_CONFIG", "ROUTER_ENV", "ROUTER_HISTORY", "ROUTER_KEYS_FILE", "ROUTER_SECRETS", "NODE_OPTIONS"]);
// Real environment at boot: these names are platform-owned and the UI never
// overrides them. Snapshotted in main() before loadEnv()/loadSecrets() run.
const SHELL_ENV_NAMES = new Set();

function readSecrets() {
  try {
    const parsed = JSON.parse(readFileSync(SECRETS_PATH, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function secretsRevision() {
  try {
    return statSync(SECRETS_PATH).mtimeMs;
  } catch {
    return 0;
  }
}

function listSecretsSnapshots() {
  try {
    return readdirSync(SECRETS_HISTORY_DIR)
      .filter((f) => f.endsWith(".json"))
      .sort()
      .reverse();
  } catch {
    return [];
  }
}

function snapshotSecrets() {
  let raw;
  try {
    raw = readFileSync(SECRETS_PATH, "utf8");
  } catch {
    return null;
  }
  mkdirSync(SECRETS_HISTORY_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const name = `${stamp}.json`;
  try {
    writeFileSync(join(SECRETS_HISTORY_DIR, name), raw);
  } catch {
    return null;
  }
  for (const stale of listSecretsSnapshots().slice(MAX_ENV_SNAPSHOTS)) {
    try {
      unlinkSync(join(SECRETS_HISTORY_DIR, stale));
    } catch {
      /* best effort prune */
    }
  }
  return name;
}

function writeSecretsAtomic(obj) {
  const tmp = SECRETS_PATH + ".tmp";
  writeFileSync(tmp, JSON.stringify(obj, null, 2) + "\n");
  renameSync(tmp, SECRETS_PATH);
}

// Validate an env name/value from the UI. Returns an error message or null.
function validateEnvEntry(name, value) {
  if (typeof name !== "string" || !/^[A-Z_][A-Z0-9_]*$/.test(name)) {
    return "name must be uppercase letters, digits and underscores (e.g. OPENCODE_GO_KEY)";
  }
  if (typeof value !== "string") return "value must be a string";
  if (value.length === 0) return "value is empty; use the remove action instead";
  if (value.length > 8192) return "value is too long (max 8 KiB)";
  if (isProtectedEnvName(name)) return `'${name}' is an authentication credential and cannot be changed from the UI`;
  if (SHELL_ENV_NAMES.has(name)) {
    return `'${name}' is set by the platform or shell environment and is not managed here; change it at the source (for example the Coolify application env vars or .env) instead`;
  }
  return null;
}

// Auth credentials are the identity that UNLOCKS the editor itself, so a wrong
// write would lock the operator out with no UI path back. Blocked server-side,
// not just hidden.
function isProtectedEnvName(name) {
  const names = [cfg && cfg.masterKeyEnv, cfg && cfg.uiPasswordEnv].filter(Boolean);
  return names.includes(name);
}

// ID for the UI: names the live config references by env NAME, whether or not
// they are currently set, so the operator can wire a key before it exists.
function referencedEnvNames() {
  const out = new Set();
  if (!cfg) return out;
  if (cfg.masterKeyEnv) out.add(cfg.masterKeyEnv);
  if (cfg.uiPasswordEnv) out.add(cfg.uiPasswordEnv);
  for (const b of cfg.backends || []) if (b && typeof b === "object" && b.apiKeyEnv) out.add(b.apiKeyEnv);
  return out;
}

// Names declared in the .env fallback file. Bounded by what the operator
// actually wrote there, unlike the whole process environment.
function dotenvNames() {
  try {
    return Object.keys(parseEnv(readFileSync(ENV_PATH, "utf8")));
  } catch {
    return [];
  }
}

function envSource(name) {
  if (SHELL_ENV_NAMES.has(name)) return "shell";
  if (name in readSecrets()) return "secrets";
  return "env";
}

// Apply the secrets file into process.env. Names the real shell owned at boot
// are skipped: the platform (or operator shell) is the more specific authority
// and the UI reports the shadowing via `source`.
function applyRuntimeEnv() {
  const secrets = readSecrets();
  const applied = [];
  for (const [name, value] of Object.entries(secrets)) {
    if (typeof value !== "string") continue;
    if (SHELL_ENV_NAMES.has(name)) continue;
    if (process.env[name] !== value) {
      process.env[name] = value;
      applied.push(name);
    }
  }
  return applied;
}

function loadSecrets() {
  try {
    applyRuntimeEnv();
  } catch (e) {
    console.error(`[${new Date().toISOString()}] secrets reload failed: ${e.message}`);
  }
}

function readRawConfig() {
  return JSON.parse(readFileSync(CONFIG_PATH, "utf8"));
}

function configRevision() {
  try {
    return statSync(CONFIG_PATH).mtimeMs;
  } catch {
    return 0;
  }
}

function listSnapshots() {
  try {
    return readdirSync(SNAPSHOT_DIR)
      .filter((f) => f.endsWith(".json"))
      .sort()
      .reverse();
  } catch {
    return [];
  }
}

function snapshotConfig() {
  mkdirSync(SNAPSHOT_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const name = `${stamp}.json`;
  try {
    writeFileSync(join(SNAPSHOT_DIR, name), readFileSync(CONFIG_PATH, "utf8"));
  } catch {
    return null;
  }
  for (const stale of listSnapshots().slice(MAX_SNAPSHOTS)) {
    try {
      unlinkSync(join(SNAPSHOT_DIR, stale));
    } catch {
      /* best effort prune */
    }
  }
  return name;
}

// rename() cannot replace a single-file bind mount: Linux refuses to rename
// onto a mount point with EBUSY, and even a successful swap would leave the
// container's mount (and its fs.watch) pinned to the original inode, so the
// router would keep reading the old file. Fall back to writing the validated
// bytes in place, which is the only thing that works there.
const IN_PLACE_WRITE_CODES = new Set(["EBUSY", "EXDEV", "ENOTSUP", "EOPNOTSUPP"]);

function writeConfigAtomic(obj, target = CONFIG_PATH, replace = renameSync) {
  const tmp = target + ".tmp";
  const text = JSON.stringify(obj, null, 2) + "\n";
  writeFileSync(tmp, text);
  try {
    replace(tmp, target);
  } catch (e) {
    // Anything else (EACCES, EROFS, ENOSPC) is a genuine failure: an in-place
    // write would fail the same way, and swallowing it would only obscure why.
    if (!IN_PLACE_WRITE_CODES.has(e.code)) throw e;
    writeFileSync(target, text);
    try {
      unlinkSync(tmp);
    } catch {
      /* best effort */
    }
  }
}

// Env var NAMES that are currently set, so the editor can offer them in a
// picker. Values are never returned.
function envNames() {
  return Object.keys(process.env).sort();
}

// v3 backend keys. `model` is the bridge-era default-upstream source and
// `idleTimeoutMs`/`connectTimeoutMs` are live fields, so all three are known
// here: a converted live config must not be told its real backend fields are
// unrecognized.
const KNOWN_BACKEND_KEYS = new Set([
  "id", "baseURL", "apiKeyEnv", "params", "dropParams",
  "connectTimeoutMs", "idleTimeoutMs", "translateDeveloperRole", "timeoutMs", "model",
]);

// v3 model keys. The dialect/typed fields live per deployment in v3, but a model
// still carries `params`/`retry`/`meta` and the optional `exposed` flag, and the
// allowlist keeps the SAME tolerance the backend layer already has: an unknown
// key warns and never refuses the save. Deliberately absent is `providers`: a
// converted model has none, and a hand-written v3 model that keeps one is
// carrying data the v3 read ignores, which the operator needs told.
const KNOWN_MODEL_KEYS = new Set([
  "deployments", "exposed", "params", "retry", "meta", "affinityPool",
  "dropParams", "paramMap", "translateDeveloperRole", "timeoutMs",
]);

// v3 deployment keys: what a provider could carry, in its new position.
const KNOWN_DEPLOYMENT_KEYS = new Set([
  "backend", "upstream", "weight", "backupOnly", "params", "dropParams", "paramMap",
]);

// Read one error path ("models.x.retry", "backends[0].apiKeyEnv") out of a document.
function valueAtPath(root, path) {
  if (!path || !root || typeof root !== "object") return undefined;
  const parts = String(path).replace(/\[(\d+)\]/g, ".$1").split(".").filter((s) => s !== "");
  let cur = root;
  for (const p of parts) {
    if (cur === null || typeof cur !== "object") return undefined;
    cur = cur[p];
  }
  return cur;
}

// A pre-v3 value that the document ALREADY has, unchanged, cannot be a reason to refuse
// the write: the live config carries empty-string params and retry, and an operator has to
// be able to save - or reload - a file they did not change. `current` is what the caller
// is comparing against, and it is the right document in every case: the running config at
// reload, the file on disk at save and restore, and nothing at boot (where the loader
// tolerates these values anyway). The exemption is deliberately narrow - two named fields,
// and only when the other side holds the same empty string - so editing or moving the value
// is still refused and no unrelated error can be silenced. The CI config check reads one
// file as both sides, which would make it blind here, but an empty string is a NOTICE-class
// finding there (only a root or backends error is fatal), so its verdict cannot change.
const GRANDFATHERED_LEGACY_KEYS = /(\.retry|\.params)$/;

// Validate a candidate config. Returns { errors, warnings }; a non-empty
// errors array means the save is refused and the file is left untouched.
function validateConfig(candidate, current, opts) {
  const errors = [];
  const warnings = [];
  const err = (path, message) => errors.push({ path, message });
  const warn = (path, message) => warnings.push({ path, message });
  // 'save' is the default so every existing two-argument caller keeps the
  // save-time strictness; the load path opts in explicitly to tolerant
  // handling of a config that is already on disk.
  const mode = (opts && opts.mode) || "save";
  const finish = () => {
    if (current && typeof current === "object" && current !== candidate) {
      for (let i = errors.length - 1; i >= 0; i--) {
        const e = errors[i];
        if (!GRANDFATHERED_LEGACY_KEYS.test(e.path || "")) continue;
        if (valueAtPath(candidate, e.path) !== "" || valueAtPath(current, e.path) !== "") continue;
        warnings.push({ path: e.path, message: e.message + " (unchanged from the config already in force; the legacy value is kept exactly as it is)" });
        errors.splice(i, 1);
      }
    }
    return { errors, warnings };
  };

  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    err("", "config must be a JSON object");
    return finish();
  }
  for (const key of RESTART_ONLY_KEYS) {
    if (key in candidate && current && candidate[key] !== current[key]) {
      err(key, `'${key}' is set at startup and cannot be changed from the UI; edit the file and restart the router`);
    }
  }
  for (const key of ["backends", "models", "presets"]) {
    if (!(key in candidate)) err(key, `'${key}' is required at the top level`);
  }
  if (errors.length) return finish();

  // Shape detection: a config is validated under v3 rules as soon as ANY model
  // carries `deployments` - a key no pre-v3 config has. Detection is
  // deliberately CONFIG-WIDE rather than per model. A hand-written mixture is a
  // config that started migrating, and a v3 router cannot route the entries
  // that have not: validating each entry under its own shape would accept a
  // file that only half works. Configs with no `deployments` anywhere never
  // enter the v3 branch and keep their behavior exactly.
  const shapeModels = candidate.models;
  const isV3 =
    Boolean(shapeModels) && typeof shapeModels === "object" && !Array.isArray(shapeModels) &&
    Object.values(shapeModels).some((m) => m && typeof m === "object" && m.deployments !== undefined);

  // ---- backends ----
  const backends = candidate.backends;
  if (!Array.isArray(backends)) {
    err("backends", "must be an array");
  } else {
    const seen = new Set();
    backends.forEach((b, i) => {
      const at = `backends[${i}]`;
      if (!b || typeof b !== "object") return err(at, "must be an object");
      if (!b.id) return err(at + ".id", "id is required");
      if (seen.has(b.id)) err(at + ".id", `duplicate backend id '${b.id}'`);
      seen.add(b.id);
      if (!b.baseURL) err(at + ".baseURL", "baseURL is required");
      if (!b.apiKeyEnv) {
        err(at + ".apiKeyEnv", "apiKeyEnv is required (the NAME of an environment variable, not the key)");
      } else if (!process.env[b.apiKeyEnv]) {
        err(
          at + ".apiKeyEnv",
          `environment variable '${b.apiKeyEnv}' is not set in this process; add it to .env (or the container env) before wiring this backend, or the backend will fail closed`
        );
      }
    });
    if (isV3) {
      // v3 backends are allowlisted: an unknown key warns and is never an
      // error, so a config carrying a field this version does not know still
      // loads.
      backends.forEach((b, i) => {
        if (!b || typeof b !== "object") return;
        for (const k of Object.keys(b)) {
          if (!KNOWN_BACKEND_KEYS.has(k)) warn(`backends[${i}]`, `unrecognized backend key '${k}'; the router ignores it`);
        }
      });
    }
  }

  // ---- models ----
  const models = candidate.models;
  const backendIds = new Set((Array.isArray(backends) ? backends : []).filter((b) => b && b.id).map((b) => b.id));
  const backendById = new Map((Array.isArray(backends) ? backends : []).filter((b) => b && b.id).map((b) => [b.id, b]));
  if (!models || typeof models !== "object" || Array.isArray(models)) {
    err("models", "must be an object keyed by model id");
  } else {
    for (const [id, m] of Object.entries(models)) {
      const at = `models.${id}`;
      if (!m || typeof m !== "object") {
        err(at, "must be an object");
        continue;
      }
      const providers = Array.isArray(m.providers) ? m.providers : null;
      if (isV3) {
        // v3 models carry `deployments` instead of `providers`. An entry
        // without them is a pre-migration model inside an otherwise v3 config,
        // so the error names that model rather than being skipped.
        const deps = Array.isArray(m.deployments) ? m.deployments : null;
        if (!deps || !deps.length) {
          err(at + ".deployments", "must be a non-empty array of {backend, upstream}");
        } else {
          deps.forEach((d, i) => {
            const dat = `${at}.deployments[${i}]`;
            if (!d || typeof d !== "object") return err(dat, "must be an object");
            if (!d.backend) return err(dat + ".backend", "backend is required");
            if (!backendIds.has(d.backend)) return err(dat + ".backend", `unknown deployment backend '${d.backend}'`);
            // A v3 deployment always names its upstream: the backend's
            // bridge-era `model` is not a v3 default-upstream source.
            if (!d.upstream) err(dat + ".upstream", "upstream is required");
            if (d.weight !== undefined && (!Number.isInteger(d.weight) || d.weight < 1)) err(dat + ".weight", "weight must be an integer >= 1");
            if (d.backupOnly !== undefined && typeof d.backupOnly !== "boolean") err(dat + ".backupOnly", "backupOnly must be a boolean");
            // A deployment holds what a provider used to hold, so the typed
            // params checks follow the field into its new position.
            if (d.params !== undefined && (!d.params || typeof d.params !== "object" || Array.isArray(d.params))) err(dat + ".params", "params must be an object");
            if (d.paramMap !== undefined && (!d.paramMap || typeof d.paramMap !== "object" || Array.isArray(d.paramMap))) err(dat + ".paramMap", "paramMap must be an object");
            for (const k of Object.keys(d)) {
              if (!KNOWN_DEPLOYMENT_KEYS.has(k)) warn(dat, `unrecognized deployment key '${k}'; the router ignores it`);
            }
          });
        }
        if (m.exposed !== undefined && typeof m.exposed !== "boolean") err(at + ".exposed", "exposed must be a boolean");
        // Allowlist, exactly as the backend layer does it: `backuponly: true`
        // (a misspelling of `backupOnly`) would otherwise leave the deployment a
        // PRIMARY and put live traffic on an account the operator marked as
        // break-glass. A warning, never an error - an unknown key must not be
        // able to refuse a save.
        for (const k of Object.keys(m)) {
          if (!KNOWN_MODEL_KEYS.has(k)) warn(at, `unrecognized model key '${k}'; the router ignores it`);
        }
      } else {
        if (!providers) {
          err(at + ".providers", "must be an array of {backend, upstream}");
          continue;
        }
        if (!providers.length) warn(at + ".providers", "no providers: this model routes nowhere and will 404");
        providers.forEach((pr, i) => {
          const pat = `${at}.providers[${i}]`;
          if (!pr || typeof pr !== "object") return err(pat, "must be an object");
          if (!pr.backend) return err(pat + ".backend", "backend is required");
          if (!backendIds.has(pr.backend)) {
            return err(pat + ".backend", `unknown backend '${pr.backend}'`);
          }
          const b = backendById.get(pr.backend);
          if (!pr.upstream && !(b && b.model)) {
            err(pat + ".upstream", `no upstream given and backend '${pr.backend}' has no default model`);
          }
        });
      }
      if (m.affinityPool !== undefined && (!Number.isInteger(m.affinityPool) || m.affinityPool < 1)) {
        err(at + ".affinityPool", "affinityPool must be an integer >= 1");
      }
      if (m.retry !== undefined) {
        if (!m.retry || typeof m.retry !== "object" || Array.isArray(m.retry)) {
          err(at + ".retry", "retry must be an object {maxRetries, baseMs, maxMs, multiplier, totalMs}");
        } else {
          if (!Number.isInteger(m.retry.maxRetries) || m.retry.maxRetries < 0) err(at + ".retry.maxRetries", "retry.maxRetries must be a non-negative integer");
          for (const k of ["baseMs", "maxMs", "multiplier", "totalMs"]) {
            if (m.retry[k] !== undefined && (typeof m.retry[k] !== "number" || !(m.retry[k] >= 0))) err(at + ".retry." + k, "retry." + k + " must be a number");
          }
        }
      }
      if (m.params !== undefined && (!m.params || typeof m.params !== "object" || Array.isArray(m.params))) {
        err(at + ".params", "params must be an object");
      }
      if (providers) {
        providers.forEach((pr, i) => {
          const pat = at + ".providers[" + i + "]";
          if (pr && pr.params !== undefined && (!pr.params || typeof pr.params !== "object" || Array.isArray(pr.params))) err(pat + ".params", "params must be an object");
          if (pr && pr.paramMap !== undefined && (!pr.paramMap || typeof pr.paramMap !== "object" || Array.isArray(pr.paramMap))) err(pat + ".paramMap", "paramMap must be an object");
        });
      }
      if (m.meta !== undefined && m.meta.pricing !== undefined) {
        const prc = m.meta.pricing;
        if (!prc || typeof prc !== "object" || Array.isArray(prc)) {
          err(at + ".meta.pricing", "meta.pricing must be an object");
        } else {
          for (const k of ["inputPerM", "outputPerM", "cacheHitInputPerM"]) {
            if (prc[k] !== undefined && typeof prc[k] !== "number") err(at + ".meta.pricing." + k, "meta.pricing." + k + " must be a number");
          }
        }
      }
      if (providers && typeof m.affinityPool === "number" && m.affinityPool > providers.length) {
        warn(at + ".affinityPool", `affinityPool ${m.affinityPool} exceeds ${providers.length} provider(s); it clamps at request time`);
      }
    }
  }

  // ---- presets ----
  const presets = candidate.presets;
  const modelIds = new Set(models && typeof models === "object" ? Object.keys(models) : []);
  const presetIds = new Set(presets && typeof presets === "object" ? Object.keys(presets) : []);
  if (!presets || typeof presets !== "object" || Array.isArray(presets)) {
    err("presets", "must be an object keyed by preset id");
  } else {
    for (const [id, p] of Object.entries(presets)) {
      const at = `presets.${id}`;
      if (!p || typeof p !== "object") {
        err(at, "must be an object");
        continue;
      }
      if (isV3) {
        // v3 narrows the valid strategies to 'fallback' alone: the bridge maps
        // every legacy value at conversion time, so anything else was authored
        // against the wrong schema. There is deliberately NO legacy allowlist in
        // this branch - pre-v3 configs never reach it, and the v2 path below
        // keeps its own values (including v2's "unknown 'fallback'") untouched.
        if (p.strategy !== "fallback") warn(at + ".strategy", `v3 preset strategy must be 'fallback'; ${JSON.stringify(p.strategy)} is not a v3 value (the bridge maps the legacy strategies at conversion time)`);
      } else if (p.strategy && !["affinity", "failover", "weighted"].includes(p.strategy)) {
        warn(at + ".strategy", `unknown strategy '${p.strategy}'; the router falls back to affinity`);
      }
      if (modelIds.has(id)) {
        // One collision rule, verdict by shape: pre-v3 keeps the warn (the real
        // configs carry 19+ of them, and aborting boot mid-migration is not an
        // option), while v3 refuses it - a converted config has already resolved
        // every identity collision.
        if (isV3) err(at, `id '${id}' is both a model and a preset; v3 ids are one or the other`);
        else warn(at, `id '${id}' is both a model and a preset; the preset wins in /v1/models`);
      }
      if (isV3 && p.exposed !== undefined && typeof p.exposed !== "boolean") err(at + ".exposed", "exposed must be a boolean");
      const list = Array.isArray(p.models) ? p.models : null;
      if (!list) {
        err(at + ".models", "must be an array of model or preset ids");
        continue;
      }
      if (!list.length) warn(at + ".models", "empty: this preset has no valid models");
      list.forEach((entry, i) => {
        const ref = typeof entry === "string" ? entry : entry && entry.model;
        const sat = `${at}.models[${i}]`;
        if (typeof entry !== "string" && (!entry || typeof entry !== "object")) return err(sat, "must be a string or {model, weight}");
        if (!ref) return err(sat, "model id is required");
        if (!modelIds.has(ref) && !presetIds.has(ref)) {
          if (mode === "load") {
            warn(sat, `unknown model or preset '${ref}'; ref dropped at load`);
          } else {
            err(sat, `unknown model or preset '${ref}'`);
          }
        }
        if (!modelIds.has(ref) && presetIds.has(ref) && ref === id) {
          err(sat, `preset '${id}' references itself`);
        }
      });
    }
  }

  // ---- settings blocks ----
  const mc = candidate.missCapture;
  if (mc && typeof mc === "object" && typeof mc.file === "string") {
    // An absolute path is legitimate: the homelab points `file` at a durable
    // named volume outside the config directory. A RELATIVE name must stay a
    // bare filename, and `..` is refused in BOTH forms so neither can escape.
    if (mc.file.includes("..") || (!isAbsolute(mc.file) && (mc.file.includes("/") || mc.file.includes("\\")))) {
      err("missCapture.file", "must be a bare filename in the config directory or an absolute path (no path separator in a relative name, and no '..')");
    }
  }

  const known = new Set([
    "port", "prefix", "host", "masterKeyEnv", "uiPasswordEnv", "backends", "models", "presets",
    "params", "backoff", "pricing", "missCapture", "alerts", "analytics", "relay",
  ]);
  for (const key of Object.keys(candidate)) {
    if (!known.has(key)) warn(key, "unrecognized top-level key; the router ignores it");
  }

  return finish();
}

function mask(key) {
  if (!key) return "";
  return key.length > 14 ? key.slice(0, 4) + "..." + key.slice(-4) : "(secret)";
}

// ---- health registry ---------------------------------------------------------

const health = new Map(); // backendId -> {state, nextAvailableAt, fails, backoffMs, lastError}

// The config contract documents rateLimit*/server*/auth* key names; the pre-v3
// docs used rate*/billing* aliases. Honour both, canonical first: an operator
// who sets a documented key and gets the default anyway has a cooldown they
// believe is configured and is not. Each class also carries its own default
// (auth is deliberately slower than server/rate), and fails is clamped so a
// zero-fail call cannot produce a fractional, sub-base backoff.
function backoffFor(kind, cfg, fails) {
  const bp = cfg.backoff || {};
  const token = kind === "rate" ? "rateLimit" : kind === "billing" ? "server" : kind;
  const defaults =
    token === "rateLimit" ? [30000, 600000] :
    token === "auth" ? [60000, 1800000] :
    [30000, 300000];
  const base = bp[token + "BaseMs"] ?? bp[kind + "BaseMs"] ?? defaults[0];
  const max = bp[token + "MaxMs"] ?? bp[kind + "MaxMs"] ?? defaults[1];
  return Math.min(base * 2 ** (Math.max(1, fails) - 1), max);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function markHealthy(id, origin) {
  // Observe the transition OUT of cooling for the dashboard cooling timeline
  // (success / uncool / reset). A backend that was never cooling emits nothing.
  const prev = health.get(id);
  if (prev && prev.state === "cooling") {
    recordCoolingEvent({ backend: id, action: origin || "success", kind: prev.lastError ? prev.lastError.kind : null, manual: !!(prev.lastError && prev.lastError.kind === "manual") });
    ANALYTICS.expiredNotified.delete(id);
  }
  health.set(id, { state: "healthy", nextAvailableAt: 0, fails: 0, backoffMs: 0, lastError: null, generation: nextGeneration(), probeInFlight: false });
}

// Read a model's deployment list as provider-shaped entries. The v3 read OWNS
// the routing semantics now (`backupOnly`, `weight`, `exposed`), so this
// resolves and filters rather than presenting the raw list: the bridge-era
// pass-through deliberately hands a semantically unusable provider into
// `deployments` so the VALIDATOR can name it, and that is not licence for the
// router to build an attempt carrying `model: undefined`. A deployment is
// routable only when it is a plain object, names a KNOWN backend, and resolves
// an upstream - the same three reasons v2's loader dropped a provider, reported
// in the same wording. `backendById` is passed in because the caller already
// holds it (normalizeConfig, candidates) and re-deriving it per model would be
// the only reason this function needs the whole config.
// The backend lookup every shape-aware read needs. Built per call rather than cached: a reload
// swaps the config object, and a cached map would keep serving the previous backend list.
function backendsById(cfg) {
  return new Map((cfg.backends || []).filter((b) => b && typeof b === "object" && b.id).map((b) => [b.id, b]));
}

function modelDeployments(m, id, backendById) {
  const list = Array.isArray(m.deployments) ? m.deployments : Array.isArray(m.providers) ? m.providers : [];
  const out = [];
  list.forEach((d, i) => {
    const at = "deployments[" + i + "]";
    if (!d || typeof d !== "object") {
      console.warn(`config warning: model '${id}': entry at ${at} is not an object, dropped`);
      return;
    }
    if (typeof d.backend !== "string") {
      console.warn(`config warning: model '${id}': entry at ${at} names no backend, dropped`);
      return;
    }
    const b = backendById.get(d.backend);
    if (!b) {
      console.warn(`config warning: model '${id}': entry at ${at} references unknown backend '${d.backend}', dropped`);
      return;
    }
    const upstream = d.upstream ?? b.model;
    if (!upstream) {
      console.warn(`config warning: model '${id}': entry at ${at} (backend '${d.backend}') has no resolvable upstream, dropped`);
      return;
    }
    out.push({ ...d, upstream });
  });
  return out;
}

// v3 primary selection: `backupOnly` deployments are the ordered fallback tail
// and the primary comes from the weighted heads, so a break-glass deployment
// never receives live traffic by hash. Weights that are not integers >= 1 are
// treated as 1 (the validator reports the type; the read must not refuse to
// route over it).
function orderDeployments(deps, sessionKey) {
  const primaries = deps.filter((d) => !d.backupOnly);
  const backups = deps.filter((d) => d.backupOnly);
  if (!primaries.length) return [...deps];
  const weights = primaries.map((d) => (Number.isInteger(d.weight) && d.weight >= 1 ? d.weight : 1));
  const total = weights.reduce((s, w) => s + w, 0);
  const h = hashStr(sessionKey) % total;
  let acc = 0;
  let pick = 0;
  for (let i = 0; i < weights.length; i++) {
    acc += weights[i];
    if (h < acc) {
      pick = i;
      break;
    }
  }
  return [primaries[pick], ...primaries.filter((_, i) => i !== pick), ...backups];
}

// The session-spread width of a model. In v2 it was `affinityPool`; the
// converted form records the same fact as the count of deployments that are
// NOT backupOnly, so a converted config keeps its session spread.
function modelPool(m) {
  if (Number.isInteger(m.affinityPool) && m.affinityPool > 0) return m.affinityPool;
  const deps = Array.isArray(m.deployments) ? m.deployments.filter((d) => d && typeof d === "object") : [];
  const primary = deps.filter((d) => !d.backupOnly).length;
  return primary > 0 ? primary : 1;
}

function ensureBackends(cfg) {
  for (const b of cfg.backends || []) {
    if (!b || typeof b !== "object") continue; // a non-object entry is the validator's to name
    if (!health.has(b.id)) markHealthy(b.id);
  }
}

// The backends a routing/UI path may actually USE. A non-object entry is kept in
// the config so the validator can name `backends.N`, which means every consumer
// that maps the array has to skip it rather than dereference it.
function usableBackends(cfg) {
  return (cfg.backends || []).filter((b) => b && typeof b === "object");
}

// Classify an upstream failure into a backoff "kind".
// The explanation an upstream error body actually carries, or "" when it
// explains nothing. A 400/422 is only attributable to the CALLER when the
// upstream says so: an empty body, whitespace, a message-less JSON object, or a
// blob we cannot parse carries no explanation, so the router cannot tell a
// payload bug from a provider-side rejection.
function upstreamExplanation(bodyText) {
  const text = String(bodyText || "").trim();
  if (!text) return "";
  try {
    const value = JSON.parse(text);
    if (typeof value === "string") return value.trim();
    if (!value || typeof value !== "object") return "";
    const parts = [
      typeof value.error === "string" ? value.error : "",
      value.error && value.error.message,
      value.message,
    ];
    return parts.find((v) => typeof v === "string" && v.trim())?.trim() || "";
  } catch {
    // Unparseable: only a raw prose sentence counts as an explanation. A
    // truncated JSON/HTML fragment is not readable evidence of anything.
    if (text.startsWith("{") || text.startsWith("[") || text.startsWith("<")) return "";
    return text;
  }
}

function classify(status, bodyText, errorBodyState = "present") {
  if (status === 429) {
    if (/GoUsageLimitError/i.test(bodyText || "")) return "weekly";
    return "rate";
  }
  if (status === 401 || status === 403) return "auth";
  if (status >= 500) return "server";
  if (status === 0) return "server"; // network error
  // 402 Payment Required is a dry account by definition, whatever its body says.
  if (status === 402) return "billing";
  if (status === 400 || status === 422) {
    // Billing failures (insufficient credits/quota/balance) are a backend-resource
    // problem, not a payload bug: they must fail over to the next provider
    // instead of aborting the whole request, and they cool the dry account.
    // "insufficient balance" is DeepSeek's direct-API wording for exactly this.
    if (/insufficient (credits|quota|balance|funds)|insufficient_quota|insufficient_balance|out of credits|no credits|billing error|payment required/i.test(bodyText || "")) return "billing";
    // Explained = the upstream blames the payload, so the backend is fine and
    // the request stops. Unexplained = "upstream_rejection": no evidence the
    // payload was at fault, so fail over instead of aborting (2026-10-07: an
    // OpenCode Go 400 with an empty body 503'd every affected session after a
    // single attempt). An incomplete read can never prove the caller was at
    // fault, so only a complete body earns the client classification.
    const explained = upstreamExplanation(bodyText);
    if (explained && errorBodyState === "present") return "client";
    return "upstream_rejection";
  }
  return "server";
}

// Whether a failure is safe to RETRY on the same provider. Distinct from the
// cooling classification: some failures are retryable (transient overload,
// 5xx, network blip) without being a reason to abandon the provider for
// everyone, and some cooling failures (429 weekly, auth) must never be retried
// because retrying cannot succeed and only wastes the retry budget.
function isRetryableError(status, bodyText, errorBodyState = "present") {
  const kind = classify(status, bodyText, errorBodyState);
  if (kind === "weekly" || kind === "auth" || kind === "client") return false;
  // An unexplained rejection has no evidence that retrying the SAME provider
  // helps - the identical payload is sent again - so fail over instead.
  if (kind === "upstream_rejection") return false;
  if (status === 429) return /overload/i.test(bodyText || "");
  if (status === 502 || status === 503 || status === 504) return true;
  if (status === 0) return true; // network error
  return false;
}

// ---- empty-completion detection --------------------------------------------

// Whether a streaming result is "empty": status 200 but no usable assistant
// output was produced. A legitimately tiny reply ("OK") has content and is
// NOT empty; a tool_calls-only response is NOT empty.
function isEmptyStreamResult({ hasContent, hasReasoning, hasToolCalls, completionTokens }) {
  if (hasContent || hasReasoning || hasToolCalls) return false;
  const ct = completionTokens == null ? 0 : Number(completionTokens) || 0;
  return ct <= 1;
}

function hasUsableContentNonStream(parsed) {
  if (!parsed || typeof parsed !== 'object') return false;
  const choices = parsed.choices;
  if (!Array.isArray(choices) || choices.length === 0) return false;
  for (const ch of choices) {
    const msg = (ch && (ch.message || ch.delta)) || {};
    if (msg.content != null && String(msg.content).trim() !== '') return true;
    if (msg.reasoning != null && String(msg.reasoning).trim() !== '') return true;
    if (msg.reasoning_content != null && String(msg.reasoning_content).trim() !== '') return true;
    if (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) return true;
    // also check choice-level tool_calls alias
    if (Array.isArray(ch.tool_calls) && ch.tool_calls.length > 0) return true;
  }
  return false;
}

function isEmptyNonStreamResult(parsed, completionTokens, isJson) {
  if (!isJson) return false;
  if (hasUsableContentNonStream(parsed)) return false;
  const ct = completionTokens == null ? 0 : Number(completionTokens) || 0;
  return ct <= 1;
}

function markFailure(cfg, id, status, bodyText) {
  const h = health.get(id) || { state: "healthy", fails: 0, backoffMs: 0 };
  const kind = classify(status, bodyText);
  // Neither a genuine client error nor an unexplained rejection is evidence that
  // the ACCOUNT is unhealthy, so neither cools it: record the last error for
  // visibility and keep the backend healthy. The opaque case additionally gets a
  // session-scoped exclusion, which is likewise not account health.
  if (kind === "client" || kind === "upstream_rejection") {
    h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
    h.generation = nextGeneration();
    health.set(id, h);
    return;
  }
  h.fails = (h.fails || 0) + 1;
  // Only a transition INTO cooling lands on the dashboard timeline; a re-cool
  // of an already-active cooling (the all-cooling fallback retrying the
  // soonest backend) is not a new failure episode.
  const wasCoolingActive = h.state === "cooling" && h.nextAvailableAt > Date.now();
  let nextMs;
  if (kind === "weekly") {
    // Parse "Resets in N days" if present, else full week.
    const m = /resets in (\d+)\s+day/i.exec(bodyText || "");
    const days = m ? parseInt(m[1], 10) : 7;
    nextMs = cfg.backoff.weeklyDefaultMs || days * 24 * 60 * 60 * 1000;
    h.fails = 100; // effectively pinned until reset
    fireAlert(cfg, "weekly_limit", { backend: id, days: m ? parseInt(m[1], 10) : null });
  } else {
    h.backoffMs = backoffFor(kind, cfg, h.fails);
    nextMs = h.backoffMs;
  }
  h.state = "cooling";
  h.nextAvailableAt = Date.now() + nextMs;
  h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
  h.generation = nextGeneration();
  h.probeInFlight = false;
  health.set(id, h);
  if (!wasCoolingActive) recordCoolingEvent({ backend: id, action: "cool", kind, manual: false });
}

function markSuccess(id, attempt) {
  const h = health.get(id);
  // A manual cool is a user decision: a success must NEVER clear it. Only the
  // Uncool button (or expiry of a timed manual cool) restores the backend.
  if (h && h.state === "cooling" && h.lastError && h.lastError.kind === "manual") return;
  // A usable completion is current proof that the ACCOUNT is answering, so it retires the
  // distinct-route outage evidence that would otherwise escalate on the next unrelated
  // failure. Only a CURRENT completion may do it: one that owns the account row and is not
  // older than a failure already retained. A slow response that started before a later
  // failure proves nothing about the state that failure recorded, and honouring it would
  // abolish the very escalation it was meant to feed.
  const ev = ACCOUNT_OUTAGE_ROUTES.get(id);
  if (ev) {
    const newestSeq = ev.reduce((m, e) => (e.dispatchSeq != null && e.dispatchSeq > m ? e.dispatchSeq : m), -Infinity);
    const current = !!attempt && attempt.dispatchSeq != null && ownsAccountSettlement(attempt) && !(newestSeq > attempt.dispatchSeq);
    if (current) ACCOUNT_OUTAGE_ROUTES.delete(id);
  }
  markHealthy(id);
}

function resetAllHealth() {
  // A reset is an operator override, so it must clear EVERY layer that can hold a
  // request out of a provider: the account rows, the route recoveries, the
  // account-outage escalation window, and this run's session preferences. A reset
  // that left routes cooling would report "healthy" and still refuse to route.
  clearSessionPreferences();
  // Fence every route with an attempt still in flight before clearing: a reset is an
  // operator override, and a late failure from work that started before it must not
  // recreate the row the override just cleared.
  for (const key of [...ATTEMPTS_IN_FLIGHT.keys()]) fenceRouteDeletion(key);
  ROUTES.clear();
  MANUAL_ROUTES.clear(); // an operator block on one route is a layer too
  SESSION_EXCLUSIONS.clear();
  ACCOUNT_OUTAGE_ROUTES.clear();
  for (const id of health.keys()) markHealthy(id, "reset");
}
// dashboard needs to explain WHY a backend was down: failure cool (with the
// classified kind), lazy expiry at request time, success clearing, admin
// uncool, and reset-health. Newest first, capped at 500 rows.

const COOLING_EVENTS = []; // newest at head: {t, backend, action, kind, manual}
const COOLING_EVENT_CAP = 500;

function recordCoolingEvent({ backend, action, kind, manual, scope }) {
  COOLING_EVENTS.unshift({ t: Date.now(), backend, action, kind: kind || null, manual: !!manual, scope: scope || "account" });
  if (COOLING_EVENTS.length > COOLING_EVENT_CAP) COOLING_EVENTS.length = COOLING_EVENT_CAP;
}

// Request-time usability check that doubles as the lazy-expiry observer: a
// cooling backend whose timer elapsed is immediately usable again, and that
// elapsed moment lands on the cooling timeline once per expiry (later success /
// uncool / reset events cover the rest of the lifecycle).
function isUsableCurrently(id, now) {
  const h = health.get(id);
  if (!h || h.state !== "cooling") return true;
  if (h.nextAvailableAt <= now) {
    if (!ANALYTICS.expiredNotified.has(id)) {
      ANALYTICS.expiredNotified.add(id);
      recordCoolingEvent({ backend: id, action: "expire", kind: h.lastError ? h.lastError.kind : null, manual: !!(h.lastError && h.lastError.kind === "manual") });
    }
    return true;
  }
  return false;
}

// ---- scoped cooldowns: route health + session exclusions ---------------------
//
// A ROUTE is one physical (backend, upstream) pair. Cooling is scoped to the
// route, so one upstream of an account failing does not disable that account's
// other upstreams, while account-wide faults (auth, billing, weekly limit, rate
// limit, network) still cool the account itself. An unexplained rejection is
// evidence of neither, so it excludes the route for the session that saw it and
// nothing else.
const ROUTES = new Map(); // routeKey -> {generation, fails, nextAvailableAt, probeInFlight, lastErrorKind, lastStatus, lastError}
const SESSION_EXCLUSIONS = new Map(); // "sessionKey routeKey" -> {blockedUntil, generation}
const OPAQUE_EXCLUSION_MS = 30000;

// Operator blocks on ONE physical route (the targeted half of /admin/backend). Kept OUT
// of the ROUTES row on purpose: a row is failure evidence and its streak has to survive
// an uncool, so an operator decision cannot live in the same fields a late failure writes.
// A sticky block carries MAX_SAFE_INTEGER, exactly like the account-level one, so /health
// renders "until uncooled" rather than a countdown.
const MANUAL_ROUTES = new Map(); // routeKey -> {until, since}

// Deletion fences. A route that is deleted while an attempt is still in flight - a config
// reload that dropped it, a targeted uncool, a reset - must not come back when that attempt
// settles: the late callback writes failure evidence for a route the operator or the config
// already removed, and the next /health then reports a route nothing routes to.
//
// The fence snapshots the attempts that were in flight WHEN IT WAS ARMED, and blocks their
// settlements. Identity, not a timestamp: an attempt is exactly the work that was running at
// that moment, while any counter or sequence comparison is a proxy that a dispatch happening
// concurrently with the arming can invalidate (a request that starts a hair after the uncool
// shares no property with the old one that distinguishes them). An attempt that starts LATER
// is not in the snapshot and settles normally, which is what keeps live evidence - a provider
// a new request really did find broken - from being swallowed.
//
// The fence outlives the last release. The window being protected is "until the route is live
// again", not "until the old attempt returns", and a settlement can land after every lease is
// back while the response is still relaying. It is dropped the moment the config declares the
// route again (unfenceRoute), which is the only moment the tombstone stops being true.
const ROUTE_FENCES = new Map(); // routeKey -> {atArm: Set<token>, inFlight}
const ATTEMPTS_IN_FLIGHT = new Map(); // routeKey -> Set<token>

function fenceFor(key) {
  let f = ROUTE_FENCES.get(key);
  if (!f) { f = { atArm: new Set(), inFlight: 0 }; ROUTE_FENCES.set(key, f); }
  return f;
}

function fenceRouteDeletion(key) {
  if (ROUTE_FENCES.has(key)) return; // already a tombstone; the snapshot is the first one
  const f = fenceFor(key);
  f.atArm = new Set(ATTEMPTS_IN_FLIGHT.get(key) || []);
}

// True when this attempt was already running at the moment the route was deleted. Its result
// describes a route that no longer exists, so the row it would write is not evidence about
// anything the config declares now.
function fenceBlocks(key, attempt) {
  const f = ROUTE_FENCES.get(key);
  if (!f) return false;
  const tok = attempt && attempt.fenceToken;
  return !!(tok && f.atArm.has(tok));
}

// A config change that re-declares a route is a NEW incarnation: the fence was about the old
// one, and keeping it would silently refuse the route the operator just configured.
function unfenceRoute(key) {
  ROUTE_FENCES.delete(key);
}

function noteDispatchForFence(token) {
  const key = token.routeKey;
  if (!key) return;
  if (!ATTEMPTS_IN_FLIGHT.has(key)) ATTEMPTS_IN_FLIGHT.set(key, new Set());
  ATTEMPTS_IN_FLIGHT.get(key).add(token);
  // A fence is NOT created here. Most routes are never deleted, and allocating a tombstone
  // per dispatch would grow the map with the traffic rather than with the deletions.
  const f = ROUTE_FENCES.get(key);
  if (f) f.inFlight++;
}

function noteReleaseForFence(token) {
  const key = token.routeKey;
  const set = key ? ATTEMPTS_IN_FLIGHT.get(key) : null;
  if (set) { set.delete(token); if (!set.size) ATTEMPTS_IN_FLIGHT.delete(key); }
  const f = key ? ROUTE_FENCES.get(key) : null;
  if (!f) return;
  // The counter tracks how many leases are out; it does NOT decide when the fence goes. A
  // tombstone stays until the config re-declares the route.
  f.inFlight = Math.max(0, f.inFlight - 1);
}

function routeFenceStats() {
  return { fences: ROUTE_FENCES.size, inFlightRoutes: ATTEMPTS_IN_FLIGHT.size };
}

function manualRouteBlock(key, now) {
  const m = MANUAL_ROUTES.get(key);
  if (!m) return null;
  if (m.until > now) return m;
  // Lazy expiry, like every other deadline here: a timed operator block is dropped on
  // sight rather than swept, so a dead entry can never keep blocking a route.
  MANUAL_ROUTES.delete(key);
  return null;
}

// A route the operator cooled is refused for EVERY caller whatever its own streak says,
// and it has no automatic recovery - which is what makes it the strongest route blocker.
function manualRouteBlocked(attempt, now = Date.now()) {
  return !!manualRouteBlock(routeKey(attempt), now);
}

// Monotonic across the whole process and NEVER reset - not by reset-health, not by
// a config reload, not by a route being removed and re-added. A generation is how a
// late callback proves the state it captured is still the state it would write to.
let stateGeneration = 0;
function nextGeneration() {
  return ++stateGeneration;
}

function routeKey(attempt) {
  return JSON.stringify([attempt.backend, attempt.upstream]);
}

function routeBackoffMs(fails, cfg) {
  return backoffFor("server", cfg, Math.max(1, fails));
}

// The opaque-rejection scope: WHO reported it (the authenticated credential, as a
// non-reversible fingerprint) plus the session that caller claims. Hashed so no raw
// credential or session id ever becomes a map key a diagnostic could print, and so
// two clients that happen to send the same x-session-affinity cannot see or clear
// each other's exclusions - the spec's "different authentication scopes with
// identical session IDs must not share state".
function sessionScopeOf(authScope, sessionKey) {
  return crypto.createHash("sha256").update(JSON.stringify([authScope, sessionKey || "no-session"])).digest("hex");
}

// An opaque rejection excludes ONE physical route for ONE caller/session/resolved
// model. The resolved logical model is part of the key on purpose: two logical models
// may alias the same physical route, and a blank 400 returned for model A is not
// evidence that model B will be refused too. Route HEALTH stays shared across models
// (zScope1/zScope2) - only this caller-scoped exclusion narrows.
function sessionExclusionKey(exclScope, attempt) {
  return crypto.createHash("sha256").update(JSON.stringify([exclScope, attempt.model, attempt.backend, attempt.upstream])).digest("hex");
}

// Lazy expiry: an elapsed exclusion is dropped on sight rather than swept.
function isSessionExcluded(exclScope, attempt, now) {
  const key = sessionExclusionKey(exclScope, attempt);
  const e = SESSION_EXCLUSIONS.get(key);
  if (!e) return false;
  if (e.blockedUntil <= now) {
    SESSION_EXCLUSIONS.delete(key);
    return false;
  }
  return true;
}

// A server-class failure is a property of the ROUTE, not the account: the streak is
// preserved across timer expiry (only a real success resets it), so a flapping
// route backs off further each time instead of restarting at the base delay.
function markRouteFailure(cfg, attempt, status, bodyText, kind) {
  const key = routeKey(attempt);
  // Deleted mid-flight: this failure describes a route that no longer exists, and writing
  // it would recreate the row the deletion just removed (see ROUTE_FENCES).
  if (fenceBlocks(key, attempt)) return null;
  const r = ROUTES.get(key) || { generation: nextGeneration(), fails: 0, nextAvailableAt: 0, probeInFlight: false, lastErrorKind: null, lastStatus: null, lastError: null };
  r.fails = (r.fails || 0) + 1;
  r.nextAvailableAt = Date.now() + routeBackoffMs(r.fails, cfg);
  r.lastErrorKind = kind;
  r.lastStatus = status;
  r.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
  r.generation = nextGeneration();
  r.probeInFlight = false;
  ROUTES.set(key, r);
  recordCoolingEvent({ backend: attempt.backend, action: "cool", kind, manual: false, scope: "route" });
  // Only a real HTTP 5xx is evidence about the ACCOUNT. A relay cut, a stalled or empty
  // 200 cools this route - the relay really is unfit - but it says nothing about the
  // account's other routes, so feeding it to the distinct-route heuristic declares an
  // account-wide outage on evidence that never described the account.
  if (status >= 500 && status <= 599) noteAccountOutage(cfg, attempt);
  return r;
}

// Called only for a genuinely usable completion, never for mere HTTP 200 headers.
function markRouteSuccess(attempt) {
  const key = routeKey(attempt);
  const r = ROUTES.get(key);
  if (!r) return;
  r.fails = 0;
  r.nextAvailableAt = 0;
  r.probeInFlight = false;
  r.generation = nextGeneration();
}

// Settlement ownership. Every attempt books itself as the current writer of its route and
// account rows at dispatch (see the chat loop); only the newest booking may write health.
// Without this an attempt that started earlier but finished later resurrects stale state:
// review 2026-10-08 verified a held success clearing a cooldown a newer request had just
// recorded (route fails 1 -> 0, account cooling -> healthy). A row with no booking (a
// config load, a manual cool, a reset) is always writable, so operator actions still win.
function ownsRouteSettlement(attempt) {
  if (!attempt || attempt.dispatchSeq == null) return true;
  const r = ROUTES.get(routeKey(attempt));
  if (!r) return true; // nothing written yet: this settlement creates the row
  if (r.dispatchSeq === attempt.dispatchSeq) return true; // this attempt's own booking
  // A row generation past our booking was written after we dispatched, so it is newer
  // evidence than ours. This also covers the row being CREATED by a newer attempt, which
  // is the common shape of the race: the cooldown row simply did not exist when we left.
  return (r.generation || 0) <= attempt.dispatchSeq;
}
function ownsAccountSettlement(attempt) {
  if (!attempt || attempt.dispatchSeq == null) return true;
  const h = health.get(attempt.backend);
  if (!h) return true;
  if (h.dispatchSeq === attempt.dispatchSeq) return true;
  return (h.generation || 0) <= attempt.dispatchSeq;
}

// An opaque rejection excludes the route for the CALLER that saw it, for that
// caller's session and that resolved model. Requests with no session id get a
// client-scoped "no-session" bucket rather than no memory at all: without one, the
// same anonymous caller re-offers the refusing provider on every single call.
function excludeRouteForSession(exclScope, attempt) {
  const key = sessionExclusionKey(exclScope, attempt);
  SESSION_EXCLUSIONS.set(key, { blockedUntil: Date.now() + OPAQUE_EXCLUSION_MS, generation: nextGeneration(), routeKey: routeKey(attempt) });
  return key;
}

// A probe the LEASE ITSELF holds, at the generation it claimed, is not a blocker: the
// dispatch IS that probe. Anything else is - another request's probe, or a newer probe that
// replaced the row this lease claimed. Holding one kind of probe is never permission for the
// other: owning a route probe does not admit a request into an account that a different
// request is already probing.
function leaseHolds(lease, kind, row) {
  if (!lease) return false;
  if (kind === "account") return !!lease.accountClaim && (row.generation ?? null) === lease.accountGeneration;
  return !!lease.routeClaim && row.generation === lease.routeGeneration;
}

// Why a candidate cannot be tried right now, or null when it can. `lease` exempts only the
// probes this dispatch already owns; callers with no lease get the strict answer.
function ineligibility(attempt, exclScope, now, lease = null) {
  const h = health.get(attempt.backend);
  if (h && h.state === "cooling") {
    if (!isUsableCurrently(attempt.backend, now)) {
      return h.lastError && h.lastError.kind === "manual" ? "manual" : "account_cooling";
    }
    // The timer elapsed but the recovery is unproven: exactly one probe at a time,
    // so a still-broken account is discovered by one request rather than by every
    // request at once.
    if ((h.fails || 0) > 0 && h.probeInFlight && !leaseHolds(lease, "account", h)) return "probe_in_flight";
  }
  // The operator's route block outranks the route's own streak and every caller-scoped
  // answer: a manual cool means "do not use this route", and a sticky one never expires.
  if (manualRouteBlocked(attempt, now)) return "manual";
  const r = ROUTES.get(routeKey(attempt));
  if (r && (r.fails || 0) > 0) {
    if (r.nextAvailableAt > now) return "route_cooling";
    if (r.probeInFlight && !leaseHolds(lease, "route", r)) return "probe_in_flight";
  }
  if (isSessionExcluded(exclScope, attempt, now)) return "session_excluded";
  return null;
}

// Nearest finite moment at which some blocked candidate returns, or null when
// nothing recovers on its own (every blocker is a manual cool).
function nearestRecoveryMs(ordered, exclScope, now) {
  let best = null;
  for (const a of ordered) {
    const why = ineligibility(a, exclScope, now);
    // "manual" covers both the account-level sticky cool and a targeted route block:
    // neither recovers on its own, so advertising a deadline for it would be a guess.
    if (!why || why === "manual") continue;
    const h = health.get(a.backend);
    const r = ROUTES.get(routeKey(a));
    const at = [];
    if (why === "account_cooling" && h) at.push(h.nextAvailableAt);
    if (why === "route_cooling" && r) at.push(r.nextAvailableAt);
    if (why === "session_excluded") {
      const e = SESSION_EXCLUSIONS.get(sessionExclusionKey(exclScope, a));
      if (e) at.push(e.blockedUntil);
    }
    // Nothing recovers on its own while a probe is running, but it will settle
    // shortly: advertise the shortest honest wait rather than no header at all.
    if (why === "probe_in_flight") at.push(now + 1000);
    for (const t of at) if (t > now && (best === null || t < best)) best = t;
  }
  return best === null ? null : best - now;
}

// Single dispatch point for an attempt's failure, so the three scopes cannot drift
// apart: caller's fault changes nothing, an opaque rejection excludes the route for
// this session, a server fault cools the route, and everything else cools the account.
function noteAttemptFailure(cfg, attempt, exclScope, status, bodyText, errorBodyState = "present", stage = null) {
  // The settle path must classify from the SAME body-read state the terminal surface used,
  // or one request reports an opaque rejection and settles as a client error, writing no
  // exclusion (review finding 14). The ownership guards keep a late settlement from
  // resurrecting health that a newer attempt already replaced.
  const kind = classify(status, bodyText, errorBodyState);
  if (kind === "client") return kind;
  if (kind === "upstream_rejection") {
    excludeRouteForSession(exclScope, attempt);
    if (ownsAccountSettlement(attempt)) {
      const h = health.get(attempt.backend) || { state: "healthy", fails: 0, backoffMs: 0 };
      h.lastError = { status, kind, body: (bodyText || "").slice(0, 300) };
      health.set(attempt.backend, h);
    }
    return kind;
  }
  if (kind === "server") {
    // A relay cut or an unusable 200 is a property of the ROUTE: the relay answered and
    // then failed, so its siblings on the same account are untouched. A failure BEFORE any
    // headers (stage "connect") is a property of the ACCOUNT - the endpoint itself was
    // unreachable - and charging a single route would make every sibling route rediscover
    // the same dead endpoint one request at a time.
    if (stage === "connect") {
      if (ownsAccountSettlement(attempt)) markFailure(cfg, attempt.backend, status, bodyText);
      return kind;
    }
    if (ownsRouteSettlement(attempt)) markRouteFailure(cfg, attempt, status, bodyText, kind);
    return kind;
  }
  if (ownsAccountSettlement(attempt)) markFailure(cfg, attempt.backend, status, bodyText);
  return kind;
}

// A recovery lease, taken synchronously BEFORE any network await, so two concurrent
// requests can never probe the same recovering account or route together; the loser
// moves on to its next candidate instead. The token records the generation it
// claimed: a late callback whose generation no longer matches must not reopen state
// that has since been replaced.
function claimAttempt(attempt, now) {
  const token = { routeKey: routeKey(attempt), backend: attempt.backend, routeGeneration: null, routeClaim: false, accountGeneration: null, accountClaim: false };
  const h = health.get(attempt.backend);
  if (h && h.state === "cooling" && h.nextAvailableAt <= now && (h.fails || 0) > 0) {
    if (h.probeInFlight) return null;
    h.probeInFlight = true;
    token.accountClaim = true;
    token.accountGeneration = h.generation ?? null;
  }
  const r = ROUTES.get(token.routeKey);
  if (r && (r.fails || 0) > 0 && r.nextAvailableAt <= now) {
    if (r.probeInFlight) {
      releaseAttempt(token);
      return null;
    }
    r.probeInFlight = true;
    token.routeClaim = true;
    token.routeGeneration = r.generation;
  }
  // Booked with the deletion fence BEFORE any await: a route deleted while this attempt
  // is in flight keeps a fence until this token is released, so the late settlement cannot
  // resurrect a route the config or the operator already removed.
  token.fenceNoted = true;
  token.attempt = attempt;
  attempt.fenceToken = token; // identity for the deletion fence (see fenceBlocks)
  noteDispatchForFence(token);
  return token;
}

// Frees only what this token still owns. A probe that already settled moved the
// generation on (and cleared the flag itself), so that newer state is left alone.
function releaseAttempt(token) {
  // Released at most ONCE. Generation checking alone is not enough to make this idempotent:
  // claiming a probe does not move a row's generation, so a repeated release of the same
  // token would still match and would free the probe a LATER request has since claimed,
  // putting two probes on one route.
  if (!token || token.released) return;
  token.released = true;
  if (token.fenceNoted) noteReleaseForFence(token);
  const r = ROUTES.get(token.routeKey);
  if (token.routeClaim && r && r.generation === token.routeGeneration) r.probeInFlight = false;
  const h = health.get(token.backend);
  if (token.accountClaim && h && (h.generation ?? null) === token.accountGeneration) h.probeInFlight = false;
}

// One failing upstream is a route problem. Two DIFFERENT routes of the same account
// failing inside a minute is an account-level outage, so the account is cooled too.
// Only server-class failures reach here, so an unexplained rejection or a caller's
// bad payload can never escalate.
const ACCOUNT_OUTAGE_WINDOW_MS = 60000;
const ACCOUNT_OUTAGE_ROUTES = new Map(); // backendId -> [{routeKey, t}]
function noteAccountOutage(cfg, attempt, now = Date.now()) {
  const key = routeKey(attempt);
  const recent = (ACCOUNT_OUTAGE_ROUTES.get(attempt.backend) || []).filter((e) => now - e.t <= ACCOUNT_OUTAGE_WINDOW_MS);
  const prior = recent.find((e) => e.routeKey === key);
  // A route that fails AGAIN is fresh evidence: refresh its timestamp and booking rather
  // than ignoring the repeat, so the window closes 60s after each route's LATEST failure.
  // Counting only first failures let a route that had just failed twice age out of the
  // account's evidence while it was still the most obviously broken route on it.
  if (prior) {
    prior.t = now;
    if (attempt.dispatchSeq != null) prior.dispatchSeq = attempt.dispatchSeq;
  } else {
    recent.push({ routeKey: key, t: now, dispatchSeq: attempt.dispatchSeq == null ? null : attempt.dispatchSeq });
  }
  ACCOUNT_OUTAGE_ROUTES.set(attempt.backend, recent);
  // Escalating writes the ACCOUNT, so only the attempt that currently owns the account
  // row may do it: a straggler finishing late must not cool an account over a newer state
  // a later request already established (the same rule the other account writes follow).
  if (recent.length >= 2 && ownsAccountSettlement(attempt)) {
    markFailure(cfg, attempt.backend, 503, "multiple routes of this account failed inside a minute: account-level outage");
  }
}

// A reload must not silently forgive a cooling route, so unchanged rows survive and
// only routes the new config no longer references are dropped.
function pruneRoutes(cfg) {
  const live = new Set();
  // Through the adapter, never `m.providers`: a legacy entry is converted in memory at load
  // (providers is replaced by deployments), so reading the raw array here would find NOTHING
  // live and a reload would drop every cooling route, session exclusion and pin.
  const byId = backendsById(cfg);
  for (const [id, m] of Object.entries(cfg.models || {})) {
    for (const p of modelDeployments(m, id, byId)) live.add(JSON.stringify([p.backend, p.upstream]));
  }
  for (const key of [...ROUTES.keys()]) if (!live.has(key)) { fenceRouteDeletion(key); ROUTES.delete(key); }
  for (const key of [...MANUAL_ROUTES.keys()]) if (!live.has(key)) MANUAL_ROUTES.delete(key);
  // A route the new config declares again is a fresh incarnation and must not inherit the
  // old one's deletion fence.
  for (const key of [...ROUTE_FENCES.keys()]) if (live.has(key)) unfenceRoute(key);
  for (const [key, e] of [...SESSION_EXCLUSIONS]) if (!live.has(e.routeKey)) SESSION_EXCLUSIONS.delete(key);
  for (const [key, row] of [...SESSION_PREFS]) if (!live.has(row.routeKey)) SESSION_PREFS.delete(key);
  for (const id of [...ACCOUNT_OUTAGE_ROUTES.keys()]) {
    if (!cfg.backends.some((b) => b.id === id)) ACCOUNT_OUTAGE_ROUTES.delete(id);
  }
}

// Route-scoped health for the dashboard: which logical models use the route, how
// long until it may be retried, its failure streak, and probe state. Session
// exclusions are deliberately NOT exposed - they are per-caller state and can carry
// session identifiers.
function routeHealthSnapshot(cfg, now) {
  const modelsByRoute = new Map();
  // Same adapter as pruneRoutes: a route's logical models must be reported for BOTH shapes, or
  // every converted (v3) model shows up as a nameless route row on the dashboard.
  const byId = backendsById(cfg);
  for (const [id, m] of Object.entries(cfg.models || {})) {
    for (const p of modelDeployments(m, id, byId)) {
      const key = JSON.stringify([p.backend, p.upstream]);
      if (!modelsByRoute.has(key)) modelsByRoute.set(key, []);
      modelsByRoute.get(key).push(id);
    }
  }
  const rows = [];
  // Every CONFIGURED route gets a row, whether or not the registry has ever heard of it.
  // A route with no history is not a route that does not exist: the operator configured
  // it, and /health is where a deployment's state is read before anything has failed.
  // Registry rows cannot be built first and merged later, because the seeded ones have to
  // win on the fields the registry owns.
  // The union of configured routes and live registry rows. ROUTES only ever holds routes the
  // current config declares (pruneRoutes drops the rest), so a registry key missing from
  // modelsByRoute is a route with no model reference - still live, still listed.
  const allKeys = [...modelsByRoute.keys()];
  for (const key of ROUTES.keys()) if (!modelsByRoute.has(key)) allKeys.push(key);
  for (const key of allKeys) {
    const [backend, upstream] = JSON.parse(key);
    const r = ROUTES.get(key) || null;
    const cooling = !!r && (r.nextAvailableAt || 0) > now;
    // An operator block is reported with lastErrorKind "manual" and no deadline for a
    // sticky one, so the dashboard renders the "uncool to restore" affordance instead of
    // counting down to a recovery that will never come.
    const manual = manualRouteBlock(key, now);
    rows.push({
      route: key,
      id: backend, // the v3 field name for "which backend this deployment belongs to"
      backend,
      upstream,
      models: modelsByRoute.get(key) || [],
      state: manual || cooling ? "cooling" : r && (r.fails || 0) > 0 ? "probing" : "healthy",
      manual: !!manual,
      fails: r ? (r.fails || 0) : 0,
      nextAvailableAtMs: manual ? manual.until : (r ? (r.nextAvailableAt || 0) : 0),
      retryInMs: manual ? (manual.until === Number.MAX_SAFE_INTEGER ? 0 : Math.max(0, manual.until - now)) : cooling ? r.nextAvailableAt - now : 0,
      probeInFlight: !!(r && r.probeInFlight),
      lastErrorKind: manual ? "manual" : (r && r.lastErrorKind) || null,
      lastStatus: manual ? null : (r ? (r.lastStatus ?? null) : null),
    });
  }
  // A registry row for a route the CURRENT config no longer declares is deliberately not
  // listed: it is deleted (and fenced) by pruneRoutes, and a stale row here would make a
  // removed route look routable.
  return rows;
}

// ---- session provider preference -------------------------------------------

// A session served by a fallback should stay there: the prompt cache lost by moving
// once is far cheaper than re-discovering a dead provider on every turn. A pin is
// keyed by (credential, session, logical model), so two callers sharing a session id
// never share state, and a pin can only ever reorder providers INSIDE the model group
// it was learned in.
const SESSION_PREF_CAP = 10000;
const SESSION_PREFS = new Map(); // prefKey -> {routeKey, latestRequestSequence, lastUsedAt}
const SESSION_PREF_STATS = { published: 0, rejectedStale: 0, rejectedIneligible: 0, evictions: 0, cleared: 0 };
let requestSequence = 0; // process-wide monotonic allocator

function nextRequestSequence() {
  return ++requestSequence;
}

// Authentication scope: a short, non-reversible fingerprint of the credential the
// request carried, so distinct keys never share a pin. The raw credential is never
// retained, printed, or persisted.
function authScopeOf(req) {
  const m = /^Bearer\s+(.+)$/i.exec((req && req.headers && req.headers["authorization"]) || "");
  if (!m || !m[1].trim()) return "anonymous";
  return crypto.createHash("sha256").update(m[1].trim()).digest("hex").slice(0, 16);
}

// 'no-session' is a shared bucket, so pinning it would make every anonymous caller
// follow whichever one succeeded last. Those callers get no pin at all.
function sessionPreferenceKey(authScope, sessionKey, logicalModel) {
  if (!sessionKey || sessionKey === "no-session") return null;
  return crypto.createHash("sha256").update(JSON.stringify([authScope, sessionKey, logicalModel])).digest("hex");
}

// The token carries a sequence number, so a slow response that STARTED before a newer
// decision cannot overwrite it when it finally lands. One request builds one token per
// RESOLVED model and hands them all the SAME sequence: the decision they race against
// is the request's, not each other's.
function beginSessionRequest(authScope, sessionKey, logicalModel, sequence) {
  const key = sessionPreferenceKey(authScope, sessionKey, logicalModel);
  if (!key) return null;
  return {
    key,
    sequence: sequence == null ? nextRequestSequence() : sequence,
    sessionKey,
    logicalModel,
    exclScope: sessionScopeOf(authScope, sessionKey),
  };
}

// Promote a pinned route to the FRONT OF ITS OWN MODEL GROUP only. Models are never
// reordered and backend dedup already happened, so a promotion cannot resurrect a
// provider this request is deliberately not allowed to repeat.
function orderSessionProviders(ordered, token, now = Date.now()) {
  if (!token || !ordered.length) return ordered;
  const row = SESSION_PREFS.get(token.key);
  if (!row) return ordered;
  const out = [];
  for (let i = 0; i < ordered.length; ) {
    let j = i;
    while (j < ordered.length && ordered[j].model === ordered[i].model) j++;
    const group = ordered.slice(i, j);
    // A token belongs to ONE resolved model. Promoting inside another model's group
    // would move a route this caller never pinned there, so every other group passes
    // through untouched (zSess4j).
    if (group[0].model !== token.logicalModel) {
      out.push(...group);
      i = j;
      continue;
    }
    const idx = group.findIndex((a) => routeKey(a) === row.routeKey);
    // A pinned route that cannot be used right now must not be promoted into a
    // doomed attempt; leave the group in its configured order.
    if (idx > 0 && ineligibility(group[idx], token.exclScope, now) === null) {
      out.push(group[idx], ...group.slice(0, idx), ...group.slice(idx + 1));
    } else {
      out.push(...group);
    }
    i = j;
  }
  touchSessionPreference(token.key, now);
  return out;
}

// Reading a pin is USING it, so it moves to the MRU end: that is what makes the cap evict
// the least recently USED pin rather than the least recently written one (review finding
// 13 -- a session that came back every day could be evicted by one that ran once).
function touchSessionPreference(key, now) {
  const row = SESSION_PREFS.get(key);
  if (!row) return false;
  row.lastUsedAt = now == null ? Date.now() : now;
  SESSION_PREFS.delete(key);
  SESSION_PREFS.set(key, row);
  return true;
}

// The cap is a parameter so eviction order can be pinned by a unit test.
function evictSessionPreferences(cap = SESSION_PREF_CAP) {
  let evicted = 0;
  while (SESSION_PREFS.size > cap) {
    const oldest = SESSION_PREFS.keys().next().value;
    SESSION_PREFS.delete(oldest);
    SESSION_PREF_STATS.evictions++;
    evicted++;
  }
  return evicted;
}

// Published only by a request that actually produced usable output, and only while
// the route is still usable; a route this request just cooled is not a preference.
function rememberProvider(token, attempt, completion) {
  if (!token || !attempt || !completion || !completion.usable) return false;
  const now = Date.now();
  if (ineligibility(attempt, token.exclScope, now) !== null) {
    SESSION_PREF_STATS.rejectedIneligible++;
    return false;
  }
  const prev = SESSION_PREFS.get(token.key);
  if (prev && prev.latestRequestSequence > token.sequence) {
    SESSION_PREF_STATS.rejectedStale++;
    return false;
  }
  if (prev) SESSION_PREFS.delete(token.key); // re-insert: Map order is the LRU order
  SESSION_PREFS.set(token.key, { routeKey: routeKey(attempt), latestRequestSequence: token.sequence, lastUsedAt: now });
  SESSION_PREF_STATS.published++;
  // A healthy pin has NO idle TTL: coming back tomorrow should still land on the
  // provider that worked. The bound is size, not age.
  evictSessionPreferences();
  return true;
}

// Counts and sizes only: never a session key, never a pin's identity.
// An operator health action (manual cool/uncool, reset) is an explicit routing
// decision and must not be silently overruled by a session's learned preference, so
// the preference table is dropped and re-learned from the next successful call.
function clearSessionPreferences() {
  if (SESSION_PREFS.size) SESSION_PREF_STATS.cleared += SESSION_PREFS.size;
  SESSION_PREFS.clear();
}

function sessionPreferenceStats() {
  return { size: SESSION_PREFS.size, cap: SESSION_PREF_CAP, ...SESSION_PREF_STATS };
}

// ---- history + stats ----------------------------------------------------------

const HISTORY_CAP = 500;
const HISTORY = []; // ring buffer (newest at tail): {t, callId, model, backend, stream, status, latencyMs, ttftMs, promptTokens, completionTokens, cacheHit, genMs, session}
// Collision-safe counter + entropy mix so two calls in the same ms still
// produce distinct ids; the `c` prefix keeps the id grep-able in logs.
let callCounter = 0;
function newCallId() {
  callCounter = (callCounter + 1) % 1296; // 36^2, wraps harmlessly
  const entropy = crypto.randomBytes(5).toString("base64url").slice(0, 8);
  return "c" + callCounter.toString(36).padStart(2, "0") + entropy;
}
const STATS = {
  since: Date.now(),
  total: 0,
  byBackend: {}, // backendId -> {requests, ok, errors, latencySumMs, latencyMaxMs}
  byModel: {}, // modelId -> count
};
const startedAt = Date.now();
// Dedup set for preset-of-presets cycle warnings: warn once per cyclic path
// (a repeated cycle on a hot path must not spam the router log).
const cycleWarned = new Set();
// JSONL history lands next to the config so tests write into their temp dir.
const historyPath = process.env.ROUTER_HISTORY || join(dirname(CONFIG_PATH), "router-history.jsonl");

function recordHistory(entry) {
  HISTORY.push(entry);
  if (HISTORY.length > HISTORY_CAP) HISTORY.shift();
  try {
    appendFileSync(historyPath, JSON.stringify(entry) + "\n");
  } catch {
    // Logging must never crash the router.
  }
}

// ---- bounded cache-miss payload capture (persistent) -------------------------
//
// OPTIONAL. Controlled by the top-level `missCapture` config block; when the
// block is absent or `enabled` is false the whole path is a no-op so existing
// behavior is untouched. A cache MISS means the upstream had to re-process
// (and the user got billed for) the full prompt; capturing the exact payload
// that missed makes a bad night recoverable for forensics. Storage is bounded:
// `boundedPayload()` clips content/tools, and the file rotates to `<file>.old`
// once it exceeds `maxFileBytes`.
const MISS_CAPTURE = {
  enabled: false,
  maxMissPct: 50,
  minMissTokens: 1000,
  file: "router-misses.jsonl",
  maxFileBytes: 25000000,
};

function missCapturePath(cfg) {
  const name = (cfg.missCapture && cfg.missCapture.file) || MISS_CAPTURE.file;
  // join() does NOT reset on an absolute second argument, so an absolute `file`
  // must come back as-is: joining it produced a bogus path inside the config
  // dir and every append died with ENOENT (swallowed by captureMiss).
  return isAbsolute(name) ? name : join(dirname(CONFIG_PATH), name);
}

// Append one bounded payload row for a low-cache-hit request. Must NEVER crash
// the router, so the whole body is guarded. The payload never contains keys:
// key values live only in env / request headers and are never copied.
function captureMiss(cfg, entry, payload) {
  const mc = cfg.missCapture;
  if (!mc || mc.enabled === false) return;
  const pct = entry.cacheHitPct;
  if (pct == null || pct >= (mc.maxMissPct ?? MISS_CAPTURE.maxMissPct)) return;
  let missTokens = Math.max(0, Math.round((entry.promptTokens || 0) * (1 - pct / 100)));
  if (missTokens < (mc.minMissTokens ?? MISS_CAPTURE.minMissTokens)) return;
  const file = missCapturePath(cfg);
  const row = {
    t: entry.t,
    callId: entry.callId,
    backend: entry.backend,
    model: entry.model,
    session: entry.session,
    cacheHitPct: pct,
    promptTokens: entry.promptTokens,
    cacheMissTokens: missTokens,
    payload: boundedPayload(payload),
  };
  try {
    // Rotate before append so the active file never grows unbounded.
    try {
      if (statSync(file).size > (mc.maxFileBytes ?? MISS_CAPTURE.maxFileBytes)) {
        renameSync(file, file + ".old");
      }
    } catch {
      // First write (no file yet) or a transient stat failure is fine.
    }
    appendFileSync(file, JSON.stringify(row) + "\n");
  } catch {
    // Capture must never crash the router.
  }
}

// ---- ntfy push alerts (OPTIONAL) ---------------------------------------------
//
// OPTIONAL. Controlled by the top-level `alerts.ntfy` block ({baseUrl, topic,
// minMissPct, minMissTokens, cooldownMs, events?}). When it is absent, or the
// kind's event is explicitly disabled (`events[kind] === false`), nothing is
// sent. Alerts are fire-and-forget: they must never block the hot request path
// or crash the router if ntfy is unreachable. Cooldown per kind suppresses an
// alert storm. Messages carry NO secrets (no keys).
const alertCooldowns = new Map();
const NTFY_DEFAULT_BASE = "https://ntfy.sh";
const ALERT_DEFS = {
  cache_miss: { title: "Cache break on switchblade", tag: "warning", priority: 3 },
  weekly_limit: { title: "Backend weekly usage limit reached", tag: "rotating_light", priority: 4 },
  all_cooling: { title: "All backends cooling", tag: "sos", priority: 5 },
  request_exhausted: { title: "Request failed - every provider down", tag: "exclamation", priority: 4 },
  relay_cut: { title: "Stream cut mid-relay", tag: "warning", priority: 3 },
};

function alertMessage(kind, f) {
  switch (kind) {
    case "cache_miss":
      return `Cache break on ${f.backend}: ${f.cacheHitPct}% hit, ~${f.missTokens} miss tokens (prompt ${f.promptTokens}, model ${f.model}, session ${f.session})`;
    case "weekly_limit":
      return `${f.backend} hit its weekly usage limit (resets in ${f.days ?? "?"} days); other providers may still serve`;
    case "all_cooling":
      return `All backends are cooling (${f.manualCooled} manually cooled); requests fail with 503`;
    case "request_exhausted":
      return `Request ${f.callId} (model ${f.model}) failed on every provider: ${f.error || "unknown"}`;
    case "relay_cut":
      return `${f.backend} cut mid-relay on ${f.model}: ${f.error}`;
    default:
      return null;
  }
}

function buildAlert(cfg, kind, fields) {
  const n = cfg.alerts && cfg.alerts.ntfy;
  if (!n || !n.topic || !ALERT_DEFS[kind]) return null;
  const events = n.events || {};
  if (events[kind] === false) return null;
  const message = alertMessage(kind, fields);
  if (!message) return null;
  const last = alertCooldowns.get(kind) || 0;
  const cooldownMs = n.cooldownMs ?? 600000;
  if (Date.now() - last < cooldownMs) return null;
  alertCooldowns.set(kind, Date.now());
  return {
    url: (n.baseUrl || NTFY_DEFAULT_BASE).replace(/\/$/, ""),
    body: {
      topic: n.topic,
      title: ALERT_DEFS[kind].title,
      message,
      tags: [ALERT_DEFS[kind].tag],
      priority: ALERT_DEFS[kind].priority,
    },
  };
}

function fireAlert(cfg, kind, fields) {
  const a = buildAlert(cfg, kind, fields);
  if (!a) return;
  // Fire-and-forget: a slow/unreachable ntfy must not stall or crash the router.
  fetch(a.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(a.body),
    signal: AbortSignal.timeout(5000),
  }).catch(() => {});
}

function resetAlertCooldowns() {
  alertCooldowns.clear();
}

// ---- request detail store (deep-dive side panel) -----------------------------
//
// Full per-request debug records live IN MEMORY ONLY, capped and size-guarded,
// so the deep dive never touches disk and cannot grow unboundedly:
// - DETAIL_CAP: at most 100 detail records (ring buffer, oldest dropped).
// - Size guard: requests whose raw body exceeds DETAIL_MAX_BODY_BYTES (100 KB)
//   are skipped - a detail record is only worth the memory when the payload is
//   small enough to inspect comfortably anyway.
// - The JSONL history file (router-history.jsonl) keeps ONLY the summary
//   entries; detail records are never appended to disk. Persistent capture is a
//   future change.
// - Detail records are keyed by the summary entry's `t` timestamp, which is
//   unique enough within the ring buffer (Date.now() per request).
const DETAIL_CAP = 100;
const DETAIL_MAX_BODY_BYTES = 100000;
const DETAILS = []; // ring buffer (newest at tail): {t, callId, payload, attempts, ...}

// Bounded copy of the outgoing payload for the deep-dive store. Requests whose
// raw body exceeds DETAIL_MAX_BODY_BYTES would otherwise be skipped entirely
// (no detail record at all); instead we keep a truncated copy so every request
// stays inspectable without holding multi-hundred-KB payloads in memory:
// message content is clipped per message, and tool schemas are reduced to the
// first few with the true count preserved.
function boundedPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const out = { ...payload };
  if (Array.isArray(out.messages)) {
    out.messages = out.messages.map((m) => {
      const c = { ...m };
      if (typeof c.content === "string" && c.content.length > 400) c.content = c.content.slice(0, 400) + "…[truncated]";
      return c;
    });
  }
  if (Array.isArray(out.tools)) {
    out._truncated = { tools: out.tools.length, kept: Math.min(out.tools.length, 5) };
    out.tools = out.tools.slice(0, 5);
  }
  return out;
}

// Record (or update) the deep-dive detail record for the request whose summary
// entry timestamp is `t` (and whose call id is `callId`). Pass the outgoing
// request payload (what forward() sent, never API keys - keys live only in
// env) and an append-style attempt record. Retries may already exist on the
// detail record (a parallel thread can call this per attempt), so this
// function merges into attempts[] by appending.
function recordDetail(t, callId, payload, attempt) {
  if (!t || !payload || typeof payload !== "object") return;
  let rec = DETAILS.find((d) => d.t === t);
  if (!rec) {
    rec = { t, callId: callId || null, payload, attempts: [] };
    DETAILS.push(rec);
    if (DETAILS.length > DETAIL_CAP) DETAILS.shift();
  }
  if (attempt) rec.attempts.push(attempt);
}

// Finalize a detail record once the attempt loop has run its course (success or
// exhausted failure): attach the response summary. No-op when the request was
// never captured (payload too large / no attempts) or already finalized.
function finalizeDetail(t, summary) {
  const rec = DETAILS.find((d) => d.t === t);
  if (!rec || rec.finalized) return;
  rec.finalized = true;
  if (summary) rec.response = summary;
}

// Get a detail record by summary timestamp, or by call id. Returns the record
// or null.
function getDetail(t) {
  return DETAILS.find((d) => d.t === t) || null;
}

function getDetailByCall(callId) {
  if (!callId) return null;
  return DETAILS.find((d) => d.callId === callId) || null;
}

// Canonical history entry shape with metric defaults, so every row (success
// or failure) carries the same fields for the UI. Backward compatible: the
// original fields (t, model, backend, stream, status, latencyMs, session)
// keep their exact semantics.
function newHistoryEntry(f) {
  return {
    t: f.t ?? Date.now(),
    // Stable per-request id for cross-referencing ("that call c-…"). Legacy
    // entries (pre-callId JSONL lines or rows built without one) get a
    // deterministic id derived from their timestamp so every row is
    // referenceable, not just new ones.
    callId: f.callId ?? "t" + (f.t ?? Date.now()).toString(36),
    model: f.model,
    backend: f.backend,
    stream: !!f.stream,
    status: f.status,
    latencyMs: f.latencyMs ?? 0,
    ttftMs: f.ttftMs ?? 0,                       // time to first upstream response (== latencyMs on success)
    promptTokens: f.promptTokens ?? 0,           // prompt usage tokens (0 if upstream did not report)
    completionTokens: f.completionTokens ?? 0,   // completion usage tokens (0 if upstream did not report)
    reasoningTokens: f.reasoningTokens ?? 0,     // reasoning-only completion tokens (usage.completion_tokens_details.reasoning_tokens; 0 if not reported - "0" and "not reported" are indistinguishable)
    cacheHit: f.cacheHit,                          // tri-state: true (header/usage hit), false (miss reported), null (unknown - no signal)
    cacheHitPct: f.cacheHitPct ?? null,            // 0..100 share of prompt tokens served from cache (hit/(hit+miss)); low % = early cache break; null = no measurable signal
    genMs: f.genMs ?? 0,                         // generation time: stream duration, or total - ttft for non-stream
    tps: f.tps ?? 0,                             // completion tokens per second (derived)
    session: f.session,
    routedModel: f.routedModel ?? null,          // resolved model id (success: serving attempt; failure: first candidate; null: none)
    wireParams: f.wireParams ?? null,            // redacted summary of the params actually sent upstream (for explaining reasoning on/off per row)
    retries: f.retries ?? 0,                     // number of retry attempts performed on the serving provider (0 = none)
    retryWaitedMs: f.retryWaitedMs ?? 0,         // total ms spent waiting in backoff across retries
    relayError: f.relayError ?? null,            // relay cut mid-body on a 200 row (idle timeout / upstream drop) - null = clean
    promptCacheHitTokens: f.promptCacheHitTokens ?? 0, // raw prompt tokens served from cache (0 = upstream did not report a split)
    promptCacheMissTokens: f.promptCacheMissTokens != null ? f.promptCacheMissTokens : Math.max(0, (f.promptTokens ?? 0) - (f.promptCacheHitTokens ?? 0)), // raw miss tokens; derived from prompt - hit when the upstream only reports hits
    errorKind: f.errorKind ?? null,              // classified upstream failure kind (weekly/rate/auth/server/client); null = no upstream failure
    selectionReason: f.selectionReason ?? null,   // why the FIRST attempt was chosen: configured | session_preference | cooldown
    fallbackCount: f.fallbackCount ?? 0,          // cross-provider transitions (a same-provider retry is NOT a fallback)
    cacheMayBeCold: f.cacheMayBeCold ?? null,     // true once the request changed provider: the prompt cache MAY be cold (never an asserted miss)
    attemptsSummary: f.attemptsSummary ?? [],     // body-free durable per-attempt outcomes (capped; see capAttemptsSummary)
    attemptsOmitted: f.attemptsOmitted ?? 0,      // outcomes dropped from attemptsSummary by that cap
    cost: f.cost ?? null,                        // request cost in USD (see computeCost); null = model has no meta.pricing
    cacheSavings: f.cacheSavings ?? null,        // USD saved by cache hits vs full-price miss (peak rates)
    offPeakSavings: f.offPeakSavings ?? null,    // USD saved by the off-peak discount (0 when it did not apply)
    offPeak: f.offPeak ?? null,                  // whether off-peak pricing applied to this request
  };
}

// Redacted wire-params summary for history: captures which reasoning-relevant
// keys were actually sent upstream and their values (thinking type,
// reasoning_effort, max_tokens), WITHOUT copying messages, tools, or any
// key values. Used to explain per-request reasoning behavior.
function extractWireParams(payload) {
  if (!payload || typeof payload !== "object") return null;
  const p = payload;
  const out = {};
  if (p.thinking && typeof p.thinking === "object") out.thinking = String(p.thinking.type || "?");
  if (p.reasoning_effort != null) out.reasoning_effort = String(p.reasoning_effort);
  if (p.max_tokens != null) out.max_tokens = p.max_tokens;
  if (p.temperature != null) out.temperature = p.temperature;
  if (Array.isArray(p.tools) && p.tools.length) out.tools = p.tools.length;
  return Object.keys(out).length ? out : null;
}

function recordStats(entry) {
  STATS.total++;
  const b = STATS.byBackend[entry.backend] || {
    requests: 0, ok: 0, errors: 0, latencySumMs: 0, latencyMaxMs: 0,
    promptTokensSum: 0, completionTokensSum: 0, cacheHits: 0, cacheHitPctSum: 0, cachePctKnown: 0, ttftSumMs: 0,
  };
  b.requests++;
  if (entry.status >= 200 && entry.status < 400) b.ok++;
  else b.errors++;
  b.latencySumMs += entry.latencyMs || 0;
  if (entry.latencyMs > b.latencyMaxMs) b.latencyMaxMs = entry.latencyMs;
  if (entry.ttftMs != null) b.ttftSumMs += entry.ttftMs;
  if (entry.promptTokens) b.promptTokensSum += entry.promptTokens;
  if (entry.completionTokens) b.completionTokensSum += entry.completionTokens;
  if (entry.cacheHit) b.cacheHits++;
  if (typeof entry.cacheHitPct === "number") { b.cacheHitPctSum = (b.cacheHitPctSum || 0) + entry.cacheHitPct; b.cachePctKnown = (b.cachePctKnown || 0) + 1; }
  STATS.byBackend[entry.backend] = b;
  STATS.byModel[entry.model] = (STATS.byModel[entry.model] || 0) + 1;
}

// ---- analytics aggregates (dashboard) ----------------------------------------
//
// In-memory KPI + bucket store backing /api/dashboard. Fed at every
// history-entry finalization site via recordAnalytics() and seeded from the
// JSONL tail at startup so the charts survive restarts. Hour slots are
// hour-start epoch ms; day slots are UTC-midnight epoch ms. The payload keeps
// cost/cache/rate fields null until real data exists - null is the contract's
// "no data" signal, never a fake zero.

const ANALYTICS = {
  kpis: {
    requests: 0, ok: 0, errors: 0,
    promptTokens: 0, completionTokens: 0,
    cacheHitTokens: 0, cacheMissTokens: 0, cacheKnown: 0,
    priced: 0, cost: 0, cacheSavings: 0, offPeakSavings: 0, offPeakRequests: 0,
    models: new Set(), // distinct model ids seen (seeded rows too)
  },
  byBackend: new Map(), // backendId -> accumulator (shape below)
  byModel: new Map(),   // modelId -> accumulator (same shape)
  hourly: new Map(),    // hourSlot -> {requests, ok, errors, promptTokens, completionTokens, cacheHitTokens, cacheMissTokens, cost, priced, latencySumMs, latencyMaxMs}
  daily: new Map(),     // daySlot -> {requests, errors, promptTokens, completionTokens, cacheHitTokens, cacheMissTokens, cost, priced}
  expiredNotified: new Set(), // cooling backends whose expiry already hit the ring
};

const okStatus = (s) => s >= 200 && s <= 299;

function newAnalyticsAcc() {
  return { requests: 0, ok: 0, errors: 0, latencySumMs: 0, latencyMaxMs: 0, promptTokens: 0, completionTokens: 0, cost: 0, priced: 0, cacheHits: 0, cacheKnown: 0, cacheHitTokens: 0, cacheMissTokens: 0 };
}

// One accumulator update for a fully-shaped history entry (live or seeded).
// Cache denominators only count requests that actually carried a hit/miss
// split; a signal-less row (hit+miss == 0) must not drag the rate toward 0.
function recordAnalytics(entry) {
  const status = entry.status || 0;
  const promptTokens = entry.promptTokens || 0;
  const completionTokens = entry.completionTokens || 0;
  const hit = entry.promptCacheHitTokens || 0;
  const miss = entry.promptCacheMissTokens || 0;
  const cost = Number.isFinite(entry.cost) ? entry.cost : null;
  const k = ANALYTICS.kpis;
  k.requests++;
  if (okStatus(status)) k.ok++;
  else k.errors++;
  k.promptTokens += promptTokens;
  k.completionTokens += completionTokens;
  if (hit + miss > 0) {
    k.cacheHitTokens += hit;
    k.cacheMissTokens += miss;
    k.cacheKnown++;
  }
  if (cost != null) {
    k.priced++;
    k.cost += cost;
    if (entry.cacheSavings != null) k.cacheSavings += entry.cacheSavings;
    if (entry.offPeakSavings != null) k.offPeakSavings += entry.offPeakSavings;
  }
  if (entry.offPeak === true) k.offPeakRequests++;
  if (entry.model != null) k.models.add(entry.model);

  const backend = entry.backend || "none";
  let b = ANALYTICS.byBackend.get(backend);
  if (!b) {
    b = newAnalyticsAcc();
    ANALYTICS.byBackend.set(backend, b);
  }
  b.requests++;
  if (okStatus(status)) b.ok++;
  else b.errors++;
  b.latencySumMs += entry.latencyMs || 0;
  if ((entry.latencyMs || 0) > b.latencyMaxMs) b.latencyMaxMs = entry.latencyMs || 0;
  b.promptTokens += promptTokens;
  b.completionTokens += completionTokens;
  if (cost != null) { b.priced++; b.cost += cost; }
  if (entry.cacheHit === true) b.cacheHits++;
  if (entry.cacheHit === true || entry.cacheHit === false) b.cacheKnown++;
  if (hit + miss > 0) {
    b.cacheHitTokens += hit;
    b.cacheMissTokens += miss;
  }

  if (entry.model != null) {
    let m = ANALYTICS.byModel.get(entry.model);
    if (!m) {
      m = newAnalyticsAcc();
      ANALYTICS.byModel.set(entry.model, m);
    }
    m.requests++;
    if (okStatus(status)) m.ok++;
    else m.errors++;
    m.latencySumMs += entry.latencyMs || 0;
    if ((entry.latencyMs || 0) > m.latencyMaxMs) m.latencyMaxMs = entry.latencyMs || 0;
    m.promptTokens += promptTokens;
    m.completionTokens += completionTokens;
    if (cost != null) { m.priced++; m.cost += cost; }
    if (hit + miss > 0) {
      m.cacheHitTokens += hit;
      m.cacheMissTokens += miss;
    }
  }

  const hSlot = Math.floor(entry.t / 3600000) * 3600000;
  let hb = ANALYTICS.hourly.get(hSlot);
  if (!hb) {
    hb = { requests: 0, ok: 0, errors: 0, promptTokens: 0, completionTokens: 0, cacheHitTokens: 0, cacheMissTokens: 0, cost: 0, priced: 0, latencySumMs: 0, latencyMaxMs: 0 };
    ANALYTICS.hourly.set(hSlot, hb);
  }
  hb.requests++;
  if (okStatus(status)) hb.ok++;
  else hb.errors++;
  hb.promptTokens += promptTokens;
  hb.completionTokens += completionTokens;
  if (hit + miss > 0) {
    hb.cacheHitTokens += hit;
    hb.cacheMissTokens += miss;
  }
  if (cost != null) { hb.priced++; hb.cost += cost; }
  hb.latencySumMs += entry.latencyMs || 0;
  if ((entry.latencyMs || 0) > hb.latencyMaxMs) hb.latencyMaxMs = entry.latencyMs || 0;

  const dSlot = Math.floor(entry.t / 86400000) * 86400000;
  let db = ANALYTICS.daily.get(dSlot);
  if (!db) {
    db = { requests: 0, errors: 0, promptTokens: 0, completionTokens: 0, cacheHitTokens: 0, cacheMissTokens: 0, cost: 0, priced: 0 };
    ANALYTICS.daily.set(dSlot, db);
  }
  db.requests++;
  if (!okStatus(status)) db.errors++;
  db.promptTokens += promptTokens;
  db.completionTokens += completionTokens;
  if (hit + miss > 0) {
    db.cacheHitTokens += hit;
    db.cacheMissTokens += miss;
  }
  if (cost != null) { db.priced++; db.cost += cost; }
}

function coolingCount(now) {
  let n = 0;
  for (const h of health.values()) {
    if (h.state === "cooling" && h.nextAvailableAt > now) n++;
  }
  return n;
}

// Build the exact /api/dashboard payload: windows are generated at read time
// (exactly 24 hour-slots oldest first, exactly 30 day-slots ending today), so
// empty slots materialize as zeroed rows without needing a store sweep.
function dashboardPayload(cfg) {
  const now = Date.now();
  const k = ANALYTICS.kpis;
  const priced = k.priced > 0;
  const kpis = {
    requests: k.requests,
    ok: k.ok,
    errors: k.errors,
    errorRate: k.requests > 0 ? k.errors / k.requests : null,
    promptTokens: k.promptTokens,
    completionTokens: k.completionTokens,
    cacheHitTokens: k.cacheHitTokens,
    cacheMissTokens: k.cacheMissTokens,
    cacheHitRate: k.cacheKnown > 0 ? k.cacheHitTokens / (k.cacheHitTokens + k.cacheMissTokens) : null,
    cost: priced ? k.cost : null,
    cacheSavings: priced ? k.cacheSavings : null,
    offPeakSavings: priced ? k.offPeakSavings : null,
    offPeakRequests: k.offPeakRequests,
    offPeakShare: k.requests > 0 ? k.offPeakRequests / k.requests : null,
    cooledBackends: coolingCount(now),
    activeModelCount: k.models.size,
  };

  const byBackend = usableBackends(cfg).map((b) => {
    const acc = ANALYTICS.byBackend.get(b.id);
    const h = health.get(b.id) || {};
    const isCooling = h.state === "cooling" && h.nextAvailableAt > now;
    return {
      id: b.id,
      requests: acc ? acc.requests : 0,
      ok: acc ? acc.ok : 0,
      errors: acc ? acc.errors : 0,
      errorRate: acc && acc.requests > 0 ? acc.errors / acc.requests : null,
      latencyAvgMs: acc && acc.requests > 0 ? acc.latencySumMs / acc.requests : null,
      latencyMaxMs: acc ? acc.latencyMaxMs : 0,
      promptTokens: acc ? acc.promptTokens : 0,
      completionTokens: acc ? acc.completionTokens : 0,
      cost: acc && acc.priced > 0 ? acc.cost : null,
      cacheHits: acc ? acc.cacheHits : 0,
      cacheKnown: acc ? acc.cacheKnown : 0,
      cacheHitRate: acc && acc.cacheKnown > 0 ? acc.cacheHitTokens / (acc.cacheHitTokens + acc.cacheMissTokens) : null,
      state: isCooling ? "cooling" : "healthy",
      fails: h.fails || 0,
      lastErrorKind: (h.lastError && h.lastError.kind) || null,
      manual: !!(isCooling && h.lastError && h.lastError.kind === "manual"),
    };
  });

  const byModel = [];
  for (const [id, m] of ANALYTICS.byModel) {
    const mcfg = (cfg.models || {})[id];
    byModel.push({
      id,
      label: (mcfg && mcfg.meta && mcfg.meta.label) || null,
      requests: m.requests,
      ok: m.ok,
      errors: m.errors,
      errorRate: m.requests > 0 ? m.errors / m.requests : null,
      latencyAvgMs: m.requests > 0 ? m.latencySumMs / m.requests : null,
      latencyMaxMs: m.latencyMaxMs,
      promptTokens: m.promptTokens,
      completionTokens: m.completionTokens,
      cost: m.priced > 0 ? m.cost : null,
      cacheHitRate: m.cacheHitTokens + m.cacheMissTokens > 0 ? m.cacheHitTokens / (m.cacheHitTokens + m.cacheMissTokens) : null,
    });
  }

  const curHour = Math.floor(now / 3600000) * 3600000;
  const hourly = [];
  for (let i = 23; i >= 0; i--) {
    const slot = curHour - i * 3600000;
    const hb = ANALYTICS.hourly.get(slot);
    hourly.push({
      t: slot,
      requests: hb ? hb.requests : 0,
      ok: hb ? hb.ok : 0,
      errors: hb ? hb.errors : 0,
      promptTokens: hb ? hb.promptTokens : 0,
      completionTokens: hb ? hb.completionTokens : 0,
      cacheHitTokens: hb ? hb.cacheHitTokens : 0,
      cacheMissTokens: hb ? hb.cacheMissTokens : 0,
      cost: hb && hb.priced > 0 ? hb.cost : null,
      latencyAvgMs: hb && hb.requests > 0 ? hb.latencySumMs / hb.requests : null,
      latencyMaxMs: hb ? hb.latencyMaxMs : 0,
    });
  }

  const curDay = Math.floor(now / 86400000) * 86400000;
  const daily = [];
  for (let i = 29; i >= 0; i--) {
    const slot = curDay - i * 86400000;
    const db = ANALYTICS.daily.get(slot);
    daily.push({
      t: slot,
      requests: db ? db.requests : 0,
      errors: db ? db.errors : 0,
      cost: db && db.priced > 0 ? db.cost : null,
      tokens: db ? db.promptTokens + db.completionTokens : 0,
      cacheHitRate: db && db.cacheHitTokens + db.cacheMissTokens > 0 ? db.cacheHitTokens / (db.cacheHitTokens + db.cacheMissTokens) : null,
    });
  }

  const pr = cfg.pricing && typeof cfg.pricing === "object" ? cfg.pricing : {};
  const pricing = {
    offPeakMultiplier: typeof pr.offPeakMultiplier === "number" ? pr.offPeakMultiplier : 0.5,
    peakWindows: Array.isArray(pr.peakWindows) ? pr.peakWindows : DEFAULT_PEAK_WINDOWS,
    modelsWithOffPeak: Object.keys(cfg.models || {}).filter((id) => {
      const m = cfg.models[id];
      return !!(m && m.meta && m.meta.pricing && m.meta.pricing.offPeak === true);
    }),
  };

  return { since: startedAt, uptimeMs: now - startedAt, kpis, byBackend, byModel, hourly, daily, cooling: COOLING_EVENTS.slice(0, COOLING_EVENT_CAP), pricing };
}

// Seed the aggregate store from the JSONL tail at startup so the dashboard
// charts survive restarts. Reads at most the last 20 MB (a pathological
// history must not delay boot); a partial first line from a tail read is
// dropped. Malformed lines are skipped; legacy rows still count as requests
// with zero tokens, exactly like a live entry would.
function seedAnalytics(file) {
  let raw;
  try {
    const size = statSync(file).size;
    const MAX_SEED_BYTES = 20 * 1024 * 1024;
    if (size > MAX_SEED_BYTES) {
      const fd = openSync(file, "r");
      try {
        const buf = Buffer.alloc(MAX_SEED_BYTES);
        const n = readSync(fd, buf, 0, MAX_SEED_BYTES, size - MAX_SEED_BYTES);
        raw = buf.subarray(0, n).toString("utf8");
      } finally {
        closeSync(fd);
      }
      const nl = raw.indexOf("\n");
      if (nl >= 0) raw = raw.slice(nl + 1);
    } else {
      raw = readFileSync(file, "utf8");
    }
  } catch {
    return; // no history file yet - a fresh install
  }
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }
    if (!row || typeof row !== "object" || typeof row.t !== "number" || !Number.isFinite(row.t)) continue;
    recordAnalytics(row);
  }
}

// ---- legacy -> v3 bridge ----------------------------------------------------

// Convert today's two-layer config into the three-layer shape. The ORDER is
// load-bearing and fixed: the identity-preset drop runs FIRST, so every later
// pass sees the same ids a golden run would, and the residual-collision refusal
// runs LAST, after the repair sweep has removed the reasons a converted payload
// would otherwise be unsaveable. Returns { config, warnings }; throws only when
// a model and a preset still share an id after the drop, which needs a human to
// decide which one keeps the name.
// The pre-fix config editor wrote "" for any JSON textarea it rendered empty, so
// a config saved through it can carry `retry: ""` or `params: ""`. The sentence
// is ONE constant because two call sites report the same repair (the preset
// params mapping and the sweep below) and a count that depends on two literals
// staying byte-identical would drift the moment either is reworded.
const EMPTY_STRING_REPAIR_MSG = " was an empty string (an older config editor wrote it); dropped - it was already ignored at runtime";
const repairEmptyString = (obj, at, key, warnings) => {
  delete obj[key];
  warnings.push(at + "." + key + EMPTY_STRING_REPAIR_MSG);
};

function convertLegacyConfig(raw) {
  const warnings = [];
  const cfg = JSON.parse(JSON.stringify(raw));
  cfg.models = cfg.models || {};
  cfg.presets = cfg.presets || {};
  // 1) a preset's empty-string `params` is repaired FIRST, before the identity
  // drop: an IDENTITY preset is deleted in step 2, so a repair reported only
  // from a later pass would silently skip it - and the live config's only
  // empty-string preset params sits on exactly such a preset. Reporting it over
  // the whole set keeps the warning list independent of which presets survive.
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (p && typeof p === "object" && p.params === "") repairEmptyString(p, "presets." + id, "params", warnings);
  }
  // 2) identity-preset drop: a preset named exactly like a model, listing only
  // that same name, is a pure pass-through, so dropping it leaves the id served
  // by the same-named model and frees the name for the v3 XOR rule. Its
  // `params` move onto that model FIRST, with the preset's value winning: the v2
  // merge chain applied the preset layer LAST, so folding is exactly equivalent
  // key by key and the dropped alias keeps the values it used to serve. Doing it
  // before the drop is what makes it a fold rather than a silent loss.
  for (const [id, p] of Object.entries(cfg.presets)) {
    // A structurally malformed preset (null, a number) is NOT the converter's
    // to judge: it passes through untouched so the validator can name its path,
    // which is what keeps a hand-edited file a refusal instead of a crash.
    if (!p || typeof p !== "object") continue;
    const refs = Array.isArray(p.models) ? p.models : null;
    if (refs && refs.length === 1) {
      const only = typeof refs[0] === "string" ? refs[0] : refs[0] && refs[0].model;
      if (only === id && cfg.models[id]) {
        const target = cfg.models[id];
        if (p.params && typeof p.params === "object" && !Array.isArray(p.params) && Object.keys(p.params).length) {
          target.params = Object.assign({}, target.params && typeof target.params === "object" ? target.params : {}, p.params);
          warnings.push(
            "preset '" + id + "' is an identity preset; its params were folded onto model '" + id + "' (preset values win, as the v2 merge order did) and the preset was dropped"
          );
        }
        delete cfg.presets[id];
        warnings.push("preset '" + id + "' is an identity preset and was dropped; the model stays directly addressable");
      }
    }
  }
  // 3) per-ref weights and preset affinityPool: warn ONLY when routing-relevant.
  // A weighted preset's per-ref weights WERE honored at request time (weighted
  // random over models), so losing them changes behavior; affinity picks by
  // session hash and failover by declared order, so their weights were already
  // ignored. preset-affinityPool always warns: pool > 1 widened the session
  // spread under every strategy and v3's fallback has no pool at all.
  const skippedPresets = [];
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (!p || typeof p !== "object") {
      skippedPresets.push("presets." + id);
      continue;
    }
    for (const e of p.models || []) {
      if (e && typeof e === "object" && e.weight != null && p.strategy === "weighted") warnings.push("preset '" + id + "' per-ref weight dropped; weights live on deployments");
    }
    if (p.affinityPool !== undefined) warnings.push("preset '" + id + "' affinityPool dropped");
  }
  if (skippedPresets.length) {
    warnings.push("entry at " + skippedPresets.join(", ") + " is not an object and was left for the validator to refuse");
  }
  // 4) providers -> deployments 1:1, in declared order. affinityPool N of M
  // marks the providers past N as backupOnly, which is what the v2 pool meant:
  // the first N are the session-spread set and the rest are ordered fallbacks.
  for (const [id, m] of Object.entries(cfg.models)) {
    if (!m || typeof m !== "object" || !Array.isArray(m.providers)) continue;
    if (Array.isArray(m.deployments) && m.deployments.length) {
      warnings.push("model '" + id + "' carries both providers and deployments; providers win and the hand-written deployments are dropped");
    }
    const pool = m.affinityPool;
    m.deployments = m.providers.map((pr, i) => {
      // A malformed provider (null, a scalar) is passed through as-is: mapping
      // it to a fabricated deployment would edit what the operator wrote.
      if (!pr || typeof pr !== "object") return pr;
      const d = { backend: pr.backend, upstream: pr.upstream };
      if (pr.weight !== undefined && pr.weight !== 1) d.weight = pr.weight;
      if (typeof pool === "number" && i >= pool) d.backupOnly = true;
      for (const k of ["params", "dropParams", "paramMap"]) if (pr[k] !== undefined) d[k] = pr[k];
      return d;
    });
    delete m.providers;
    delete m.affinityPool;
    if (m.exposed === undefined) m.exposed = true;
  }
  // 5) preset strategies collapse to fallback, preserving declared ref order as
  // plain model id strings. Only a behavior change warns: an affinity preset
  // with more than one ref loses its session spread under ordered fallback,
  // while failover and weighted keep declared order and stay silent.
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (!p || typeof p !== "object") continue;
    if (p.strategy !== "fallback") {
      if (p.strategy && p.strategy !== "affinity" && p.strategy !== "failover" && p.strategy !== "weighted") warnings.push("preset '" + id + "' unknown strategy treated as fallback");
      else if (p.strategy === "affinity" && (p.models || []).length > 1) warnings.push("preset '" + id + "' affinity spread mapped to fallback (ordered failover); session-spread behavior changes");
      p.strategy = "fallback";
    }
    p.models = (p.models || []).map((e) => (typeof e === "string" ? e : e.model));
    delete p.affinityPool;
    // Dropping a preset's params is a real behavior change when the value is a
    // non-empty object (the v2 merge chain had a preset layer, v3 has none), so
    // that case warns. Every OTHER present value - an empty string, or a
    // non-object such as `params: 5` that the object check above rejects - is an
    // editor artifact of the class the repair pass drops, and is dropped here
    // without a second message.
    if (p.params !== undefined) {
      if (p.params && typeof p.params === "object" && !Array.isArray(p.params) && Object.keys(p.params).length) {
        warnings.push("preset '" + id + "' params dropped; v3 has no preset-level params layer");
      }
      delete p.params;
    }
    // /api/config serves a derived `members` key; nobody reads it in v3, and a
    // mixed-era snapshot would otherwise convert carrying a stale key.
    delete p.members;
    if (p.exposed === undefined) p.exposed = true;
  }
  // 6) empty-string repair sweep. Only MODEL-level retry/params are object-
  // validated, so only those are save/reload ERRORS - but POST /api/config
  // validates the WHOLE candidate, so ONE surviving error-class value blocks
  // every save. All of them are dropped regardless: none means anything at
  // runtime, so this is a repair, and it warns because it rewrites data the
  // operator never asked to change.
  const repairEmpty = (obj, at) => {
    if (!obj || typeof obj !== "object") return;
    for (const k of ["params", "retry", "dropParams", "paramMap", "offPeak"]) {
      if (obj[k] === "") repairEmptyString(obj, at, k, warnings);
    }
  };
  // A non-object entry PASSES THROUGH UNTOUCHED: it is the validator's job to
  // name `backends.N`, and dereferencing it here is what used to take the router
  // down at boot on a hand-edited file.
  for (const b of cfg.backends || []) if (b && typeof b === "object") repairEmpty(b, "backends." + b.id);
  for (const [id, m] of Object.entries(cfg.models)) {
    repairEmpty(m, "models." + id);
    (m.deployments || []).forEach((d, i) => repairEmpty(d, "models." + id + ".deployments[" + i + "]"));
  }
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (p && typeof p === "object") repairEmpty(p, "presets." + id);
  }
  // 7) residual model/preset collision refuses: a converted config where the
  // same id is still both cannot be resolved by rule, only by a human.
  for (const id of Object.keys(cfg.models)) {
    if (cfg.presets[id]) throw new Error("conversion refused: id '" + id + "' is both a model and a preset");
  }
  return { config: cfg, warnings };
}

// ---- config normalization ---------------------------------------------------

// Normalize the three-layer config at load (initial + hot-reload), in place.
// This is DUAL-SHAPE for the whole bridge era: it reads the v3 `deployments`
// shape AND both legacy eras, and the legacy branches are removed only when the
// bridge is retired. Detection is PER ENTRY so single-era configs and
// accidental mixes both work:
// - `models[id]` with `deployments` (v3 shape) is passed through untouched.
// - `models[id]` with `providers` (new shape) is validated.
// - `models[id]` with `backends` (pre-presets era, path A) is synthesized into
//   providers `{backend, upstream: <that backend's model>}`.
// - `presets[id]` with `models` (new shape) is validated, keeping the v3
//   `fallback` strategy when the config is v3.
// - `presets[id]` with `members` (DESIGN-PRESETS era, path B) synthesizes ONE
//   model per preset (providers = members in order with per-provider upstream)
//   and rewrites the preset to `{strategy, models: [{model, weight: 1}]}`.
// Validation is lenient (warn + drop, never crash the router). Derived
// read-only `preset.members` (flattened provider backends, first occurrence,
// deduped) keeps the UI/tests working across the phased rollout. The
// normalized form lives only in memory; never written back to disk.
const collisionWarned = new Set(); // model id == preset id warns once per process

function normalizeConfig(cfg) {
  if (!cfg || typeof cfg !== "object") cfg = {};
  const backendById = new Map((cfg.backends || []).filter((b) => b && typeof b === "object" && b.id).map((b) => [b.id, b]));
  if (!cfg.models || typeof cfg.models !== "object") cfg.models = {};
  if (!cfg.presets || typeof cfg.presets !== "object") cfg.presets = {};

  const warnOnce = (set, key, msg) => {
    if (set.has(key)) return;
    set.add(key);
    console.warn(msg);
  };

  // ---- 1. models map -----------------------------------------------------
  // v3 detection mirrors the validator's rule (any model carrying
  // `deployments` makes the config v3) so the two never disagree about a file.
  // A v3 model has no legacy branch to run, and synthesizing `providers` onto
  // it would be the silently-wrong repair R6 forbids.
  const isV3 = Object.values(cfg.models).some((m) => m && typeof m === "object" && Array.isArray(m.deployments));
  const legacySynthesized = new Set(); // path A model ids (need presets later)
  for (const [id, m] of Object.entries(cfg.models)) {
    if (!m || typeof m !== "object") {
      console.warn(`config warning: model '${id}': invalid entry dropped`);
      delete cfg.models[id];
      continue;
    }
    if (Array.isArray(m.deployments)) {
      // v3 shape: the legacy synthesis (providers/backends, affinityPool) is
      // skipped entirely; the entry is left exactly as written.
    } else if (Array.isArray(m.providers)) {
      // NEW shape: validate each provider; keep optional dialect fields.
      const valid = [];
      for (const pr of m.providers) {
        if (!pr || typeof pr !== "object" || typeof pr.backend !== "string") {
          console.warn(`config warning: model '${id}': provider without backend id dropped`);
          continue;
        }
        const b = backendById.get(pr.backend);
        if (!b) {
          console.warn(`config warning: model '${id}': provider references unknown backend '${pr.backend}', dropped`);
          continue;
        }
        const upstream = pr.upstream ?? b.model;
        if (!upstream) {
          console.warn(`config warning: model '${id}': provider '${pr.backend}' has no resolvable upstream, dropped`);
          continue;
        }
        valid.push({ ...pr, upstream });
      }
      m.providers = valid;
    } else if (Array.isArray(m.backends)) {
      // path A: pre-presets era, `{backends: [id...], affinityPool}`.
      const valid = [];
      for (const bid of m.backends) {
        const b = typeof bid === "string" ? backendById.get(bid) : null;
        if (!b) {
          console.warn(`config warning: model '${id}': unknown backend '${bid}' dropped`);
          continue;
        }
        if (!b.model) {
          console.warn(`config warning: model '${id}': backend '${bid}' has no model (upstream), dropped`);
          continue;
        }
        valid.push({ backend: bid, upstream: b.model });
      }
      m.providers = valid;
      delete m.backends;
      legacySynthesized.add(id);
    } else {
      console.warn(`config warning: model '${id}': no providers/backends, entry dropped`);
      delete cfg.models[id];
      continue;
    }
    if ("params" in m && (m.params === null || typeof m.params !== "object")) {
      console.warn(`config warning: model '${id}': params must be an object, ignored`);
      delete m.params;
    }
    if (m.retry !== undefined) {
      if (m.retry === null || typeof m.retry !== "object") {
        console.warn(`config warning: model '${id}': retry must be an object, ignored`);
        delete m.retry;
      } else {
        const r = m.retry;
        if (typeof r.maxRetries !== "number" || r.maxRetries < 0 || !Number.isInteger(r.maxRetries)) {
          console.warn(`config warning: model '${id}': retry.maxRetries must be a non-negative integer, defaulting to 0`);
          r.maxRetries = 0;
        }
        for (const k of ["baseMs", "maxMs", "totalMs"]) {
          if (r[k] !== undefined && (typeof r[k] !== "number" || r[k] < 0)) {
            console.warn(`config warning: model '${id}': retry.${k} must be a non-negative number, ignored`);
            delete r[k];
          }
        }
        if (r.multiplier !== undefined && (typeof r.multiplier !== "number" || r.multiplier < 1)) {
          console.warn(`config warning: model '${id}': retry.multiplier must be a number >= 1, defaulting to 2`);
          r.multiplier = 2;
        }
      }
    }
  }

  // ---- 2. presets map -----------------------------------------------------
  for (const [id, p] of Object.entries(cfg.presets)) {
    if (!p || typeof p !== "object") {
      console.warn(`config warning: preset '${id}': invalid entry dropped`);
      delete cfg.presets[id];
      continue;
    }
    if (Array.isArray(p.models)) {
      // NEW shape: normalize model refs to {model, weight} objects. A ref may
      // name a MODEL id or another PRESET id (preset-of-presets): both
      // namespaces are legal, unknown ids are dropped.
      const refs = [];
      for (const ref of p.models) {
        let mref = null;
        let weight = 1;
        if (typeof ref === "string") {
          mref = ref;
        } else if (ref && typeof ref === "object" && typeof ref.model === "string") {
          mref = ref.model;
          weight = typeof ref.weight === "number" && ref.weight > 0 ? ref.weight : 1;
        } else {
          console.warn(`config warning: preset '${id}': invalid model entry dropped`);
          continue;
        }
        if (!cfg.models[mref] && !cfg.presets[mref]) {
          console.warn(`config warning: preset '${id}': dropping unknown model '${mref}'`);
          continue;
        }
        refs.push({ model: mref, weight });
      }
      p.models = refs;
      // v3 narrows the strategy to 'fallback' alone, and that value is absent
      // from the legacy allowlist below: rewriting it here would turn every
      // converted preset into an affinity preset at the first reload, so a v3
      // config keeps its strategy.
      if (!isV3 && p.strategy !== "affinity" && p.strategy !== "failover" && p.strategy !== "weighted") {
        console.warn(`config warning: preset '${id}': unknown strategy '${p.strategy}', defaulting to affinity`);
        p.strategy = "affinity";
      }
      if ("params" in p && (p.params === null || typeof p.params !== "object")) {
        console.warn(`config warning: preset '${id}': params must be an object, ignored`);
        delete p.params;
      }
      continue;
    }
    if (Array.isArray(p.members)) {
      // path B: DESIGN-PRESETS era. ONE model per preset; providers keep their
      // own per-provider upstream (mixed upstreams are exactly the target shape).
      const valid = [];
      for (const mem of p.members) {
        const bid = typeof mem === "string" ? mem : mem && typeof mem.backend === "string" ? mem.backend : null;
        const b = bid ? backendById.get(bid) : null;
        if (!b) {
          console.warn(`config warning: preset '${id}': dropping unknown backend member '${bid || mem}'`);
          continue;
        }
        if (!b.model) {
          console.warn(`config warning: preset '${id}': backend '${bid}' has no model (upstream), dropped`);
          continue;
        }
        valid.push({ backend: bid, upstream: b.model });
      }
      // Model id = the preset id; reuse an existing model only if its provider
      // backend-id set equals the synthesized set (order-insensitive).
      let modelId = id;
      const existing = cfg.models[modelId];
      if (existing) {
        const existingSet = new Set((existing.providers || []).map((pr) => pr.backend));
        const synthSet = new Set(valid.map((v) => v.backend));
        const equal =
          existingSet.size === synthSet.size && [...synthSet].every((bid) => existingSet.has(bid));
        if (!equal) {
          modelId = `${id}-legacy`;
          console.warn(`config warning: preset '${id}': existing model '${id}' provider set differs; synthesized model '${modelId}'`);
        }
      }
      if (!cfg.models[modelId]) {
        const nm = { providers: valid };
        if (typeof p.affinityPool === "number") nm.affinityPool = p.affinityPool;
        cfg.models[modelId] = nm;
      }
      // Strategy: the original, EXCEPT legacy weighted -> affinity (weights over
      // backends are not representable at the model layer; declared order kept).
      let strategy = p.strategy || "affinity";
      if (strategy !== "affinity" && strategy !== "failover" && strategy !== "weighted") {
        console.warn(`config warning: preset '${id}': unknown strategy '${strategy}', defaulting to affinity`);
        strategy = "affinity";
      }
      if (strategy === "weighted") {
        console.warn(`config warning: preset '${id}': legacy weighted preset collapsed to affinity (weights not representable at the model layer)`);
        strategy = "affinity";
      }
      const np = { strategy, models: [{ model: modelId, weight: 1 }] };
      if (p.params && typeof p.params === "object") np.params = p.params;
      if (p.meta && typeof p.meta === "object") np.meta = p.meta;
      cfg.presets[id] = np;
      if ("params" in p && (p.params === null || typeof p.params !== "object")) {
        console.warn(`config warning: preset '${id}': params must be an object, ignored`);
      }
      continue;
    }
    console.warn(`config warning: preset '${id}': no models/members, entry dropped`);
    delete cfg.presets[id];
  }

  // ---- 3. path A models with no preset get an identity preset ------------
  for (const id of legacySynthesized) {
    if (!cfg.presets[id]) {
      cfg.presets[id] = { strategy: "affinity", models: [{ model: id, weight: 1 }] };
    }
  }

  // ---- 4. derived read-only preset members --------------------------------
  // members = flattened provider backends of the preset's (recursively
  // expanded) model list, first-occurrence order, deduped. Nested preset refs
  // contribute their models' providers in place.
  const flattenPresetModels = (pid, seenPresets) => {
    const out = [];
    const p = cfg.presets[pid];
    if (!p || seenPresets.has(pid)) return out;
    const nextSeen = new Set(seenPresets).add(pid);
    for (const ref of p.models || []) {
      // Identity case (path B): models: [{model: <same id>}] where the id is
      // BOTH preset and model -> the ref is a MODEL, not a nested preset.
      if (ref.model === pid && cfg.models[pid]) {
        out.push(pid);
        continue;
      }
      if (cfg.presets[ref.model]) {
        out.push(...flattenPresetModels(ref.model, nextSeen));
      } else {
        out.push(ref.model);
      }
    }
    return out;
  };
  for (const [pid, p] of Object.entries(cfg.presets)) {
    const members = [];
    const seen = new Set();
    for (const mid of flattenPresetModels(pid, new Set())) {
      const m = cfg.models[mid];
      if (!m) continue;
      for (const pr of modelDeployments(m, mid, backendById)) {
        if (seen.has(pr.backend)) continue;
        seen.add(pr.backend);
        members.push({ backend: pr.backend, weight: 1 });
      }
    }
    p.members = members;
  }

  // ---- 5. collision + params validation warnings --------------------------
  for (const mid of Object.keys(cfg.models)) {
    if (cfg.presets[mid]) {
      warnOnce(collisionWarned, mid, `config warning: model id '${mid}' collides with a preset id; the preset wins at request time`);
    }
  }
  const warnModelInParams = (layer, where) => {
    if (layer && typeof layer === "object" && "model" in layer) {
      console.warn(`config warning: params layer ${where} contains reserved 'model' key, ignored`);
    }
  };
  warnModelInParams(cfg.params, "global");
  for (const b of cfg.backends || []) if (b && typeof b === "object") warnModelInParams(b.params, `backend ${b.id}`);
  for (const [mid, m] of Object.entries(cfg.models)) {
    warnModelInParams(m.params, `model ${mid}`);
    for (const pr of modelDeployments(m, mid, backendById)) warnModelInParams(pr.params, `provider ${mid}/${pr.backend}`);
  }
  for (const [pid, p] of Object.entries(cfg.presets)) warnModelInParams(p.params, `preset ${pid}`);
  // dropParams union warnings: every dropped public/request key is a silent
  // no-op for requests that use it.
  const warnedDrop = new Set();
  for (const [pid, p] of Object.entries(cfg.presets)) {
    for (const ref of p.models || []) {
      const m = cfg.models[ref.model];
      if (!m) continue;
      for (const pr of modelDeployments(m, ref.model, backendById)) {
        const b = backendById.get(pr.backend);
        if (!b) continue;
        const drop = new Set([...(b.dropParams || []), ...(pr.dropParams || [])]);
        for (const k of drop) {
          warnOnce(
            warnedDrop,
            `${pid}|${ref.model}|${pr.backend}|${k}`,
            `config warning: preset ${pid} model ${ref.model} provider ${pr.backend} drops param ${k} - requests using it will not receive it`
          );
        }
      }
    }
  }
  return cfg;
}

// ---- routing ---------------------------------------------------------------

function sessionKeyOf(req, body) {
  const h = req.headers;
  return (
    h["x-session-affinity"] ||
    h["x-session-id"] ||
    (body && (body.session_id || body.sessionId || body.user)) ||
    "no-session"
  );
}

function hashStr(s) {
  return crypto.createHash("sha256").update(String(s)).digest().readUInt32BE(0);
}

// Recursively expand a preset's models list into a flat model-id list.
// - A preset's models array may contain MODEL ids and/or PRESET ids
//   (preset-of-presets). Preset refs are expanded in place, preserving
//   declared order. A nested preset contributes ONLY its model list (flattened
//   recursively); its own strategy/affinityPool/weights are ignored for
//   ordering - the TOP-LEVEL preset's strategy governs the expanded list.
// - Weights: the top-level ref carries the weight (nested preset ids use the
//   weight of the position they were referenced from). Expansion itself is
//   weight-less; weights only matter when the top-level strategy is weighted.
// - Cycle handling: a visited set per expansion. On a cycle (a preset already
//   in the current expansion path is referenced again), console.warn ONCE and
//   stop expanding that path; the id that caused the cycle is kept as a MODEL
//   id if it resolves to a model, else dropped.
// - Unknown refs (neither preset nor model) are dropped (they are already
//   warned about at normalizeConfig time; this is a defensive re-check).
// Returns {modelIds, modelRefs} where modelRefs are the TOP-LEVEL refs
// (weights kept, expanded ordering).
function expandPresetModels(cfg, topPreset, topId) {
  const presets = cfg.presets || {};
  const models = cfg.models || {};
  const modelIds = [];
  const modelRefs = [];

  const expand = (ref, visited) => {
    const id = typeof ref === "string" ? ref : ref && ref.model;
    if (!id) return;
    const isPreset = !!presets[id];
    const isModel = !!models[id];
    if (!isPreset && !isModel) return; // unknown id (defensive; normalizeConfig warns)
    if (isPreset) {
      if (visited.has(id)) {
        const pathKey = [...visited, id].join("->");
        if (!cycleWarned.has(pathKey)) {
          cycleWarned.add(pathKey);
          console.warn(
            `config warning: preset-of-presets cycle detected at '${id}' (path: ${[...visited, id].join(" -> ")}); keeping it as a model id if it resolves, else dropping`
          );
        }
        if (isModel) modelIds.push(id); // keep as model if it resolves, else drop
        return;
      }
      const sub = presets[id];
      const subRefs = (sub.models || []).map((m) => (typeof m === "string" ? { model: m, weight: 1 } : m));
      for (const sr of subRefs) expand(sr, new Set(visited).add(id));
    } else {
      modelIds.push(id);
      modelRefs.push({ model: id, weight: ref && typeof ref === "object" ? ref.weight || 1 : 1 });
    }
  };

  for (const ref of topPreset.models || []) {
    const m = typeof ref === "string" ? { model: ref, weight: 1 } : ref;
    const id = m.model;
    // Identity case: a preset whose models list references its OWN colliding
    // id (path B synthesis produces {models: [{model: <same id>}]} where the
    // id is BOTH preset and model). That is a MODEL ref, not a nested preset:
    // treat it as a model without recursion or cycle warning.
    if (id === topId && models[id]) {
      modelIds.push(id);
      modelRefs.push({ model: id, weight: m.weight || 1 });
      continue;
    }
    expand(m, new Set([topId]));
  }
  return { modelIds, modelRefs };
}

// Build the flattened, backend-deduped ordered attempt list for a request:
// - preset-or-implicit-model resolution: a preset id uses its strategy; a bare
//   model id acts as an implicit single-model preset
//   {strategy: "affinity", models: [id], affinityPool: 1}.
// - preset level: order the candidate MODELS per strategy. affinity picks
//   models[hash(sessionKey) % pool]; failover keeps declared order; weighted
//   picks weighted-random (rng injectable for tests), rest sorted by weight
//   desc (stable sort keeps declared-order tiebreak).
// - model level: each model orders its PROVIDERS by affinity (pool =
//   min(model.affinityPool || 1, providers.length); primary =
//   providers[hash(sessionKey) % pool]; rest in declared order).
// - flatten in model order into {model, backend, upstream} attempts, deduped
//   by backend id (first occurrence wins, keeping that occurrence's model +
//   upstream) so a shared account is never retried twice in one request.
// hashStr and the modulo arithmetic are UNCHANGED from the preset-only era so
// affinity routing stays byte-identical for identical effective configs.
// Returns null for unknown ids, else {kind, presetId, preset, models, primary,
// ordered} where preset is the normalized preset object or null (implicit
// model), primary is ordered[0] or null, ordered is the attempt list. A preset
// with zero valid models returns ordered: [] (not null) so handleChat can
// produce the specific 404 message.
// Preset-of-presets: when a preset's models list contains another preset id,
// that nested preset's model list is EXPANDED in place (recursively, with
// cycle detection). The TOP-LEVEL preset's strategy governs ordering over the
// expanded list; a nested preset's own strategy is ignored (only its model
// list is taken). See expandPresetModels() above.
function candidates(cfg, requestedId, sessionKey, rng = Math.random) {
  const preset = (cfg.presets || {})[requestedId] || null;
  let kind;
  let modelIds;
  let modelRefs;
  if (preset) {
    kind = "preset";
    // Expand nested preset refs to a flat model list (cycle-safe).
    const expanded = expandPresetModels(cfg, preset, requestedId);
    modelIds = expanded.modelIds;
    modelRefs = expanded.modelRefs;
  } else if ((cfg.models || {})[requestedId]) {
    kind = "model";
    modelIds = [requestedId];
    modelRefs = [{ model: requestedId, weight: 1 }];
  } else {
    return null;
  }

  // Preset-level model ordering over the (possibly expanded) model list.
  let orderedModels;
  if (!preset) {
    orderedModels = modelIds; // implicit model: single candidate
  } else if (preset.strategy === "failover") {
    orderedModels = modelIds; // declared order
  } else if (preset.strategy === "weighted") {
    const weights = modelRefs.map((m) => m.weight || 1);
    const total = weights.reduce((s, w) => s + w, 0);
    const r = (typeof rng === "function" ? rng() : Math.random()) * total;
    let pickIdx = modelIds.length - 1;
    let acc = 0;
    for (let i = 0; i < modelIds.length; i++) {
      acc += weights[i];
      if (r < acc) {
        pickIdx = i;
        break;
      }
    }
    const rest = modelRefs
      .map((m, i) => ({ id: modelIds[i], weight: m.weight || 1 }))
      .filter((_, i) => i !== pickIdx)
      .sort((a, b) => (b.weight || 1) - (a.weight || 1)); // stable -> declared-order tiebreak
    orderedModels = [modelIds[pickIdx], ...rest.map((x) => x.id)];
  } else {
    // affinity (default)
    const pool = Math.min(preset.affinityPool || 1, modelIds.length);
    const primaryIdx = hashStr(sessionKey) % pool;
    orderedModels = [modelIds[primaryIdx], ...modelIds.filter((_, i) => i !== primaryIdx)];
  }

  // Model-level deployment ordering + flatten + backend dedup. The shape is
  // dispatched PER MODEL so a converted config (which is entirely `deployments`)
  // is not forced through the legacy `providers`/`affinityPool` read, while an
  // old-shape entry that never went through the bridge keeps working.
  const ordered = [];
  // Two dedup contracts meet here. A legacy entry has no upstream identity of its own beyond
  // the account, so it keeps backend-only dedup and its routing stays byte-identical. A v3
  // entry names a PHYSICAL deployment, and one account may hold several (different upstreams,
  // different dialect settings), so the pair is the unit: cooling is scoped to the pair
  // (see ROUTES/routeKey), and collapsing the pair here would make that granularity
  // unreachable - the second deployment could never be tried at all.
  const seen = new Set();
  const backendById = backendsById(cfg);
  for (const mid of orderedModels) {
    const m = (cfg.models || {})[mid];
    if (!m) continue;
    const providers = modelDeployments(m, mid, backendById);
    if (providers.length === 0) continue;
    const providerOrder = Array.isArray(m.deployments)
      ? orderDeployments(providers, sessionKey)
      : (() => {
          const pool = Math.min(modelPool(m), providers.length);
          const primaryIdx = hashStr(sessionKey) % pool;
          return [providers[primaryIdx], ...providers.filter((_, i) => i !== primaryIdx)];
        })();
    const perDeployment = Array.isArray(m.deployments);
    for (const pr of providerOrder) {
      const key = perDeployment ? pr.backend + "\u0000" + pr.upstream : pr.backend;
      if (seen.has(key)) continue;
      seen.add(key);
      ordered.push({ model: mid, backend: pr.backend, upstream: pr.upstream });
    }
  }

  return {
    kind,
    presetId: requestedId,
    preset,
    models: modelIds,
    primary: ordered.length ? ordered[0] : null,
    ordered,
  };
}

// ---- upstream error bodies, correlation ids, redaction ----------------------
//
// An error body is diagnostic: it explains a failure to an operator and to the
// caller, and it is never worth waiting on. Before this, an upstream that sent
// headers and then never ended its error body held the whole request open
// indefinitely. The read is now bounded twice over - 16 KiB of bytes and ONE
// five-second deadline for the whole read - and the outcome is recorded, so a
// blank rejection can be told apart from an unreadable one.

const ERROR_BODY_MAX_BYTES = 16384;
const ERROR_BODY_TIMEOUT_MS = 5000;
const ERROR_MSG_CAP = 512;
const CORRELATION_ID_CAP = 160;
// Only these three are captured. Upstreams echo an arbitrary set of headers, and a
// diagnostic envelope is not a place to launder the rest of them.
const CORRELATION_HEADERS = ["x-request-id", "request-id", "cf-ray"];

// Read a bounded prefix of an error body under one total deadline. Returns
// {body, errorBodyState, errorBodyBytes}; errorBodyBytes counts bytes actually
// captured, never an advertised length. States: present | empty | truncated |
// timeout | read_error.
async function readUpstreamError(response, { maxBytes = ERROR_BODY_MAX_BYTES, timeoutMs = ERROR_BODY_TIMEOUT_MS } = {}) {
  const out = { body: "", errorBodyState: "empty", errorBodyBytes: 0 };
  if (!response || !response.body) return out; // no body at all IS an empty body
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let body = "";
  let bytes = 0;
  let truncated = false;
  let readError = null;
  let timer = null;
  const work = (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        if (!value || !value.length) continue;
        const room = maxBytes - bytes;
        if (room <= 0) { truncated = true; return; }
        if (value.length > room) {
          body += decoder.decode(value.subarray(0, room), { stream: true });
          bytes += room;
          truncated = true;
          return;
        }
        body += decoder.decode(value, { stream: true });
        bytes += value.length;
        if (bytes >= maxBytes) {
          // Exactly at the limit: one more read is the only way to tell a body that
          // fits from one that was cut, so ask before calling it complete.
          const next = await reader.read().catch(() => ({ done: true }));
          if (!next.done) truncated = true;
          return;
        }
      }
    } catch (e) {
      readError = e;
    }
  })();
  const deadline = new Promise((resolve) => { timer = setTimeout(() => resolve("timeout"), timeoutMs); });
  const outcome = await Promise.race([work.then(() => "read"), deadline]);
  if (timer !== null) { clearTimeout(timer); timer = null; }
  if (outcome === "timeout") {
    out.errorBodyState = "timeout";
    // Fire-and-forget release: the deadline is a promise to the CALLER, so waiting
    // on cancellation could cost the same five seconds again.
    try {
      const c = reader.cancel("error body read deadline");
      if (c && typeof c.catch === "function") c.catch(() => {});
    } catch { /* release is best-effort */ }
  } else if (readError) {
    out.errorBodyState = "read_error";
  } else if (truncated) {
    out.errorBodyState = "truncated";
  } else if (bytes > 0) {
    out.errorBodyState = "present";
  }
  body += decoder.decode(); // flush a partial multi-byte tail; never throws
  out.errorBodyBytes = bytes;
  out.body = bytes > 0 ? body : "";
  return out;
}

// The request ids an upstream returned, if it returned any we allow.
function correlationIdsOf(headers) {
  const out = {};
  if (!headers || typeof headers.get !== "function") return out;
  for (const name of CORRELATION_HEADERS) {
    const v = headers.get(name);
    if (typeof v === "string" && v.trim()) out[name] = v.trim().slice(0, CORRELATION_ID_CAP);
  }
  return out;
}

function parseJsonMaybe(text) {
  if (typeof text !== "string") return null;
  const t = text.trim();
  if (!t || (t[0] !== "{" && t[0] !== "[")) return null;
  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
}

// Configured credential VALUES must never reach a caller, a log line, or a durable
// record. Values shorter than MIN_SECRET_REDACT are skipped: a fixture or stub key like
// "k" would otherwise redact the alphabet. The floor used to be 8, which let a genuinely
// short credential through unredacted (review 2026-10-08, finding 3); it is now 4, and a
// value under 8 characters is replaced only as a whole word so that removing it cannot
// corrupt unrelated text around it.
const MIN_SECRET_REDACT = 4;
function secretValues(cfg, req) {
  const out = [];
  const add = (v) => {
    if (typeof v === "string" && v.trim().length >= MIN_SECRET_REDACT && !out.includes(v)) out.push(v);
  };
  if (cfg) {
    if (cfg.masterKeyEnv) add(process.env[cfg.masterKeyEnv]);
    if (cfg.uiPasswordEnv) add(process.env[cfg.uiPasswordEnv]);
    for (const b of cfg.backends || []) if (b && b.apiKeyEnv) add(process.env[b.apiKeyEnv]);
  }
  const m = /^Bearer (.+)$/i.exec((req && req.headers && req.headers.authorization) || "");
  if (m) add(m[1].trim());
  return out;
}

function redactSecrets(text, cfg, req) {
  if (typeof text !== "string" || !text) return typeof text === "string" ? text : "";
  let out = text;
  for (const v of secretValues(cfg, req)) {
    if (v.length >= 8) {
      out = out.split(v).join("[redacted]");
      continue;
    }
    const esc = v.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out = out.replace(new RegExp(`(?<![\\w-])${esc}(?![\\w-])`, "g"), "[redacted]");
  }
  return out;
}

// Correlation headers are upstream-controlled and durable (attemptsSummary) as well as
// served by the open detail endpoint, so an upstream is free to echo a credential into
// one: they get the same redaction and the same cap as every other upstream text.
function redactIds(ids, cfg, req) {
  const out = {};
  for (const [k, v] of Object.entries(ids || {})) out[k] = redactSecrets(String(v), cfg, req).slice(0, CORRELATION_ID_CAP);
  return out;
}

// Keep only primitive, redacted, capped fields from an upstream error body: the
// provider's own explanation survives, nothing structural or nested does.
function safeErrorFields(upstream, cfg, req) {
  const parsed = parseJsonMaybe(upstream);
  let e = {};
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    const d = parsed.error;
    if (d && typeof d === "object" && !Array.isArray(d)) e = { ...d };
    else if (typeof d === "string") e = { message: d };
    else if (typeof parsed.message === "string") e = { message: parsed.message };
  }
  for (const k of Object.keys(e)) {
    const v = e[k];
    if (typeof v === "string") e[k] = redactSecrets(v, cfg, req).slice(0, ERROR_MSG_CAP);
    else if (v !== null && typeof v !== "number" && typeof v !== "boolean") delete e[k];
  }
  return e;
}

// One builder for every terminal CHAT failure: always nonempty, always valid JSON,
// always the same shape. An upstream that explained itself keeps its own words
// (redacted and capped); every other case gets the router's own wording.
function terminalChatError(o) {
  const opts = o || {};
  const e = safeErrorFields(opts.upstream, opts.cfg, opts.req);
  if (typeof e.message !== "string" || !e.message.trim()) {
    const text = redactSecrets(String(opts.message == null ? "" : opts.message).trim(), opts.cfg, opts.req).slice(0, ERROR_MSG_CAP);
    e.message = text || "the request could not be served";
  }
  if (!e.type) e.type = opts.type || "router_error";
  if (e.code == null) e.code = opts.code == null ? null : opts.code;
  // callId, backend and upstreamStatus describe THIS call, so the router's own values win:
  // an upstream whose error object carries those keys must not be able to forge or
  // overwrite them, and whatever it did supply is bounded (review finding 11).
  e.callId = opts.callId
    ? String(opts.callId).slice(0, CORRELATION_ID_CAP)
    : (typeof e.callId === "string" ? e.callId.slice(0, CORRELATION_ID_CAP) : null);
  e.backend = opts.backend || (typeof e.backend === "string" ? e.backend.slice(0, ERROR_MSG_CAP) : null);
  e.upstreamStatus = opts.upstreamStatus == null
    ? (e.upstreamStatus == null ? null : e.upstreamStatus)
    : opts.upstreamStatus;
  if (e.nextAvailableAt == null && opts.nextAvailableAt != null) e.nextAvailableAt = opts.nextAvailableAt;
  if (typeof e.type === "string") e.type = e.type.slice(0, 64);
  if (typeof e.code === "string") e.code = e.code.slice(0, 64);
  return JSON.stringify({ error: e });
}

// ---- durable per-attempt outcomes ------------------------------------------

const ATTEMPTS_SUMMARY_CAP = 32;

// Body-free, prompt-free and header-free: a durable attempt outcome carries the
// DECISION and its consequences, never the request or the upstream's words.
function attemptOutcome(a) {
  return {
    backend: a.attempt ? a.attempt.backend ?? null : null,
    upstream: a.attempt ? a.attempt.upstream ?? null : null,
    model: a.attempt ? a.attempt.model ?? null : null,
    status: a.status ?? null,
    kind: a.kind || null,
    decision: a.decision || null,
    latencyMs: a.latencyMs ?? 0,
    retries: a.retries ?? 0,
    errorBodyState: a.errorBodyState || null,
    errorBodyBytes: a.errorBodyBytes ?? 0,
    cooldownScope: a.cooldownScope || null,
    nextAvailableAt: a.nextAvailableAt ?? null,
    requestIds: a.requestIds && Object.keys(a.requestIds).length ? a.requestIds : null,
  };
}

// Keep the first 31 outcomes and the last: the opening attempts explain why the
// request had to move, the final one explains how it ended. What was dropped is
// counted rather than silently lost.
function capAttemptsSummary(list) {
  const all = Array.isArray(list) ? list : [];
  if (all.length <= ATTEMPTS_SUMMARY_CAP) return { attemptsSummary: all, attemptsOmitted: 0 };
  return {
    attemptsSummary: [...all.slice(0, ATTEMPTS_SUMMARY_CAP - 1), all[all.length - 1]],
    attemptsOmitted: all.length - ATTEMPTS_SUMMARY_CAP,
  };
}

// An effectively-infinite deadline means "until an operator uncools it"; that is
// reported as null (no automatic recovery) rather than as a 285-million-year date.
function finiteDeadline(v) {
  return typeof v === "number" && Number.isFinite(v) && v < 1e15 ? v : null;
}

// Which scope did this failure actually charge? Read from the state the failure
// just wrote instead of re-deriving it from the kind.
function cooldownScopeOf(attempt, kind, now = Date.now()) {
  if (kind === "upstream_rejection") return { cooldownScope: "session", nextAvailableAt: null };
  const acct = health.get(attempt.backend);
  if (acct && acct.state === "cooling" && acct.nextAvailableAt > now) {
    return { cooldownScope: "account", nextAvailableAt: finiteDeadline(acct.nextAvailableAt) };
  }
  const row = ROUTES.get(routeKey(attempt));
  if (row && row.nextAvailableAt > now) return { cooldownScope: "route", nextAvailableAt: finiteDeadline(row.nextAvailableAt) };
  return { cooldownScope: null, nextAvailableAt: null };
}

function newError(status, body) {
  return { status, body };
}

// ---- layered parameters ----------------------------------------------------

// Merge request parameters across the config layers, highest priority last:
// global < backend.params < deployment.params < model.params < request body.
// There is NO preset layer in v3: `preset` is still a parameter so existing
// callers keep working, but its params are deliberately not read - a preset
// selects an ordered model list and a deployment holds the per-provider
// dialect. `model` is RESERVED: config-layer model keys are ignored and the
// forwarded `model` is ALWAYS the deployment's upstream. Dialect handling runs
// AFTER the merge, on the PUBLIC/request key space: dropParams (union of
// backend + deployment, the legacy provider field still honored) deletes keys
// first, then paramMap renames the survivors, then the developer-role
// translation (backend field, special-cased). Pure: never mutates `body`.
function buildPayload(body, cfg, preset, model, deployment, backend) {
  const out = {};
  Object.assign(out, cfg.params || {}); // lowest layer
  Object.assign(out, backend.params || {});
  Object.assign(out, deployment.params || {});
  Object.assign(out, model.params || {});
  // A pre-v3 config keeps the documented merge order, whose preset layer sits between
  // the model and the request. The v3 contract has no preset-level params layer at all,
  // so this applies only to a model that is still a provider list (v3 models keep
  // `deployments` in the normalized form).
  if (preset && preset.params && !model.deployments) Object.assign(out, preset.params);
  delete out.model; // reserved in config layers
  Object.assign(out, body); // request body wins
  out.model = deployment.upstream; // always
  // An explicit thinking:disabled is authoritative for the reasoning group:
  // strip any config-default reasoning_effort so upstream never receives the
  // contradictory pair (disabled thinking + effort set). 2026-09-09 fix.
  if (out.thinking && out.thinking.type === 'disabled' && 'reasoning_effort' in out) {
    delete out.reasoning_effort;
  }
  // Dialect: drop FIRST (public/request key space), then rename survivors.
  const drop = new Set([...(backend.dropParams || []), ...(deployment.dropParams || [])]);
  for (const k of drop) delete out[k];
  for (const [from, to] of Object.entries(deployment.paramMap || {})) {
    if (from in out) {
      out[to] = out[from];
      delete out[from];
    }
  }
  // Role translation last (unchanged field-flag behavior).
  if (backend.translateDeveloperRole && Array.isArray(out.messages)) {
    out.messages = out.messages.map((m) => (m && m.role === "developer" ? { ...m, role: "system" } : m));
  }
  return out;
}

// ---- request forwarding ----------------------------------------------------

// The session header we send upstream must be non-empty: OpenCode Go answers a
// blank/absent x-opencode-session with 400 MissingSessionID, which the router
// then classifies as a non-retryable client error and charges to the request.
// The rule is explicit rather than a bare || chain because an empty or
// whitespace-only header is falsy but PRESENT, and a blank sessionKey would
// fall through the chain unchanged. Fall back to the router's resolved session
// key, then the same 'no-session' literal the resolver uses.
function sessionHeaderFor(incomingHeaders, sessionKey) {
  const raw = incomingHeaders && incomingHeaders["x-opencode-session"];
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (trimmed) return trimmed;
  } else if (raw) {
    return String(raw);
  }
  const key = typeof sessionKey === "string" ? sessionKey.trim() : sessionKey;
  return key || "no-session";
}

async function forward(req, cfg, backend, payload, incomingHeaders, stream, sessionKey, callerSignal) {
  const key = process.env[backend.apiKeyEnv];
  const upstream = backend.baseURL.replace(/\/$/, "") + "/chat/completions";

  const outHeaders = {
    "content-type": "application/json",
    authorization: `Bearer ${key}`,
    accept: stream ? "text/event-stream" : "application/json",
    // Console Go refuses service without a session id (400 MissingSessionID)
    // and OpenCode Go's docs require an identifiable client UA for abuse
    // monitoring. Prefer the client's own session header, else reuse the
    // router's resolved session key (x-session-affinity -> x-session-id ->
    // user -> no-session) so per-conversation routing/caching still works.
    "x-opencode-session": sessionHeaderFor(incomingHeaders, sessionKey),
    "user-agent": "switchblade/1.0",
  };
  // Forward prompt-cache headers verbatim.
  for (const h of ["x-cache-key", "cachekey", "set-cache-key"]) {
    if (incomingHeaders[h]) outHeaders[h] = incomingHeaders[h];
  }

  const payloadText = JSON.stringify(payload);

  // Debug wire format: log the exact outgoing request body (redacted - never
  // print key values; the payload holds messages + params only) so operators
  // can verify what the upstream actually receives.
  if (process.env.ROUTER_DEBUG_WIRE === "1") {
    try {
      const p = JSON.parse(payloadText);
      const dbg = { ...p };
      if (Array.isArray(dbg.messages)) dbg.messages = dbg.messages.map((m) => ({ role: m.role, content: String(m.content || "").slice(0, 80) }));
      console.error(`[${new Date().toISOString()}] [wire] ${backend.id} -> ${dbg.model} params=${JSON.stringify(Object.keys(dbg).filter((k) => !["messages", "model", "stream"].includes(k)))} ${JSON.stringify(Object.fromEntries(Object.entries(dbg).filter(([k]) => !["messages", "model", "stream"].includes(k))))}`);
    } catch {}
  }

  // Connect/header timeout only: bound how long we wait for the upstream to
  // send response HEADERS (TTFT), NOT the whole body. A wall-clock signal on
  // fetch() is also tied to the response body in undici, so a slow-but-alive
  // SSE relay used to be aborted mid-stream exactly at timeoutMs (see router.log
  // "relay error: The operation was aborted due to timeout" on long thinking
  // generations). The body is instead protected by an idle/stall watchdog in
  // relay() (idleTimeoutMs): it only fires when actual silence exceeds the
  // bound, so healthy long streams run to completion while stuck ones still
  // fail.
  const connectTimeoutMs = backend.connectTimeoutMs ?? backend.timeoutMs ?? cfg.connectTimeoutMs ?? cfg.timeoutMs ?? 45000;
  const connectController = new AbortController();
  const connectTimer = setTimeout(() => connectController.abort(), connectTimeoutMs);
  // The caller's own cancellation joins the header timeout, so work for a caller who has
  // already gone stops at the socket instead of running on behind them.
  const requestSignal = callerSignal ? AbortSignal.any([connectController.signal, callerSignal]) : connectController.signal;
  let upstreamRes;
  try {
    upstreamRes = await fetch(upstream, {
      method: "POST",
      headers: outHeaders,
      body: payloadText,
      signal: requestSignal,
    });
  } catch (e) {
    clearTimeout(connectTimer);
    // No headers ever came back, and the stage marker is what tells the settle path WHY.
    // A HEADER TIMEOUT (our own connect abort fired: the endpoint accepted the connection and
    // then stayed silent) is a property of the ROUTE - the gateway is up and one upstream is
    // stalling - which is what z17 pins. A TRANSPORT failure (DNS, refused, TLS, reset before
    // any headers) means the endpoint itself is unreachable, which is equally true of every
    // route on that account, so charging one route would make each sibling rediscover the same
    // dead endpoint in turn. The HTTP status is 0 for both, so the number cannot carry this.
    const err = newError(0, String(e && e.message));
    // A caller who left is not evidence about the provider at all, so it gets its own stage
    // rather than being folded into "the endpoint is unreachable".
    err.stage = callerSignal && callerSignal.aborted
      ? "caller_aborted"
      : connectController.signal.aborted ? "header_timeout" : "connect";
    return { err };
  }
  clearTimeout(connectTimer); // headers arrived: the wall clock no longer applies to the body

  if (!upstreamRes.ok) {
    // Bounded and deadline-guarded: an upstream that never ends its error body must
    // not hold the caller's request open (see readUpstreamError).
    const eb = await readUpstreamError(upstreamRes, {
      maxBytes: cfg.errorBodyMaxBytes ?? ERROR_BODY_MAX_BYTES,
      timeoutMs: cfg.errorBodyTimeoutMs ?? ERROR_BODY_TIMEOUT_MS,
    });
    const err = newError(upstreamRes.status, eb.body);
    err.errorBodyState = eb.errorBodyState;
    err.errorBodyBytes = eb.errorBodyBytes;
    err.requestIds = redactIds(correlationIdsOf(upstreamRes.headers), cfg, req);
    return { err, statusFromUpstream: upstreamRes.status };
  }
  return { res: upstreamRes };
}

// ---- HTTP handlers ---------------------------------------------------------

async function handleChat(req, res, cfg, bodyText) {
  const started = Date.now();
  let body;
  try {
    body = JSON.parse(bodyText);
  } catch {
    const e = newError(400, JSON.stringify({ error: { message: "invalid JSON body", type: "invalid_request_error" } }));
    return writeError(res, e);
  }
  const modelId = body && body.model;
  const stream = !!(body && body.stream);
  if (!modelId) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' not found`, type: "model_not_found" } }));
    return writeError(res, e);
  }

  const sessionKey = sessionKeyOf(req, body);
  // Two scopes from one request, deliberately separate: the RAW session key drives
  // affinity hashing and is forwarded upstream, while the hashed exclusion scope is
  // who may be excluded from which resolution. Keeping them apart is what puts the
  // credential INTO the opaque-rejection bucket without putting it in a header.
  const authScope = authScopeOf(req);
  const exclScope = sessionScopeOf(authScope, sessionKey);
  // One call id per incoming request, shared by the history entry, the detail
  // record, and the x-router-call response header so any request can be
  // referenced by a single short id.
  const callId = newCallId();
  const c = candidates(cfg, modelId, sessionKey);
  if (!c) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' not found`, type: "model_not_found" } }));
    return writeError(res, e);
  }
  if (c.kind === "preset" && c.models.length === 0) {
    const e = newError(404, JSON.stringify({ error: { message: `preset '${modelId}' has no valid models`, type: "model_not_found" } }));
    return writeError(res, e);
  }
  if (c.kind === "model" && c.ordered.length === 0) {
    const e = newError(404, JSON.stringify({ error: { message: `model '${modelId}' has no valid providers`, type: "model_not_found" } }));
    return writeError(res, e);
  }

  // Session provider preference: the route THIS caller's session last succeeded on is
  // promoted inside its model group, so a session stays on the fallback that worked
  // instead of re-walking to a provider that keeps failing. The pin is per RESOLVED
  // MODEL: a preset, a second preset expanding to the same model, and the bare model id
  // are three names for one decision, and keying by the REQUESTED id split one session's
  // history three ways and let it re-walk a provider it had already given up on.
  const declaredHead = c.ordered[0] || null;
  const preferenceSequence = nextRequestSequence();
  const prefTokens = new Map();
  for (const a of c.ordered) {
    if (!prefTokens.has(a.model)) prefTokens.set(a.model, beginSessionRequest(authScope, sessionKey, a.model, preferenceSequence));
  }
  const activePrefTokens = [...prefTokens.values()].filter(Boolean);
  for (const tok of activePrefTokens) c.ordered = orderSessionProviders(c.ordered, tok, Date.now());
  if (activePrefTokens.length) c.primary = c.ordered.length ? c.ordered[0] : null;
  // True when the session's own preference moved a DIFFERENT provider to the front:
  // that is a different story from "the configured order was used as declared".
  const pinnedHead = !!(declaredHead && c.ordered[0] && c.ordered[0] !== declaredHead);

  // Strict eligibility. Every candidate must be serviceable right now: its account
  // must not be cooled (auth / billing / weekly / rate) and its own route must not
  // be cooling from a server-class failure, and this session must not have been
  // refused by it. There is deliberately NO fallback that tries a cooling provider
  // anyway - sending a request to a provider already known to be failing wastes the
  // caller's time and hides the real state behind a bogus "all cooling" story.
  const now = Date.now();
  const manualCooled = new Set(
    c.ordered
      .filter((a) => {
        const h = health.get(a.backend);
        return !!h && h.state === "cooling" && h.lastError && h.lastError.kind === "manual";
      })
      .map((a) => a.backend)
  );
  const pool = [];
  const blocked = {};
  let skippedAhead = 0; // candidates walked past before the first eligible one
  for (const a of c.ordered) {
    const reason = ineligibility(a, exclScope, now);
    if (reason) {
      blocked[reason] = (blocked[reason] || 0) + 1;
      if (!pool.length) skippedAhead++;
    } else pool.push(a);
  }
  // Why the FIRST attempt is the one it is: the session's own preference, a
  // candidate list that was partly ineligible, or the configured order untouched.
  const selectionReason = pool.length === 0 ? null : pinnedHead ? "session_preference" : skippedAhead > 0 ? "cooldown" : "configured";
  // Durable, body-free per-attempt outcomes for this request (see attemptOutcome).
  const attemptsSummary = [];
  let fallbackCount = 0; // cross-provider transitions: a same-provider retry is not a fallback
  let priorBackend = null;
  if (pool.length === 0) {
    const latencyMs = Date.now() - started;
    const detailT = Date.now();
    const allManual = manualCooled.size >= c.ordered.length;
    // The all-manual case keeps its long-standing wording (the dashboard's
    // "uncool to restore" affordance is the actionable one); every other empty
    // pool names the actual blocker counts instead of blaming cooling in general.
    const reasonText = allManual
      ? `all available backends are cooling (${manualCooled.size} manually cooled - uncool to restore)`
      : `no provider is currently eligible for this request (${Object.entries(blocked).map(([k, n]) => `${n} ${k.replace(/_/g, " ")}`).join(", ")})`;
    // Advertise the soonest automatic recovery. A pool held only by manual cools
    // has no automatic recovery, so the header is omitted rather than guessed.
    const recoveryMs = nearestRecoveryMs(c.ordered, exclScope, now);
    if (recoveryMs !== null) res.setHeader("Retry-After", String(Math.max(1, Math.ceil(recoveryMs / 1000))));
    const entry = applyCost(newHistoryEntry({ t: detailT, callId, model: modelId, backend: "none", stream, status: 503, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null, errorKind: null }), cfg);
    recordHistory(entry);
    recordAnalytics(entry);
    // A session-scoped exclusion is not an outage: alert only when something is
    // actually cooling.
    if ((blocked.account_cooling || 0) + (blocked.route_cooling || 0) + manualCooled.size > 0) {
      fireAlert(cfg, "all_cooling", { manualCooled: manualCooled.size });
    }
    recordStats({ backend: "none", model: modelId, status: 503, latencyMs });
    // Detail record even with no attempts: captures the payload + a "no
    // candidates were eligible" note so the empty-pool 503 is inspectable.
    // Always recorded; oversized bodies are stored bounded (see
    // boundedPayload) so the panel never silently 404s.
    recordDetail(detailT, callId, bodyText.length <= DETAIL_MAX_BODY_BYTES ? { ...body } : boundedPayload(body), null);
    finalizeDetail(detailT, {
      status: 503,
      ttftMs: 0,
      latencyMs,
      genMs: 0,
      promptTokens: 0,
      completionTokens: 0,
      reasoningTokens: 0,
      cacheHit: null,
      cacheHitPct: null,
      preview: null,
      errorBody: reasonText,
      noEligibleBackends: true,
      cooldownScope: blocked,
    });
    res.setHeader("x-router-backend", "none");
    res.setHeader("x-router-call", callId);
    const scopeKeys = Object.keys(blocked);
    const msg = terminalChatError({
      message: reasonText,
      type: "router_unavailable",
      // The dominant blocker scope, so a caller can tell "the account is cooling"
      // from "this session was refused by that route" without reading prose.
      code: scopeKeys.length === 1 ? scopeKeys[0] : scopeKeys.length ? "mixed" : "none",
      callId,
      backend: "none",
      nextAvailableAt: recoveryMs === null ? null : now + recoveryMs,
      cfg,
      req,
    });
    return writeError(res, newError(503, msg));
  }

  let lastErr = null;
  let lastTried = null;
  // Failed-attempt tallies: the terminal message may only claim that EVERY
  // provider rejected the request unexplained when that is literally what
  // happened. If an earlier provider failed WITH an explanation (a 500, say) and
  // only the last one was opaque, the universal claim would mislabel the request.
  let callerGone = false;
  let failedAttempts = 0;
  let opaqueFailures = 0;
  // Accounts that proved dead for THIS request only. A 5xx or a refused connection says the
  // endpoint is gone, so the account's remaining deployments are skipped rather than probed
  // in turn - but only here: the siblings stay globally healthy, recoverable, and eligible
  // for every other request (v3 spec T5).
  const deadAccounts = new Set();
  const captureDetail = bodyText.length <= DETAIL_MAX_BODY_BYTES; // size guard: skip huge payloads
  const detailT = Date.now();
  // Request-lifetime cancellation. The caller can vanish at ANY moment, including while the
  // router is still waiting for upstream headers, and nothing above this line observes it.
  // Without this the upstream request runs to completion for nobody and its failure is charged
  // to the provider, so a client that abandons requests cools providers that are healthy.
  const callerAbort = new AbortController();
  const onCallerGone = () => {
    if (res.writableEnded) return; // a FINISHED response closing is not an abandonment
    callerGone = true;
    callerAbort.abort();
  };
  // Both watchers are one-shot on PER-REQUEST emitters, so they cannot accumulate on a
  // keep-alive connection: close fires on every response and consumes the second one.
  req.once("aborted", onCallerGone);
  res.once("close", onCallerGone);
  const disposeCallerWatch = () => {
    req.off("aborted", onCallerGone);
    res.off("close", onCallerGone);
  };
  // Built once per request rather than per attempt: the payload builder and the
  // deployment read both need it, and `modelDeployments` re-resolves the entry.
  const backendById = backendsById(cfg);
  for (let poolIndex = 0; poolIndex < pool.length; poolIndex++) {
    const attempt = pool[poolIndex];
    if (deadAccounts.has(attempt.backend)) continue; // this request already lost that account
    if (priorBackend && priorBackend !== attempt.backend) fallbackCount++;
    priorBackend = attempt.backend;
    lastTried = attempt;
    const backend = backendById.get(attempt.backend);
    const model = (cfg.models || {})[attempt.model];
    const provider = (model && modelDeployments(model, attempt.model, backendById).find((p) => p.backend === attempt.backend && p.upstream === attempt.upstream)) || null;
    if (!backend || !model || !provider) continue; // defensive: candidates built the attempt
    // Take the recovery lease BEFORE any network await (see claimAttempt) and give
    // it back on every exit: a client or opaque failure settles no route state, so
    // nothing else would release it.
    const lease = claimAttempt(attempt, now);
    if (!lease) continue; // another request is already probing this recovering route
    // Eligibility is re-read HERE, at the moment of dispatch, not only when the candidate
    // list was built: an earlier candidate's network work, or another request, can cool this
    // route in between, and dispatching into a live cooldown is the bypass that keeps a
    // session on a provider that keeps failing (review 2026-10-08, finding 1). The lease is
    // passed in so the check exempts EXACTLY the probes this dispatch holds at the generation
    // it claimed: the old coarse "owns some probe" exemption let an attempt holding only a
    // route probe walk into an account another request was already probing.
    const blockedAtDispatch = ineligibility(attempt, exclScope, Date.now(), lease);
    if (blockedAtDispatch) {
      releaseAttempt(lease);
      continue;
    }
    const payload = buildPayload(body, cfg, c.preset, model, provider, backend);
    // Per-model retry: on retryable failures, retry the SAME provider with
    // exponential backoff before falling through to the next candidate. This
    // preserves the session's prompt cache (a fallback backend has zero cached
    // context) and gives chronically-overloaded free models a chance to catch
    // a free slot. Retries NEVER re-cool the backend - markFailure runs once
    // after retries are exhausted, exactly as before.
    const rcfg = (model.retry && typeof model.retry === "object" ? model.retry : null) || { maxRetries: 0 };
    const maxRetries = Math.max(0, Math.floor(rcfg.maxRetries || 0));
    const retryBaseMs = rcfg.baseMs ?? 1000;
    const retryMaxMs = rcfg.maxMs ?? 8000;
    const retryMult = rcfg.multiplier ?? 2;
    const retryTotalMs = rcfg.totalMs ?? 15000;
    let retries = 0;
    let retryWaitedMs = 0;
    let result = null;
    const attemptStarted = Date.now();
    // Failover first: a same-provider retry only makes sense once nothing else in the
    // pool can serve the request. Retrying a struggling provider on the way to the
    // same wall is what kept sessions pinned to a provider that kept failing; moving
    // on costs the prompt cache but gets the answer. The remaining candidates are
    // re-checked before every retry, so one that cooled meanwhile is not a reason to
    // keep waiting.
    const hasEligibleAlternative = () => {
      for (let k = poolIndex + 1; k < pool.length; k++) {
        if (ineligibility(pool[k], exclScope, Date.now()) === null) return true;
      }
      return false;
    };
    while (true) {
      // A cooldown that landed during the backoff stops the retry, with NO exemption for a
      // cooldown this attempt recorded itself: a retry is a fresh dispatch, and dispatching
      // into a live cooldown is the bypass that keeps a session on a provider that keeps
      // failing (review finding 1). The lease still exempts the probe this attempt holds,
      // which is the only thing a retry legitimately carries.
      if (retries > 0) {
        const why = ineligibility(attempt, exclScope, Date.now(), lease);
        if (why) break;
      }
      // Book before EVERY dispatch, first try and retry alike: our own failure moves the
      // row's generation past this attempt's booking, and the retry must still be able to
      // settle what its own failure recorded (see ownsRouteSettlement).
      attempt.dispatchSeq = nextGeneration();
      const bookRoute = ROUTES.get(routeKey(attempt));
      if (bookRoute) bookRoute.dispatchSeq = attempt.dispatchSeq;
      const bookAccount = health.get(attempt.backend);
      if (bookAccount) bookAccount.dispatchSeq = attempt.dispatchSeq;
      result = await forward(req, cfg, backend, payload, req.headers, stream, sessionKey, callerAbort.signal);
      if (result.err && isRetryableError(result.err.status, result.err.body) && retries < maxRetries && !hasEligibleAlternative()) {
        const retryCount = retries + 1;
        const waitMs = Math.min(retryBaseMs * retryMult ** (retryCount - 1), retryMaxMs);
        const budgetExceeded = retryWaitedMs + waitMs > retryTotalMs;
        if (budgetExceeded) break;
        retries = retryCount;
        retryWaitedMs += waitMs;
        await sleep(waitMs);
        continue;
      }
      break;
    }
    const attemptRec = {
      backend: attempt.backend,
      upstream: attempt.upstream || provider.upstream || null,
      model: attempt.model,
      startedAt: attemptStarted,
      status: result.err ? (result.err.status || 0) : 200,
      latencyMs: Date.now() - attemptStarted,
      // Redacted when RECORDED: this body is durable and is served by the open detail
      // endpoint, so an upstream echoing a credential is never written at all.
      errorBody: result.err ? (result.err.body ? redactSecrets(result.err.body, cfg, req) : (result.err.body || null)) : null,
      retry: typeof attempt.retry === "number" ? attempt.retry : (attempt.attemptIndex != null ? attempt.attemptIndex : null),
    };
    // Always record the attempt in the deep-dive store; oversized bodies are
    // stored bounded (see boundedPayload) so the panel never silently 404s.
    recordDetail(detailT, callId, captureDetail ? payload : boundedPayload(payload), attemptRec);
    if (result.err) {
      lastErr = result.err;
      const errKind = classify(result.err.status, result.err.body, result.err.errorBodyState);
      // A caller who left before any header is not a provider failure: the attempt is charged
      // nothing, and with nobody left to serve there is no failover to attempt either.
      const callerAborted = result.err.stage === "caller_aborted";
      failedAttempts++;
      if (errKind === "upstream_rejection") opaqueFailures++;
      if (!callerAborted) noteAttemptFailure(cfg, attempt, exclScope, result.err.status, result.err.body, result.err.errorBodyState, result.err.stage || null);
      const scope = callerAborted ? { cooldownScope: null, nextAvailableAt: null } : cooldownScopeOf(attempt, errKind);
      attemptsSummary.push(attemptOutcome({
        attempt,
        status: result.err.status,
        kind: errKind,
        decision: callerAborted ? "client_cancelled" : errKind === "client" ? "client_error" : "failover",
        latencyMs: Date.now() - attemptStarted,
        errorBodyState: result.err.errorBodyState || null,
        errorBodyBytes: result.err.errorBodyBytes ?? 0,
        retries,
        cooldownScope: scope.cooldownScope,
        nextAvailableAt: scope.nextAvailableAt,
        requestIds: result.err.requestIds || null,
      }));
      // An EXPLAINED client error is the caller's payload, so it would fail
      // identically on every provider: fail fast, no retry, no failover. An
      // unexplained rejection proves nothing about the payload, so it falls
      // through to the next provider instead.
      // A transport-class failure taints the whole account for the rest of this request (see
      // deadAccounts). An auth, billing or rate failure needs no entry here: those cool the
      // ACCOUNT, so the next deployment of it is refused by the eligibility check anyway.
      if (errKind === "server" || result.err.stage === "connect") deadAccounts.add(attempt.backend);
      releaseAttempt(lease);
      if (callerAborted || errKind === "client") break;
      continue;
    }
    // The upstream answered, but a status line is not a success yet: mark healthy
    // only after the body actually carried usable output (below).
    const latencyMs = Date.now() - started;
    let metrics;
    try {
      // Stall bound per backend (idleTimeoutMs) with a 120s default; this is
      // silence-based (see relay), never a wall-clock kill. modelId is threaded
      // through so relay-cut log lines are request-scoped.
      const idleTimeoutMs = backend.idleTimeoutMs ?? cfg.idleTimeoutMs ?? 120000;
      metrics = await relay(res, result.res, stream, attempt.backend, started, callId, idleTimeoutMs, modelId);
    } catch (e) {
      // Belt-and-suspenders: relay() already catches internally, but metric
      // collection must never crash the router.
      writeRelayLog({
        level: "relay-threw",
        time: new Date().toISOString(),
        callId: callId || null,
        backend: attempt.backend,
        model: modelId,
        stream: !!stream,
        elapsedMs: Date.now() - started,
        idleTimeoutMs: backend.idleTimeoutMs ?? cfg.idleTimeoutMs ?? 120000,
        idleAborted: false,
        error: String((e && e.message) || e),
      });
      metrics = { ttftMs: latencyMs, cacheHit: false, promptTokens: 0, completionTokens: 0, reasoningTokens: 0, genMs: 0, relayError: String((e && e.message) || e) };
    }
    // A 200 that stalled, got cut, carried nothing, or was abandoned by the caller is
    // an upstream failure wearing a success status. The response is already on the
    // wire so it is never replayed or re-statused: the route is charged for later
    // requests, it is not pinned as a success, and the row keeps 200 + telemetry.
    let usableFailure = null;
    let usableDecision = "success";
    if (metrics.clientCancelled) {
      // The caller hung up: the provider did nothing wrong, so nothing is charged.
      usableDecision = "client_cancelled";
    } else if (metrics.usable) {
      // The pin is still remembered (this route really did serve the session), but the
      // health writes are refused when a newer attempt already owns these rows.
      if (ownsAccountSettlement(attempt)) markSuccess(attempt.backend, attempt);
      if (ownsRouteSettlement(attempt)) markRouteSuccess(attempt);
      rememberProvider(prefTokens.get(attempt.model), attempt, metrics);
    } else {
      usableFailure = metrics.relayError || "upstream returned no usable output";
      metrics.relayError = usableFailure;
      usableDecision = "unusable";
      noteAttemptFailure(cfg, attempt, exclScope, 0, usableFailure, "present", "relay");
    }
    {
      const uKind = usableFailure ? classify(0, usableFailure) : null;
      const uScope = usableFailure ? cooldownScopeOf(attempt, uKind) : { cooldownScope: null, nextAvailableAt: null };
      attemptsSummary.push(attemptOutcome({
        attempt,
        status: 200,
        kind: uKind,
        decision: usableDecision,
        latencyMs,
        errorBodyState: null,
        errorBodyBytes: 0,
        retries,
        cooldownScope: uScope.cooldownScope,
        nextAvailableAt: uScope.nextAvailableAt,
        requestIds: null,
      }));
    }
    const genSec = (metrics.genMs || 0) / 1000;
    const entry = applyCost(newHistoryEntry({
      t: detailT, callId, model: modelId, backend: attempt.backend, stream, status: 200, latencyMs,
      ttftMs: metrics.ttftMs ?? latencyMs,
      promptTokens: metrics.promptTokens ?? 0,
      completionTokens: metrics.completionTokens ?? 0,
      reasoningTokens: metrics.reasoningTokens ?? 0,
      cacheHit: metrics.cacheHit,
      cacheHitPct: metrics.cacheHitPct,
      promptCacheHitTokens: metrics.promptCacheHitTokens,
      promptCacheMissTokens: metrics.promptCacheMissTokens,
      genMs: metrics.genMs ?? 0,
      tps: genSec > 0 && (metrics.completionTokens || 0) > 0 ? Math.round((metrics.completionTokens / genSec) * 10) / 10 : 0,
      session: sessionKey,
      routedModel: attempt.model,
      wireParams: extractWireParams(payload),
      retries,
      retryWaitedMs,
      relayError: metrics.relayError || null, // relay cut (idle timeout / upstream drop) for a 200 row
      errorKind: usableFailure ? classify(0, usableFailure) : null, // a 2xx that carried nothing usable is still a failure
      selectionReason,
      fallbackCount,
      cacheMayBeCold: fallbackCount > 0, // the request changed provider: the prompt cache MAY be cold (never a measured miss)
      ...capAttemptsSummary(attemptsSummary),
    }), cfg);
    recordHistory(entry);
    recordStats(entry);
    recordAnalytics(entry);
    // Persistent bounded capture of the payload behind a big cache miss, and a
    // push alert when the miss crosses the configured thresholds. Both must
    // never break the successful response path.
    captureMiss(cfg, entry, payload);
    if (typeof metrics.cacheHitPct === "number" && metrics.cacheHitPct < (cfg.alerts && cfg.alerts.ntfy ? (cfg.alerts.ntfy.minMissPct ?? 50) : 50)) {
      const missTokens = Math.max(0, Math.round((metrics.promptTokens || 0) * (1 - metrics.cacheHitPct / 100)));
      const amin = cfg.alerts && cfg.alerts.ntfy ? (cfg.alerts.ntfy.minMissTokens ?? 20000) : 20000;
      if (missTokens >= amin) fireAlert(cfg, "cache_miss", { backend: attempt.backend, cacheHitPct: metrics.cacheHitPct, missTokens, promptTokens: metrics.promptTokens || 0, model: modelId, session: sessionKey });
    }
    if (metrics.relayError) fireAlert(cfg, "relay_cut", { backend: attempt.backend, model: modelId, error: metrics.relayError });
    // Finalize the deep-dive detail record (attempt already recorded above) with
    // the response summary.
    finalizeDetail(detailT, {
      status: 200,
      ttftMs: metrics.ttftMs ?? latencyMs,
      latencyMs,
      genMs: metrics.genMs ?? 0,
      promptTokens: metrics.promptTokens ?? 0,
      completionTokens: metrics.completionTokens ?? 0,
      reasoningTokens: metrics.reasoningTokens ?? 0,
      cacheHit: metrics.cacheHit,
      cacheHitPct: metrics.cacheHitPct,
      preview: metrics.preview || null,
      relayError: metrics.relayError || null,
      errorKind: usableFailure ? classify(0, usableFailure) : null,
      selectionReason,
      fallbackCount,
      cacheMayBeCold: fallbackCount > 0,
      ...capAttemptsSummary(attemptsSummary),
    });
    releaseAttempt(lease);
    disposeCallerWatch();
    return;
  }

  // All attempts failed (or were skipped). Two outcomes share this path and must
  // not be conflated: an EXPLAINED client error means the caller's payload was
  // rejected, so the router surfaces the real status and the upstream's own
  // explanation and raises no exhaustion alert; an unexplained rejection
  // exhausted every provider, which IS provider exhaustion and stays a 503. The
  // detail record keyed by detailT holds every attempt's outcome.
  const latencyMs = Date.now() - started;
  // errorBodyState decides explained vs opaque: a body that was cut off mid-read
  // must never be trusted as the provider explaining itself.
  const lastKind = lastErr ? classify(lastErr.status, lastErr.body, lastErr.errorBodyState) : null;
  const explainedClient = lastKind === "client" && !!lastErr;
  const finalStatus = explainedClient ? (lastErr.status || 400) : 503;
  const failEntry = applyCost(newHistoryEntry({ t: detailT, callId, model: modelId, backend: lastTried ? lastTried.backend : "none", stream, status: finalStatus, latencyMs, session: sessionKey, routedModel: c.ordered[0] ? c.ordered[0].model : null, selectionReason, fallbackCount, cacheMayBeCold: fallbackCount > 0, ...capAttemptsSummary(attemptsSummary), errorKind: lastKind }), cfg);
  recordHistory(failEntry);
  recordAnalytics(failEntry);
  // A caller who left is not provider exhaustion, so it must not raise the operator alert.
  if (!explainedClient && !callerGone) fireAlert(cfg, "request_exhausted", { callId, model: modelId, error: lastErr ? String(lastErr.body || lastErr.status) : "all providers unavailable" });
  recordStats({ backend: lastTried ? lastTried.backend : "none", model: modelId, status: finalStatus, latencyMs });
  // A detail record exists whenever at least one attempt ran (recorded per
  // attempt above); finalize it unconditionally.
  finalizeDetail(detailT, {
    status: finalStatus,
    ttftMs: 0,
    latencyMs,
    genMs: 0,
    promptTokens: 0,
    completionTokens: 0,
    reasoningTokens: 0,
    cacheHit: null,
    cacheHitPct: null,
    preview: null,
    // Redacted: the detail endpoint is open, and a provider is free to echo a
    // credential back inside its error text.
    errorBody: lastErr ? redactSecrets(String(lastErr.body ?? ""), cfg, req) : null,
    errorKind: lastKind,
    upstreamStatus: lastErr ? lastErr.status : null,
    selectionReason,
    fallbackCount,
    cacheMayBeCold: fallbackCount > 0,
    ...capAttemptsSummary(attemptsSummary),
  });
  res.setHeader("x-router-backend", lastTried ? lastTried.backend : "none");
  res.setHeader("x-router-call", callId);
  const msg = lastErr ? lastErr.body : null;
  // Never dress an unexplained rejection up as "all backends cooling": that
  // claimed a cooling state the router never entered, which is what made the
  // 2026-10-07 incident unreadable. Genuine eligibility failures have their own
  // path above and keep their own wording. The universal claim is reserved for an
  // exhaustion in which NO provider explained itself.
  const allRejectionsOpaque = failedAttempts > 0 && opaqueFailures === failedAttempts;
  // Used only when the provider gave no usable explanation of its own. "all
  // backends cooling" is deliberately NOT the fallback here: an eligibility
  // failure has its own path with its own wording, and dressing a real upstream
  // failure up as cooling is what made the 2026-10-07 incident unreadable.
  const noExplanation =
    lastKind === "upstream_rejection"
      ? allRejectionsOpaque
        ? `every provider rejected this request without explaining why (${failedAttempts} tried)`
        : `the last provider (${lastTried ? lastTried.backend : "unknown"}) rejected this request without explaining why, after ${failedAttempts} failed attempt(s)`
      : lastErr
        ? `the last provider (${lastTried ? lastTried.backend : "unknown"}) failed with HTTP ${lastErr.status}, which it did not explain`
        : "no provider could serve this request";
  // Every terminal CHAT failure leaves through one builder: never empty, always
  // valid JSON, the provider's own words when it gave them (redacted and capped),
  // plus the ids an operator needs to chase the failure upstream.
  const recoveryMs = explainedClient ? null : nearestRecoveryMs(c.ordered, exclScope, Date.now());
  const errBody = terminalChatError({
    upstream: typeof msg === "string" && msg ? msg : null,
    message: noExplanation,
    type: explainedClient ? "invalid_request_error" : lastKind === "upstream_rejection" ? "upstream_rejection" : "router_unavailable",
    code: lastKind === "upstream_rejection" ? "upstream_rejection" : null,
    callId,
    backend: lastTried ? lastTried.backend : "none",
    upstreamStatus: lastErr ? lastErr.status : null,
    nextAvailableAt: recoveryMs === null ? null : Date.now() + recoveryMs,
    cfg,
    req,
  });
  disposeCallerWatch();
  return writeError(res, newError(finalStatus, errBody));
}

// Cache detection: the router forwards x-cache-key/cachekey/set-cache-key as
// REQUEST headers; when the upstream response echoes any of them back it is
// signaling a cache hit (or a cache write) for this request.
function hasCacheHeader(upstreamRes) {
  const h = upstreamRes.headers;
  return !!(h && h.get && (h.get("x-cache-key") || h.get("cachekey") || h.get("set-cache-key")));
}

// Per-call cache-hit percentage: 0..100 share of prompt tokens served from
// cache, rounded to 1 decimal, or null when there is no measurable cache signal.
// A cache break at message N means everything before N hits and everything from
// N on misses, so a LOW percentage means the break happened EARLY in the prompt
// (0% = cold account / break at the very start; 95% = break near the end). This
// is the number that tells an operator WHERE the break happened, which the
// boolean cacheHit cannot.
function cacheHitPctOf(hit, miss, cached, promptTokens) {
  const hitN = Number(hit);
  const missN = Number(miss);
  const cachedN = Number(cached);
  if (Number.isFinite(hitN) && Number.isFinite(missN)) {
    const total = hitN + missN;
    if (total > 0) return Math.round((hitN / total) * 100 * 10) / 10;
    return null; // both zero = nothing measured
  }
  // Folded shapes: some providers only expose cached_tokens (folded into hit),
  // or hit alone; compare against promptTokens as the denominator.
  if (Number.isFinite(hitN)) {
    if (Number.isFinite(Number(promptTokens)) && Number(promptTokens) > 0) return Math.round((hitN / Number(promptTokens)) * 100 * 10) / 10;
    return null;
  }
  if (Number.isFinite(cachedN)) {
    if (Number.isFinite(Number(promptTokens)) && Number(promptTokens) > 0) return Math.round((cachedN / Number(promptTokens)) * 100 * 10) / 10;
    return null;
  }
  return null;
}

// ---- per-request cost engine --------------------------------------------------
//
// Billing mirrors DeepSeek's published price shape: models declare
// meta.pricing (USD per 1M tokens, optional cache-split rate) and OPT IN to
// off-peak pricing with meta.pricing.offPeak === true. Peak windows are
// weekday UTC hour ranges; everything else (weekends, nights, window gaps) is
// off-peak at the top-level pricing.offPeakMultiplier (default 0.5 = DeepSeek's
// half price).
const DEFAULT_PEAK_WINDOWS = [
  { days: "1-5", start: "01:00", end: "04:00" },
  { days: "1-5", start: "06:00", end: "10:00" },
];

// True when t (epoch ms) is OUTSIDE every peak window. Days are
// Date.getUTCDay() numbers where "1-5" means Mon-Fri; window times are UTC
// "HH:MM", start inclusive, end exclusive. Weekends have no windows at all.
function isOffPeak(t, cfg) {
  const d = new Date(Number(t) || 0);
  const day = d.getUTCDay();
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
  const windows = cfg && cfg.pricing && Array.isArray(cfg.pricing.peakWindows)
    ? cfg.pricing.peakWindows
    : DEFAULT_PEAK_WINDOWS;
  for (const w of windows) {
    if (!w || typeof w.days !== "string" || typeof w.start !== "string" || typeof w.end !== "string") continue;
    const dash = w.days.indexOf("-");
    const dayStart = Number(dash >= 0 ? w.days.slice(0, dash) : w.days);
    const dayEnd = Number(dash >= 0 ? w.days.slice(dash + 1) : w.days);
    if (!Number.isFinite(dayStart) || !Number.isFinite(dayEnd) || day < dayStart || day > dayEnd) continue;
    const [hs, ms] = w.start.split(":").map(Number);
    const [he, me] = w.end.split(":").map(Number);
    const startMins = hs * 60 + ms;
    const endMins = he * 60 + me;
    if (!Number.isFinite(startMins) || !Number.isFinite(endMins)) continue;
    if (mins >= startMins && mins < endMins) return false; // inside a peak window
  }
  return true; // outside every window (or none configured) -> off-peak
}

// Fixed precision for reported USD. Token-count * rate/1e6 products sit well
// below 1e-9 per token, so 10 decimals absorbs binary-fraction noise while
// keeping exact-number assertions stable.
const roundCost = (n) => Math.round(n * 1e10) / 1e10;

// Per-request cost in USD from entry + config. Returns null when the model has
// no meta.pricing (the UI renders "n/a"); otherwise { cost, cacheSavings,
// offPeakSavings, offPeak } with every number >= 0. cost is the billed amount
// at effective (possibly off-peak-scaled) rates; cacheSavings is what cache
// hits saved vs paying full input price, measured at PEAK rates so it does not
// depend on when the request ran; offPeakSavings is the portion of cost the
// off-peak discount removed.
function computeCost(entry, cfg) {
  const modelCfg = (cfg && cfg.models) ? cfg.models[entry.model] : null;
  const pricing = modelCfg && modelCfg.meta ? modelCfg.meta.pricing : null;
  if (!pricing || typeof pricing !== "object") return null;
  const inputPerM = Number(pricing.inputPerM);
  const outputPerM = Number(pricing.outputPerM);
  if (!Number.isFinite(inputPerM) || !Number.isFinite(outputPerM)) return null;
  const cacheHitInputPerM = pricing.cacheHitInputPerM != null ? Number(pricing.cacheHitInputPerM) : inputPerM;
  const promptTokens = Math.max(0, Number(entry.promptTokens) || 0);
  const completionTokens = Math.max(0, Number(entry.completionTokens) || 0);
  let hitTokens = entry.promptCacheHitTokens == null ? 0 : Number(entry.promptCacheHitTokens);
  if (!Number.isFinite(hitTokens) || hitTokens < 0) hitTokens = 0;
  if (hitTokens > promptTokens) hitTokens = promptTokens; // clamp: hits can never exceed the prompt
  let missTokens = entry.promptCacheMissTokens == null ? NaN : Number(entry.promptCacheMissTokens);
  if (!Number.isFinite(missTokens)) missTokens = Math.max(0, promptTokens - hitTokens); // miss not reported -> derive from prompt - hit
  else missTokens = Math.max(0, missTokens);
  const offPeakTime = isOffPeak(entry.t, cfg);
  const offPeak = offPeakTime && pricing.offPeak === true; // discount applies only to opted-in models
  let mult = 1;
  if (offPeak) {
    mult = cfg && cfg.pricing && typeof cfg.pricing.offPeakMultiplier === "number" ? cfg.pricing.offPeakMultiplier : 0.5;
    if (!(mult > 0)) mult = 0.5; // 0/negative multiplier is a config bug; fall back to the standard half price
  }
  const cost = roundCost(missTokens * ((inputPerM * mult) / 1e6) + hitTokens * ((cacheHitInputPerM * mult) / 1e6) + completionTokens * ((outputPerM * mult) / 1e6));
  const cacheSavings = roundCost(hitTokens * ((inputPerM - cacheHitInputPerM) / 1e6)); // peak rates: what the cache hit saved vs full-price miss
  const offPeakSavings = offPeak ? roundCost(cost * (1 / mult - 1)) : 0; // discount saved vs the peak price
  return { cost, cacheSavings, offPeakSavings, offPeak };
}

// Attach the computed cost fields to a history entry in place (all null when
// the model has no pricing). Returns the entry for chaining.
function applyCost(entry, cfg) {
  const c = computeCost(entry, cfg);
  entry.cost = c ? c.cost : null;
  entry.cacheSavings = c ? c.cacheSavings : null;
  entry.offPeakSavings = c ? c.offPeakSavings : null;
  entry.offPeak = c ? c.offPeak : null;
  return entry;
}

// Incremental SSE usage collector. Streaming chat completions normally place
// the cumulative `usage` object on the final chunk before [DONE], so the LAST
// usage object seen wins. Chunk boundaries are handled by keeping a partial
// trailing line between feeds.
function makeUsageCollector() {
  let leftover = "";
  let promptTokens = 0;
  let completionTokens = 0;
  let reasoningTokens = 0;
  let promptCacheHit = 0;
  let promptCacheMiss = 0;
  let promptCacheMissSeen = false; // miss field actually reported (0 default != "reported zero")
  let cacheFieldSeen = false;
  // Usability signals, observed from the PARSED payload rather than inferred from
  // token counts: a 200 that carried no content, no reasoning and no tool call is
  // an upstream failure wearing a success status.
  let hasContent = false;
  let hasReasoning = false;
  let hasToolCalls = false;
  function consume(line) {
    const t = line.trim();
    if (!t.startsWith("data:")) return;
    const payload = t.slice(5).trim();
    if (!payload || payload === "[DONE]") return;
    try {
      const obj = JSON.parse(payload);
      const ch0 = Array.isArray(obj && obj.choices) ? obj.choices[0] : null;
      const slot = ch0 ? ch0.delta || ch0.message || null : null;
      if (slot) {
        if (slot.content != null && String(slot.content).length > 0) hasContent = true;
        // Present-but-empty is NOT output: a provider emits reasoning_content:"" on a
        // truncated or filtered reply, and counting that as usable reports success, marks
        // the route healthy and pins it for a session it served nothing (review finding 9).
        // hasUsableContentNonStream already applies exactly this rule.
        const rslot = slot.reasoning_content != null ? slot.reasoning_content : slot.reasoning;
        if (rslot != null && String(rslot).trim() !== "") hasReasoning = true;
        if (Array.isArray(slot.tool_calls) && slot.tool_calls.length > 0) hasToolCalls = true;
      }
      if (obj && obj.usage) {
        promptTokens = Number(obj.usage.prompt_tokens) || 0;
        completionTokens = Number(obj.usage.completion_tokens) || 0;
        // Reasoning token split: reasoning-only completion tokens. Not every
        // upstream reports it; 0 means either "no reasoning" or "not reported".
        const rt = obj.usage.completion_tokens_details && obj.usage.completion_tokens_details.reasoning_tokens;
        reasoningTokens = Number(rt) || 0;
        // DeepSeek/OpenAI-style prompt-cache accounting. Some providers expose
        // prompt_cache_hit_tokens + prompt_cache_miss_tokens; others expose
        // prompt_tokens_details.cached_tokens. Either one means the upstream
        // reports cache state at all (cacheFieldSeen), which lets us tell a
        // real miss from "no signal".
        const hit = Number(obj.usage.prompt_cache_hit_tokens);
        const miss = Number(obj.usage.prompt_cache_miss_tokens);
        const cached = obj.usage.prompt_tokens_details && obj.usage.prompt_tokens_details.cached_tokens;
        const cachedN = Number(cached);
        if (!Number.isNaN(hit) || !Number.isNaN(miss) || !Number.isNaN(cachedN)) cacheFieldSeen = true;
        if (!Number.isNaN(hit)) promptCacheHit = hit;
        else if (!Number.isNaN(cachedN)) promptCacheHit = cachedN;
        if (!Number.isNaN(miss)) {
          promptCacheMiss = miss;
          promptCacheMissSeen = true;
        }
      }
    } catch {
      // Non-JSON SSE event (keepalive comment, etc.) — ignore.
    }
  }
  return {
    feed(chunk) {
      let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
      const lines = text.split("\n");
      leftover = lines.pop() || "";
      for (const line of lines) consume(line);
    },
    finalize() {
      if (leftover) consume(leftover);
      leftover = "";
      return { promptTokens, completionTokens, reasoningTokens, promptCacheHit, promptCacheMiss, promptCacheMissSeen, cacheFieldSeen, hasContent, hasReasoning, hasToolCalls };
    },
  };
}

// Pass-through transform that feeds every chunk to the usage collector.
// Metrics collection must never break the stream, so failures are swallowed.
function usageTransform(collector) {
  return new Transform({
    transform(chunk, _enc, cb) {
      try {
        collector.feed(chunk);
      } catch {
        /* ignore */
      }
      cb(null, chunk);
    },
  });
}

// Pass-through transform that captures the FIRST content fragment from a
// streaming SSE response (first delta.content or delta.reasoning_content,
// truncated to 200 chars) for the request-detail preview. Never breaks the
// stream; if nothing is captured the preview stays null.
function previewTransform() {
  let leftover = "";
  let captured = false;
  let preview = null;
  const cap = (s) => (typeof s === "string" ? (s.length > 200 ? s.slice(0, 200) + "…" : s) : s);
  const tr = new Transform({
    transform(chunk, _enc, cb) {
      try {
        if (!captured) {
          let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
          const lines = text.split("\n");
          leftover = lines.pop() || "";
          for (const line of lines) {
            const t = line.trim();
            if (t.startsWith("data:")) {
              const payload = t.slice(5).trim();
              if (payload && payload !== "[DONE]") {
                try {
                  const obj = JSON.parse(payload);
                  if (obj && obj.choices && obj.choices[0]) {
                    const delta = obj.choices[0].delta || obj.choices[0].message || {};
                    const frag = delta.content != null ? delta.content : (delta.reasoning_content != null ? delta.reasoning_content : null);
                    if (frag != null) {
                      preview = cap(frag);
                      captured = true;
                      break;
                    }
                  }
                } catch {
                  /* non-JSON data line - pass through unchanged */
                }
              }
            }
          }
        }
        cb(null, chunk);
      } catch (e) {
        // Never break the stream: fall back to passthrough.
        cb(null, chunk);
      }
    },
  });
  tr.getPreview = () => preview;
  return tr;
}

// Reasoning-key normalization. OpenCode's client parser only reads
// delta.reasoning_content (DeepSeek-native) while commandcode/OpenAI-Reasoning
// API style upstreams emit delta.reasoning (+ reasoning_details). Without this,
// reasoning generated through commandcode never renders in OpenCode. The router
// keeps the original key AND exposes the normalized one - the playground reads
// both, so nothing already working changes.
// Works on a parsed chunk (choices[0].delta or .message) in place; returns the
// object for convenience.
function normalizeReasoningKey(obj) {
  if (!obj || typeof obj !== "object") return obj;
  for (const holder of ["delta", "message"]) {
    const slot = obj[holder];
    if (!slot || typeof slot !== "object") continue;
    if (slot.reasoning != null && slot.reasoning_content == null) {
      slot.reasoning_content = slot.reasoning;
      // Merge the full reasoning_details text when present (commandcode sends
      // reasoning as short per-token strings with details carrying the full
      // block; OpenCode renders reasoning_content as one stream).
      if (Array.isArray(slot.reasoning_details)) {
        const detailText = slot.reasoning_details
          .map((d) => (d && typeof d.text === "string" ? d.text : ""))
          .join("");
        if (detailText && slot.reasoning_content !== detailText) {
          slot.reasoning_content = detailText;
        }
      }
    }
  }
  return obj;
}

// SSE transform that normalizes the reasoning key on every data payload while
// passing everything else through byte-for-byte. Line-boundary safe: it buffers
// partial lines between chunks, exactly like the usage collector does.
function reasoningNormalizeTransform() {
  let leftover = "";
  return new Transform({
    transform(chunk, _enc, cb) {
      try {
        let text = leftover + (Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk));
        const lines = text.split("\n");
        leftover = lines.pop() || "";
        let out = "";
        for (const line of lines) {
          const t = line.trim();
          if (t.startsWith("data:")) {
            const payload = t.slice(5).trim();
            if (payload && payload !== "[DONE]") {
              try {
                const obj = JSON.parse(payload);
                if (obj && obj.choices && obj.choices[0]) {
                  normalizeReasoningKey(obj.choices[0]);
                  // Re-serialize the normalized object back into the SSE line.
                  out += "data: " + JSON.stringify(obj) + "\n";
                  continue;
                }
              } catch {
                /* non-JSON data line - pass through unchanged */
              }
            }
          }
          out += line + "\n";
        }
        cb(null, out);
      } catch (e) {
        // Never break the stream: fall back to passthrough.
        cb(null, chunk);
      }
    },
    flush(cb) {
      if (leftover) cb(null, leftover);
      else cb();
    },
  });
}

// Timestamped, structured relay/log line. Unlike the old "relay error:"
// console.error (no timestamp, no request id), every entry carries an ISO
// timestamp + callId + backend + model so a relay cut can be traced back to
// the exact request via /api/history?call=<callId> or the JSONL detail rows.
// Emitted as a single line for easy grep.
function writeRelayLog(f) {
  const tags = [
    f.callId ? `call=${f.callId}` : "call=-",
    f.backend ? `backend=${f.backend}` : "backend=-",
    f.model ? `model=${f.model}` : "model=-",
    `stream=${!!f.stream}`,
    `elapsedMs=${f.elapsedMs ?? 0}`,
    f.idleAborted ? "cause=idle-timeout" : "cause=relay-error",
  ];
  console.error(`[${f.time || new Date().toISOString()}] ${f.level || "relay"} ${tags.join(" ")} ${f.error ? ("err=" + String(f.error)) : ""}`);
}

// Idle/stall watchdog for the streaming path: if NO upstream bytes arrive for
// idleTimeoutMs the relay is aborted (genuine stall), but a slow-but-alive
// stream that keeps emitting resets the timer every chunk and is never cut.
// Returns a Transform to insert into the relay pipeline plus the controller
// whose signal bounds the upstream body read (fromWeb honors it).
function makeIdleWatchdog(idleTimeoutMs) {
  const controller = new AbortController();
  let timer = null;
  const arm = () => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => controller.abort(), idleTimeoutMs);
  };
  const clear = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };
  arm();
  const reset = new Transform({
    transform(chunk, enc, cb) {
      arm();
      cb(null, chunk);
    },
    flush(cb) {
      clear();
      cb();
    },
    _destroy(err, cb) {
      clear(); // never leave a post-error timer armed
      cb(err);
    },
  });
  return { controller, reset };
}

// Non-stream body read with the same stall semantics as the streaming
// watchdog: reset on every chunk, abort (via reader.cancel) if idleTimeoutMs
// passes with no data. A slow non-stream response completes; a stuck one is
// cut. Returns a Buffer (the caller treats it as the response body bytes).
async function readBodyWithIdle(webBody, idleTimeoutMs) {
  if (!webBody) return Buffer.from(await webBody.arrayBuffer()); // throws like Response.arrayBuffer() on null body
  const reader = webBody.getReader();
  const chunks = [];
  let timer = null;
  const arm = () => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => reader.cancel("relay idle timeout"), idleTimeoutMs);
  };
  arm();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(Buffer.from(value));
      arm(); // any progress re-arms the stall bound
    }
    return Buffer.concat(chunks);
  } finally {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  }
}

// Relay the upstream response to the client while capturing per-request
// metrics and normalizing the reasoning key (delta.reasoning ->
// delta.reasoning_content) for OpenCode compatibility. Returns
// { ttftMs, cacheHit, promptTokens, completionTokens, genMs, preview } where
// preview is the first content fragment (stream: first content delta;
// non-stream: first choice's message content), truncated to 200 chars, plus
// relayError (string|null) when the upstream body was cut mid-relay so the
// history row can expose truncations that otherwise look like HTTP 200.
// Never throws: on any failure it closes the response and returns what it has.
// idleTimeoutMs bounds SILENCE (stall detection), never total wall time, so a
// long-but-alive stream survives; the old wall-clock kill is gone (see forward).
async function relay(res, upstreamRes, stream, backendId, started, callId, idleTimeoutMs = 120000, modelId = null) {
  const ttftMs = Date.now() - started;
  const headers = {
    "content-type": upstreamRes.headers.get("content-type") || "application/json",
    "x-router-backend": backendId || "unknown",
  };
  if (callId) headers["x-router-call"] = callId;
  let cacheHit = null;   // tri-state: true / false / null (unknown - no signal)
  let cacheHitPct = null; // 0..100 share of prompt tokens from cache; null = no signal
  let promptTokens = 0;
  let completionTokens = 0;
  let reasoningTokens = 0;
  let genMs = 0;
  let promptCacheHitTokens = 0;  // raw cache-hit prompt tokens (0 = not reported)
  let promptCacheMissTokens = null; // raw miss tokens; null = not reported -> history derives from prompt - hit
  let preview = null;    // first content fragment, truncated to 200 chars
  let relayError = null; // set when the upstream body was cut mid-relay
  let hasContent = false;
  let hasReasoning = false;
  let hasToolCalls = false;
  // What ended this relay, recorded as a CAUSE rather than a one-way latch. A one-way
  // "the caller left" latch is wrong: pipeline() destroys the downstream when the UPSTREAM
  // fails, which fires the very same close a caller hang-up does, so a genuine relay cut
  // would be excused as a cancellation and charge no health at all.
  let clientCancelled = false;
  let cause = null;
  const note = (c) => { if (cause === null) cause = c; };
  const force = (c) => { cause = c; };
  // 'close' also fires after a normal end, where writableEnded is already true, so only an
  // unfinished response is even a candidate for "the caller left".
  const onClientClose = () => {
    if (!res.writableEnded) note("client");
  };
  res.on("close", onClientClose);
  const cap = (s) => (typeof s === "string" ? (s.length > 200 ? s.slice(0, 200) + "…" : s) : s);
  try {
    if (upstreamRes.headers.get("x-request-id")) headers["x-request-id"] = upstreamRes.headers.get("x-request-id");
    res.writeHead(200, headers);
    if (hasCacheHeader(upstreamRes)) cacheHit = true;

    if (stream && upstreamRes.body) {
      // Tee the stream: one branch goes to the client, the transform branch
      // accumulates usage data from the final SSE chunk.
      const collector = makeUsageCollector();
      const pv = previewTransform();
      const genStart = Date.now();
      // Idle/stall bound: aborts the upstream body read only when NO bytes
      // arrive for idleTimeoutMs. Alive streams (any chunk within the bound)
      // run to completion regardless of total duration.
      const idle = makeIdleWatchdog(idleTimeoutMs);
      // Declared HERE, not inside the try: a catch block is a SIBLING scope, so a binding
      // created inside the try is not visible in the catch at all. Reading it there throws a
      // ReferenceError, and that error would be swallowed as the relay's own failure.
      let sourceError = null;
      // The upstream body failing counts as the CAUSE only if the caller has not already gone.
      // Destroying the downstream tears the SOURCE down too, and that teardown error is a
      // consequence of the caller leaving, never evidence that the upstream failed.
      let upstreamFailedFirst = false;
      const onSourceError = (e) => {
        sourceError = e;
        if (cause === null) upstreamFailedFirst = true;
      };
      try {
        // Naming the source lets the cause be decided from the UPSTREAM side before teardown:
        // pipeline() destroys the downstream too, so a cause inferred afterwards would blame
        // the caller for the upstream's failure.
        const source = Readable.fromWeb(upstreamRes.body, { signal: idle.controller.signal });
        source.on("error", onSourceError);
        await pipeline(
          source,
          idle.reset,
          usageTransform(collector),
          pv,
          reasoningNormalizeTransform(),
          res
        );
      } catch (e) {
        // Precedence: the stall watchdog, then the upstream body, then the caller. Deciding
        // from the source side first is what keeps an upstream cut from being written off as
        // a caller who left (zStage2 pins that cut, zSess5 pins the real cancellation).
        if (idle.controller.signal.aborted) force("idle");
        else if (upstreamFailedFirst) force("upstream");
        else note("client");
        relayError = cause === "idle"
          ? `upstream idle timeout (no data for ${idleTimeoutMs}ms)`
          : cause === "client"
            ? "client cancelled the request"
            : String((sourceError && sourceError.message) || (e && e.message));
        throw e; // let the outer catch close the response + log once
      }
      genMs = Date.now() - genStart;
      const usage = collector.finalize();
      hasContent = usage.hasContent;
      hasReasoning = usage.hasReasoning;
      hasToolCalls = usage.hasToolCalls;
      promptTokens = usage.promptTokens;
      completionTokens = usage.completionTokens;
      reasoningTokens = usage.reasoningTokens;
      preview = pv.getPreview() || null;
      if (cacheHit !== true) {
        cacheHit = usage.cacheFieldSeen ? usage.promptCacheHit > 0 : null;
      }
      // cached-only upstreams fold their tokens into promptCacheHit while the
      // miss field stays unset; passing the 0 default would make the ratio read
      // 100%, so pass NaN unless the miss was actually reported.
      cacheHitPct = usage.cacheFieldSeen
        ? cacheHitPctOf(usage.promptCacheHit, usage.promptCacheMissSeen ? usage.promptCacheMiss : NaN, NaN, usage.promptTokens)
        : null;
      promptCacheHitTokens = usage.promptCacheHit; // raw cache-hit prompt tokens (0 when the upstream does not report a split)
      promptCacheMissTokens = usage.promptCacheMissSeen ? usage.promptCacheMiss : null; // null = miss not reported -> history derives it
    } else {
      try {
        let buf = await readBodyWithIdle(upstreamRes.body, idleTimeoutMs);
        try {
          const parsed = JSON.parse(buf.toString("utf8"));
          const usage = parsed && parsed.usage;
          if (usage) {
            promptTokens = Number(usage.prompt_tokens) || 0;
            completionTokens = Number(usage.completion_tokens) || 0;
            const rt = usage.completion_tokens_details && usage.completion_tokens_details.reasoning_tokens;
            reasoningTokens = Number(rt) || 0;
            if (cacheHit !== true) {
              const hit = Number(usage.prompt_cache_hit_tokens);
              const miss = Number(usage.prompt_cache_miss_tokens);
              const cached = usage.prompt_tokens_details && usage.prompt_tokens_details.cached_tokens;
              const cachedN = Number(cached);
              const seen = !Number.isNaN(hit) || !Number.isNaN(miss) || !Number.isNaN(cachedN);
              if (seen) cacheHit = (!Number.isNaN(hit) ? hit : cachedN) > 0;
              // else keep null (no signal)
              cacheHitPct = cacheHitPctOf(hit, miss, cachedN, promptTokens);
              if (!Number.isNaN(hit)) promptCacheHitTokens = hit;
              else if (!Number.isNaN(cachedN) && cachedN > 0) promptCacheHitTokens = cachedN;
              if (!Number.isNaN(miss)) promptCacheMissTokens = miss;
            }
          }
          // Non-stream: normalize the reasoning key on the first choice message.
          if (parsed && Array.isArray(parsed.choices) && parsed.choices[0]) {
            normalizeReasoningKey(parsed.choices[0]);
            buf = Buffer.from(JSON.stringify(parsed));
            const firstMsg = parsed.choices[0].message || parsed.choices[0].delta || {};
            preview = cap(firstMsg.content != null ? firstMsg.content : (firstMsg.text != null ? firstMsg.text : null));
            hasContent = firstMsg.content != null && String(firstMsg.content).length > 0;
            const rmsg = firstMsg.reasoning_content != null ? firstMsg.reasoning_content : firstMsg.reasoning;
            hasReasoning = rmsg != null && String(rmsg).trim() !== "";
            hasToolCalls = Array.isArray(firstMsg.tool_calls) && firstMsg.tool_calls.length > 0;
          }
        } catch {
          // Body not JSON (e.g. upstream proxy error) — keep token counts at 0.
        }
        res.end(buf);
        genMs = Math.max(0, Date.now() - started - ttftMs);
      } catch (e) {
        // Non-stream body stall/cut (readBodyWithIdle cancels on idle) or body read error.
        // The READ failing is the upstream failing, so it outranks a downstream close.
        if (/idle timeout/.test(String(e && e.message))) force("idle");
        else force("upstream");
        relayError = cause === "idle" ? `upstream idle timeout (no data for ${idleTimeoutMs}ms)` : String(e && e.message);
        throw e;
      }
    }
  } catch (e) {
    // Relay must never crash the router: record what we have, log a
    // TIMESTAMPED, request-scoped line so relay cuts are traceable to a call
    // (callId cross-references /api/history and the JSONL detail rows), then
    // close cleanly. elapsedMs = wall time from request start; idleAborted =
    // the stall watchdog fired vs an upstream/network drop.
    const idleAborted = relayError && /idle timeout/.test(relayError);
    if (idleAborted) force("idle"); // the upstream stalled; the caller did not leave
    // Headers never went out and the response is already destroyed: the caller is gone.
    if (cause === null && !res.writableEnded && res.destroyed) note("client");
    if (cause !== "client") writeRelayLog({
      level: "relay-error",
      time: new Date().toISOString(),
      callId: callId || null,
      backend: backendId || null,
      model: modelId,
      stream: !!stream,
      elapsedMs: Date.now() - started,
      idleTimeoutMs,
      idleAborted,
      error: relayError || String((e && e.message) || e),
    });
    try {
      if (!res.writableEnded) res.end();
    } catch {
      /* ignore */
    }
  }
  res.off("close", onClientClose);
  clientCancelled = cause === "client";
  // A clean, usable completion is content, reasoning or a tool call actually
  // parsed out of the upstream body - not merely a 200 status line.
  const usable = !relayError && !clientCancelled && (hasContent || hasReasoning || hasToolCalls);
  return { ttftMs, cacheHit, cacheHitPct, promptCacheHitTokens, promptCacheMissTokens, promptTokens, completionTokens, reasoningTokens, genMs, preview, relayError, hasContent, hasReasoning, hasToolCalls, clientCancelled, usable };
}

// Read and JSON-parse a request body. Returns { value } or { error } (a
// newError-shaped object ready for writeError) so each route stays short.
async function readJsonBody(req, limitBytes = 4 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limitBytes) {
      return { error: newError(413, JSON.stringify({ error: { message: "request body too large", type: "invalid_request_error" } })) };
    }
    chunks.push(c);
  }
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text.trim()) return { value: {} };
  try {
    return { value: JSON.parse(text) };
  } catch (e) {
    return { error: newError(400, JSON.stringify({ error: { message: `invalid JSON body: ${e.message}`, type: "invalid_request_error" } })) };
  }
}

function writeError(res, e) {
  const status = e.status || 500;
  let buf;
  try {
    buf = Buffer.from(e.body || "");
  } catch {
    buf = Buffer.from(JSON.stringify({ error: { message: "router error", type: "router_error" } }));
  }
  // If upstream returned an error body that is JSON-shaped, pass it through;
  // the status reflects it.
  res.writeHead(status, { "content-type": "application/json" });
  res.end(buf);
}

// ---- inbound bearer auth ---------------------------------------------------

// Constant-time string compare over a fixed-length digest so that
// crypto.timingSafeEqual's equal-length buffers requirement holds without
// revealing the raw secret length to a wire-side observer.
function safeEqual(a, b) {
  const ha = crypto.createHash("sha256").update(String(a)).digest();
  const hb = crypto.createHash("sha256").update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function hashSecret(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

// ---- issued API keys -------------------------------------------------------
// Chat-only keys, persisted as SHA-256 hashes in a gitignored api-keys.json
// (LMR_KEYS_FILE overrides the path). The raw key is shown exactly once at
// creation; everything stored or served later is hash-only.
const ISSUED_KEYS_PATH = process.env.LMR_KEYS_FILE || join(__dirname, "api-keys.json");
const ISSUED_KEYS_MAX_NAME = 64;
let issuedKeys = null; // { mtimeMs, keys: [...] }; null = not loaded yet
let issuedKeysWriteChain = Promise.resolve(); // serializes atomic writes

function loadIssuedKeys() {
  // mtime-aware cache: the keys file may be edited by the UI, so a fresh stat
  // decides whether the in-memory copy is still current (cheap: one stat).
  try {
    const st = statSync(ISSUED_KEYS_PATH);
    if (issuedKeys && issuedKeys.mtimeMs === st.mtimeMs) return issuedKeys;
    const raw = readFileSync(ISSUED_KEYS_PATH, "utf8");
    const parsed = JSON.parse(raw);
    issuedKeys = { mtimeMs: st.mtimeMs, keys: Array.isArray(parsed.keys) ? parsed.keys : [] };
  } catch {
    issuedKeys = { mtimeMs: 0, keys: [] };
  }
  return issuedKeys;
}

function persistIssuedKeys(keys) {
  // Chain writes so two rapid mutations (issue then revoke) cannot interleave
  // temp-file renames; write-then-rename keeps the file atomic for readers.
  issuedKeysWriteChain = issuedKeysWriteChain.then(async () => {
    const tmp = ISSUED_KEYS_PATH + ".tmp";
    await writeFileP(tmp, JSON.stringify({ keys }, null, 2) + "\n", "utf8");
    await renameP(tmp, ISSUED_KEYS_PATH);
  });
  return issuedKeysWriteChain;
}

function issuedKeyValid(token) {
  if (!token) return null;
  const { keys } = loadIssuedKeys();
  const want = hashSecret(token);
  for (const k of keys) {
    if (k.revoked) continue;
    if (safeEqual(k.hash, want)) return k;
  }
  return null;
}

async function issueKey(name) {
  const raw = "sk-lmr-" + crypto.randomBytes(24).toString("base64url");
  const rec = { id: crypto.randomBytes(6).toString("hex"), name, hash: hashSecret(raw), createdAt: Date.now(), revoked: false };
  const { keys } = loadIssuedKeys();
  keys.push(rec);
  await persistIssuedKeys(keys);
  return { id: rec.id, name: rec.name, raw, createdAt: rec.createdAt };
}

async function revokeKey(id) {
  const { keys } = loadIssuedKeys();
  const rec = keys.find((k) => k.id === id);
  if (!rec) return false;
  rec.revoked = true;
  await persistIssuedKeys(keys);
  return true;
}

// ---- UI session (dashboard login) ------------------------------------------
// In-memory session tokens; logout removes them, restart invalidates all. The
// cookie is HttpOnly + SameSite=Strict, no Secure so plain-http localhost/LAN
// works. Login throttled per client IP: 5 attempts per 10s window.
const UI_LOGIN_MAX_ATTEMPTS = 5;
const UI_LOGIN_WINDOW_MS = 10000;
const uiSessions = new Set();
const loginAttempts = new Map(); // ip -> [timestamps of failed attempts]
const UI_SESSION_COOKIE = "lmr_ui";

function uiPasswordValue(cfg) {
  const name = cfg.uiPasswordEnv;
  if (!name) return null; // unset -> UI open
  return process.env[name] || ""; // set but empty -> fail closed
}

function uiAuthed(cfg, req) {
  if (!cfg.uiPasswordEnv) return true;
  const m = /(?:^|;\s*)lmr_ui=([^;\s]+)/.exec(req.headers.cookie || "");
  return !!(m && uiSessions.has(m[1]));
}

function loginRateLimited(ip) {
  const now = Date.now();
  const hits = (loginAttempts.get(ip) || []).filter((t) => now - t < UI_LOGIN_WINDOW_MS);
  loginAttempts.set(ip, hits);
  return hits.length >= UI_LOGIN_MAX_ATTEMPTS;
}

function recordLoginAttempt(ip) {
  const now = Date.now();
  const hits = (loginAttempts.get(ip) || []).filter((t) => now - t < UI_LOGIN_WINDOW_MS);
  hits.push(now);
  loginAttempts.set(ip, hits);
}

// ---- inbound gate -----------------------------------------------------------
// Gated paths: `/v1/chat/completions`, `/admin/*`, `/api/keys`, `/api/config/*`.
// NOTE: `GET /api/config` (the plain read-only view) shares the `/api/config`
// prefix and is therefore gated too. That is a deliberate consequence of using
// a prefix match here: the viewer endpoint is read-only and harmless, and
// keeping one simple rule beats a special case that later routes can slip
// past. The dashboard's own config viewer already sends auth when a master key
// is configured, so the UI is unaffected. `masterKeyEnv`
// names an env var whose value the client must echo as `Authorization: Bearer
// <v>` (scheme matched case-insensitively); chat additionally accepts valid
// issued keys. Both config keys are read from the LIVE cfg object at request
// time, exactly like cfg.backoff / cfg.prefix, so a config hot-reload toggles
// the gate without a restart. Unset masterKeyEnv keeps today's open posture
// for every endpoint. A set name whose env value is empty at request time
// FAILS CLOSED: the gated endpoints reject every request rather than silently
// treating a missing expected key as "no auth". Read-only endpoints (health,
// models, stats, history, config, UI) never pass through here, so they stay
// open. `/api/auth/*` are the login itself and are deliberately NOT gated.
function requireAuth(cfg, req) {
  const name = cfg.masterKeyEnv;
  if (!name) return null;
  const url = req.url.split("?")[0];
  const p = (cfg.prefix || "/v1").replace(/\/$/, "");
  const isChat = url === p + "/chat/completions";
  if (!isChat && !url.startsWith("/admin") && !url.startsWith("/api/config") && !url.startsWith("/api/keys") && !url.startsWith("/api/env")) return null;
  const expected = process.env[name];
  const unauthorized = newError(401, JSON.stringify({ error: { message: "authentication required", type: "authentication_error" } }));
  if (!expected) {
    console.error(`[${new Date().toISOString()}] masterKeyEnv '${name}' set but process.env['${name}'] is empty; rejecting request (fail closed)`);
    return unauthorized;
  }
  const m = /^Bearer\s+(.+)$/i.exec(req.headers["authorization"] || "");
  if (m && safeEqual(m[1].trim(), expected)) return null;
  // Chat only: a non-revoked issued key also unlocks /v1/chat/completions.
  // Admin and /api/keys master-only, EXCEPT a logged-in dashboard session
  // (the lmr_ui cookie, set by POST /api/auth/login) counts as the admin
  // credential too -- the password IS the admin identity. Only when a
  // dashboard password is actually configured (uiPasswordEnv set AND its env
  // value non-empty); otherwise a session cannot exist and the master gate
  // stays authoritative. Machines keep using the master key; humans use the
  // password they logged in with.
  if (isChat && m && issuedKeyValid(m[1].trim())) return null;
  if (!isChat && cfg.uiPasswordEnv && process.env[cfg.uiPasswordEnv] && uiAuthed(cfg, req)) return null;
  return unauthorized;
}

// ---- server ---------------------------------------------------------------

let cfg = null;
// The RAW document whose normalized form is running. The reload path compares a
// candidate against THIS, not against `cfg`: normalization repairs and drops values
// (an empty-string retry is deleted outright), so the running object cannot answer
// "is this value unchanged from what the operator wrote?".
let rawInForce = null;

// The ONE place that decides what the router RUNS and what the validator JUDGES,
// shared by every entry point that can be handed a document: boot, hot-reload, a
// save from the Config tab, and a snapshot restore. Three inline copies of this
// policy is the failure mode it exists to prevent - a save judged differently
// from a load is a config whose meaning depends on how it arrived.
//
// It no longer CONVERTS. A pre-v3 document is run and stored exactly as written:
// the runtime synthesizes both shapes on its own (normalizeConfig), so migrating
// a file is an operator decision, never a side effect of loading or saving it.
// convertLegacyConfig stays exported as the pure converter such a migration
// would use, and the validator is given a canonicalized COPY instead.
//
// The container restore is part of the policy, not an option: the converter
// SYNTHESIZES `models`/`presets` to work on them, so a truncated file would come
// back looking complete and the validator's "'presets' is required at the top
// level" would be repaired away instead of reported. A key the input LACKED is
// therefore removed again after conversion, so the validator judges what the
// operator actually wrote.
//
// Conversion failure is RETURNED rather than thrown: the converter refuses only
// on a residual id that is both a model and a preset, which no rule can resolve,
// and the three callers answer it differently by design - boot aborts on the
// small set it cannot interpret, a reload keeps the config already running, and
// save/restore refuse with 400 - so the shared half must not pick one for them.
const MISSING_TOP_CONTAINERS = ["models", "presets"];

// The validator judges ONE shape, so it is handed a canonicalized copy: the two
// pre-three-layer forms are mapped onto the three-layer ones and NOTHING else is
// touched. No value is dropped or repaired on the way in - an unknown backend, a
// provider with no upstream the runtime could resolve, a legacy empty-string param -
// because a sanitizing converter in front of the validator is exactly how a bad
// reference reaches the disk unjudged.
// Exported under the name its consumers know it by: the editor harness measures a
// draft through THIS function so the UI's view of a saved payload and the server's
// are the same function rather than two policies that drift. It canonicalizes a COPY
// and never converts a document that has not started migrating - which is what makes
// a legacy no-op save a legacy save.
function structuralViewForValidation(rawInput) {
  if (!rawInput || typeof rawInput !== "object" || Array.isArray(rawInput)) return rawInput;
  const copy = JSON.parse(JSON.stringify(rawInput));
  const backends = Array.isArray(copy.backends) ? copy.backends : [];
  const backendById = new Map(backends.filter((b) => b && b.id).map((b) => [b.id, b]));
  const models = copy.models && typeof copy.models === "object" && !Array.isArray(copy.models) ? copy.models : null;
  // Detected exactly as the validator detects it (see the shape detection below):
  // config-wide, on the presence of `deployments` anywhere. A document that has not
  // started migrating is judged under its OWN rules, which is the contract the live
  // pre-v3 config satisfies today.
  const v3Document = Boolean(models) && Object.values(models).some((m) => m && typeof m === "object" && m.deployments !== undefined);
  for (const m of models ? Object.values(models) : []) {
    if (!m || typeof m !== "object" || Array.isArray(m)) continue;
    // Era 1: `backends: ["id", ...]` predates the per-provider `providers` array.
    if (!m.providers && Array.isArray(m.backends)) {
      m.providers = m.backends.map((b) => (typeof b === "string" ? { backend: b } : b));
      delete m.backends;
    }
    // A `providers` model is routable as it stands, so it is left alone - unless the
    // document also carries a v3 model, which puts the whole file under v3 rules. That
    // missing shape is completed here so the file validates as the one shape it runs,
    // and an upstream the runtime cannot resolve is left MISSING rather than invented,
    // so the validator names that provider by path.
    if (v3Document && !m.deployments && Array.isArray(m.providers)) {
      m.deployments = m.providers.map((pr) => {
        if (!pr || typeof pr !== "object") return pr;
        const out = Object.assign({}, pr);
        if (!out.upstream) {
          const b = backendById.get(out.backend);
          if (b && b.model) out.upstream = b.model;
        }
        return out;
      });
      delete m.providers;
    }
  }
  const presets = copy.presets && typeof copy.presets === "object" && !Array.isArray(copy.presets) ? copy.presets : null;
  for (const p of presets ? Object.values(presets) : []) {
    // Era 2: a preset listed `members` before it listed `models`.
    if (p && typeof p === "object" && !Array.isArray(p) && !p.models && Array.isArray(p.members)) {
      p.models = p.members;
      delete p.members;
    }
  }
  return copy;
}

function bridgeToEffectiveConfig(rawInput) {
  const missingContainers = MISSING_TOP_CONTAINERS.filter((k) => !(rawInput && typeof rawInput === "object" && k in rawInput));
  const subject = structuralViewForValidation(rawInput);
  // A container the document LACKED is not added for the validator either: a truncated
  // file has to be reported, not judged as the completed document this view would be.
  for (const k of missingContainers) if (subject && typeof subject === "object" && !Array.isArray(subject)) delete subject[k];
  return { effective: rawInput, subject, missingContainers };
}

async function loadConfig() {
  const raw = await readFileP(CONFIG_PATH, "utf8");
  const parsed = JSON.parse(raw);
  // Validate the parsed shape BEFORE normalizeConfig repairs it: normalize
  // silently synthesizes legacy shapes, so validating afterwards would judge
  // the repair instead of what was written. `cfg` is undefined at boot, which
  // makes the RESTART_ONLY checks vacuous there; load mode is what makes a
  // dangling preset reference a warning on this path (save still refuses it).
  //
  // The SUBJECT is the canonicalized copy, never the object the router runs: an
  // old-shape file is judged by its three-layer equivalent so one shape error is
  // not reported per model, while the file itself stays exactly as written.
  const bridge = bridgeToEffectiveConfig(parsed);
  const effective = bridge.effective;
  // Captured BEFORE normalizeConfig mutates the document in place: the exemption has to
  // compare against what the FILE says, and normalization drops exactly the legacy values
  // it exists to tolerate. Adopted only if this load is the one that takes effect.
  const rawSnapshot = JSON.parse(JSON.stringify(parsed));
  // The abort is deliberately narrow, because the validator's error set is the
  // SAVE-side contract and is stricter than what this loader tolerates: it
  // repairs a non-object `retry` (the live production config carries
  // retry: ""), synthesizes `models`/`presets` when they are absent or not an
  // object, and leaves an unset apiKeyEnv to fail closed per backend. A
  // save-side error the loader repairs must never be able to stop a running
  // router, so at boot only what the loader genuinely cannot interpret aborts:
  // a non-object root, or a missing `backends` (ensureBackends and the startup
  // banner both walk cfg.backends unguarded). A pre-presets-era config file
  // keeps booting. Everything else is logged and the router starts. At reload,
  // where a config is already running, ANY error-class finding rejects the
  // write and the previous config is kept.
  const verdict = validateConfig(bridge.subject, rawInForce, { mode: "load" });
  if (verdict.errors.length) {
    const detail = verdict.errors.map((e) => (e.path || "(root)") + ": " + e.message).join("; ");
    if (cfg) {
      console.error(`[${new Date().toISOString()}] config reload rejected: ${detail}`);
      return cfg;
    }
    const fatal = verdict.errors.filter((e) => e.path === "" || e.path === "backends");
    if (fatal.length) throw new Error("config invalid: " + detail);
    for (const e of verdict.errors) {
      console.error(`[${new Date().toISOString()}] config warning (not fatal at boot): ${e.path || "(root)"}: ${e.message}`);
    }
  }
  for (const w of verdict.warnings) {
    console.error(`[${new Date().toISOString()}] config warning: ${w.path}: ${w.message}`);
  }
  // Normalize the three-layer shape (backends -> models -> presets) on every
  // load: validate new-shape entries, synthesize legacy configs, derive preset
  // members. Routing and /api/config only ever see the normalized form.
  normalizeConfig(effective);
  // Reconcile: keep health registry keyed by backend id on reload.
  const prevIds = new Set(cfg ? (cfg.backends || []).filter((b) => b && typeof b === "object").map((b) => b.id) : []);
  rawInForce = rawSnapshot;
  cfg = effective;
  if (cfg.masterKeyEnv && !process.env[cfg.masterKeyEnv]) {
    console.error(
      `[${new Date().toISOString()}] config warning: masterKeyEnv '${cfg.masterKeyEnv}' set but process.env['${cfg.masterKeyEnv}'] is empty; auth-gated endpoints will reject all requests (fail closed)`
    );
  }
  if (cfg.uiPasswordEnv && !process.env[cfg.uiPasswordEnv]) {
    console.error(
      `[${new Date().toISOString()}] config warning: uiPasswordEnv '${cfg.uiPasswordEnv}' set but process.env['${cfg.uiPasswordEnv}'] is empty; dashboard login will reject all passwords (fail closed)`
    );
  }
  ensureBackends(cfg);
  pruneRoutes(cfg);
  return cfg;
}

function server() {
  return http.createServer(async (req, res) => {
    const url = req.url.split("?")[0];
    const prefix = cfg.prefix || "/v1";
    const p = prefix.replace(/\/$/, "");

    const authErr = requireAuth(cfg, req);
    if (authErr) return writeError(res, authErr);

    if (req.method === "GET" && url === "/health") {
      const now = Date.now();
      // ONE snapshot for the whole response: the per-backend deployments[] projection and
      // the flat routes[] list are two views of the same registry, and computing it twice
      // would let a route cool between them.
      const routeRows = routeHealthSnapshot(cfg, now);
      const backends = usableBackends(cfg).map((b) => {
        const h = health.get(b.id) || {};
        const isCooling = h.state === "cooling" && h.nextAvailableAt > now;
        const accountCooling = !!isCooling; // an ACCOUNT block applies to every route of it
        return {
          id: b.id,
          state: isCooling ? "cooling" : "healthy",
          nextAvailableAtMs: h.nextAvailableAt || 0,
          fails: h.fails || 0,
          lastError: h.lastError || null,
          manual: !!(isCooling && h.lastError && h.lastError.kind === "manual"),
          probeInFlight: !!h.probeInFlight,
          // v3 projection, from the same route registry the dashboard already reads, so a
          // configured route is listed before it has any history to report. The row is the
          // route's own state, EXCEPT that an account-wide block is reported on every route
          // of that account: ineligibility consults the account first, so a route with no
          // failure of its own is still refused while its account is cooling, and a row
          // claiming "healthy" there would be a lie about routing. lastErrorKind is
          // deliberately the ROUTE's own kind - what the route itself last failed with.
          deployments: routeRows
            .filter((r) => r.backend === b.id)
            .map((r) => ({
              id: b.id,
              upstream: r.upstream,
              state: accountCooling || r.state === "cooling" ? "cooling" : r.state,
              models: r.models,
              fails: r.fails,
              nextAvailableAtMs: accountCooling ? (h.nextAvailableAt || 0) : r.nextAvailableAtMs,
              retryInMs: accountCooling ? Math.max(0, (h.nextAvailableAt || 0) - now) : r.retryInMs,
              probeInFlight: accountCooling ? !!h.probeInFlight : r.probeInFlight,
              lastErrorKind: r.lastErrorKind,
              lastStatus: r.lastStatus,
              manual: accountCooling
                ? !!(h.lastError && h.lastError.kind === "manual")
                : (r.state === "cooling" && r.lastErrorKind === "manual"),
            })),
        };
      });
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, port: cfg.port, backends, routes: routeRows, preferences: sessionPreferenceStats() }));
    }

    if (req.method === "POST" && url === "/admin/reset-health") {
      resetAllHealth();
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (req.method === "GET" && url === p + "/models") {
      // Exposed presets first (config order), then exposed models not already
      // listed. `exposed: false` is a LISTING rule only: the entry stays
      // routable, it is just not advertised. The legacy collision rule is gone
      // with the v3 XOR constraint - a converted config has no collisions left.
      const ids = [];
      for (const id of Object.keys(cfg.presets || {})) {
        if (cfg.presets[id] && cfg.presets[id].exposed === false) continue;
        if (!ids.includes(id)) ids.push(id);
      }
      for (const id of Object.keys(cfg.models || {})) {
        if (cfg.models[id] && cfg.models[id].exposed === false) continue;
        if (!ids.includes(id)) ids.push(id);
      }
      const data = ids.map((id) => ({ id, object: "model", owned_by: "local-router" }));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ object: "list", data }));
    }

    if (req.method === "POST" && url === p + "/chat/completions") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      return handleChat(req, res, cfg, bodyText);
    }

    if (req.method === "GET" && url === "/") {
      let html;
      try {
        html = readFileSync(join(__dirname, "index.html"), "utf8");
      } catch {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "ui not found", type: "not_found" } }));
      }
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
      return res.end(html);
    }

    if (req.method === "GET" && url === "/api/history") {
      const raw = new URL(req.url, "http://localhost").searchParams.get("limit");
      let limit = 100;
      if (raw) {
        const n = parseInt(raw, 10);
        if (!Number.isNaN(n)) limit = Math.max(1, Math.min(500, n));
      }
      const entries = [...HISTORY].reverse().slice(0, limit);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ entries, limit }));
    }

    if (req.method === "GET" && url === "/api/history/detail") {
      const params = new URL(req.url, "http://localhost").searchParams;
      // Look up by call id (preferred, from the "Call" column) or legacy t.
      const callId = params.get("call");
      const rawT = params.get("t");
      const t = rawT != null && rawT !== "" ? Number(rawT) : NaN;
      const detail = (callId ? getDetailByCall(callId) : null) || (!Number.isNaN(t) ? getDetail(t) : null);
      if (!detail) {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: "not found" }));
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ detail }));
    }

    if (req.method === "GET" && url === "/api/stats") {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({ since: STATS.since, uptimeMs: Date.now() - startedAt, total: STATS.total, byBackend: STATS.byBackend, byModel: STATS.byModel })
      );
    }

    if (req.method === "GET" && url === "/api/dashboard") {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify(dashboardPayload(cfg)));
    }

    if (req.method === "GET" && url === "/api/config") {
      // Safe: cfg.backends carry apiKeyEnv NAMES, never resolved key values.
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify(cfg));
    }

    // ---- config editing (raw read / save / restore) ------------------------
    // The editor's read path is deliberately NOT /api/config: that serves the
    // normalized config, which drops top-level keys normalization does not
    // model (pricing, missCapture, alerts). See the block above loadConfig.

    if (req.method === "GET" && url === "/api/config/raw") {
      let raw;
      try {
        raw = readRawConfig();
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `config file is not valid JSON: ${e.message}`, type: "router_error" } })));
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({
          config: raw,
          revision: configRevision(),
          snapshots: listSnapshots(),
          envNames: envNames(),
        })
      );
    }

    if (req.method === "POST" && url === "/api/config") {
      const body = await readJsonBody(req);
      if (body.error) return writeError(res, body.error);
      const input = body.value || {};
      const candidate = input.config;
      const dryRun = input.dryRun === true;

      let current;
      try {
        current = readRawConfig();
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `cannot read the current config: ${e.message}`, type: "router_error" } })));
      }

      // What is WRITTEN is the payload the operator sent, in the shape they sent it:
      // a save never migrates a document. What is JUDGED is the canonicalized copy,
      // so every check below still runs against the shape the router will run.
      const bridge = bridgeToEffectiveConfig(candidate);
      const toSave = bridge.effective;

      const verdict = validateConfig(bridge.subject, current);
      const warnings = verdict.warnings.slice();
      if (verdict.errors.length) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false, errors: verdict.errors, warnings }));
      }

      // Lost-update guard: the browser tells us which revision it edited. If
      // the file moved underneath it (another tab, or a hand edit in an
      // editor), refuse rather than silently clobbering, unless forced.
      const base = input.baseRevision;
      const currentRevision = configRevision();
      if (!dryRun && base != null && Number(base) !== currentRevision && input.force !== true) {
        res.writeHead(409, { "content-type": "application/json" });
        return res.end(
          JSON.stringify({
            ok: false,
            error: {
              message: "the config file changed on disk since this page loaded it",
              type: "conflict",
            },
            warnings,
            current,
            revision: currentRevision,
          })
        );
      }

      if (dryRun) {
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true, dryRun: true, warnings, revision: currentRevision }));
      }

      const snapshot = snapshotConfig();
      try {
        writeConfigAtomic(toSave);
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `failed to write config: ${e.message}`, type: "router_error" } })));
      }
      for (const w of verdict.warnings) {
        console.warn(`[${new Date().toISOString()}] config save warning: ${w.path}: ${w.message}`);
      }
      console.error(`[${new Date().toISOString()}] config saved via web UI (snapshot ${snapshot || "unavailable"})`);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, warnings, snapshot, revision: configRevision() }));
    }

    if (req.method === "POST" && url === "/api/config/reset") {
      const body = await readJsonBody(req);
      if (body.error) return writeError(res, body.error);
      const input = body.value || {};
      const name = String(input.snapshot || "");
      const bad = newError(400, JSON.stringify({ error: { message: "invalid snapshot name", type: "invalid_request_error" } }));
      if (!name || !/^[A-Za-z0-9._-]+\.json$/.test(name) || name.includes("..")) return writeError(res, bad);
      const available = listSnapshots();
      if (!available.includes(name)) {
        return writeError(res, newError(404, JSON.stringify({ error: { message: `snapshot '${name}' not found`, type: "not_found" } })));
      }
      let restored;
      try {
        restored = JSON.parse(readFileSync(join(SNAPSHOT_DIR, name), "utf8"));
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `snapshot is unreadable: ${e.message}`, type: "router_error" } })));
      }
      // Save-class, like every other write to the live file: `current` is what
      // is actually on disk, so the startup-only keys the UI cannot change are
      // refused here too, and the snapshot is judged as the config it would
      // become rather than against itself. A restore is a deliberate user
      // action, so a bad snapshot is refused with its field paths instead of
      // being silently loaded.
      //
      // A snapshot is restored in the shape it was taken in, exactly as a save keeps
      // the shape it was sent in: restoring a pre-v3 snapshot must not migrate the live
      // file, and the snapshot is judged as the config it would become.
      const bridge = bridgeToEffectiveConfig(restored);
      const toRestore = bridge.effective;
      let currentRaw;
      try {
        currentRaw = readRawConfig();
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `cannot read the current config: ${e.message}`, type: "router_error" } })));
      }
      const verdict = validateConfig(bridge.subject, currentRaw);
      const warnings = verdict.warnings.slice();
      if (verdict.errors.length) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false, errors: verdict.errors, warnings }));
      }
      // Same lost-update guard as POST /api/config. Conditional on the caller
      // having sent a revision: a reset that omits baseRevision is a deliberate
      // "restore this snapshot now" and must still be accepted.
      if (input.baseRevision != null && Number(input.baseRevision) !== configRevision() && input.force !== true) {
        res.writeHead(409, { "content-type": "application/json" });
        return res.end(
          JSON.stringify({
            ok: false,
            error: { message: "the config file changed on disk since this page loaded it", type: "conflict" },
            warnings,
            revision: configRevision(),
          })
        );
      }
      // Snapshot the state being replaced so a restore is itself undoable.
      const safety = snapshotConfig();
      try {
        writeConfigAtomic(toRestore);
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `failed to restore: ${e.message}`, type: "router_error" } })));
      }
      console.error(`[${new Date().toISOString()}] config restored from snapshot ${name} (previous state saved as ${safety || "unavailable"})`);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, restored: name, warnings, revision: configRevision() }));
    }

    // ---- env editor (managed secrets store) -----------------------------------
    // The list endpoint carries NAMES and state only. The value exists in
    // exactly one place: the reveal call, made deliberately per var.

    if (req.method === "GET" && url === "/api/env") {
      const secrets = readSecrets();
      // Only variables the router actually cares about, not the whole process
      // environment: listing every inherited shell var (PATH, ComSpec, ...)
      // would bury the handful that matter and surface unrelated secrets from
      // the operator's shell. A shell/`.env` name appears only when the config
      // references it, plus anything already in the managed store.
      const names = new Set([...referencedEnvNames(), ...Object.keys(secrets), ...dotenvNames()]);
      const vars = [...names].sort().map((name) => {
        const source = envSource(name);
        const secretValue = secrets[name];
        const current = process.env[name];
        return {
          name,
          set: current != null && current !== "",
          editable: !isProtectedEnvName(name) && source !== "shell",
          reloadable: !STARTUP_ONLY_VARS.has(name),
          source,
          live: source === "shell" ? true : secretValue === undefined ? true : secretValue === current,
        };
      });
      const restartRequired = vars.some((v) => !v.live && !STARTUP_ONLY_VARS.has(v.name));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ vars, restartRequired, revision: secretsRevision() }));
    }

    if (req.method === "POST" && url === "/api/env/reveal") {
      const body = await readJsonBody(req);
      if (body.error) return writeError(res, body.error);
      const name = body.value && body.value.name;
      if (typeof name !== "string" || !/^[A-Z_][A-Z0-9_]*$/.test(name)) {
        return writeError(res, newError(400, JSON.stringify({ error: { message: "invalid env name", type: "invalid_request_error" } })));
      }
      // Same scope the list serves: without this gate any name in the process
      // environment (PATH, ComSpec, ...) could be read one call at a time.
      const known = new Set([...referencedEnvNames(), ...Object.keys(readSecrets()), ...dotenvNames()]);
      if (!known.has(name)) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: { message: `${name} is not managed by the env editor; only config-referenced, managed, and .env-declared variables can be revealed`, type: "invalid_request_error" } }));
      }
      const value = process.env[name];
      // Auth credentials are readable only as "set: true" from the list. The
      // dashboard session is gated by uiPasswordEnv, but masterKeyEnv is a
      // wider-scope machine credential (dsh/OpenCode hold it), so revealing it
      // to any session holder would escalate past what the UI needs.
      if (isProtectedEnvName(name)) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: { message: `${name} is an authentication credential and is never revealed`, type: "invalid_request_error" } }));
      }
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ name, value: value == null ? null : value }));
    }

    if (req.method === "POST" && url === "/api/env") {
      const body = await readJsonBody(req);
      if (body.error) return writeError(res, body.error);
      const input = body.value || {};
      const name = input.name;
      const value = input.value;
      const problem = validateEnvEntry(name, value);
      if (problem) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: { message: problem, type: "invalid_request_error" } }));
      }

      const secrets = readSecrets();
      const base = input.baseRevision;
      const currentRevision = secretsRevision();
      if (base != null && Number(base) !== currentRevision && input.force !== true) {
        res.writeHead(409, { "content-type": "application/json" });
        return res.end(
          JSON.stringify({
            ok: false,
            error: { message: "the secrets file changed on disk since this page loaded it", type: "conflict" },
            revision: currentRevision,
          })
        );
      }

      const action = name in secrets ? "updated" : "added";
      const snapshot = snapshotSecrets();
      secrets[name] = value;
      try {
        writeSecretsAtomic(secrets);
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `failed to write secrets store: ${e.message}`, type: "router_error" } })));
      }
      // The file watcher also fires for our own write; the direct call makes
      // the response's applied/live claims true without depending on event
      // timing.
      const applied = applyRuntimeEnv().includes(name);
      const live = process.env[name] === value;
      console.error(`[${new Date().toISOString()}] env var '${name}' ${action} via web UI (snapshot ${snapshot || "unavailable"})`);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, name, action, applied, live, snapshot, revision: secretsRevision() }));
    }

    if (req.method === "DELETE" && url === "/api/env") {
      const name = (req.url.split("?")[1] || "").match(/name=([A-Z_][A-Z0-9_]*)/);
      if (!name) {
        return writeError(res, newError(400, JSON.stringify({ error: { message: "missing or invalid name parameter", type: "invalid_request_error" } })));
      }
      const nameStr = name[1];
      if (isProtectedEnvName(nameStr)) {
        return writeError(res, newError(400, JSON.stringify({ error: { message: `'${nameStr}' is an authentication credential and cannot be changed from the UI`, type: "invalid_request_error" } })));
      }
      const secrets = readSecrets();
      if (!(nameStr in secrets)) {
        return writeError(
          res,
          newError(
            409,
            JSON.stringify({
              error: {
                message: `'${nameStr}' is not managed by the editor (source: ${envSource(nameStr)}); edit .env or the platform env instead`,
                type: "conflict",
              },
            })
          )
        );
      }
      const snapshot = snapshotSecrets();
      delete secrets[nameStr];
      try {
        writeSecretsAtomic(secrets);
      } catch (e) {
        return writeError(res, newError(500, JSON.stringify({ error: { message: `failed to write secrets store: ${e.message}`, type: "router_error" } })));
      }
      console.error(`[${new Date().toISOString()}] env var '${nameStr}' removed via web UI (snapshot ${snapshot || "unavailable"})`);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, name: nameStr, action: "removed" }));
    }

    if (req.method === "POST" && url === "/admin/backend") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "invalid JSON body", type: "invalid_request_error" } }));
      }
      const id = body && body.id;
      const action = body && body.action;
      if (!id || !usableBackends(cfg).some((b) => b.id === id)) {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: `backend '${id}' not found`, type: "not_found" } }));
      }
      if (action !== "cool" && action !== "uncool") {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "action must be 'cool' or 'uncool'", type: "invalid_request_error" } }));
      }
      // The optional `upstream` narrows the action to ONE physical route. It must name a
      // string, and it must be a route some model actually declares - cooling a route
      // nothing routes to would be a no-op reported as success.
      if (body.upstream !== undefined && (typeof body.upstream !== "string" || !body.upstream)) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "upstream must be a non-empty string", type: "invalid_request_error" } }));
      }
      if (body.upstream !== undefined) {
        const rKey = JSON.stringify([id, body.upstream]);
        if (!routeHealthSnapshot(cfg, Date.now()).some((r) => r.route === rKey)) {
          res.writeHead(404, { "content-type": "application/json" });
          return res.end(JSON.stringify({ error: { message: "route '" + id + "::" + body.upstream + "' is not declared by any model", type: "not_found" } }));
        }
        if (action === "cool") {
          const forMs = typeof body.forMs === "number" && body.forMs > 0 ? body.forMs : null;
          MANUAL_ROUTES.set(rKey, { until: forMs ? Date.now() + forMs : Number.MAX_SAFE_INTEGER, since: Date.now() });
          recordCoolingEvent({ backend: id, action: "cool", kind: "manual", manual: true, scope: "route" });
        } else {
          MANUAL_ROUTES.delete(rKey);
          // The route row goes with it: the operator cleared the only reason it was
          // cooling, so leaving it behind would keep lastErrorKind "manual" on the
          // dashboard for a route that is now eligible again.
          //
          // The fence is armed WHETHER OR NOT a row exists right now. A route that was never
          // cooled has no row yet, and a request already in flight against it is exactly the
          // work that must not create one after the operator cleared it. Hanging the fence off
          // the row's presence made the guard conditional on a coincidence.
          fenceRouteDeletion(rKey);
          if (ROUTES.has(rKey)) {
            ROUTES.delete(rKey);
            recordCoolingEvent({ backend: id, action: "uncool", kind: "manual", manual: true, scope: "route" });
          }
        }
        clearSessionPreferences(); // re-learn rather than overrule the operator
        const after = routeHealthSnapshot(cfg, Date.now()).find((r) => r.route === rKey);
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true, id, upstream: body.upstream, state: after ? after.state : "healthy" }));
      }
      if (action === "cool") {
        const h = health.get(id) || { state: "healthy", fails: 0, backoffMs: 0 };
        // No forMs = STICKY manual cool: cooled until uncooled. MAX_SAFE_INTEGER
        // is set exactly so /health can render "until uncooled" instead of a
        // countdown. A positive forMs = timed manual cool.
        const forMs = typeof body.forMs === "number" && body.forMs > 0 ? body.forMs : null;
        h.state = "cooling";
        h.nextAvailableAt = forMs ? Date.now() + forMs : Number.MAX_SAFE_INTEGER;
        h.fails = h.fails || 0;
        h.backoffMs = h.backoffMs || 0;
        h.lastError = { status: 0, kind: "manual", body: "manually cooled via UI" };
        health.set(id, h);
      } else {
        markHealthy(id, "uncool");
        // An uncool that leaves the backend's ROUTES cooling is a lie: /health would report
        // the account healthy while the router still refuses every route it was cooling
        // (review finding 12). The operator's decision clears the whole account layer. A
        // route row does not store its backend -- the key is [backend, upstream] -- so the
        // owner is read from the key, not from a field (zUncool3 caught exactly that).
        for (const rkey of [...ROUTES.keys()]) {
          let owner = null;
          try { owner = JSON.parse(rkey)[0]; } catch { owner = null; }
          if (owner === id) ROUTES.delete(rkey);
        }
        // A targeted route block is an operator decision about this account too, so an
        // account-wide uncool clears it. Leaving it would report "healthy" while the
        // router still refused that route - the same lie finding 12 named.
        for (const rkey of [...MANUAL_ROUTES.keys()]) {
          let owner = null;
          try { owner = JSON.parse(rkey)[0]; } catch { owner = null; }
          if (owner === id) MANUAL_ROUTES.delete(rkey);
        }
      }
      clearSessionPreferences(); // re-learn rather than overrule the operator
      const h = health.get(id);
      const state = h.state === "cooling" && h.nextAvailableAt > Date.now() ? "cooling" : "healthy";
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ ok: true, id, state }));
    }

    // ---- dashboard login (never gated: these ARE the login) ----------------

    if (req.method === "GET" && url === "/api/auth/status") {
      // passwordSet reflects the config key (set = login required), NOT the
      // env value, so a password left empty still reports "required" and the
      // login endpoint fails closed. Never echoes the password.
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ passwordSet: !!cfg.uiPasswordEnv, uiAuthed: uiAuthed(cfg, req) }));
    }

    if (req.method === "POST" && url === "/api/auth/login") {
      if (!cfg.uiPasswordEnv) {
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true, uiAuthed: true }));
      }
      const ip = req.socket.remoteAddress || "unknown";
      if (loginRateLimited(ip)) {
        res.writeHead(429, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: "too many login attempts, try again shortly", type: "rate_limit_error" } }));
      }
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        body = null;
      }
      const expected = uiPasswordValue(cfg);
      if (!expected) {
        // Fail closed: password configured but env value empty at request time.
        res.writeHead(401, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false }));
      }
      const given = body && typeof body.password === "string" ? body.password : "";
      if (!safeEqual(hashSecret(given), hashSecret(expected))) {
        recordLoginAttempt(ip);
        res.writeHead(401, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: false }));
      }
      const token = crypto.randomBytes(24).toString("base64url");
      uiSessions.add(token);
      res.writeHead(200, {
        "content-type": "application/json",
        "set-cookie": `${UI_SESSION_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Strict`,
      });
      return res.end(JSON.stringify({ ok: true, uiAuthed: true }));
    }

    if (req.method === "POST" && url === "/api/auth/logout") {
      const m = /(?:^|;\s*)lmr_ui=([^;\s]+)/.exec(req.headers.cookie || "");
      if (m) uiSessions.delete(m[1]);
      res.writeHead(200, {
        "content-type": "application/json",
        "set-cookie": `${UI_SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`,
      });
      return res.end(JSON.stringify({ ok: true }));
    }

    // ---- issued-keys management (master-key gated by requireAuth) ----------

    if (req.method === "GET" && url === "/api/keys") {
      // Never expose hashes or raw values, only the listing metadata.
      const { keys } = loadIssuedKeys();
      const list = keys.map((k) => ({ id: k.id, name: k.name, createdAt: k.createdAt, revoked: !!k.revoked }));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ keys: list }));
    }

    if (req.method === "POST" && url === "/api/keys") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bodyText = Buffer.concat(chunks).toString("utf8");
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        body = null;
      }
      const name = body && typeof body.name === "string" ? body.name.trim() : "";
      if (!name || name.length > ISSUED_KEYS_MAX_NAME) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: { message: `name must be a non-empty string of at most ${ISSUED_KEYS_MAX_NAME} chars`, type: "invalid_request_error" } }));
      }
      try {
        const key = await issueKey(name);
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ key }));
      } catch (e) {
        console.error(`[${new Date().toISOString()}] issueKey failed: ${e.message}`);
        return writeError(res, newError(500, JSON.stringify({ error: { message: "failed to persist keys", type: "router_error" } })));
      }
    }

    if (req.method === "DELETE" && url === "/api/keys") {
      const id = new URL(req.url, "http://localhost").searchParams.get("id") || "";
      try {
        const ok = await revokeKey(id);
        if (!ok) {
          res.writeHead(404, { "content-type": "application/json" });
          return res.end(JSON.stringify({ error: { message: `key '${id}' not found`, type: "not_found" } }));
        }
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        console.error(`[${new Date().toISOString()}] revokeKey failed: ${e.message}`);
        return writeError(res, newError(500, JSON.stringify({ error: { message: "failed to persist keys", type: "router_error" } })));
      }
    }

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: `not found: ${req.method} ${url}`, type: "not_found" } }));
  });
}

// ---- startup ---------------------------------------------------------------

async function main() {
  for (const k of Object.keys(process.env)) SHELL_ENV_NAMES.add(k);
  loadEnv();
  loadSecrets();
  await loadConfig();
  seedAnalytics(historyPath);
  watchF(CONFIG_PATH, () => {
    loadConfig().catch((e) => console.error("config reload failed:", e.message));
  });
  // The managed secrets store hot-reloads like the config: a UI write (or a
  // hand edit) reaches process.env in ~300ms. Idempotent by design: editors
  // can fire several change events per save. fs.watch needs the file to exist,
  // so a fresh install gets an empty store rather than a dead watcher.
  try {
    if (!statSync(SECRETS_PATH).isFile()) throw new Error("not a file");
  } catch {
    try {
      writeSecretsAtomic({});
    } catch (e) {
      console.error(`[${new Date().toISOString()}] secrets store not writable: ${e.message}`);
    }
  }
  try {
    watchF(SECRETS_PATH, () => {
      loadSecrets();
    });
  } catch (e) {
    console.error(`[${new Date().toISOString()}] secrets watcher unavailable: ${e.message}`);
  }

  // Always listen on localhost (existing consumers: OpenCode, dsh, local UI).
  // When cfg.host is set (e.g. a Tailscale IP to expose the router on the
  // tailnet), listen on that address too. Two server instances share the same
  // request handler. A wildcard host (0.0.0.0 / ::) already covers loopback, so
  // bind ONLY the wildcard: binding both on one port is EADDRINUSE on Linux.
  const hosts = new Set(["127.0.0.1"]);
  if (cfg.host) hosts.add(String(cfg.host));
  if (cfg.host === "0.0.0.0" || cfg.host === "::") hosts.delete("127.0.0.1");
  for (const bindHost of hosts) {
    const srv = server();
    srv.listen(cfg.port, bindHost, () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : cfg.port;
      console.error(`[${new Date().toISOString()}] switchblade listening on http://${bindHost}:${port}${cfg.prefix || "/v1"}`);
    });
  }
  console.error(`[${new Date().toISOString()}] config: ${CONFIG_PATH}`);
  console.error(`[${new Date().toISOString()}] backends: ${(cfg.backends || []).filter((b) => b && typeof b === "object").map((b) => `${b.id}(${mask(process.env[b.apiKeyEnv])})`).join(", ")}`);
}

// Running the file as a script starts the server; importing it (test.mjs
// imports candidates/normalizeConfig/buildPayload for unit tests) does not.
const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  main().catch((e) => {
    console.error("fatal:", e);
    process.exit(1);
  });
}

export { claimAttempt, releaseAttempt, ineligibility, health, ROUTES };
export { structuralViewForValidation as validationSubjectOf };
export { backoffFor, sessionHeaderFor, noteAccountOutage, markSuccess, ACCOUNT_OUTAGE_ROUTES, classify, readUpstreamError, terminalChatError, redactSecrets, capAttemptsSummary, attemptOutcome, candidates, touchSessionPreference, evictSessionPreferences, SESSION_PREFS, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, routeKey, routeBackoffMs, authScopeOf, sessionPreferenceKey, beginSessionRequest, orderSessionProviders, rememberProvider, sessionPreferenceStats, clearSessionPreferences, validateConfig, convertLegacyConfig };
