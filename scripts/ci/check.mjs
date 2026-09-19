#!/usr/bin/env node
// scripts/ci/check.mjs — the one CI checker for this repo.
//
// Local and CI run the SAME code path: `npm run ci` here, or
// `node scripts/ci/check.mjs static|suite` from the workflow. Every check
// prints one greppable PASS/FAIL line; the process exits non-zero if any
// check failed. Node built-ins only, no network, deterministic.

import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");

// The suite's pass count is a coverage ratchet: it may only ever be RAISED.
// Lowering it is legitimate only when a commit deliberately removes tests,
// and then it is lowered in that SAME commit so the coverage reduction is
// visible in review. Never lower it to turn a red run green — fix the code
// or the test instead.
const SUITE_FLOOR = 395;

// Sanity floor for the S5 id scan: the suite carries far more ids than this,
// so a count below the floor means the extractor broke, not that the ids went
// away, so it must never pass by matching nothing. SUITE_FLOOR is the coverage
// ratchet; this one only guards the guard.
const TEST_ID_FLOOR = 300;

// Wall-clock guard: a suite that hangs must fail loudly, not hang CI forever.
const SUITE_TIMEOUT_MS = 10 * 60 * 1000;

const CONFIG_FILES = ["config.json", "config.server.json"];
const SYNTAX_FILES = ["server.mjs", "test.mjs"];
const IMPORT_SCAN_TARGETS = ["server.mjs", "test.mjs", "scripts"];
const IMPORT_EXTENSIONS = [".mjs", ".js", ".cjs"];
const DEP_BLOCKS = ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"];
const CONSUMER_FILE = "test.mjs";
const CONSUMER_SPECIFIER = "./server.mjs";

let ran = 0;
let failed = 0;

// Output budget: a clean run is read for exactly two things, the PASS/FAIL
// lines and the suite tail. The non-fatal validator notices repeat every run
// and would bury them, so by default they are counted, not printed. They are
// still printed in full when a check FAILs (a failure must be self-diagnosing
// without a second invocation) and whenever --notices is passed.
const args = process.argv.slice(2);
const SHOW_NOTICES = args.includes("--notices");
const MODE = args.find((a) => !a.startsWith("--")) || "";
const UNKNOWN_FLAG = args.find((a) => a.startsWith("--") && a !== "--notices");

function report(id, title, ok, evidence, notes) {
  ran++;
  if (!ok) failed++;
  console.log(`${ok ? "PASS" : "FAIL"} ${id} ${title}: ${evidence}`);
  const list = notes || [];
  if (list.length && (!ok || SHOW_NOTICES)) {
    for (const note of list) console.log(`  ${note}`);
  } else if (list.length) {
    console.log(`  ${list.length} notice(s) suppressed (informational only); re-run with --notices to list them`);
  }
  return ok;
}

function rel(p) {
  return relative(ROOT, p).split(sep).join("/");
}

function lineOf(src, index) {
  let line = 1;
  for (let i = 0; i < index && i < src.length; i++) if (src[i] === "\n") line++;
  return line;
}

function firstLines(text, count) {
  return (text || "").trim().split(/\r?\n/).slice(0, count).join(" | ");
}

function walk(dir, acc = [], depth = 0) {
  if (depth > 6) return acc;
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return acc;
  }
  for (const entry of entries) {
    if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) walk(full, acc, depth + 1);
    else acc.push(full);
  }
  return acc;
}

// ---------------------------------------------------------------------------
// S1 — syntax
// ---------------------------------------------------------------------------

function checkSyntax() {
  const broken = [];
  for (const file of SYNTAX_FILES) {
    if (!existsSync(join(ROOT, file))) {
      broken.push(`${file}: missing, cannot syntax-check`);
      continue;
    }
    const res = spawnSync(process.execPath, ["--check", file], { cwd: ROOT, encoding: "utf8" });
    if (res.error) broken.push(`${file}: ${res.error.message}`);
    else if (res.status !== 0) broken.push(`${file}: ${firstLines(res.stderr, 4)}`);
  }
  return {
    ok: broken.length === 0,
    evidence: `${SYNTAX_FILES.length} files parsed via node --check`,
    notes: broken,
  };
}

// ---------------------------------------------------------------------------
// S2 — zero dependencies (package.json blocks, bare imports, external assets)
// ---------------------------------------------------------------------------

function isBareSpecifier(spec) {
  return !(
    spec.startsWith("node:") ||
    spec.startsWith("./") ||
    spec.startsWith("../") ||
    spec.startsWith("/") ||
    spec.startsWith("file:") ||
    spec.startsWith("data:") ||
    spec === "." ||
    spec === ".."
  );
}

// Each pattern captures the specifier in group 1. The `from` form is anchored
// on an import/export keyword: an unrelated `from "text"` inside a comment or
// a message string is not a specifier and must not be reported as one.
const SPECIFIER_PATTERNS = [
  /(?:^|[\n\r;}])\s*(?:import|export)\s+[^;]*?\s+from\s*["']([^"']+)["']/gm,
  /(?:^|[\n\r;}])\s*import\s*["']([^"']+)["']/gm,
  /\bimport\s*\(\s*["']([^"']+)["']\s*\)/g,
  /\brequire\s*\(\s*["']([^"']+)["']\s*\)/g,
];

function checkBareImports() {
  const notes = [];
  let scanned = 0;
  for (const target of IMPORT_SCAN_TARGETS) {
    const full = join(ROOT, target);
    if (!existsSync(full)) {
      notes.push(`scan target '${target}' is missing; the import scan would be vacuous`);
      continue;
    }
    const files = statSync(full).isDirectory() ? walk(full) : [full];
    for (const file of files) {
      if (!IMPORT_EXTENSIONS.includes(file.slice(file.lastIndexOf(".")))) continue;
      scanned++;
      const src = readFileSync(file, "utf8");
      for (const pattern of SPECIFIER_PATTERNS) {
        pattern.lastIndex = 0;
        let m;
        while ((m = pattern.exec(src)) !== null) {
          if (isBareSpecifier(m[1])) {
            notes.push(`${rel(file)}:${lineOf(src, m.index)}: bare specifier '${m[1]}' (only node: and relative specifiers are allowed)`);
          }
        }
      }
    }
  }
  return { scanned, notes };
}

function isExternalAsset(value) {
  if (/^(https?:)?\/\//i.test(value)) return true;
  const host = value.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "").split("/")[0];
  if (/(^|\.)cdn\./i.test(host)) return true;
  return /(cdnjs|unpkg|jsdelivr|gstatic|googleapis|bootstrapcdn|cloudflare|staticfile|cdn\.)/i.test(value);
}

function checkExternalAssets() {
  const file = join(ROOT, "index.html");
  if (!existsSync(file)) return { notes: ["index.html is missing; external-asset check cannot run"] };
  const html = readFileSync(file, "utf8");
  const notes = [];
  let refs = 0;
  // `(?<!\.)` keeps JS member writes (`el.href = ...`, `img.src = ...`) out of
  // the asset scan: this is about markup references, not script assignments.
  // A declaration (`const src = ...`) is skipped for the same reason.
  const re = /(?<!\.)(?:src|href)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'`=<>]+))/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    if (/\b(?:const|let|var)\s+$/.test(html.slice(Math.max(0, m.index - 8), m.index))) continue;
    const value = (m[1] ?? m[2] ?? m[3] ?? "").trim();
    if (!value) continue;
    refs++;
    if (isExternalAsset(value)) {
      notes.push(`index.html:${lineOf(html, m.index)}: external asset reference '${value}' (the UI is single-file: no http(s), protocol-relative, or CDN assets)`);
    }
  }
  return { refs, notes };
}

function checkDependencies() {
  const notes = [];
  let declared = 0;
  const pkgFile = join(ROOT, "package.json");
  if (!existsSync(pkgFile)) {
    notes.push("package.json is missing; the dependency check would be vacuous");
  } else {
    let pkg = null;
    try {
      pkg = JSON.parse(readFileSync(pkgFile, "utf8"));
    } catch (e) {
      notes.push(`package.json is not valid JSON: ${e.message}`);
    }
    for (const key of DEP_BLOCKS) {
      if (pkg && key in pkg) {
        declared++;
        notes.push(`package.json declares '${key}': ${JSON.stringify(pkg[key])}`);
      }
    }
  }

  const imports = checkBareImports();
  notes.push(...imports.notes);
  const assets = checkExternalAssets();
  notes.push(...assets.notes);

  return {
    ok: notes.length === 0,
    evidence: `${declared} dependency block(s), ${imports.scanned} import-scanned file(s), ${assets.refs ?? 0} index.html asset ref(s)`,
    notes,
  };
}

// ---------------------------------------------------------------------------
// S3 — inline UI script syntax
// ---------------------------------------------------------------------------

// Classic bodies are checked as .cjs and module bodies as .mjs, so the parse
// route never depends on an ambient package.json "type".
function checkInlineUi() {
  const file = join(ROOT, "index.html");
  if (!existsSync(file)) {
    return { ok: false, evidence: "index.html is missing", notes: ["cannot verify inline UI syntax"] };
  }
  const html = readFileSync(file, "utf8");
  const blocks = [];
  const re = /<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const gt = m[0].indexOf(">");
    blocks.push({ attrs: m[1], body: m[2], bodyStart: m.index + gt + 1 });
  }
  const openTags = (html.match(/<script\b/gi) || []).length;
  const notes = [];
  if (blocks.length !== openTags) {
    notes.push(`found ${openTags} opening <script> tag(s) but only ${blocks.length} closed block(s); the extraction is incomplete`);
  }
  if (blocks.length === 0) {
    return {
      ok: false,
      evidence: "0 inline <script> blocks",
      notes: [...notes, "no inline script to parse, so this check would be vacuous"],
    };
  }

  const dir = mkdtempSync(join(tmpdir(), "switchblade-ci-"));
  try {
    blocks.forEach((block, i) => {
      const isModule = /\btype\s*=\s*["']?\s*module/i.test(block.attrs);
      const ext = isModule ? "mjs" : "cjs";
      const tmpFile = join(dir, `inline-${i + 1}.${ext}`);
      writeFileSync(tmpFile, block.body, "utf8");
      const res = spawnSync(process.execPath, ["--check", tmpFile], { encoding: "utf8" });
      if (res.error) {
        notes.push(`inline script ${i + 1}: ${res.error.message}`);
        return;
      }
      if (res.status === 0) return;
      const text = `${res.stderr || ""}${res.stdout || ""}`;
      const escaped = tmpFile.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const found = new RegExp(`${escaped}:(\\d+)`).exec(text);
      const bodyLine = lineOf(html, block.bodyStart);
      const at = found ? `index.html:${bodyLine + Number(found[1]) - 1}` : "index.html: (line unknown)";
      notes.push(`${at}: syntax error in inline <script> ${i + 1} (${isModule ? "module" : "classic"})`);
      notes.push(`  ${firstLines(text, 3)}`);
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }

  return {
    ok: notes.length === 0,
    evidence: `${blocks.length} inline <script> block(s) parsed (${blocks.map((b) => (/\btype\s*=\s*["']?\s*module/i.test(b.attrs) ? "module" : "classic")).join(", ")})`,
    notes,
  };
}

// ---------------------------------------------------------------------------
// S4 — config shape and export surface
// ---------------------------------------------------------------------------

function importedNamesFrom(src, specifier) {
  const escaped = specifier.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  // `[^;]*?` is the import clause and cannot cross a statement terminator, so
  // the clause is never borrowed from an earlier import on another line.
  const re = new RegExp(`(?:^|[\\n\\r;}])\\s*import\\s+([^;]*?)\\s+from\\s*["']${escaped}["']`, "gm");
  const names = [];
  let m;
  while ((m = re.exec(src)) !== null) {
    const clause = m[1].trim();
    const brace = clause.indexOf("{");
    const before = (brace < 0 ? clause : clause.slice(0, brace)).replace(/,\s*$/, "").trim();
    if (before && before !== "*") names.push("default");
    if (brace >= 0) {
      const inner = clause.slice(brace + 1, clause.lastIndexOf("}"));
      for (const part of inner.split(",")) {
        const name = part.split(/\s+as\s+/)[0].trim();
        if (name) names.push(name);
      }
    }
  }
  return names;
}

async function checkConfigShape() {
  const notes = [];
  const fatalFiles = [];
  let server = null;
  try {
    server = await import(pathToFileURL(join(ROOT, "server.mjs")).href);
  } catch (e) {
    return { ok: false, evidence: "cannot import server.mjs", notes: [e.message] };
  }

  for (const name of CONFIG_FILES) {
    const file = join(ROOT, name);
    if (!existsSync(file)) {
      notes.push(`${name}: MISSING (a tracked config must exist)`);
      fatalFiles.push(name);
      continue;
    }
    let parsed;
    try {
      parsed = JSON.parse(readFileSync(file, "utf8"));
    } catch (e) {
      notes.push(`${name}: invalid JSON: ${e.message}`);
      fatalFiles.push(name);
      continue;
    }
    const verdict = server.validateConfig(parsed, parsed, { mode: "load" });
    // Boot treats only these two as fatal (server.mjs:2962); everything else
    // the validator reports is a notice the router survives.
    const isFatal = (e) => e.path === "" || e.path === "backends";
    const fatal = verdict.errors.filter(isFatal);
    for (const entry of verdict.errors) {
      notes.push(`${name}: ${isFatal(entry) ? "FATAL" : "notice"} ${entry.path || "(root)"}: ${entry.message}`);
    }
    for (const warning of verdict.warnings) {
      notes.push(`${name}: notice (warning) ${warning.path || "(root)"}: ${warning.message}`);
    }
    if (fatal.length) fatalFiles.push(name);
  }

  const consumer = join(ROOT, CONSUMER_FILE);
  if (!existsSync(consumer)) {
    notes.push(`${CONSUMER_FILE} is missing; the export-surface check would be vacuous`);
    fatalFiles.push(CONSUMER_FILE);
  } else {
    const names = importedNamesFrom(readFileSync(consumer, "utf8"), CONSUMER_SPECIFIER);
    if (names.length === 0) {
      notes.push(`${CONSUMER_FILE} no longer imports named exports from '${CONSUMER_SPECIFIER}'; the guard would be vacuous`);
      fatalFiles.push(CONSUMER_FILE);
    }
    for (const name of names) {
      if (name === "default" || !(name in server)) notes.push(`${CONSUMER_FILE} imports '${name}' from ${CONSUMER_SPECIFIER}, which does not export it`);
    }
  }

  return {
    ok: fatalFiles.length === 0 && !notes.some((n) => n.includes("does not export it")),
    evidence: `${CONFIG_FILES.length} config(s) loaded, export surface checked against ${CONSUMER_FILE}`,
    notes,
  };
}

// ---------------------------------------------------------------------------
// S5 — test id hygiene
// ---------------------------------------------------------------------------

// A test's id is the leading token of the name passed as assert()'s second
// argument. Conditions routinely contain commas (arrays, calls), so the
// argument boundary is found by tracking bracket depth, not by regex.
function scanAsserts(src) {
  const sites = [];
  const re = /\bassert\s*\(/g;
  let m;
  while ((m = re.exec(src)) !== null) {
    const comma = topLevelComma(src, m.index + m[0].length);
    sites.push({ index: m.index, name: comma < 0 ? null : readStringLiteral(src, skipTrivia(src, comma + 1)) });
  }
  return sites;
}

function skipTrivia(src, i) {
  for (;;) {
    const c = src[i];
    if (c === " " || c === "\t" || c === "\n" || c === "\r") {
      i++;
      continue;
    }
    if (c === "/" && src[i + 1] === "/") {
      const nl = src.indexOf("\n", i);
      i = nl < 0 ? src.length : nl + 1;
      continue;
    }
    if (c === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      i = end < 0 ? src.length : end + 2;
      continue;
    }
    return i;
  }
}

function readStringLiteral(src, i) {
  const quote = src[i];
  if (quote !== '"' && quote !== "'" && quote !== "`") return null;
  let out = "";
  for (let j = i + 1; j < src.length; j++) {
    const c = src[j];
    if (c === "\\") {
      out += src[j + 1] ?? "";
      j++;
      continue;
    }
    if (c === quote) return out;
    if (quote === "`" && c === "$" && src[j + 1] === "{") return null;
    out += c;
  }
  return null;
}

function topLevelComma(src, i) {
  let depth = 0;
  for (let j = i; j < src.length; j++) {
    const c = src[j];
    if (c === '"' || c === "'" || c === "`") {
      const quote = c;
      j++;
      while (j < src.length && src[j] !== quote) {
        if (src[j] === "\\") j++;
        j++;
      }
      continue;
    }
    if (c === "/" && src[j + 1] === "/") {
      const nl = src.indexOf("\n", j);
      j = nl < 0 ? src.length : nl;
      continue;
    }
    if (c === "/" && src[j + 1] === "*") {
      const end = src.indexOf("*/", j + 2);
      j = end < 0 ? src.length : end + 1;
      continue;
    }
    if (c === "(" || c === "[" || c === "{") depth++;
    else if (c === ")" || c === "]" || c === "}") {
      if (depth === 0) return -1;
      depth--;
    } else if (c === "," && depth === 0) return j;
  }
  return -1;
}

function checkTestIds() {
  const file = join(ROOT, "test.mjs");
  if (!existsSync(file)) {
    return { ok: false, evidence: "test.mjs is missing", notes: ["cannot extract test ids"] };
  }
  const src = readFileSync(file, "utf8");
  const sites = scanAsserts(src);
  const names = sites.filter((s) => s.name !== null);
  const unnamed = sites.filter((s) => s.name === null);
  const ids = [];
  const idless = [];
  for (const { name } of names) {
    // The id is everything up to and including the label's first ")". It is
    // read permissively (leading space, internal space, dots, dashes, digits)
    // because the suite really does carry ids like "zCE5 h)". A "(" inside the
    // candidate means it is prose, not an id, so it is rejected.
    const m = /^\s*([^()]{1,40})\)/.exec(name);
    if (m) ids.push(m[1]);
    else idless.push(name);
  }
  const counts = new Map();
  for (const id of ids) counts.set(id, (counts.get(id) || 0) + 1);
  const duplicates = [...counts].filter(([, n]) => n > 1).sort((a, b) => b[1] - a[1]);

  const notes = [];
  for (const [id, n] of duplicates) notes.push(`duplicate test id '${id})' used ${n} times`);
  if (ids.length < TEST_ID_FLOOR) {
    notes.push(`extracted only ${ids.length} test id(s), below the sanity floor of ${TEST_ID_FLOOR}; the extractor is probably broken`);
  }
  for (const { index } of unnamed) {
    notes.push(`line ${lineOf(src, index)}: assert call carries no literal name, so no id could be read (not a failure)`);
  }
  for (const name of idless) {
    notes.push(`id-less label: ${JSON.stringify(name.slice(0, 60))} (not a failure)`);
  }

  return {
    ok: duplicates.length === 0 && ids.length >= TEST_ID_FLOOR,
    evidence: `${ids.length} test id(s) from ${sites.length} assert call site(s) (${names.length} named), ${duplicates.length} duplicate(s)`,
    notes,
  };
}

// ---------------------------------------------------------------------------
// S6 — suite contract
// ---------------------------------------------------------------------------

function checkSuite() {
  const file = join(ROOT, "test.mjs");
  if (!existsSync(file)) {
    return { ok: false, evidence: "test.mjs is missing", notes: ["cannot run the suite"] };
  }
  const res = spawnSync(process.execPath, ["test.mjs"], {
    cwd: ROOT,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    timeout: SUITE_TIMEOUT_MS,
  });
  const stdout = res.stdout || "";
  const stderr = res.stderr || "";

  let tail = null;
  for (const raw of stdout.split(/\r?\n/)) {
    const m = /^(\d+) passed, (\d+) failed$/.exec(raw.trim());
    if (m) tail = { passed: Number(m[1]), failed: Number(m[2]), line: raw.trim() };
  }

  const notes = [];
  let ok = true;
  if (res.error && res.error.code === "ETIMEDOUT") {
    ok = false;
    notes.push(`suite exceeded the ${SUITE_TIMEOUT_MS} ms timeout`);
  } else if (res.error) {
    ok = false;
    notes.push(`could not run the suite: ${res.error.message}`);
  } else if (res.status !== 0) {
    ok = false;
    notes.push(`suite exited with code ${res.status}`);
  }

  if (!tail) {
    ok = false;
    notes.push("no tail line matching ^(\\d+) passed, (\\d+) failed$ — a crashed or truncated run must never pass");
  } else {
    if (tail.failed !== 0) {
      ok = false;
      notes.push(`${tail.failed} test(s) failed`);
    }
    if (tail.passed < SUITE_FLOOR) {
      ok = false;
      notes.push(`passed ${tail.passed} is below the floor of ${SUITE_FLOOR}`);
    }
  }

  if (!ok) {
    const stderrTail = stderr.trim().split(/\r?\n/).slice(-8).join(" | ");
    if (stderrTail) notes.push(`stderr tail: ${stderrTail}`);
  }

  return {
    ok,
    evidence: tail ? `tail "${tail.line}" (floor ${SUITE_FLOOR})` : "no tail line observed",
    notes,
  };
}

// ---------------------------------------------------------------------------
// S7 — tracked junk
// ---------------------------------------------------------------------------

const JUNK_BASENAMES = new Set([".env", "secrets.json", "router-history.jsonl", "router-misses.jsonl", "api-keys.json"]);

function isJunkPath(path) {
  const base = path.split("/").pop();
  if (JUNK_BASENAMES.has(base)) return true;
  if (base.endsWith(".json.tmp")) return true;
  if (base.endsWith(".log")) return true;
  if (path === "secrets.history" || path.startsWith("secrets.history/")) return true;
  return false;
}

function checkTrackedJunk() {
  const res = spawnSync("git", ["ls-files", "-z"], { cwd: ROOT, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  if (res.error) {
    return { ok: false, evidence: "git ls-files could not run", notes: [res.error.message] };
  }
  if (res.status !== 0) {
    return {
      ok: false,
      evidence: "git ls-files failed",
      notes: ["not a git checkout, so the tracked-file list cannot be verified", firstLines(res.stderr, 3)],
    };
  }
  const tracked = (res.stdout || "").split("\0").filter(Boolean);
  if (tracked.length === 0) {
    return { ok: false, evidence: "git ls-files returned nothing", notes: ["an empty file list would make this check vacuous"] };
  }
  const junk = tracked.filter(isJunkPath);
  return {
    ok: junk.length === 0,
    evidence: `${tracked.length} tracked file(s) scanned`,
    notes: junk.map((p) => `tracked: ${p} (secrets, payloads, or logs must never be committed)`),
  };
}

// ---------------------------------------------------------------------------

function runStatic() {
  const s1 = checkSyntax();
  report("S1", "syntax", s1.ok, s1.evidence, s1.notes);

  const s2 = checkDependencies();
  report("S2", "zero dependencies", s2.ok, s2.evidence, s2.notes);

  const s3 = checkInlineUi();
  report("S3", "inline UI syntax", s3.ok, s3.evidence, s3.notes);

  return checkConfigShape().then((s4) => {
    report("S4", "config shape + export surface", s4.ok, s4.evidence, s4.notes);

    const s5 = checkTestIds();
    report("S5", "test id hygiene", s5.ok, s5.evidence, s5.notes);

    const s7 = checkTrackedJunk();
    report("S7", "tracked junk", s7.ok, s7.evidence, s7.notes);
  });
}

function runSuite() {
  const s6 = checkSuite();
  report("S6", "suite contract", s6.ok, s6.evidence, s6.notes);
}

async function main() {
  if (UNKNOWN_FLAG) {
    console.error(`unknown flag '${UNKNOWN_FLAG}'; only --notices is recognised`);
    console.error("usage: node scripts/ci/check.mjs [static|suite] [--notices]");
    process.exit(2);
  }
  if (MODE && MODE !== "static" && MODE !== "suite") {
    console.error(`unknown mode '${MODE}'; expected 'static', 'suite', or no argument`);
    console.error("usage: node scripts/ci/check.mjs [static|suite] [--notices]");
    process.exit(2);
  }
  if (MODE !== "suite") await runStatic();
  if (MODE !== "static") runSuite();
  console.log(`CI RESULT: ${failed ? "FAIL" : "PASS"} (${failed} of ${ran} checks failed)`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error("checker crashed:", e);
  process.exit(1);
});
