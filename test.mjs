// test.mjs — mock-based test for switchblade. No real keys or network.
// Spins up local mock backends and a server instance using a temp config.

import http from "node:http";
import { once } from "node:events";
import { writeFile, mkdtemp, rm, readFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import vm from "node:vm";
// Unit-level imports: server.mjs only starts the server when run as a script,
// so importing it here gives direct access to strategy selection for tests.
import { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost, writeConfigAtomic, validateConfig, convertLegacyConfig } from "./server.mjs";
// Namespace import so a not-yet-implemented helper reads as undefined and fails
// its own assertion, rather than crashing the whole file at load time.
import * as SRV from "./server.mjs";

const HOST = "127.0.0.1";
let passed = 0;
let failed = 0;

function assert(cond, name) {
  if (cond) { passed++; console.log("PASS " + name); }
  else { failed++; console.log("FAIL " + name); }
}

function listen(srv) {
  return new Promise((resolve) => srv.listen(0, HOST, () => resolve(srv.address().port)));
}

// Mock backend factory: returns a function that creates an http server.
// `responses` objects per-path map to {status, body}. `hits` records calls.
function mockBackend(id, responses) {
  const hits = [];
  const srv = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = Buffer.concat(chunks).toString("utf8");
    const auth = req.headers["authorization"] || "";
    const cache = req.headers["x-cache-key"] || null;
    const sess = req.headers["x-session-affinity"] || null;
    const ocSession = req.headers["x-opencode-session"] || null;
    const ua = req.headers["user-agent"] || null;
    const rec = { method: req.method, url: req.url, auth, cache, sess, ocSession, ua, body };
    hits.push(rec);
    const wantStream = body.includes("\"stream\":true") || body.includes("\"stream\": true");
    const spec = responses[req.url] || responses["default"] || { status: 200, body: { id: "mock", object: "chat.completion", choices: [{ message: { role: "assistant", content: `mock:${id}` } }] } };
    if (spec.body && typeof spec.body !== "string") spec.body = JSON.stringify(spec.body);
    if (wantStream) {
      res.writeHead(spec.status, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write("data: " + JSON.stringify({ id: "mock", choices: [{ delta: { content: "mock:" + id } }] }) + "\n\n");
      res.end();
      return;
    }
    res.writeHead(spec.status, { "content-type": "application/json" });
    res.end(spec.body);
  });
  return { id, srv, hits };
}

// Start the real server.mjs as a child process with a temp config pointing at
// the mock backends and a temp .env.
async function startRouter(mocks, extraEnv = {}) {
  const dir = await mkdtemp(join(tmpdir(), "lmr-test-"));
  const baseURLs = {};
  for (const m of mocks) baseURLs[m.id] = `http://${HOST}:${m.port}`;

  const cfg = {
    port: 0,
    prefix: "/v1",
    masterKeyEnv: null,
    backends: mocks.map((m) => ({ id: m.id, baseURL: baseURLs[m.id], apiKeyEnv: `KEY_${m.id.toUpperCase()}`, model: "deepseek-v4-flash" })),
    models: {
      "deepseek-v4-flash": { backends: mocks.map((m) => m.id), affinityPool: Math.max(1, mocks.length - 1) },
      "deepseek-v4-flash-direct": { backends: [mocks[mocks.length - 1].id], affinityPool: 1 },
    },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
  };
  const cfgPath = join(dir, "config.json");
  const envPath = join(dir, ".env");
  const envLines = mocks.map((m) => `KEY_${m.id.toUpperCase()}=test-key-${m.id}`).join("\n");
  await writeFile(cfgPath, JSON.stringify(cfg, null, 2));
  await writeFile(envPath, envLines);

  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: join(import.meta.dirname),
    env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath, PORT: "0", ...extraEnv },
    stdio: ["ignore", "pipe", "pipe"],
  });
  // Wait for the listening banner. A child that never prints it is the opaque
  // failure this file already paid for once: carry the child's own stderr tail
  // and exit state into the thrown error, so a config the loader rejects names
  // its field paths instead of only "server did not start".
  let port = null;
  let childErr = "";
  let childExit = null;
  child.on("exit", (code, signal) => { childExit = code === null ? "signal:" + signal : code; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("server did not start (exit=" + childExit + ", stderr tail: " + JSON.stringify(childErr.slice(-600)) + ")")),
      8000
    );
    let out = "";
    child.stdout.on("data", (d) => { out += d; });
    child.stderr.on("data", (d) => {
      const s = d.toString();
      childErr += s;
      const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
      if (m) { port = parseInt(m[1], 10); clearTimeout(timer); resolve(); }
    });
  });
  return { child, base: `http://${HOST}:${port}`, cfgPath, envPath, dir };
}

// Start the router from an explicit config object + env lines (new-schema
// presets configs, or any custom fixture). Returns child + base URL + dir.
async function startRouterCfg(cfg, envLines, extraEnv = {}) {
  const dir = await mkdtemp(join(tmpdir(), "lmr-test-"));
  const cfgPath = join(dir, "config.json");
  const envPath = join(dir, ".env");
  await writeFile(cfgPath, JSON.stringify(cfg, null, 2));
  await writeFile(envPath, envLines);
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: join(import.meta.dirname),
    env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath, ...extraEnv },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let port = null;
  let childErr = "";
  let childExit = null;
  child.on("exit", (code, signal) => { childExit = code === null ? "signal:" + signal : code; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("server did not start (exit=" + childExit + ", stderr tail: " + JSON.stringify(childErr.slice(-600)) + ")")),
      8000
    );
    child.stderr.on("data", (d) => {
      const s = d.toString();
      childErr += s;
      const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
      if (m) { port = parseInt(m[1], 10); clearTimeout(timer); resolve(); }
    });
  });
  return { child, base: `http://${HOST}:${port}`, cfgPath, envPath, dir };
}

function api(base, path, { method = "POST", body, headers = {} } = {}) {
  return fetch(base + path, {
    method,
    headers: { "content-type": "application/json", authorization: "Bearer local-master", ...headers },
    body: body ? JSON.stringify(body) : undefined,
  });
}

// ---- main test driver ------------------------------------------------------

async function main() {
  let r; // response handle shared by the standalone test blocks below
  const mk = (id, responses) => mockBackend(id, responses);
  const p1 = mk("go-primary", {});
  const p2 = mk("go-alt", {});
  const d = mk("direct", {});
  const ports = await Promise.all([listen(p1.srv), listen(p2.srv), listen(d.srv)]);
  p1.port = ports[0]; p2.port = ports[1]; d.port = ports[2];
  const mocks = [p1, p2, d];
  const { child, base, dir } = await startRouter(mocks);

  try {
    let r = await api(base, "/health", { method: "GET" });
    let body = await r.json();
    assert(r.status === 200 && body.ok === true && Array.isArray(body.backends), "a) /health returns ok + backends");

    r = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [{ role: "user", content: "hi" }] } });
    body = await r.json();
    assert(r.status === 200 && body && body.choices && body.choices[0] && /^mock:/.test(body.choices[0].message.content), "b) non-stream returns completion");

    r = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [{ role: "user", content: "hi" }], stream: true } });
    const text = await r.text();
    assert(r.status === 200 && text.includes("data:"), "c) stream returns SSE data lines");

    const h1 = { "x-session-affinity": "sess-A" };
    const served = [];
    for (let i = 0; i < 4; i++) {
      const rr = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] }, headers: h1 });
      const bb = await rr.json();
      const m = /^mock:([a-z0-9_-]+)/.exec(bb && bb.choices && bb.choices[0] && bb.choices[0].message && bb.choices[0].message.content);
      if (m) served.push(m[1]);
    }
    const distinct = new Set(served).size;
    assert(served.length === 4 && distinct === 1, "d) same session affinity header routes to one backend (served=" + JSON.stringify(served) + ")");

    r = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash-direct", messages: [] } });
    assert(r.status === 200 && d.hits.length >= 1, "e) direct alias routes to direct");

    const downSpec = { status: 503, body: { error: { message: "down" } } };
    const p3 = mockBackend("x1", { default: downSpec });
    const p4 = mockBackend("x2", { default: downSpec });
    const p5 = mockBackend("x3", { default: downSpec });
    const ports2 = await Promise.all([listen(p3.srv), listen(p4.srv), listen(p5.srv)]);
    p3.port = ports2[0]; p4.port = ports2[1]; p5.port = ports2[2];
    const { child: child2, base: base2 } = await startRouter([p3, p4, p5]);
    try {
      r = await api(base2, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
      assert([503, 502].includes(r.status), "f) all backends down -> 503/502");
    } finally {
      child2.kill();
      p3.srv.close(); p4.srv.close(); p5.srv.close();
    }

    await api(base, "/admin/reset-health", { method: "POST" });
    r = await api(base, "/health", { method: "GET" });
    body = await r.json();
    assert(body.backends.every((b) => b.state === "healthy"), "g) /admin/reset-health clears cooling");

    r = await api(base, "/v1/chat/completions", { body: { model: "glm-5.2", messages: [] } });
    assert(r.status === 404, "h) unknown model -> 404");

    await api(base, "/v1/models", { method: "GET" });
    assert(true, "i) /v1/models reachable");

    // j) developer role translated to system on backends that request it.
    //    Force a single-backend config with translateDeveloperRole so the
    //    router must rewrite the role before forwarding.
    const devMk = mockBackend("dev-d", {});
    const devPort = await listen(devMk.srv);
    devMk.port = devPort;
    const dir2 = await mkdtemp(join(tmpdir(), "lmr-dev-"));
    const cfgDev = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "dev-d", baseURL: `http://${HOST}:${devPort}`, apiKeyEnv: "KEY_DEV_D", model: "deepseek-v4-flash", translateDeveloperRole: true }],
      models: { "deepseek-v4-flash": { backends: ["dev-d"], affinityPool: 1 } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const cfgDevPath = join(dir2, "config.json");
    const envDevPath = join(dir2, ".env");
    await writeFile(cfgDevPath, JSON.stringify(cfgDev, null, 2));
    await writeFile(envDevPath, "KEY_DEV_D=test-key-dev-d\n");
    const childDev = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgDevPath, ROUTER_ENV: envDevPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let devBase = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("dev server did not start")), 8000);
      childDev.stderr.on("data", (dd) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(dd.toString());
        if (m) { devBase = `http://${HOST}:${m[1]}`; clearTimeout(timer); resolve(); }
      });
    });
    try {
      await api(devBase, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [{ role: "developer", content: "be brief" }, { role: "user", content: "hi" }] } });
      const lastRec = devMk.hits[devMk.hits.length - 1];
      const sentBody = JSON.parse(lastRec.body);
      assert(sentBody.messages[0].role === "system" && sentBody.messages[0].content === "be brief", "j) developer role translated to system for backend");
    } finally {
      childDev.kill();
      devMk.srv.close();
      await rm(dir2, { recursive: true, force: true });
    }

    // k) client error (400) does NOT cool the backend down.
    const clientMk = mockBackend("client-x", { default: { status: 400, body: { error: { message: "bad payload", type: "invalid_request_error" } } } });
    const clientPort = await listen(clientMk.srv);
    clientMk.port = clientPort;
    const dir3 = await mkdtemp(join(tmpdir(), "lmr-client-"));
    const cfgClient = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "client-x", baseURL: `http://${HOST}:${clientPort}`, apiKeyEnv: "KEY_CLIENT_X", model: "deepseek-v4-flash" }],
      models: { "deepseek-v4-flash": { backends: ["client-x"], affinityPool: 1 } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const cfgClientPath = join(dir3, "config.json");
    const envClientPath = join(dir3, ".env");
    await writeFile(cfgClientPath, JSON.stringify(cfgClient, null, 2));
    await writeFile(envClientPath, "KEY_CLIENT_X=test-key-client-x\n");
    const childClient = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgClientPath, ROUTER_ENV: envClientPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let clientBase = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("client server did not start")), 8000);
      childClient.stderr.on("data", (dd) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(dd.toString());
        if (m) { clientBase = `http://${HOST}:${m[1]}`; clearTimeout(timer); resolve(); }
      });
    });
    try {
      await api(clientBase, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
      const rr = await api(clientBase, "/health", { method: "GET" });
      const hb = await rr.json();
      assert(hb.backends[0].state === "healthy", "k) client 400 does not cool backend (state=" + hb.backends[0].state + ")");
    } finally {
      childClient.kill();
      clientMk.srv.close();
      await rm(dir3, { recursive: true, force: true });
    }

    // l) GET / serves the self-contained web UI.
    r = await api(base, "/", { method: "GET" });
    const htmlText = await r.text();
    assert(
      r.status === 200 && (r.headers.get("content-type") || "").includes("text/html") && htmlText.includes("<!DOCTYPE html"),
      "l) GET / serves web UI (status=" + r.status + ", doctype=" + htmlText.includes("<!DOCTYPE html") + ")"
    );

    // m) /api/history records the earlier successful completions with backend ids.
    r = await api(base, "/api/history", { method: "GET" });
    body = await r.json();
    const hit = (body.entries || []).find(
      (e) => e.model === "deepseek-v4-flash" && e.status === 200 && ["go-primary", "go-alt", "direct"].includes(e.backend)
    );
    assert(Array.isArray(body.entries) && !!hit, "m) /api/history has a 200 deepseek-v4-flash entry with backend (entries=" + (body.entries || []).length + ", hit=" + JSON.stringify(hit) + ")");

    // m2) /api/history/detail returns the stored deep-dive payload for a request.
    const detailReqBody = { model: "deepseek-v4-flash", messages: [{ role: "user", content: "detail-check" }], max_tokens: 7 };
    r = await api(base, "/v1/chat/completions", { body: detailReqBody });
    assert(r.status === 200, "m2a) fresh request for detail lookup succeeds (status=" + r.status + ")");
    const servedBackendM2 = r.headers.get("x-router-backend");
    const callHeaderM2 = r.headers.get("x-router-call");
    assert(!!callHeaderM2 && /^c/.test(callHeaderM2), "m2a2) response carries an x-router-call id (header=" + callHeaderM2 + ")");
    r = await api(base, "/api/history?limit=5", { method: "GET" });
    body = await r.json();
    const latest = (body.entries || [])[0];
    assert(latest && latest.model === "deepseek-v4-flash" && latest.status === 200, "m2b) newest history entry is the fresh request (latest=" + JSON.stringify(latest) + ")");
    assert(latest && latest.callId && latest.callId === callHeaderM2, "m2b2) history entry carries the same callId as the response header (entry=" + (latest && latest.callId) + ", header=" + callHeaderM2 + ")");
    r = await api(base, "/api/history/detail?t=" + latest.t, { method: "GET" });
    const detailResp = await r.json();
    const dr = detailResp && detailResp.detail;
    assert(
      r.status === 200 && dr && dr.payload && dr.payload.model === "deepseek-v4-flash" &&
      dr.payload.messages && dr.payload.messages[0] && dr.payload.messages[0].content === "detail-check" &&
      Array.isArray(dr.attempts) && dr.attempts.length >= 1 && dr.attempts[0].backend === servedBackendM2 &&
      dr.attempts[0].status === 200 && dr.response && dr.response.status === 200 && dr.response.preview != null,
      "m2c) detail endpoint returns stored payload + attempts + response summary (backend=" + servedBackendM2 + ", attempts=" + (dr && dr.attempts && dr.attempts.length) + ")"
    );
    // m2c2) detail is also reachable by call id (the "Call" column lookup).
    r = await api(base, "/api/history/detail?call=" + encodeURIComponent(latest.callId), { method: "GET" });
    const drCall = await r.json();
    assert(
      r.status === 200 && drCall.detail && drCall.detail.t === latest.t &&
      drCall.detail.payload && drCall.detail.payload.messages && drCall.detail.payload.messages[0].content === "detail-check",
      "m2c2) detail endpoint resolves by call id (call=" + latest.callId + ", t=" + (drCall.detail && drCall.detail.t) + ")"
    );
    r = await api(base, "/api/history/detail?t=" + (latest.t + 999999), { method: "GET" });
    const missResp = await r.json();
    assert(r.status === 404 && missResp.error === "not found", "m2d) detail endpoint 404s for unknown t (status=" + r.status + ")");
    r = await api(base, "/api/history/detail", { method: "GET" });
    assert(r.status === 404, "m2e) detail endpoint 404s when t is missing (status=" + r.status + ")");

    // m3) oversized request bodies still get a (bounded) detail record.
    const bigContent = "x".repeat(120000); // > DETAIL_MAX_BODY_BYTES (100 KB)
    r = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [{ role: "user", content: bigContent }], max_tokens: 5 } });
    assert(r.status === 200, "m3a) oversized-body request succeeds (status=" + r.status + ")");
    const bigCall = r.headers.get("x-router-call");
    r = await api(base, "/api/history/detail?call=" + encodeURIComponent(bigCall), { method: "GET" });
    const bigDr = await r.json();
    assert(
      r.status === 200 && bigDr.detail && bigDr.detail.payload && Array.isArray(bigDr.detail.payload.messages) &&
      bigDr.detail.payload.messages[0].content.length < bigContent.length &&
      /truncated/.test(bigDr.detail.payload.messages[0].content),
      "m3b) oversized body stored bounded (contentLen=" + (bigDr.detail && bigDr.detail.payload && bigDr.detail.payload.messages && bigDr.detail.payload.messages[0].content.length) + ")"
    );

    // n) manual cool / uncool via /admin/backend, then reflect in /health.
    const coolStates = [];
    for (const mid of ["go-primary", "go-alt", "direct"]) {
      r = await api(base, "/admin/backend", { method: "POST", body: { id: mid, action: "cool" } });
      const cj = await r.json();
      coolStates.push(cj.state);
    }
    assert(coolStates.every((s) => s === "cooling"), "n1) /admin/backend cool -> cooling for all mocks (" + JSON.stringify(coolStates) + ")");
    r = await api(base, "/health", { method: "GET" });
    body = await r.json();
    assert(body.backends.find((b) => b.id === "go-primary").state === "cooling", "n2) /health shows cooling after manual cool");
    r = await api(base, "/admin/backend", { method: "POST", body: { id: "go-primary", action: "uncool" } });
    body = await r.json();
    assert(r.status === 200 && body.ok === true && body.state === "healthy", "n3) /admin/backend uncool -> healthy (status=" + r.status + ")");
    r = await api(base, "/health", { method: "GET" });
    body = await r.json();
    assert(body.backends.find((b) => b.id === "go-primary").state === "healthy", "n4) /health shows healthy after uncool");
    r = await api(base, "/admin/backend", { method: "POST", body: { id: "no-such-backend", action: "cool" } });
    assert(r.status === 404, "n5) /admin/backend unknown id -> 404 (status=" + r.status + ")");
    r = await api(base, "/admin/backend", { method: "POST", body: { id: "go-primary", action: "explode" } });
    assert(r.status === 400, "n6) /admin/backend invalid action -> 400 (status=" + r.status + ")");

    // o) /api/stats aggregates the requests made in this run.
    r = await api(base, "/api/stats", { method: "GET" });
    body = await r.json();
    assert(typeof body.since === "number" && typeof body.uptimeMs === "number" && typeof body.total === "number" && body.total > 0, "o1) /api/stats shape + total>0 (total=" + body.total + ")");
    const statIds = Object.keys(body.byBackend || {});
    assert(
      statIds.length > 0 && statIds.every((id) => body.byBackend[id].requests > 0 && typeof body.byBackend[id].ok === "number" && typeof body.byBackend[id].errors === "number"),
      "o2) /api/stats byBackend has request counters (" + JSON.stringify(statIds) + ")"
    );
    assert(body.byModel && body.byModel["deepseek-v4-flash"] > 0, "o3) /api/stats byModel counts deepseek-v4-flash (" + JSON.stringify(body.byModel) + ")");

    // p) successful completions carry the X-Router-Backend header (playground debug).
    r = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [{ role: "user", content: "hi" }] } });
    const servedBackend = r.headers.get("x-router-backend");
    assert(!!servedBackend && ["go-primary", "go-alt", "direct"].includes(servedBackend), "p) X-Router-Backend header on success (got=" + servedBackend + ")");

    // q) failed completions (all backends down) carry the header too.
    const q1 = mockBackend("q1", { default: downSpec });
    const q2 = mockBackend("q2", { default: downSpec });
    const q3 = mockBackend("q3", { default: downSpec });
    const portsQ = await Promise.all([listen(q1.srv), listen(q2.srv), listen(q3.srv)]);
    q1.port = portsQ[0]; q2.port = portsQ[1]; q3.port = portsQ[2];
    const { child: childQ, base: baseQ } = await startRouter([q1, q2, q3]);
    try {
      r = await api(baseQ, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
      const failedBackend = r.headers.get("x-router-backend");
      assert([503, 502].includes(r.status) && !!failedBackend, "q) X-Router-Backend header on failure (status=" + r.status + ", backend=" + failedBackend + ")");
    } finally {
      childQ.kill();
      q1.srv.close(); q2.srv.close(); q3.srv.close();
    }

    // r) synthesis path: this router runs an OLD-schema config (models key);
    //    /v1/models must list the synthesized preset ids and affinity routing
    //    must still bind a session to one backend, identical to before.
    r = await api(base, "/v1/models", { method: "GET" });
    body = await r.json();
    const modelIds = (body.data || []).map((m) => m.id);
    assert(
      r.status === 200 && modelIds.includes("deepseek-v4-flash") && modelIds.includes("deepseek-v4-flash-direct"),
      "r1) /v1/models lists synthesized preset ids (" + JSON.stringify(modelIds) + ")"
    );
    const synSess = "synthesis-session";
    const synServed = [];
    for (let i = 0; i < 2; i++) {
      const rr = await api(base, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] }, headers: { "x-session-affinity": synSess } });
      const bb = await rr.json();
      const mm = /^mock:([a-z0-9_-]+)/.exec(bb && bb.choices && bb.choices[0] && bb.choices[0].message && bb.choices[0].message.content);
      if (mm) synServed.push(mm[1]);
    }
    assert(synServed.length === 2 && new Set(synServed).size === 1, "r2) synthesized affinity routes one session to one backend (" + JSON.stringify(synServed) + ")");
  } finally {
    child.kill();
    p1.srv.close(); p2.srv.close(); d.srv.close();
    await rm(dir, { recursive: true, force: true });
  }

  // ---- preset-strategy + sticky-cool tests (new presets schema) -------------

  // s) failover: first member returns 500, second member serves; the response
  //    carries the second backend's x-router-backend header.
  const f1 = mockBackend("f1", { default: { status: 500, body: { error: { message: "boom" } } } });
  const f2 = mockBackend("f2", {});
  const portsF = await Promise.all([listen(f1.srv), listen(f2.srv)]);
  f1.port = portsF[0]; f2.port = portsF[1];
  const cfgF = {
    port: 0, prefix: "/v1", masterKeyEnv: null,
    backends: [
      { id: "f1", baseURL: `http://${HOST}:${f1.port}`, apiKeyEnv: "KEY_F1", model: "deepseek-v4-flash" },
      { id: "f2", baseURL: `http://${HOST}:${f2.port}`, apiKeyEnv: "KEY_F2", model: "deepseek-v4-flash" },
    ],
    presets: {
      "deepseek-v4-flash": {
        strategy: "failover",
        members: ["f1", "f2"],
        meta: { label: "DeepSeek V4 Flash", contextWindow: 128000, pricing: { inputPerM: 0.18, outputPerM: 0.87 } },
      },
    },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 60000, serverMaxMs: 300000, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
  };
  const { child: childF, base: baseF, dir: dirF } = await startRouterCfg(cfgF, "KEY_F1=test-key-f1\nKEY_F2=test-key-f2\n");
  try {
    let r = await api(baseF, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
    const fb = await r.json();

    assert(
      r.status === 200 && /^mock:f2/.test(fb.choices[0].message.content) && r.headers.get("x-router-backend") === "f2",
      "s1) failover: first member 500, second serves (status=" + r.status + ", backend=" + r.headers.get("x-router-backend") + ")"
    );
    const fh = await (await api(baseF, "/health", { method: "GET" })).json();
    assert(
      fh.backends.find((b) => b.id === "f1").state === "healthy" && fh.backends.find((b) => b.id === "f2").state === "healthy",
      "s2) a 500 cools f1's ROUTE, not the f1 account: neither account is blamed (f1=" + fh.backends.find((b) => b.id === "f1").state + ")"
    );
    // ...and that route really is out of the pool: the retry skips it entirely.
    const f1HitsBefore = f1.hits.length;
    const r2f = await api(baseF, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
    await r2f.text();
    assert(
      r2f.headers.get("x-router-backend") === "f2" && f1.hits.length === f1HitsBefore,
      "s2b) the cooling route is not retried (backend=" + r2f.headers.get("x-router-backend") + ", f1Hits=" + f1.hits.length + ")"
    );

    // t) meta passthrough + normalization via /api/config (never key values).
    r = await api(baseF, "/api/config", { method: "GET" });
    const cfgBody = await r.json();
    const meta = cfgBody.presets && cfgBody.presets["deepseek-v4-flash"] && cfgBody.presets["deepseek-v4-flash"].meta;
    const mem = cfgBody.presets["deepseek-v4-flash"].members;
    assert(
      r.status === 200 && meta && meta.label === "DeepSeek V4 Flash" && meta.contextWindow === 128000 && meta.pricing.inputPerM === 0.18 && meta.pricing.outputPerM === 0.87,
      "t1) /api/config exposes preset meta (" + JSON.stringify(meta) + ")"
    );
    const models = cfgBody.presets["deepseek-v4-flash"].models;
    assert(
      mem.length === 2 && mem.every((m) => typeof m.backend === "string" && m.weight === 1) &&
      Array.isArray(models) && models.length === 1 && models[0].model === "deepseek-v4-flash" && models[0].weight === 1,
      "t2) /api/config presets normalized: models [{model, weight}] + derived members {backend, weight} (" + JSON.stringify({ models, mem }) + ")"
    );
    assert(!JSON.stringify(cfgBody).includes("test-key"), "t3) /api/config never leaks api key values");
  } finally {
    childF.kill();
    f1.srv.close(); f2.srv.close();
    await rm(dirF, { recursive: true, force: true });
  }

  // u) sticky manual cool, full loop: cool the only non-cooling backend (no
  //    forMs) while another is weekly-pinned; requests must NOT route through
  //    the manual one; success elsewhere must not clear it; uncool restores.
  const u1 = mockBackend("u1", {});
  const u2 = mockBackend("u2", { default: { status: 429, body: { error: { message: "GoUsageLimitError: Resets in 1 day" } } } });
  const u3 = mockBackend("u3", {});
  const portsU = await Promise.all([listen(u1.srv), listen(u2.srv), listen(u3.srv)]);
  u1.port = portsU[0]; u2.port = portsU[1]; u3.port = portsU[2];
  const cfgU = {
    port: 0, prefix: "/v1", masterKeyEnv: null,
    backends: [
      { id: "u1", baseURL: `http://${HOST}:${u1.port}`, apiKeyEnv: "KEY_U1", model: "deepseek-v4-flash" },
      { id: "u2", baseURL: `http://${HOST}:${u2.port}`, apiKeyEnv: "KEY_U2", model: "deepseek-v4-flash" },
      { id: "u3", baseURL: `http://${HOST}:${u3.port}`, apiKeyEnv: "KEY_U3", model: "deepseek-v4-flash" },
    ],
    presets: {
      "main": { strategy: "affinity", members: ["u2", "u1"], affinityPool: 1 },
      "spare": { strategy: "affinity", members: ["u3"], affinityPool: 1 },
    },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 604800000 },
  };
  const { child: childU, base: baseU, dir: dirU } = await startRouterCfg(cfgU, "KEY_U1=test-key-u1\nKEY_U2=test-key-u2\nKEY_U3=test-key-u3\n");
  try {
    // Weekly-pin u2 (primary member), fall through to u1.
    r = await api(baseU, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    let ub = await r.json();
    assert(r.status === 200 && /^mock:u1/.test(ub.choices[0].message.content), "u1) weekly-pin u2, fall through to u1 (status=" + r.status + ")");
    // Sticky manual cool u1 (no forMs).
    r = await api(baseU, "/admin/backend", { method: "POST", body: { id: "u1", action: "cool" } });
    ub = await r.json();
    assert(ub.ok && ub.state === "cooling", "u2) sticky cool u1 -> cooling");
    let uh = await (await api(baseU, "/health", { method: "GET" })).json();
    const u1H = uh.backends.find((b) => b.id === "u1");
    const u2H = uh.backends.find((b) => b.id === "u2");
    assert(u1H.state === "cooling" && u1H.manual === true, "u3) /health shows u1 cooling + manual");
    assert(u2H.state === "cooling" && u2H.manual === false, "u4) /health shows weekly-pinned u2 is cooling, NOT manual");
    // Request now: u1 manually cooled, u2 weekly pinned. With no eligible provider
    // the request is refused outright - NOT forced at the pinned u2, whose failure
    // is already known.
    const u1HitsBefore = u1.hits.length;
    const u2HitsBefore = u2.hits.length;
    r = await api(baseU, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    await r.text();
    assert(
      r.status === 503 && r.headers.get("x-router-backend") === "none" && !!r.headers.get("retry-after"),
      "u5) an all-cooling pool admits nothing and advertises the recovery (status=" + r.status + ", header=" + r.headers.get("x-router-backend") + ", retryAfter=" + r.headers.get("retry-after") + ")"
    );
    assert(u1.hits.length === u1HitsBefore, "u6) manual-cooled u1 never received the request (hits=" + u1.hits.length + ")");
    assert(u2.hits.length === u2HitsBefore, "u6b) weekly-pinned u2 is not forcibly retried (hits=" + u2.hits.length + ")");
    // A success on another backend does NOT clear the manual cool.
    r = await api(baseU, "/v1/chat/completions", { body: { model: "spare", messages: [] } });
    ub = await r.json();
    assert(r.status === 200 && /^mock:u3/.test(ub.choices[0].message.content), "u7) spare backend still serves");
    uh = await (await api(baseU, "/health", { method: "GET" })).json();
    assert(
      uh.backends.find((b) => b.id === "u1").state === "cooling" && uh.backends.find((b) => b.id === "u1").manual === true,
      "u8) success on u3 did not clear u1 manual cool"
    );
    // Uncool restores routing.
    r = await api(baseU, "/admin/backend", { method: "POST", body: { id: "u1", action: "uncool" } });
    ub = await r.json();
    assert(ub.ok && ub.state === "healthy", "u9) uncool u1 -> healthy");
    r = await api(baseU, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    ub = await r.json();
    assert(r.status === 200 && /^mock:u1/.test(ub.choices[0].message.content), "u10) routing restored through u1 (status=" + r.status + ")");
    // Empty-pool 503: cool everything manually -> documented message.
    await api(baseU, "/admin/backend", { method: "POST", body: { id: "u1", action: "cool" } });
    await api(baseU, "/admin/backend", { method: "POST", body: { id: "u2", action: "cool" } });
    r = await api(baseU, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    const uBody = await r.json();
    assert(
      r.status === 503 && uBody.error && /\(2 manually cooled - uncool to restore\)/.test(uBody.error.message),
      "u11) empty fallback pool 503s with documented message (" + JSON.stringify(uBody.error) + ")"
    );
  } finally {
    childU.kill();
    u1.srv.close(); u2.srv.close(); u3.srv.close();
    await rm(dirU, { recursive: true, force: true });
  }

  // v) timed manual cool: forMs honored, expires on its own, empty-pool 503
  //    with N=1 while active.
  const v1 = mockBackend("v1", {});
  const portsV = await Promise.all([listen(v1.srv)]);
  v1.port = portsV[0];
  const cfgV = {
    port: 0, prefix: "/v1", masterKeyEnv: null,
    backends: [{ id: "v1", baseURL: `http://${HOST}:${v1.port}`, apiKeyEnv: "KEY_V1", model: "deepseek-v4-flash" }],
    presets: { "deepseek-v4-flash": { strategy: "affinity", members: ["v1"] } },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
  };
  const { child: childV, base: baseV, dir: dirV } = await startRouterCfg(cfgV, "KEY_V1=test-key-v1\n");
  try {
    r = await api(baseV, "/admin/backend", { method: "POST", body: { id: "v1", action: "cool", forMs: 100 } });
    let vb = await r.json();
    assert(vb.ok && vb.state === "cooling", "v1) timed manual cool (forMs 100) -> cooling");
    let vh = await (await api(baseV, "/health", { method: "GET" })).json();
    assert(vh.backends[0].state === "cooling" && vh.backends[0].manual === true, "v2) /health manual=true while timed cool active");
    r = await api(baseV, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
    const vBody = await r.json();
    assert(
      r.status === 503 && vBody.error && /\(1 manually cooled - uncool to restore\)/.test(vBody.error.message),
      "v3) single manual-cooled backend 503s with N=1 message (" + JSON.stringify(vBody.error) + ")"
    );
    await new Promise((res) => setTimeout(res, 250));
    vh = await (await api(baseV, "/health", { method: "GET" })).json();
    assert(vh.backends[0].state === "healthy" && vh.backends[0].manual === false, "v4) timed manual cool expires -> healthy, manual=false");
    r = await api(baseV, "/v1/chat/completions", { body: { model: "deepseek-v4-flash", messages: [] } });
    vb = await r.json();
    assert(r.status === 200 && /^mock:v1/.test(vb.choices[0].message.content), "v5) routing restored after timed expiry");
  } finally {
    childV.kill();
    v1.srv.close();
    await rm(dirV, { recursive: true, force: true });
  }

  // w) fallback exclusion: the soonest-cooling backend is manual (timed, short
  //    vs the weekly pin), the later one is system-cooled: the router tries the
  //    system-cooled one, never the manual one.
  const w1 = mockBackend("w1", {});
  const w2 = mockBackend("w2", { default: { status: 429, body: { error: { message: "GoUsageLimitError: Resets in 1 day" } } } });
  const portsW = await Promise.all([listen(w1.srv), listen(w2.srv)]);
  w1.port = portsW[0]; w2.port = portsW[1];
  const cfgW = {
    port: 0, prefix: "/v1", masterKeyEnv: null,
    backends: [
      { id: "w1", baseURL: `http://${HOST}:${w1.port}`, apiKeyEnv: "KEY_W1", model: "deepseek-v4-flash" },
      { id: "w2", baseURL: `http://${HOST}:${w2.port}`, apiKeyEnv: "KEY_W2", model: "deepseek-v4-flash" },
    ],
    presets: { "main": { strategy: "affinity", members: ["w1", "w2"], affinityPool: 1 } },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 604800000 },
  };
  const { child: childW, base: baseW, dir: dirW } = await startRouterCfg(cfgW, "KEY_W1=test-key-w1\nKEY_W2=test-key-w2\n");
  try {
    // Timed manual cool w1: nextAvailableAt = now + 600000 (SOONEST vs the
    // weekly pin's 7 days). Old code would have routed through w1 here.
    r = await api(baseW, "/admin/backend", { method: "POST", body: { id: "w1", action: "cool", forMs: 600000 } });
    await r.json();
    // First request: w1 cooling -> healthy = [w2] -> w2 429 -> weekly pin.
    r = await api(baseW, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    assert(r.status === 503 && r.headers.get("x-router-backend") === "w2", "w1) w2 weekly-pinned via system-cooled-only pool (status=" + r.status + ")");
    // Both cooling now: nothing is eligible, so neither the manual w1 NOR the
    // weekly-pinned w2 is touched, and the 503 carries w2's recovery time.
    const w1HitsBefore = w1.hits.length;
    const w2HitsBefore = w2.hits.length;
    r = await api(baseW, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    await r.text();
    assert(
      r.status === 503 && r.headers.get("x-router-backend") === "none" && !!r.headers.get("retry-after"),
      "w2) an all-cooling pool touches neither provider (status=" + r.status + ", header=" + r.headers.get("x-router-backend") + ", retryAfter=" + r.headers.get("retry-after") + ")"
    );
    assert(w1.hits.length === w1HitsBefore, "w3) manual-cooled w1 never received a request (hits=" + w1.hits.length + ")");
    assert(w2.hits.length === w2HitsBefore, "w3b) weekly-pinned w2 is not forcibly retried (hits=" + w2.hits.length + ")");
    const wh = await (await api(baseW, "/health", { method: "GET" })).json();
    assert(
      wh.backends.find((b) => b.id === "w1").manual === true && wh.backends.find((b) => b.id === "w2").manual === false,
      "w4) /health manual flags (w1 manual, w2 system)"
    );
  } finally {
    childW.kill();
    w1.srv.close(); w2.srv.close();
    await rm(dirW, { recursive: true, force: true });
  }

  // x) dropParams: a backend configured with dropParams must forward a payload
  //    without that key; a backend without it keeps the key.
  const dp1 = mockBackend("dp1", {});
  const dp2 = mockBackend("dp2", {});
  const portsDp = await Promise.all([listen(dp1.srv), listen(dp2.srv)]);
  dp1.port = portsDp[0]; dp2.port = portsDp[1];
  const cfgDp = {
    port: 0, prefix: "/v1", masterKeyEnv: null,
    backends: [
      { id: "dp1", baseURL: `http://${HOST}:${dp1.port}`, apiKeyEnv: "KEY_DP1", model: "deepseek-v4-flash", dropParams: ["reasoning_effort"] },
      { id: "dp2", baseURL: `http://${HOST}:${dp2.port}`, apiKeyEnv: "KEY_DP2", model: "deepseek-v4-flash" },
    ],
    presets: {
      "dp": { strategy: "affinity", members: ["dp1"] },
      "keep": { strategy: "affinity", members: ["dp2"] },
    },
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
  };
  const { child: childDp, base: baseDp, dir: dirDp } = await startRouterCfg(cfgDp, "KEY_DP1=test-key-dp1\nKEY_DP2=test-key-dp2\n");
  try {
    const payload = { model: "dp", messages: [], reasoning_effort: "high", max_tokens: 10 };
    r = await api(baseDp, "/v1/chat/completions", { body: payload });
    assert(r.status === 200, "x1) dropParams request succeeds (status=" + r.status + ")");
    let sentBody = JSON.parse(dp1.hits[dp1.hits.length - 1].body);
    assert(
      !("reasoning_effort" in sentBody) && sentBody.max_tokens === 10 && sentBody.model === "deepseek-v4-flash",
      "x2) reasoning_effort stripped for dp1, model translated (" + JSON.stringify(sentBody) + ")"
    );
    await api(baseDp, "/v1/chat/completions", { body: { ...payload, model: "keep" } });
    sentBody = JSON.parse(dp2.hits[dp2.hits.length - 1].body);
    assert("reasoning_effort" in sentBody, "x3) reasoning_effort kept for backend without dropParams");
  } finally {
    childDp.kill();
    dp1.srv.close(); dp2.srv.close();
    await rm(dirDp, { recursive: true, force: true });
  }

  // y) unit-level: normalization + candidates against the three-layer shape.
  //    Injected rng selects weight bands over MODELS; ordered attempt lists are
  //    [{model, backend, upstream}] flattened from each model's providers.
  {
    // New-shape fixture: models carry providers; presets reference models.
    const wcfg = {
      backends: [{ id: "a", model: "ua" }, { id: "b", model: "ub" }, { id: "c", model: "uc" }, { id: "d", model: "ud" }, { id: "no-model" }],
      models: {
        "a": { providers: [{ backend: "a", upstream: "ua" }] },
        "b": { providers: [{ backend: "b", upstream: "ub" }] },
        "c": { providers: [{ backend: "c", upstream: "uc" }] },
        "d": { providers: [{ backend: "d", upstream: "ud" }] },
        "m2": { providers: [{ backend: "no-model" }] },
      },
      presets: {
        "w": { strategy: "weighted", models: [{ model: "a", weight: 1 }, { model: "b", weight: 3 }] },
        "multi": { strategy: "weighted", models: ["a", { model: "b", weight: 3 }, { model: "c", weight: 2 }, { model: "d", weight: 2 }] },
        "empty": { strategy: "affinity", models: ["ghost"] },
        "fo": { strategy: "failover", models: ["b", "c", "a"] },
        "af": { strategy: "affinity", models: ["a", "b", "c"], affinityPool: 3 },
      },
    };
    normalizeConfig(wcfg);
    // y1) presets-era path B: members referencing an unknown backend are
    //     dropped during synthesis; a valid member keeps its backend's model.
    const cfg1 = {
      backends: [{ id: "a", model: "ma" }, { id: "b", model: "mb" }],
      presets: { "bad": { strategy: "weighted", members: [{ backend: "missing", weight: 1 }, "a"] } },
    };
    normalizeConfig(cfg1);
    assert(
      cfg1.models.bad.providers.length === 1 && cfg1.models.bad.providers[0].backend === "a" && cfg1.models.bad.providers[0].upstream === "ma",
      "y1) unknown-backend member dropped by normalization"
    );
    // y2) provider without resolvable upstream (backend has no model) dropped.
    assert(wcfg.models.m2.providers.length === 0, "y2) provider without upstream dropped by normalization");
    // y3) weighted rng bands over models: rng 0.1 -> a, rng 0.9 -> b.
    const c1 = candidates(wcfg, "w", "sess", () => 0.1);
    const c2 = candidates(wcfg, "w", "sess", () => 0.9);
    assert(
      c1.ordered[0].model === "a" && c1.ordered[0].backend === "a" && c2.ordered[0].model === "b" && c2.ordered[0].backend === "b",
      "y3) weighted rng bands pick a vs b (" + JSON.stringify(c1.ordered.map((o) => o.model)) + "/" + JSON.stringify(c2.ordered.map((o) => o.model)) + ")"
    );
    const c3 = candidates(wcfg, "multi", "sess", () => 0.4);
    // total 8, r = 3.2 -> a(1): no, b(3): 3.2 < 4 -> pick b; rest weight desc,
    // declared-order tiebreak (c before d).
    assert(
      JSON.stringify(c3.ordered.map((o) => o.model)) === JSON.stringify(["b", "c", "d", "a"]),
      "y4) pick b, rest weight desc w/ declared tiebreak (" + JSON.stringify(c3.ordered.map((o) => o.model)) + ")"
    );
    assert(candidates(wcfg, "nope", "sess") === null, "y5) unknown id -> null");
    const cEmpty = candidates(wcfg, "empty", "sess");
    assert(cEmpty.kind === "preset" && cEmpty.ordered.length === 0 && cEmpty.primary === null, "y6) empty preset -> empty ordered (" + JSON.stringify(cEmpty) + ")");
    assert(
      JSON.stringify(candidates(wcfg, "fo", "x").ordered.map((o) => o.backend)) === JSON.stringify(["b", "c", "a"]),
      "y7) failover preserves declared order"
    );
    const af = candidates(wcfg, "af", "same-session");
    const af2 = candidates(wcfg, "af", "same-session");
    assert(
      af.primary && af.ordered.length === 3 && af.primary === af.ordered[0] && af.primary.backend === af2.primary.backend,
      "y8) affinity hashing: ordered starts with hashed primary, same session sticks (" + JSON.stringify(af.ordered.map((o) => o.model)) + ")"
    );
    // y9) implicit bare-model addressing: a model id that is NOT a preset id.
    const imp = candidates(wcfg, "b", "sess");
    assert(
      imp.kind === "model" && imp.preset === null && imp.models.length === 1 && imp.models[0] === "b" &&
      imp.ordered.length === 1 && imp.ordered[0].model === "b" && imp.ordered[0].backend === "b" && imp.ordered[0].upstream === "ub",
      "y9) implicit bare-model addressing returns single-model ordered list (" + JSON.stringify(imp) + ")"
    );
  }

  // v31) typed model validation: the save-side validator rejects a bad type on
  //      every model field the editor can write (old shape, no schema change).
  {
    const base = {
      backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1" }],
      models: {
        "m1": {
          providers: [{ backend: "b1", upstream: "u1" }],
          retry: "",
          affinityPool: 1.5,
          params: "nope",
          meta: { pricing: { inputPerM: "cheap" } }
        }
      },
      presets: { "p1": { strategy: "affinity", models: ["m1"] } }
    };
    process.env.KEY_B1 = "x";
    const v = validateConfig(base, base);
    const paths = v.errors.map((e) => e.path);
    assert(paths.includes("models.m1.retry"), "v31a) retry \"\" is an error (" + JSON.stringify(paths) + ")");
    assert(paths.includes("models.m1.affinityPool"), "v31b) non-integer affinityPool is an error");
    assert(paths.includes("models.m1.params"), "v31c) non-object params is an error");
    assert(paths.includes("models.m1.meta.pricing.inputPerM"), "v31d) non-numeric pricing is an error");
    delete process.env.KEY_B1;
  }

  // v32) an id that is both a model and a preset stays WARNING-class: the save
  //      is not blocked, because old-shape configs carry such collisions today.
  {
    process.env.KEY_B1 = "x";
    const cfg = {
      backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1" }],
      models: { "same": { providers: [{ backend: "b1", upstream: "u1" }] } },
      presets: { "same": { strategy: "affinity", models: ["same"] } }
    };
    const v = validateConfig(cfg, cfg);
    assert(v.errors.length === 0, "v32a) old-shape collision is NOT an error (" + JSON.stringify(v.errors) + ")");
    assert(v.warnings.some((w) => w.path === "presets.same"), "v32b) old-shape collision warns at presets.same (" + JSON.stringify(v.warnings) + ")");
    delete process.env.KEY_B1;
  }

  // v33) save/load validation split: an unknown preset ref is an ERROR at save
  //      (existing behavior, unchanged) and a warn-and-drop at load. The
  //      unknown-strategy and unknown-top-level-key branches are characterized
  //      here as-is: both stay warning-class in BOTH modes.
  {
    process.env.KEY_B1 = "x";
    const cfg = {
      backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1" }],
      models: { "m1": { providers: [{ backend: "b1", upstream: "u1" }] } },
      presets: {
        "p1": { strategy: "affinity", models: ["ghost"] },
        "p2": { strategy: "sideways", models: ["m1"] }
      },
      extraFuture: { keep: true }
    };
    const v = validateConfig(cfg, cfg);
    assert(v.errors.some((e) => e.path === "presets.p1.models[0]"), "v33a) unknown preset ref is an error (" + JSON.stringify(v.errors) + ")");
    assert(v.warnings.some((w) => w.path === "presets.p2.strategy"), "v33b) unknown strategy warns and falls back (" + JSON.stringify(v.warnings) + ")");
    assert(v.warnings.some((w) => w.path === "extraFuture"), "v33c) unknown top-level key warns");
    // Save/load split: load tolerates the dangling ref (warn+drop), save refuses it.
    const vl = validateConfig(cfg, cfg, { mode: "load" });
    assert(vl.errors.every((e) => e.path !== "presets.p1.models[0]"), "v33d) load mode downgrades the dangling ref to warn (" + JSON.stringify(vl.errors) + ")");
    assert(vl.warnings.some((w) => w.path === "presets.p1.models[0]"), "v33e) load warn carries the ref path");
    delete process.env.KEY_B1;
  }

  // ---- v34) config load wiring: validate before normalize, non-destructive --
  // Two regimes. BOOT (no previous config): only findings that make the config
  // UNINTERPRETABLE abort; the classes the loader repairs (an unset apiKeyEnv,
  // retry: "") log a warning and the router starts. RELOAD (a config is
  // already running): ANY error-class finding rejects the write and the running
  // config is kept. The validator's error set is the stricter SAVE-side
  // contract, so it must never be able to take a running router down.
  {
    const dir = await mkdtemp(join(tmpdir(), "lmr-v34-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const good = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "v34", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V34" }],
      models: { "v34m": { providers: [{ backend: "v34", upstream: "u" }], meta: { label: "good" } } },
      presets: {}
    };
    await writeFile(cfgPath, JSON.stringify(good, null, 2));
    await writeFile(envPath, "KEY_V34=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let base = null;
    let v34Err = "";
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no start")), 8000);
      child.stderr.on("data", (d) => {
        v34Err += d.toString();
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
      });
    });
    try {
      await new Promise((r) => setTimeout(r, 300));
      // v34a) THE SAFETY GUARANTEE: a config the router judges BROKEN is never
      // adopted and the previous config keeps running. RE-PINNED by Task 11 from
      // a legacy `retry: ""` payload (old label: "bad reload keeps the previous
      // config running", with meta.label as the probe) to a V3-shaped payload
      // the converter does NOT repair: the load pipeline now converts an
      // old-shape file first, and conversion repairs `retry: ""` (and deletes
      // it) before the validator sees it, so that payload is legitimately
      // ADOPTED - pinned separately below as v34a3. The guarantee itself is
      // unchanged and must stay proven, so the probe is a v3 payload with an
      // unknown deployment backend, which no repair touches.
      // meta.label SURVIVES normalizeConfig verbatim (normalize never touches
      // meta), so flipping it proves whether the broken reload was rejected or
      // loaded.
      const badV3 = JSON.parse(JSON.stringify(good));
      badV3.models.v34m = { deployments: [{ backend: "v34-does-not-exist", upstream: "u" }], meta: { label: "broken-write" } };
      await writeFile(cfgPath, JSON.stringify(badV3, null, 2));
      await new Promise((r) => setTimeout(r, 900));
      const live = await (await fetch(base + "/api/config")).json();
      assert(
        !!live.models.v34m && live.models.v34m.meta && live.models.v34m.meta.label === "good",
        "v34a) a reload the router judges broken is not adopted and the previous config keeps running (label=" + (live.models.v34m && live.models.v34m.meta && live.models.v34m.meta.label) + ")"
      );
      assert(
        /config reload rejected/.test(v34Err) && /unknown deployment backend/.test(v34Err),
        "v34a2) the rejection is logged with the validator's own reason (" + JSON.stringify(v34Err.split("\n").filter((l) => /rejected/.test(l)).join(" | ").slice(0, 300)) + ")"
      );

      // v34a3) THE NEW OBSERVABLE Task 11 introduces, pinned rather than merely
      // noted: a LEGACY payload whose only defect is a repaired value (`retry:
      // ""`) now reloads ADOPTED, because the bridge repairs it before the
      // validator runs. The reload-rejection boundary therefore does NOT move
      // for this class, and the repair is reported.
      const legacyReload = JSON.parse(JSON.stringify(good));
      legacyReload.models.v34m = { providers: [{ backend: "v34", upstream: "u" }], retry: "", meta: { label: "legacy-repaired" } };
      await writeFile(cfgPath, JSON.stringify(legacyReload, null, 2));
      await new Promise((r) => setTimeout(r, 900));
      const live2 = await (await fetch(base + "/api/config")).json();
      assert(
        !!live2.models.v34m && live2.models.v34m.meta && live2.models.v34m.meta.label === "legacy-repaired",
        "v34a3) a legacy reload whose only defect is a repaired value is repaired and ADOPTED (label=" + (live2.models.v34m && live2.models.v34m.meta && live2.models.v34m.meta.label) + ")"
      );
      assert(
        /models\.v34m\.retry was an empty string/.test(v34Err) && /bridge:/.test(v34Err),
        "v34a4) and the repair is reported by the bridge's own warning (" + JSON.stringify(v34Err.split("\n").filter((l) => /bridge:/.test(l)).join(" | ").slice(0, 300)) + ")"
      );
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v34b) BOOT tolerance: a backend whose apiKeyEnv names a variable that is
    //       not set in the child's process is a save-side error the loader
    //       lets fail closed per backend, so it must not stop startup.
    const dir = await mkdtemp(join(tmpdir(), "lmr-v34b-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const cfgB = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "v34b", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V34B_MISSING" }],
      models: { "v34bm": { providers: [{ backend: "v34b", upstream: "u" }] } },
      presets: {}
    };
    await writeFile(cfgPath, JSON.stringify(cfgB, null, 2));
    await writeFile(envPath, "V34B_UNUSED=x\n");
    // The child must NOT see the variable: inheriting it would make the test
    // vacuous wherever the shell happens to define it.
    const childEnv = { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath };
    delete childEnv.KEY_V34B_MISSING;
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: childEnv,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let base = null;
    let err = "";
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("v34b server did not start (stderr tail: " + JSON.stringify(err.slice(-400)) + ")")), 8000);
      child.stderr.on("data", (d) => {
        const s = d.toString();
        err += s;
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
        if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
      });
    });
    try {
      const live = await (await fetch(base + "/api/config")).json();
      assert(!!(live.backends || []).find((b) => b.id === "v34b"), "v34b) an unset apiKeyEnv does not stop startup");
      assert(/backends\[0\]\.apiKeyEnv/.test(err) && /config warning/.test(err), "v34b2) the boot tolerance logs the apiKeyEnv field path as a warning");
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v34c) BOOT tolerance on the REAL production shape: the live config
    //       carries models.glm-5.2.retry === "" (a save-side error the loader
    //       repairs by warning and deleting). Aborting a boot on it would have
    //       taken the router down, so this exact shape must start and serve.
    const dir = await mkdtemp(join(tmpdir(), "lmr-v34c-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const cfgC = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "v34c", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V34C" }],
      models: { "v34cm": { providers: [{ backend: "v34c", upstream: "u" }], retry: "" } },
      presets: {}
    };
    await writeFile(cfgPath, JSON.stringify(cfgC, null, 2));
    await writeFile(envPath, "KEY_V34C=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let base = null;
    let err = "";
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("v34c server did not start (stderr tail: " + JSON.stringify(err.slice(-400)) + ")")), 8000);
      child.stderr.on("data", (d) => {
        const s = d.toString();
        err += s;
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
        if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
      });
    });
    try {
      const live = await (await fetch(base + "/api/config")).json();
      assert(!!(live.models && live.models.v34cm), "v34c) a model carrying retry:'' does not stop startup");
      assert(/models\.v34cm\.retry/.test(err) && /config warning/.test(err), "v34c2) the retry:'' boot tolerance logs the field path as a warning");
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v34d) BOOT abort, so the tolerance above cannot silently widen into
    //       "nothing ever aborts". The fatal class is what the LOADER cannot
    //       interpret: a non-object root, or a missing `backends` (ensureBackends
    //       and the startup banner both walk cfg.backends unguarded). A missing
    //       `models` or `presets` is NOT fatal, because normalizeConfig
    //       synthesizes both, so a pre-presets-era config file keeps booting.
    //
    //       A missing `backends` also dies on the pre-change code, as an
    //       incidental TypeError, so "exits without a banner" alone would pass
    //       in BOTH directions and prove nothing. The assertion therefore
    //       discriminates on the FAILURE MODE: the validator's field-path
    //       message, and no TypeError. Waits for real process exit (a fixed
    //       sleep could pass on timing alone); a timeout is a FAILURE, not a pass.
    const dir = await mkdtemp(join(tmpdir(), "lmr-v34d-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const cfgD = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      models: { "v34dm": { providers: [{ backend: "v34d", upstream: "u" }] } },
      presets: {}
      // `backends` deliberately absent.
    };
    await writeFile(cfgPath, JSON.stringify(cfgD, null, 2));
    await writeFile(envPath, "KEY_V34D=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let banner = false;
    let err = "";
    child.stderr.on("data", (d) => {
      const s = d.toString();
      err += s;
      if (/listening on http:\/\/127\.0\.0\.1:\d+/.test(s)) banner = true;
    });
    const exited = await new Promise((resolve) => {
      const t = setTimeout(() => resolve("timeout"), 15000);
      child.on("exit", (code) => { clearTimeout(t); resolve(code); });
    });
    try {
      assert(
        exited !== "timeout" && exited !== 0 && banner === false &&
        /config invalid:/.test(err) && /'backends' is required at the top level/.test(err) &&
        !/TypeError/.test(err),
        "v34d) a config missing `backends` aborts startup with the validator's field-path message, not an incidental TypeError (exit=" + exited + ", banner=" + banner + ", stderr=" + JSON.stringify(err.slice(-240)) + ")"
      );
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v34e) BOOT tolerance for the two required keys the loader SYNTHESIZES.
    //       `backends` is the only one normalizeConfig does not create, which is
    //       why v34d can abort on it; a missing `presets` or `models` must still
    //       start and serve. The legacy fixtures pin this incidentally (and fail
    //       opaquely, at the first child, if it ever regresses); this pins the
    //       modern shape directly and names the warning's field path.
    for (const missing of ["presets", "models"]) {
      const dir = await mkdtemp(join(tmpdir(), "lmr-v34e-" + missing + "-"));
      const cfgPath = join(dir, "config.json");
      const envPath = join(dir, ".env");
      const cfgE = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "v34e", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V34E" }],
        models: { "v34em": { providers: [{ backend: "v34e", upstream: "u" }] } },
        presets: { "v34ep": { strategy: "affinity", models: ["v34em"] } }
      };
      delete cfgE[missing];
      await writeFile(cfgPath, JSON.stringify(cfgE, null, 2));
      await writeFile(envPath, "KEY_V34E=k\n");
      const child = spawn(process.execPath, ["server.mjs"], {
        cwd: join(import.meta.dirname),
        env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
        stdio: ["ignore", "pipe", "pipe"]
      });
      let base = null;
      let err = "";
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("v34e (" + missing + ") did not start (stderr tail: " + JSON.stringify(err.slice(-400)) + ")")), 8000);
        child.stderr.on("data", (d) => {
          const s = d.toString();
          err += s;
          const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
          if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
        });
      });
      try {
        const live = await (await fetch(base + "/api/config")).json();
        assert(
          missing === "presets" ? !!(live.models && live.models.v34em) : !!(live.backends || []).find((b) => b.id === "v34e"),
          "v34e) a config with no `" + missing + "` still boots and serves"
        );
        assert(
          new RegExp("not fatal at boot\\): " + missing + ": ").test(err),
          "v34e2) the missing `" + missing + "` tolerance logs its boot warning at the " + missing + " path"
        );
      } finally {
        child.kill();
        await rm(dir, { recursive: true, force: true });
      }
    }
  }

  {
    // v34f) BOOT abort for a NON-OBJECT ROOT: the other half of the fatal class
    //       v34d pins. The `e.path === ""` branch is the only thing that stops
    //       this shape; without it the run falls through to normalizeConfig and
    //       ensureBackends and dies as an incidental TypeError (exit 1 with no
    //       banner in BOTH directions), so the assertion discriminates on the
    //       FAILURE MODE, exactly as v34d does for a missing `backends`.
    const dir = await mkdtemp(join(tmpdir(), "lmr-v34f-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    await writeFile(cfgPath, JSON.stringify([{ id: "v34f" }], null, 2) + "\n");
    await writeFile(envPath, "KEY_V34F=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let banner = false;
    let err = "";
    child.stderr.on("data", (d) => {
      const s = d.toString();
      err += s;
      if (/listening on http:\/\/127\.0\.0\.1:\d+/.test(s)) banner = true;
    });
    const exited = await new Promise((resolve) => {
      const t = setTimeout(() => resolve("timeout"), 15000);
      child.on("exit", (code) => { clearTimeout(t); resolve(code); });
    });
    try {
      assert(
        exited !== "timeout" && exited !== 0 && banner === false &&
        /config invalid:/.test(err) && /\(root\)/.test(err) &&
        !/TypeError/.test(err),
        "v34f) a non-object config root aborts startup with the validator's (root) message, not an incidental TypeError (exit=" + exited + ", banner=" + banner + ", stderr=" + JSON.stringify(err.slice(-240)) + ")"
      );
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  // ---- z-series: three-layer routing (new-shape configs, mock backends) ----

  const BO = { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 };

  // z1) synB shape: presets-era config with MIXED member upstreams -> one model
  //     named <presetId>, providers in member order carrying per-provider
  //     upstreams; preset models [{model, weight: 1}]; strategy preserved;
  //     derived members present.
  {
    const z1a = mockBackend("z1a", {});
    const z1b = mockBackend("z1b", {});
    const portsZ1 = await Promise.all([listen(z1a.srv), listen(z1b.srv)]);
    z1a.port = portsZ1[0]; z1b.port = portsZ1[1];
    const cfgZ1 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z1a", baseURL: `http://${HOST}:${z1a.port}`, apiKeyEnv: "KEY_Z1A", model: "deepseek-v4-flash" },
        { id: "z1b", baseURL: `http://${HOST}:${z1b.port}`, apiKeyEnv: "KEY_Z1B", model: "deepseek/deepseek-v4-flash" },
      ],
      presets: { "p1": { strategy: "affinity", members: ["z1a", "z1b"], affinityPool: 2 } },
      backoff: BO,
    };
    const { child: childZ1, base: baseZ1, dir: dirZ1 } = await startRouterCfg(cfgZ1, "KEY_Z1A=test-key-z1a\nKEY_Z1B=test-key-z1b\n");
    try {
      const rr = await api(baseZ1, "/api/config", { method: "GET" });
      const cb = await rr.json();
      const prov = cb.models.p1.providers;
      const pm = cb.presets.p1.models;
      const pmem = cb.presets.p1.members;
      assert(
        prov.length === 2 && prov[0].backend === "z1a" && prov[0].upstream === "deepseek-v4-flash" &&
        prov[1].backend === "z1b" && prov[1].upstream === "deepseek/deepseek-v4-flash" &&
        pm.length === 1 && pm[0].model === "p1" && pm[0].weight === 1 &&
        cb.presets.p1.strategy === "affinity" &&
        pmem.length === 2 && pmem[0].backend === "z1a" && pmem[0].weight === 1 && pmem[1].backend === "z1b",
        "z1) synB: one model per preset, per-provider upstreams, strategy + derived members (" + JSON.stringify({ prov, pm, pmem }) + ")"
      );
    } finally {
      childZ1.kill();
      z1a.srv.close(); z1b.srv.close();
      await rm(dirZ1, { recursive: true, force: true });
    }
  }

  // z2) synB affinity preserved: 3 requests with one session -> one backend.
  {
    const z2a = mockBackend("z2a", {});
    const z2b = mockBackend("z2b", {});
    const portsZ2 = await Promise.all([listen(z2a.srv), listen(z2b.srv)]);
    z2a.port = portsZ2[0]; z2b.port = portsZ2[1];
    const cfgZ2 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z2a", baseURL: `http://${HOST}:${z2a.port}`, apiKeyEnv: "KEY_Z2A", model: "deepseek-v4-flash" },
        { id: "z2b", baseURL: `http://${HOST}:${z2b.port}`, apiKeyEnv: "KEY_Z2B", model: "deepseek-v4-flash" },
      ],
      presets: { "p2": { strategy: "affinity", members: ["z2a", "z2b"], affinityPool: 2 } },
      backoff: BO,
    };
    const { child: childZ2, base: baseZ2, dir: dirZ2 } = await startRouterCfg(cfgZ2, "KEY_Z2A=test-key-z2a\nKEY_Z2B=test-key-z2b\n");
    try {
      const served = [];
      for (let i = 0; i < 3; i++) {
        const rr = await api(baseZ2, "/v1/chat/completions", { body: { model: "p2", messages: [] }, headers: { "x-session-affinity": "z2-sess" } });
        served.push(rr.headers.get("x-router-backend"));
      }
      assert(
        served.length === 3 && served.every((b) => b === served[0]) && ["z2a", "z2b"].includes(served[0]),
        "z2) synB affinity: 3 requests one session -> one backend (" + JSON.stringify(served) + ")"
      );
    } finally {
      childZ2.kill();
      z2a.srv.close(); z2b.srv.close();
      await rm(dirZ2, { recursive: true, force: true });
    }
  }

  // z3) synB failover preserved: first member 500 -> second serves; forwarded
  //     payload model = the second backend's upstream string.
  {
    const z3a = mockBackend("z3a", { default: { status: 500, body: { error: { message: "boom" } } } });
    const z3b = mockBackend("z3b", {});
    const portsZ3 = await Promise.all([listen(z3a.srv), listen(z3b.srv)]);
    z3a.port = portsZ3[0]; z3b.port = portsZ3[1];
    const cfgZ3 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z3a", baseURL: `http://${HOST}:${z3a.port}`, apiKeyEnv: "KEY_Z3A", model: "deepseek-v4-flash" },
        { id: "z3b", baseURL: `http://${HOST}:${z3b.port}`, apiKeyEnv: "KEY_Z3B", model: "deepseek/deepseek-v4-flash" },
      ],
      presets: { "p3": { strategy: "failover", members: ["z3a", "z3b"] } },
      backoff: BO,
    };
    const { child: childZ3, base: baseZ3, dir: dirZ3 } = await startRouterCfg(cfgZ3, "KEY_Z3A=test-key-z3a\nKEY_Z3B=test-key-z3b\n");
    try {
      const rr = await api(baseZ3, "/v1/chat/completions", { body: { model: "p3", messages: [] } });
      const sent = JSON.parse(z3b.hits[z3b.hits.length - 1].body);
      assert(
        rr.status === 200 && rr.headers.get("x-router-backend") === "z3b" && sent.model === "deepseek/deepseek-v4-flash",
        "z3) synB failover: second member serves with its own upstream (" + rr.headers.get("x-router-backend") + " -> " + sent.model + ")"
      );
    } finally {
      childZ3.kill();
      z3a.srv.close(); z3b.srv.close();
      await rm(dirZ3, { recursive: true, force: true });
    }
  }

  // z4) synB weighted collapse: legacy weighted -> normalized strategy affinity,
  //     providers in declared order.
  {
    const z4a = mockBackend("z4a", {});
    const z4b = mockBackend("z4b", {});
    const portsZ4 = await Promise.all([listen(z4a.srv), listen(z4b.srv)]);
    z4a.port = portsZ4[0]; z4b.port = portsZ4[1];
    const cfgZ4 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z4a", baseURL: `http://${HOST}:${z4a.port}`, apiKeyEnv: "KEY_Z4A", model: "deepseek-v4-flash" },
        { id: "z4b", baseURL: `http://${HOST}:${z4b.port}`, apiKeyEnv: "KEY_Z4B", model: "deepseek-v4-flash" },
      ],
      presets: { "p4": { strategy: "weighted", members: [{ backend: "z4a", weight: 1 }, { backend: "z4b", weight: 3 }] } },
      backoff: BO,
    };
    const { child: childZ4, base: baseZ4, dir: dirZ4 } = await startRouterCfg(cfgZ4, "KEY_Z4A=test-key-z4a\nKEY_Z4B=test-key-z4b\n");
    try {
      const rr = await api(baseZ4, "/api/config", { method: "GET" });
      const cb = await rr.json();
      assert(
        cb.presets.p4.strategy === "affinity" && cb.models.p4.providers.map((p) => p.backend).join(",") === "z4a,z4b",
        "z4) weighted collapse -> affinity, providers in declared order (" + JSON.stringify({ s: cb.presets.p4.strategy, b: cb.models.p4.providers.map((p) => p.backend) }) + ")"
      );
    } finally {
      childZ4.kill();
      z4a.srv.close(); z4b.srv.close();
      await rm(dirZ4, { recursive: true, force: true });
    }
  }

  // z5) implicit model: bare model id (not a preset id) routes by itself.
  {
    const z5 = mockBackend("z5", {});
    const portZ5 = await listen(z5.srv);
    z5.port = portZ5;
    const cfgZ5 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z5", baseURL: `http://${HOST}:${z5.port}`, apiKeyEnv: "KEY_Z5" }],
      models: { "imp": { providers: [{ backend: "z5", upstream: "deepseek-v4-flash" }] } },
      presets: { "pres": { strategy: "affinity", models: ["imp"] } },
      backoff: BO,
    };
    const { child: childZ5, base: baseZ5, dir: dirZ5 } = await startRouterCfg(cfgZ5, "KEY_Z5=test-key-z5\n");
    try {
      const rr = await api(baseZ5, "/v1/chat/completions", { body: { model: "imp", messages: [] } });
      const sent = JSON.parse(z5.hits[z5.hits.length - 1].body);
      assert(
        rr.status === 200 && rr.headers.get("x-router-backend") === "z5" && sent.model === "deepseek-v4-flash",
        "z5) implicit bare model routes, header + upstream model (" + rr.headers.get("x-router-backend") + " -> " + sent.model + ")"
      );
    } finally {
      childZ5.kill();
      z5.srv.close();
      await rm(dirZ5, { recursive: true, force: true });
    }
  }

  // z6) /v1/models dedup + order: presets [p1] + models [p1, m2] -> ["p1", "m2"].
  {
    const z6 = mockBackend("z6", {});
    const portZ6 = await listen(z6.srv);
    z6.port = portZ6;
    const cfgZ6 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z6", baseURL: `http://${HOST}:${z6.port}`, apiKeyEnv: "KEY_Z6" }],
      models: {
        "p1": { providers: [{ backend: "z6", upstream: "u1" }] },
        "m2": { providers: [{ backend: "z6", upstream: "u2" }] },
      },
      presets: { "p1": { strategy: "affinity", models: ["p1"] } },
      backoff: BO,
    };
    const { child: childZ6, base: baseZ6, dir: dirZ6 } = await startRouterCfg(cfgZ6, "KEY_Z6=test-key-z6\n");
    try {
      const rr = await api(baseZ6, "/v1/models", { method: "GET" });
      const ids = (await rr.json()).data.map((m) => m.id);
      assert(JSON.stringify(ids) === JSON.stringify(["p1", "m2"]), "z6) /v1/models collision dedup + preset-first order (" + JSON.stringify(ids) + ")");
    } finally {
      childZ6.kill();
      z6.srv.close();
      await rm(dirZ6, { recursive: true, force: true });
    }
  }

  // z7) preset-over-models failover: m1's sole provider 500 -> m2 serves; payload
  //     model = m2's upstream; header = m2's backend.
  {
    const z7a = mockBackend("z7a", { default: { status: 500, body: { error: { message: "boom" } } } });
    const z7b = mockBackend("z7b", {});
    const portsZ7 = await Promise.all([listen(z7a.srv), listen(z7b.srv)]);
    z7a.port = portsZ7[0]; z7b.port = portsZ7[1];
    const cfgZ7 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z7a", baseURL: `http://${HOST}:${z7a.port}`, apiKeyEnv: "KEY_Z7A" },
        { id: "z7b", baseURL: `http://${HOST}:${z7b.port}`, apiKeyEnv: "KEY_Z7B" },
      ],
      models: {
        "m1": { providers: [{ backend: "z7a", upstream: "u1" }] },
        "m2": { providers: [{ backend: "z7b", upstream: "u2" }] },
      },
      presets: { "pf": { strategy: "failover", models: ["m1", "m2"] } },
      backoff: BO,
    };
    const { child: childZ7, base: baseZ7, dir: dirZ7 } = await startRouterCfg(cfgZ7, "KEY_Z7A=test-key-z7a\nKEY_Z7B=test-key-z7b\n");
    try {
      const rr = await api(baseZ7, "/v1/chat/completions", { body: { model: "pf", messages: [] } });
      const sent = JSON.parse(z7b.hits[z7b.hits.length - 1].body);
      assert(
        rr.status === 200 && rr.headers.get("x-router-backend") === "z7b" && sent.model === "u2",
        "z7) preset failover over models: m2 serves with its upstream (" + rr.headers.get("x-router-backend") + " -> " + sent.model + ")"
      );
    } finally {
      childZ7.kill();
      z7a.srv.close(); z7b.srv.close();
      await rm(dirZ7, { recursive: true, force: true });
    }
  }

  // z8) model-level affinity: 3 providers, pool 3; same session sticks to one
  //     backend; a second session (hash-dictated) reaches a different one.
  {
    const z8a = mockBackend("z8a", {});
    const z8b = mockBackend("z8b", {});
    const z8c = mockBackend("z8c", {});
    const portsZ8 = await Promise.all([listen(z8a.srv), listen(z8b.srv), listen(z8c.srv)]);
    z8a.port = portsZ8[0]; z8b.port = portsZ8[1]; z8c.port = portsZ8[2];
    const cfgZ8 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z8a", baseURL: `http://${HOST}:${z8a.port}`, apiKeyEnv: "KEY_Z8A" },
        { id: "z8b", baseURL: `http://${HOST}:${z8b.port}`, apiKeyEnv: "KEY_Z8B" },
        { id: "z8c", baseURL: `http://${HOST}:${z8c.port}`, apiKeyEnv: "KEY_Z8C" },
      ],
      models: {
        "m8": {
          providers: [
            { backend: "z8a", upstream: "u8" },
            { backend: "z8b", upstream: "u8" },
            { backend: "z8c", upstream: "u8" },
          ],
          affinityPool: 3,
        },
      },
      presets: { "p8": { strategy: "affinity", models: ["m8"] } },
      backoff: BO,
    };
    const { child: childZ8, base: baseZ8, dir: dirZ8 } = await startRouterCfg(cfgZ8, "KEY_Z8A=test-key-z8a\nKEY_Z8B=test-key-z8b\nKEY_Z8C=test-key-z8c\n");
    try {
      const served = [];
      for (let i = 0; i < 4; i++) {
        const rr = await api(baseZ8, "/v1/chat/completions", { body: { model: "p8", messages: [] }, headers: { "x-session-affinity": "z8-sess" } });
        served.push(rr.headers.get("x-router-backend"));
      }
      const distinctSame = new Set(served).size;
      let other = null;
      for (const s2 of ["z8-2", "z8-3", "z8-4", "z8-5", "z8-6", "z8-7"]) {
        const rr = await api(baseZ8, "/v1/chat/completions", { body: { model: "p8", messages: [] }, headers: { "x-session-affinity": s2 } });
        const b = rr.headers.get("x-router-backend");
        if (b !== served[0]) { other = b; break; }
      }
      assert(
        distinctSame === 1 && !!other && other !== served[0],
        "z8) model-level affinity: same session sticks, another session spreads (" + JSON.stringify(served) + " other=" + other + ")"
      );
    } finally {
      childZ8.kill();
      z8a.srv.close(); z8b.srv.close(); z8c.srv.close();
      await rm(dirZ8, { recursive: true, force: true });
    }
  }

  // z9) layered params full stack: global {temperature}, backend {top_p},
  //     provider {frequency_penalty}, model {reasoning_effort},
  //     preset {max_tokens: 512}, body {max_tokens: 33}.
  {
    const z9 = mockBackend("z9", {});
    const portZ9 = await listen(z9.srv);
    z9.port = portZ9;
    const cfgZ9 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      params: { temperature: 0.7 },
      backends: [{ id: "z9", baseURL: `http://${HOST}:${z9.port}`, apiKeyEnv: "KEY_Z9", params: { top_p: 0.9 } }],
      models: {
        "m9": { providers: [{ backend: "z9", upstream: "deepseek-v4-flash", params: { frequency_penalty: 0.5 } }], params: { reasoning_effort: "high" } },
      },
      presets: { "p9": { strategy: "affinity", models: ["m9"], params: { max_tokens: 512 } } },
      backoff: BO,
    };
    const { child: childZ9, base: baseZ9, dir: dirZ9 } = await startRouterCfg(cfgZ9, "KEY_Z9=test-key-z9\n");
    try {
      const rr = await api(baseZ9, "/v1/chat/completions", { body: { model: "p9", messages: [], max_tokens: 33 } });
      const sent = JSON.parse(z9.hits[z9.hits.length - 1].body);
      assert(
        sent.temperature === 0.7 && sent.top_p === 0.9 && sent.frequency_penalty === 0.5 &&
        sent.reasoning_effort === "high" && sent.max_tokens === 33 && sent.model === "deepseek-v4-flash",
        "z9) layered params merge (global/backend/provider/model/preset/body) (" + JSON.stringify(sent) + ")"
      );
    } finally {
      childZ9.kill();
      z9.srv.close();
      await rm(dirZ9, { recursive: true, force: true });
    }
  }

  // z10) same-key precedence + reserved model: the MODEL layer applies (0.5),
  //      body 0.9 wins, params-layer `model` keys ignored, forwarded model =
  //      upstream. The preset layer is gone: see the re-pin note below.
  {
    const z10 = mockBackend("z10", {});
    const portZ10 = await listen(z10.srv);
    z10.port = portZ10;
    const cfgZ10 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z10", baseURL: `http://${HOST}:${z10.port}`, apiKeyEnv: "KEY_Z10" }],
      models: {
        "m10": { providers: [{ backend: "z10", upstream: "deepseek-v4-flash" }], params: { temperature: 0.5, model: "evil-model" } },
      },
      presets: { "p10": { strategy: "affinity", models: ["m10"], params: { temperature: 0.2, model: "evil-preset" } } },
      backoff: BO,
    };
    // Spawned directly rather than through startRouterCfg so the child's stderr
    // is available: the dropped preset params must be REPORTED, and reading that
    // warning is half of what this block now pins.
    const dirZ10 = await mkdtemp(join(tmpdir(), "lmr-z10-"));
    const cfgZ10Path = join(dirZ10, "config.json");
    const envZ10Path = join(dirZ10, ".env");
    await writeFile(cfgZ10Path, JSON.stringify(cfgZ10, null, 2));
    await writeFile(envZ10Path, "KEY_Z10=test-key-z10\n");
    const childZ10 = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgZ10Path, ROUTER_ENV: envZ10Path },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const z10Warnings = [];
    let baseZ10 = null;
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("z10 server did not start")), 8000);
      childZ10.stderr.on("data", (d) => {
        const s = d.toString();
        z10Warnings.push(s);
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
        if (m) { baseZ10 = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
      });
    });
    try {
      await api(baseZ10, "/v1/chat/completions", { body: { model: "p10", messages: [], temperature: 0.9 } });
      const bodyWin = JSON.parse(z10.hits[z10.hits.length - 1].body);
      await api(baseZ10, "/v1/chat/completions", { body: { model: "p10", messages: [] } });
      const presetWin = JSON.parse(z10.hits[z10.hits.length - 1].body);
      // RE-PINNED by Task 11 (dispatch ruling C): a SURVIVING non-identity preset
      // carrying `params` has no v3 layer to merge it at, so the load pipeline's
      // bridge drops it and says so. The old expectation here (preset 0.2 beats
      // model 0.5) was the pre-bridge behaviour. The model layer still applies,
      // the reserved `model` key is still ignored, and the loss is reported by
      // the converter's own warning, which is asserted below rather than noted.
      assert(
        bodyWin.temperature === 0.9 && bodyWin.model === "deepseek-v4-flash" &&
        presetWin.temperature === 0.5 && presetWin.model === "deepseek-v4-flash",
        "z10) precedence body>model after the bridge (a preset-level params layer no longer exists in v3); params-layer model ignored (" + JSON.stringify({ bodyWin, presetWin }) + ")"
      );
      assert(
        z10Warnings.some((w) => w.includes("p10") && /params dropped/.test(w)),
        "z10b) the dropped preset params are REPORTED, naming the preset, not silently lost (" + JSON.stringify(z10Warnings.join("").split("\n").filter((l) => /warning/.test(l))) + ")"
      );
    } finally {
      childZ10.kill();
      z10.srv.close();
      await rm(dirZ10, { recursive: true, force: true });
    }
  }

  // z11) dialect order: provider dropParams + paramMap -> drop first, rename
  //      survivors; body's dropped keys are gone, mapped key present.
  {
    const z11 = mockBackend("z11", {});
    const portZ11 = await listen(z11.srv);
    z11.port = portZ11;
    const cfgZ11 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z11", baseURL: `http://${HOST}:${z11.port}`, apiKeyEnv: "KEY_Z11" }],
      models: {
        "m11": { providers: [{ backend: "z11", upstream: "deepseek-v4-flash", dropParams: ["reasoning_effort"], paramMap: { top_p: "topP" } }] },
      },
      presets: { "p11": { strategy: "affinity", models: ["m11"] } },
      backoff: BO,
    };
    const { child: childZ11, base: baseZ11, dir: dirZ11 } = await startRouterCfg(cfgZ11, "KEY_Z11=test-key-z11\n");
    try {
      const rr = await api(baseZ11, "/v1/chat/completions", { body: { model: "p11", messages: [], reasoning_effort: "high", top_p: 0.5 } });
      const sent = JSON.parse(z11.hits[z11.hits.length - 1].body);
      assert(
        rr.status === 200 && !("reasoning_effort" in sent) && !("top_p" in sent) && sent.topP === 0.5,
        "z11) dialect: dropParams before paramMap (" + JSON.stringify(sent) + ")"
      );
    } finally {
      childZ11.kill();
      z11.srv.close();
      await rm(dirZ11, { recursive: true, force: true });
    }
  }

  // z11a) reasoning-off: model default {thinking enabled, reasoning_effort high}
  //       + client body {thinking:{type:"disabled"}} -> disabled wins AND the
  //       config-default reasoning_effort is stripped (no contradictory pair).
  {
    const z11a = mockBackend("z11a", {});
    const portZ11a = await listen(z11a.srv);
    z11a.port = portZ11a;
    const cfgZ11a = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z11a", baseURL: `http://${HOST}:${z11a.port}`, apiKeyEnv: "KEY_Z11A" }],
      models: {
        "m11a": { providers: [{ backend: "z11a", upstream: "deepseek-v4-flash" }], params: { thinking: { type: "enabled" }, reasoning_effort: "high" } },
      },
      presets: { "p11a": { strategy: "affinity", models: ["m11a"] } },
      backoff: BO,
    };
    const { child: childZ11a, base: baseZ11a, dir: dirZ11a } = await startRouterCfg(cfgZ11a, "KEY_Z11A=test-key-z11a\n");
    try {
      await api(baseZ11a, "/v1/chat/completions", { body: { model: "p11a", messages: [], thinking: { type: "disabled" } } });
      const sent = JSON.parse(z11a.hits[z11a.hits.length - 1].body);
      assert(
        sent.thinking.type === "disabled" && !("reasoning_effort" in sent) && sent.model === "deepseek-v4-flash",
        "z11a) thinking:disabled strips config-default reasoning_effort (" + JSON.stringify(sent) + ")"
      );
    } finally {
      childZ11a.kill();
      z11a.srv.close();
      await rm(dirZ11a, { recursive: true, force: true });
    }
  }

  // z11b) reasoning-off control: same model defaults, client body WITHOUT any
  //       reasoning keys -> default path untouched (thinking enabled + effort
  //       high both forwarded).
  {
    const z11b = mockBackend("z11b", {});
    const portZ11b = await listen(z11b.srv);
    z11b.port = portZ11b;
    const cfgZ11b = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z11b", baseURL: `http://${HOST}:${z11b.port}`, apiKeyEnv: "KEY_Z11B" }],
      models: {
        "m11b": { providers: [{ backend: "z11b", upstream: "deepseek-v4-flash" }], params: { thinking: { type: "enabled" }, reasoning_effort: "high" } },
      },
      presets: { "p11b": { strategy: "affinity", models: ["m11b"] } },
      backoff: BO,
    };
    const { child: childZ11b, base: baseZ11b, dir: dirZ11b } = await startRouterCfg(cfgZ11b, "KEY_Z11B=test-key-z11b\n");
    try {
      await api(baseZ11b, "/v1/chat/completions", { body: { model: "p11b", messages: [] } });
      const sent = JSON.parse(z11b.hits[z11b.hits.length - 1].body);
      assert(
        sent.thinking.type === "enabled" && sent.reasoning_effort === "high" && sent.model === "deepseek-v4-flash",
        "z11b) control: no body reasoning keys -> model defaults intact (" + JSON.stringify(sent) + ")"
      );
    } finally {
      childZ11b.kill();
      z11b.srv.close();
      await rm(dirZ11b, { recursive: true, force: true });
    }
  }

  // z11c) reasoning-off authoritative: model default has reasoning_effort only
  //       (no thinking default); client body explicitly sets BOTH thinking
  //       disabled AND reasoning_effort high -> disable still wins, effort is
  //       stripped even when the client itself sent it.
  {
    const z11c = mockBackend("z11c", {});
    const portZ11c = await listen(z11c.srv);
    z11c.port = portZ11c;
    const cfgZ11c = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z11c", baseURL: `http://${HOST}:${z11c.port}`, apiKeyEnv: "KEY_Z11C" }],
      models: {
        "m11c": { providers: [{ backend: "z11c", upstream: "deepseek-v4-flash" }], params: { reasoning_effort: "high" } },
      },
      presets: { "p11c": { strategy: "affinity", models: ["m11c"] } },
      backoff: BO,
    };
    const { child: childZ11c, base: baseZ11c, dir: dirZ11c } = await startRouterCfg(cfgZ11c, "KEY_Z11C=test-key-z11c\n");
    try {
      await api(baseZ11c, "/v1/chat/completions", { body: { model: "p11c", messages: [], thinking: { type: "disabled" }, reasoning_effort: "high" } });
      const sent = JSON.parse(z11c.hits[z11c.hits.length - 1].body);
      assert(
        sent.thinking.type === "disabled" && !("reasoning_effort" in sent) && sent.model === "deepseek-v4-flash",
        "z11c) thinking:disabled strips explicit reasoning_effort too (" + JSON.stringify(sent) + ")"
      );
    } finally {
      childZ11c.kill();
      z11c.srv.close();
      await rm(dirZ11c, { recursive: true, force: true });
    }
  }

  // z12) validation + empty 404: unknown strategy defaults; preset whose model
  //      ref is dropped -> 404 "preset '<id>' has no valid models".
  {
    const z12 = mockBackend("z12", {});
    const portZ12 = await listen(z12.srv);
    z12.port = portZ12;
    const cfgZ12 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z12", baseURL: `http://${HOST}:${z12.port}`, apiKeyEnv: "KEY_Z12" }],
      models: { "ok": { providers: [{ backend: "z12", upstream: "u" }] } },
      presets: { "p12": { strategy: "bogus", models: ["ghost"] } },
      backoff: BO,
    };
    const { child: childZ12, base: baseZ12, dir: dirZ12 } = await startRouterCfg(cfgZ12, "KEY_Z12=test-key-z12\n");
    try {
      const rr = await api(baseZ12, "/v1/chat/completions", { body: { model: "p12", messages: [] } });
      const eb = await rr.json();
      assert(
        rr.status === 404 && eb.error && eb.error.message === "preset 'p12' has no valid models",
        "z12) empty preset 404 message (" + JSON.stringify(eb.error) + ")"
      );
    } finally {
      childZ12.kill();
      z12.srv.close();
      await rm(dirZ12, { recursive: true, force: true });
    }
  }

  // z13) routedModel history: preset id != model id; newest entry keeps model =
  //      requested id and adds routedModel = serving model id.
  {
    const z13 = mockBackend("z13", {});
    const portZ13 = await listen(z13.srv);
    z13.port = portZ13;
    const cfgZ13 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z13", baseURL: `http://${HOST}:${z13.port}`, apiKeyEnv: "KEY_Z13" }],
      models: { "m13": { providers: [{ backend: "z13", upstream: "u13" }] } },
      presets: { "p13": { strategy: "affinity", models: ["m13"] } },
      backoff: BO,
    };
    const { child: childZ13, base: baseZ13, dir: dirZ13 } = await startRouterCfg(cfgZ13, "KEY_Z13=test-key-z13\n");
    try {
      await api(baseZ13, "/v1/chat/completions", { body: { model: "p13", messages: [] } });
      const h = await (await api(baseZ13, "/api/history", { method: "GET" })).json();
      const newest = (h.entries || [])[0];
      assert(
        newest && newest.model === "p13" && newest.routedModel === "m13" && newest.status === 200,
        "z13) history routedModel (" + JSON.stringify({ model: newest && newest.model, routedModel: newest && newest.routedModel }) + ")"
      );
    } finally {
      childZ13.kill();
      z13.srv.close();
      await rm(dirZ13, { recursive: true, force: true });
    }
  }

  // z14) shared-backend dedup: both models' sole provider is one backend with
  //      different upstreams -> exactly 1 hit, payload model = first upstream.
  {
    const z14 = mockBackend("z14", {});
    const portZ14 = await listen(z14.srv);
    z14.port = portZ14;
    const cfgZ14 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z14", baseURL: `http://${HOST}:${z14.port}`, apiKeyEnv: "KEY_Z14" }],
      models: {
        "m1": { providers: [{ backend: "z14", upstream: "upstream-1" }] },
        "m2": { providers: [{ backend: "z14", upstream: "upstream-2" }] },
      },
      presets: { "p14": { strategy: "failover", models: ["m1", "m2"] } },
      backoff: BO,
    };
    const { child: childZ14, base: baseZ14, dir: dirZ14 } = await startRouterCfg(cfgZ14, "KEY_Z14=test-key-z14\n");
    try {
      const rr = await api(baseZ14, "/v1/chat/completions", { body: { model: "p14", messages: [] } });
      const sent = JSON.parse(z14.hits[z14.hits.length - 1].body);
      assert(
        rr.status === 200 && z14.hits.length === 1 && sent.model === "upstream-1",
        "z14) shared backend deduped: 1 hit, first upstream wins (hits=" + z14.hits.length + ", model=" + sent.model + ")"
      );
    } finally {
      childZ14.kill();
      z14.srv.close();
      await rm(dirZ14, { recursive: true, force: true });
    }
  }

  // z15) manual cool through the model layer: cool the session's primary
  //      provider; request served by the other; success does NOT clear the
  //      manual cool; uncool restores.
  {
    const z15a = mockBackend("z15a", {});
    const z15b = mockBackend("z15b", {});
    const portsZ15 = await Promise.all([listen(z15a.srv), listen(z15b.srv)]);
    z15a.port = portsZ15[0]; z15b.port = portsZ15[1];
    const cfgZ15 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z15a", baseURL: `http://${HOST}:${z15a.port}`, apiKeyEnv: "KEY_Z15A" },
        { id: "z15b", baseURL: `http://${HOST}:${z15b.port}`, apiKeyEnv: "KEY_Z15B" },
      ],
      models: {
        "m15": { providers: [{ backend: "z15a", upstream: "u15" }, { backend: "z15b", upstream: "u15" }], affinityPool: 2 },
      },
      presets: { "p15": { strategy: "affinity", models: ["m15"] } },
      backoff: BO,
    };
    const { child: childZ15, base: baseZ15, dir: dirZ15 } = await startRouterCfg(cfgZ15, "KEY_Z15A=test-key-z15a\nKEY_Z15B=test-key-z15b\n");
    try {
      const sess = "z15-sess";
      let rr = await api(baseZ15, "/v1/chat/completions", { body: { model: "p15", messages: [] }, headers: { "x-session-affinity": sess } });
      const primary = rr.headers.get("x-router-backend");
      await api(baseZ15, "/admin/backend", { method: "POST", body: { id: primary, action: "cool" } });
      rr = await api(baseZ15, "/v1/chat/completions", { body: { model: "p15", messages: [] }, headers: { "x-session-affinity": sess } });
      const other = rr.headers.get("x-router-backend");
      const hAfter = await (await api(baseZ15, "/health", { method: "GET" })).json();
      const primaryH = hAfter.backends.find((b) => b.id === primary);
      const manualStill = primaryH.state === "cooling" && primaryH.manual === true;
      await api(baseZ15, "/admin/backend", { method: "POST", body: { id: primary, action: "uncool" } });
      rr = await api(baseZ15, "/v1/chat/completions", { body: { model: "p15", messages: [] }, headers: { "x-session-affinity": sess } });
      const restored = rr.headers.get("x-router-backend") === primary;
      assert(
        other !== primary && other !== "none" && manualStill && restored,
        "z15) manual cool through model layer: other serves, cool sticks, uncool restores (primary=" + primary + ", other=" + other + ", manualStill=" + manualStill + ", restored=" + restored + ")"
      );
    } finally {
      childZ15.kill();
      z15a.srv.close(); z15b.srv.close();
      await rm(dirZ15, { recursive: true, force: true });
    }
  }

  // z16) cache tri-state: usage with prompt_cache_hit_tokens > 0 -> hit=true;
  //      usage with hit=0 + miss present -> hit=false; usage with NO cache
  //      fields -> hit=null (unknown, not counted as hit or miss).
  {
    const mk = (id, usage) => {
      const srv = http.createServer(async (req, res) => {
        const chunks = [];
        for await (const c of req) chunks.push(c);
        const body = Buffer.concat(chunks).toString("utf8");
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          id, object: "chat.completion",
          choices: [{ message: { role: "assistant", content: "ok" } }],
          usage,
        }));
      });
      return srv;
    };
    const hitSrv = mk("z16h", { prompt_tokens: 100, completion_tokens: 5, prompt_cache_hit_tokens: 40, prompt_cache_miss_tokens: 60 });
    const missSrv = mk("z16m", { prompt_tokens: 100, completion_tokens: 5, prompt_cache_hit_tokens: 0, prompt_cache_miss_tokens: 100 });
    const unkSrv = mk("z16u", { prompt_tokens: 100, completion_tokens: 5 });
    const ports16 = await Promise.all([listen(hitSrv), listen(missSrv), listen(unkSrv)]);
    const cfgZ16 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z16h", baseURL: `http://${HOST}:${ports16[0]}`, apiKeyEnv: "KEY_Z16H" },
        { id: "z16m", baseURL: `http://${HOST}:${ports16[1]}`, apiKeyEnv: "KEY_Z16M" },
        { id: "z16u", baseURL: `http://${HOST}:${ports16[2]}`, apiKeyEnv: "KEY_Z16U" },
      ],
      models: {
        "m16h": { providers: [{ backend: "z16h", upstream: "u16" }], affinityPool: 1 },
        "m16m": { providers: [{ backend: "z16m", upstream: "u16" }], affinityPool: 1 },
        "m16u": { providers: [{ backend: "z16u", upstream: "u16" }], affinityPool: 1 },
      },
      presets: {
        "p16h": { strategy: "affinity", models: ["m16h"] },
        "p16m": { strategy: "affinity", models: ["m16m"] },
        "p16u": { strategy: "affinity", models: ["m16u"] },
      },
      backoff: BO,
    };
    const { child: childZ16, base: baseZ16, dir: dirZ16 } = await startRouterCfg(cfgZ16, "KEY_Z16H=k\nKEY_Z16M=k\nKEY_Z16U=k\n");
    try {
      for (const p of ["p16h", "p16m", "p16u"]) await api(baseZ16, "/v1/chat/completions", { body: { model: p, messages: [] } });
      const hist = await (await api(baseZ16, "/api/history?limit=10", { method: "GET" })).json();
      const entries = hist.entries;
      const h = entries.find((e) => e.model === "p16h");
      const m = entries.find((e) => e.model === "p16m");
      const u = entries.find((e) => e.model === "p16u");
      const stats = await (await api(baseZ16, "/api/stats", { method: "GET" })).json();
      const byBackend = stats.byBackend;
      assert(
        h && h.cacheHit === true &&
        m && m.cacheHit === false &&
        u && u.cacheHit === null &&
        (byBackend["z16h"] ? byBackend["z16h"].cacheHits === 1 : false) &&
        (!byBackend["z16m"] || byBackend["z16m"].cacheHits === 0) &&
        (!byBackend["z16u"] || byBackend["z16u"].cacheHits === 0),
        "z16) cache tri-state: usage hit=true, miss=false, no-signal=null; stats count only true (h=" + (h && h.cacheHit) + ", m=" + (m && m.cacheHit) + ", u=" + (u && u.cacheHit) + ")"
      );
      // The raw cache token split is now retained on the history entry instead
      // of being discarded after cacheHitPct: hits/miss as reported, errorKind
      // null on success, and cost null because these models have no pricing.
      assert(
        h && h.promptCacheHitTokens === 40 && h.promptCacheMissTokens === 60 && h.errorKind === null && h.cost === null,
        "z16b) cost fields: raw cache split retained (hit=" + (h && h.promptCacheHitTokens) + ", miss=" + (h && h.promptCacheMissTokens) + "), errorKind null, cost null without pricing"
      );
      // No cache signal at all -> hit 0, miss derived from prompt (full prompt
      // billed as miss: conservative when the upstream reports no split).
      assert(
        u && u.promptCacheHitTokens === 0 && u.promptCacheMissTokens === 100,
        "z16c) no cache signal -> hit 0, miss derived = prompt (hit=" + (u && u.promptCacheHitTokens) + ", miss=" + (u && u.promptCacheMissTokens) + ")"
      );
    } finally {
      childZ16.kill();
      hitSrv.close(); missSrv.close(); unkSrv.close();
      await rm(dirZ16, { recursive: true, force: true });
    }
  }

  // z31) unit-level: computeCost - pricing resolution, peak-window boundaries,
  //      the off-peak multiplier, and cache-split math with exact numbers. The
  //      default peak windows are DeepSeek's Mon-Fri 01:00-04:00 + 06:00-10:00
  //      UTC; 2026-01-05 is a Monday and 2026-01-10 is a Saturday.
  {
    const tMonOff1 = Date.UTC(2026, 0, 5, 0, 59, 59);  // Mon 00:59:59 UTC (before window 1)
    const tMonPeak1 = Date.UTC(2026, 0, 5, 1, 0, 0);   // Mon 01:00:00 UTC (window 1 start, inclusive)
    const tMonPeak2 = Date.UTC(2026, 0, 5, 3, 59, 59); // Mon 03:59:59 UTC (inside window 1)
    const tMonOff2 = Date.UTC(2026, 0, 5, 4, 0, 0);    // Mon 04:00:00 UTC (window 1 end, exclusive)
    const tMonOff3 = Date.UTC(2026, 0, 5, 10, 0, 0);   // Mon 10:00:00 UTC (window 2 end, exclusive)
    const tSat = Date.UTC(2026, 0, 10, 2, 0, 0);       // Sat 02:00 UTC (no windows on weekends)
    const opCfg = { models: { m: { meta: { pricing: { inputPerM: 1, outputPerM: 1, offPeak: true } } } } };
    const flag = (t, cfg) => computeCost({ t, model: "m", promptTokens: 0, completionTokens: 0 }, cfg).offPeak;
    assert(flag(tMonOff1, opCfg) === true, "z31a) Mon 00:59:59 UTC -> offPeak true");
    assert(flag(tMonPeak1, opCfg) === false, "z31b) Mon 01:00:00 UTC -> offPeak false (window start inclusive)");
    assert(flag(tMonPeak2, opCfg) === false, "z31c) Mon 03:59:59 UTC -> offPeak false (inside window 1)");
    assert(flag(tMonOff2, opCfg) === true, "z31d) Mon 04:00:00 UTC -> offPeak true (window end exclusive)");
    assert(flag(tMonOff3, opCfg) === true, "z31e) Mon 10:00:00 UTC -> offPeak true (window 2 end exclusive)");
    assert(flag(tSat, opCfg) === true, "z31f) Sat 02:00 UTC -> offPeak true (weekends have no windows)");
    const customCfg = {
      models: { m: { meta: { pricing: { inputPerM: 1, outputPerM: 1, offPeak: true } } } },
      pricing: { peakWindows: [{ days: "6-6", start: "00:00", end: "23:59" }] },
    };
    assert(flag(tSat, customCfg) === false, "z31g) custom peakWindows override: Sat 02:00 UTC -> peak when Saturday is a window");
    const none = computeCost({ t: tMonPeak1, model: "m", promptTokens: 10 }, { models: { m: { meta: {} } } });
    assert(none === null, "z31h) computeCost -> null when the model has no meta.pricing");
    const unknown = computeCost({ t: tMonPeak1, model: "m", promptTokens: 10 }, { models: {} });
    assert(unknown === null, "z31i) computeCost -> null when the model id is unknown");
    // Multiplier: offPeak 0.5 halves the bill for an opted-in model; a model
    // without offPeak:true pays peak rates off-peak with the flag false.
    const peakCfg = { models: { m: { meta: { pricing: { inputPerM: 1, outputPerM: 2, offPeak: true } } } } };
    const rPeak = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1000, completionTokens: 100 }, peakCfg);
    const rOff = computeCost({ t: tMonOff1, model: "m", promptTokens: 1000, completionTokens: 100 }, peakCfg);
    assert(
      rPeak && rOff && rPeak.cost === 0.0012 && rOff.cost === 0.0006 && rOff.cost === rPeak.cost / 2 && rOff.offPeak === true,
      "z31j) off-peak multiplier 0.5 halves cost for an offPeak:true model (peak=" + (rPeak && rPeak.cost) + ", off=" + (rOff && rOff.cost) + ")"
    );
    const noOptCfg = { models: { m: { meta: { pricing: { inputPerM: 1, outputPerM: 2 } } } } };
    const rNoOptPeak = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1000, completionTokens: 100 }, noOptCfg);
    const rNoOptOff = computeCost({ t: tMonOff1, model: "m", promptTokens: 1000, completionTokens: 100 }, noOptCfg);
    assert(
      rNoOptPeak && rNoOptOff && rNoOptOff.cost === rNoOptPeak.cost && rNoOptOff.cost === 0.0012 && rNoOptOff.offPeak === false,
      "z31k) model without offPeak:true pays peak rates off-peak, flag false (cost=" + (rNoOptOff && rNoOptOff.cost) + ", offPeak=" + (rNoOptOff && rNoOptOff.offPeak) + ")"
    );
    // Cache split: hits billed at cacheHitInputPerM, misses at inputPerM.
    const splitCfg = { models: { m: { meta: { pricing: { inputPerM: 2.8, cacheHitInputPerM: 0.7, outputPerM: 0.87 } } } } };
    const rSplit = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1000, promptCacheHitTokens: 400, promptCacheMissTokens: 600, completionTokens: 100 }, splitCfg);
    assert(
      rSplit && rSplit.cost === 0.002047 && rSplit.cacheSavings === 0.00084 && rSplit.offPeakSavings === 0 && rSplit.offPeak === false,
      "z31l) cache split billed at both rates; savings exact at peak rates (cost=" + (rSplit && rSplit.cost) + ", savings=" + (rSplit && rSplit.cacheSavings) + ")"
    );
    const noHitRateCfg = { models: { m: { meta: { pricing: { inputPerM: 2.8, outputPerM: 0.87 } } } } };
    const rNoRate = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1000, promptCacheHitTokens: 400, promptCacheMissTokens: 600, completionTokens: 100 }, noHitRateCfg);
    assert(
      rNoRate && rNoRate.cacheSavings === 0 && rNoRate.cost === 0.002887,
      "z31m) missing cacheHitInputPerM falls back to inputPerM (savings 0) (cost=" + (rNoRate && rNoRate.cost) + ")"
    );
    const rDerive = computeCost({ t: tMonPeak1, model: "m", promptTokens: 100, promptCacheHitTokens: 40, completionTokens: 0 }, splitCfg);
    assert(rDerive && rDerive.cost === 0.000196, "z31n) missing miss tokens derive from prompt - hit (cost=" + (rDerive && rDerive.cost) + ")");
    const rClamp = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1000, promptCacheHitTokens: 1500, promptCacheMissTokens: 0, completionTokens: 0 }, { models: { m: { meta: { pricing: { inputPerM: 1, cacheHitInputPerM: 0.5, outputPerM: 1 } } } } });
    assert(rClamp && rClamp.cost === 0.0005, "z31o) hit tokens clamped to prompt (cost=" + (rClamp && rClamp.cost) + ")");
    const rExact = computeCost({ t: tMonPeak1, model: "m", promptTokens: 1234, promptCacheHitTokens: 1234, promptCacheMissTokens: 0, completionTokens: 0 }, { models: { m: { meta: { pricing: { inputPerM: 1, cacheHitInputPerM: 0.0028, outputPerM: 1 } } } } });
    assert(
      rExact && rExact.cost === 0.0000034552 && rExact.cacheSavings === 0.0012305448,
      "z31p) exact numbers: 1234 hit tokens * 0.0028 / 1e6 (cost=" + (rExact && rExact.cost) + ", savings=" + (rExact && rExact.cacheSavings) + ")"
    );
    // cacheSavings uses peak rates even when off-peak pricing applied.
    const offSplitCfg = { models: { m: { meta: { pricing: { inputPerM: 2.8, cacheHitInputPerM: 0.7, outputPerM: 0.87, offPeak: true } } } } };
    const rOffSplit = computeCost({ t: tMonOff1, model: "m", promptTokens: 1000, promptCacheHitTokens: 400, promptCacheMissTokens: 600, completionTokens: 100 }, offSplitCfg);
    assert(
      rOffSplit && rOffSplit.cacheSavings === 0.00084 && rOffSplit.cost === 0.0010235 && rOffSplit.offPeakSavings === 0.0010235,
      "z31q) cacheSavings stays at peak rates off-peak; offPeakSavings = cost at mult 0.5 (cost=" + (rOffSplit && rOffSplit.cost) + ", savings=" + (rOffSplit && rOffSplit.cacheSavings) + ", offSav=" + (rOffSplit && rOffSplit.offPeakSavings) + ")"
    );
    const badMultCfg = { models: { m: { meta: { pricing: { inputPerM: 1, outputPerM: 2, offPeak: true } } } }, pricing: { offPeakMultiplier: 0 } };
    const rBadMult = computeCost({ t: tMonOff1, model: "m", promptTokens: 1000, completionTokens: 100 }, badMultCfg);
    assert(rBadMult && rBadMult.cost === 0.0006, "z31r) 0 offPeakMultiplier falls back to 0.5 (cost=" + (rBadMult && rBadMult.cost) + ")");
  }

  // z32) integration: cost + cache-split fields land on the history entry with
  //      EXACT numbers through a real request flow. peakWindows: [] pins every
  //      hour off-peak so the result is deterministic no matter when the test
  //      runs (the model opts in with offPeak:true -> multiplier 0.5 applies).
  {
    const srv32 = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({
        id: "z32", object: "chat.completion",
        choices: [{ message: { role: "assistant", content: "ok" } }],
        usage: { prompt_tokens: 1000, completion_tokens: 50, prompt_cache_hit_tokens: 400, prompt_cache_miss_tokens: 600 },
      }));
    });
    const port32 = await listen(srv32);
    const cfgZ32 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z32", baseURL: `http://${HOST}:${port32}`, apiKeyEnv: "KEY_Z32" }],
      models: {
        "mc": { providers: [{ backend: "z32", upstream: "u32" }], meta: { pricing: { inputPerM: 0.28, cacheHitInputPerM: 0.07, outputPerM: 0.87, offPeak: true } } },
      },
      pricing: { peakWindows: [] },
      backoff: BO,
    };
    const { child: childZ32, base: baseZ32, dir: dirZ32 } = await startRouterCfg(cfgZ32, "KEY_Z32=test-key-z32\n");
    try {
      const rr = await api(baseZ32, "/v1/chat/completions", { body: { model: "mc", messages: [] } });
      assert(rr.status === 200, "z32a) request through a priced model succeeds (status=" + rr.status + ")");
      const hist = await (await api(baseZ32, "/api/history?limit=5", { method: "GET" })).json();
      const e = (hist.entries || []).find((x) => x.model === "mc");
      assert(
        e && e.promptCacheHitTokens === 400 && e.promptCacheMissTokens === 600 && e.errorKind === null &&
        e.cost === 0.00011975 && e.cacheSavings === 0.000084 && e.offPeakSavings === 0.00011975 && e.offPeak === true,
        "z32b) history entry cost fields: hit 400 / miss 600, cost 0.00011975 (off-peak half of 0.0002395), savings 0.000084, errorKind null, offPeak true (got " + JSON.stringify(e && { pcht: e.promptCacheHitTokens, pcmt: e.promptCacheMissTokens, cost: e.cost, cs: e.cacheSavings, ops: e.offPeakSavings, offPeak: e.offPeak, ek: e.errorKind }) + ")"
      );
      // The appended JSONL row must carry the same fields (survives the append path).
      const jsonl = await readFile(join(dirZ32, "router-history.jsonl"), "utf8");
      const rows = jsonl.trim().split("\n").map((l) => JSON.parse(l));
      const last = rows[rows.length - 1];
      assert(
        last && last.model === "mc" && last.promptCacheHitTokens === 400 && last.promptCacheMissTokens === 600 &&
        last.cost === 0.00011975 && typeof last.offPeak === "boolean" && last.errorKind === null,
        "z32c) router-history.jsonl row carries the new cost fields (cost=" + (last && last.cost) + ")"
      );
    } finally {
      childZ32.kill();
      srv32.close();
      await rm(dirZ32, { recursive: true, force: true });
    }
  }

  // z33) integration: an upstream 429 (rate-limit class) survives to the
  //      history entry as errorKind "rate" with a 0-token cost of 0 (not null).
  {
    const srv33 = http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      res.writeHead(429, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "rate limited" } }));
    });
    const port33 = await listen(srv33);
    const cfgZ33 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z33", baseURL: `http://${HOST}:${port33}`, apiKeyEnv: "KEY_Z33" }],
      models: { "mf": { providers: [{ backend: "z33", upstream: "u33" }], meta: { pricing: { inputPerM: 1, outputPerM: 1 } } } },
      backoff: BO,
    };
    const { child: childZ33, base: baseZ33, dir: dirZ33 } = await startRouterCfg(cfgZ33, "KEY_Z33=test-key-z33\n");
    try {
      await api(baseZ33, "/v1/chat/completions", { body: { model: "mf", messages: [] } });
      const hist = await (await api(baseZ33, "/api/history?limit=5", { method: "GET" })).json();
      const e = (hist.entries || []).find((x) => x.model === "mf");
      assert(
        e && e.status === 503 && e.errorKind === "rate" && e.promptCacheHitTokens === 0 && e.promptCacheMissTokens === 0 &&
        e.cost === 0 && e.cacheSavings === 0 && e.offPeakSavings === 0,
        "z33) 429 exhausted -> errorKind rate, 0-token cost 0 not null (status=" + (e && e.status) + ", kind=" + (e && e.errorKind) + ", cost=" + (e && e.cost) + ")"
      );
    } finally {
      childZ33.kill();
      srv33.close();
      await rm(dirZ33, { recursive: true, force: true });
    }
  }

  // z27) unit-level: cacheHitPctOf - the per-call cache-hit percentage. Low % =
  //      early cache break (0% = miss from the very start), 95% = break near
  //      the end. Non-finite / zero-total signals yield null.
  {
    const pct = (h, m, c, p) => cacheHitPctOf(h, m, c, p);
    assert(pct(100, 900, NaN, 1000) === 10, "z27a) cacheHitPctOf(100,900) == 10");
    assert(pct(0, 100, NaN, 100) === 0, "z27b) cacheHitPctOf(0,100) == 0");
    assert(pct(40, 60, NaN, 100) === 40, "z27c) cacheHitPctOf(40,60) == 40");
    assert(pct(1000, 0, NaN, 1000) === 100, "z27d) cacheHitPctOf(1000,0) == 100");
    assert(pct(NaN, NaN, NaN, 1000) === null, "z27e) cacheHitPctOf(no signal) == null");
    assert(pct(0, 0, NaN, 1000) === null, "z27f) cacheHitPctOf(0,0) == null (nothing measured)");
    assert(pct(NaN, NaN, 500, 1000) === 50, "z27g) cacheHitPctOf(cached 500/1000) == 50 (cached-only fallback)");
    assert(pct(100, NaN, NaN, 1000) === 10, "z27h) cacheHitPctOf(hit 100/1000 prompt) == 10 (hit-only fallback)");
    assert(pct(1, 2, NaN, 3) === 33.3, "z27i) cacheHitPctOf rounding: (1,2) == 33.3");
  }

  // z28) unit-level: buildAlert / resetAlertCooldowns - config-gated, cooldown-
  //      throttled ntfy alert construction. Never sends; pure shape checks.
  {
    resetAlertCooldowns(); // order independence: clear the shared cooldown map
    assert(buildAlert({}, "cache_miss", {}) === null, "z28a) no alerts.ntfy block -> null");
    const cfgn = { alerts: { ntfy: { baseUrl: "http://x", topic: "t" } } };
    const missFields = { backend: "b1", cacheHitPct: 10, missTokens: 100, promptTokens: 1000, model: "m", session: "s" };
    const a1 = buildAlert(cfgn, "cache_miss", missFields);
    assert(
      a1 && a1.url === "http://x" && a1.body.topic === "t" &&
      typeof a1.body.message === "string" && a1.body.message.includes("Cache break") &&
      a1.body.message.includes("b1") && Array.isArray(a1.body.tags) && a1.body.tags.length === 1 &&
      typeof a1.body.priority === "number",
      "z28b) enabled block -> {url, body{topic,title,message,tags,priority}}"
    );
    const cfge = { alerts: { ntfy: { baseUrl: "http://x", topic: "t", events: { cache_miss: false } } } };
    assert(buildAlert(cfge, "cache_miss", {}) === null, "z28c) kind disabled via events -> null");
    assert(buildAlert(cfgn, "nope", {}) === null, "z28d) unknown kind -> null");
    resetAlertCooldowns(); // drop the cooldown a1 set so the pair below starts clean
    const a4 = buildAlert(cfgn, "cache_miss", missFields);
    const a5 = buildAlert(cfgn, "cache_miss", missFields);
    assert(a4 !== null && a5 === null, "z28e) cooldown: same kind twice -> second is null");
    const a6 = buildAlert(cfgn, "weekly_limit", { backend: "b1", days: 7 });
    assert(a6 !== null, "z28f) different kind still fires during cooldown");
    resetAlertCooldowns();
    const a7 = buildAlert(cfgn, "cache_miss", missFields);
    assert(a7 !== null, "z28g) resetAlertCooldowns clears -> same kind fires again");
    resetAlertCooldowns();
  }

  // z29) integration: a big cache MISS persists a bounded payload row AND fires
  //      an ntfy alert; a full cache HIT does neither. Exercises Part 1 (pct
  //      on /api/history) + Part 2 (persistent capture) + Part 3 (ntfy) end to
  //      end through a real server child against mock backends and a mock ntfy
  //      receiver.
  {
    const received = []; // POST bodies recorded by the mock ntfy receiver
    const ntfySrv = http.createServer((req, res) => {
      const chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const b = Buffer.concat(chunks).toString("utf8");
        try { received.push(JSON.parse(b)); } catch { received.push({ raw: b }); }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      });
    });
    const ntfyPort = await listen(ntfySrv);
    const mk = (id, usage) => {
      const srv = http.createServer(async (req, res) => {
        const chunks = [];
        for await (const c of req) chunks.push(c);
        const body = Buffer.concat(chunks).toString("utf8");
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          id, object: "chat.completion",
          choices: [{ message: { role: "assistant", content: "ok" } }],
          usage,
        }));
      });
      return srv;
    };
    const missSrv = mk("z29m", { prompt_tokens: 1000, completion_tokens: 5, prompt_cache_hit_tokens: 100, prompt_cache_miss_tokens: 900 });
    const hitSrv = mk("z29h", { prompt_tokens: 1000, completion_tokens: 5, prompt_cache_hit_tokens: 1000, prompt_cache_miss_tokens: 0 });
    const ports29 = await Promise.all([listen(missSrv), listen(hitSrv)]);
    const cfgZ29 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z29m", baseURL: `http://${HOST}:${ports29[0]}`, apiKeyEnv: "KEY_Z29M" },
        { id: "z29h", baseURL: `http://${HOST}:${ports29[1]}`, apiKeyEnv: "KEY_Z29H" },
      ],
      models: {
        "m29a": { providers: [{ backend: "z29m", upstream: "u29" }], affinityPool: 1 },
        "m29b": { providers: [{ backend: "z29h", upstream: "u29" }], affinityPool: 1 },
      },
      presets: {
        "p29a": { strategy: "affinity", models: ["m29a"] },
        "p29b": { strategy: "affinity", models: ["m29b"] },
      },
      backoff: BO,
      alerts: { ntfy: { baseUrl: `http://${HOST}:${ntfyPort}`, topic: "t29", minMissPct: 50, minMissTokens: 0, cooldownMs: 600000 } },
      missCapture: { enabled: true, maxMissPct: 50, minMissTokens: 0, file: "misses.jsonl" },
    };
    const { child: childZ29, base: baseZ29, dir: dirZ29 } = await startRouterCfg(cfgZ29, "KEY_Z29M=k\nKEY_Z29H=k\n");
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const waitFor = async (fn, timeoutMs) => {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        if (await fn()) return true;
        await sleep(25);
      }
      return false;
    };
    try {
      // p29a: 10% cache hit -> a real miss. Tri-state stays true (hit > 0); the
      // percentage is what the new feature reports.
      const ra = await api(baseZ29, "/v1/chat/completions", { body: { model: "p29a", messages: [{ role: "user", content: "z29 miss probe" }] } });
      assert(ra.status === 200, "z29a) p29a request succeeds (status=" + ra.status + ")");
      let hist = await (await api(baseZ29, "/api/history?limit=10", { method: "GET" })).json();
      let ea = hist.entries.find((e) => e.model === "p29a");
      assert(
        ea && ea.cacheHit === true && ea.cacheHitPct === 10,
        "z29b) p29a history: cacheHit=true, cacheHitPct=10 (hit=" + (ea && ea.cacheHit) + ", pct=" + (ea && ea.cacheHitPct) + ")"
      );
      const alerted = await waitFor(() => received.length >= 1, 2000);
      assert(
        alerted && received[0] && received[0].topic === "t29" && typeof received[0].message === "string" && received[0].message.includes("10%"),
        "z29c) cache_miss ntfy alert fired with 10% (received=" + received.length + ")"
      );
      let missesFile = join(dirZ29, "misses.jsonl");
      const missWritten = await waitFor(async () => {
        try { return (await readFile(missesFile, "utf8")).split("\n").filter(Boolean).length === 1; } catch { return false; }
      }, 2000);
      let rows = missWritten ? (await readFile(missesFile, "utf8")).split("\n").filter(Boolean) : [];
      let row0 = rows.length ? JSON.parse(rows[0]) : null;
      assert(
        missWritten && row0 && row0.cacheHitPct === 10 && row0.cacheMissTokens === 900 &&
        Array.isArray(row0.payload && row0.payload.messages) && row0.payload.messages.some((m) => m.content === "z29 miss probe"),
        "z29d) misses.jsonl has exactly 1 row with cacheHitPct=10, cacheMissTokens=900, and the sent message (rows=" + rows.length + ")"
      );
      // p29b: 100% cache hit -> no capture, no alert.
      const rb = await api(baseZ29, "/v1/chat/completions", { body: { model: "p29b", messages: [{ role: "user", content: "z29 hit probe" }] } });
      assert(rb.status === 200, "z29e) p29b request succeeds (status=" + rb.status + ")");
      hist = await (await api(baseZ29, "/api/history?limit=10", { method: "GET" })).json();
      const eb = hist.entries.find((e) => e.model === "p29b");
      assert(eb && eb.cacheHitPct === 100, "z29f) p29b history: cacheHitPct=100 (pct=" + (eb && eb.cacheHitPct) + ")");
      await sleep(100); // allow any (wrong) fire-and-forget to land
      let rowsAfter = [];
      try { rowsAfter = (await readFile(missesFile, "utf8")).split("\n").filter(Boolean); } catch { /* file may be absent -> 0 rows */ }
      assert(
        rowsAfter.length === 1 && received.length === 1,
        "z29g) 100% hit: misses.jsonl still 1 row and ntfy count still 1 (rows=" + rowsAfter.length + ", ntfy=" + received.length + ")"
      );
    } finally {
      childZ29.kill();
      missSrv.close(); hitSrv.close(); ntfySrv.close();
      await rm(dirZ29, { recursive: true, force: true });
    }
  }

  // z34) integration: an ABSOLUTE missCapture.file is used as-is rather than
  //      joined to the config directory, so capture really appends there. The
  //      homelab config points at /data/history/router-misses.jsonl on a durable
  //      named volume; join() ignored the absolute name, produced a bogus path
  //      inside the config dir, and captureMiss swallowed the resulting ENOENT -
  //      the feature was silently dead while the config still looked valid.
  {
    const srv34 = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({
        id: "z34m", object: "chat.completion",
        choices: [{ message: { role: "assistant", content: "ok" } }],
        usage: { prompt_tokens: 10000, completion_tokens: 5, prompt_cache_hit_tokens: 1000, prompt_cache_miss_tokens: 9000 },
      }));
    });
    const port34 = await listen(srv34);
    // The capture target lives OUTSIDE the config dir on purpose: if the path
    // were joined to the config dir, the write would land somewhere else.
    const absDir34 = await mkdtemp(join(tmpdir(), "lmr-miss-abs-"));
    const absFile34 = join(absDir34, "router-misses.jsonl");
    const cfgZ34 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z34", baseURL: `http://${HOST}:${port34}`, apiKeyEnv: "KEY_Z34" }],
      models: { "m34": { providers: [{ backend: "z34", upstream: "u34" }], affinityPool: 1 } },
      presets: { "p34": { strategy: "affinity", models: ["m34"] } },
      backoff: BO,
      missCapture: { enabled: true, maxMissPct: 50, minMissTokens: 1000, file: absFile34 },
    };
    const { child: childZ34, base: baseZ34, dir: dirZ34 } = await startRouterCfg(cfgZ34, "KEY_Z34=k\n");
    try {
      const r34 = await api(baseZ34, "/v1/chat/completions", { body: { model: "p34", messages: [{ role: "user", content: "z34 absolute probe" }] } });
      assert(r34.status === 200, "z34a) p34 request succeeds (status=" + r34.status + ")");
      await new Promise((r) => setTimeout(r, 300));
      let rows34 = [];
      try { rows34 = (await readFile(absFile34, "utf8")).split("\n").filter(Boolean); } catch { /* absent -> 0 rows */ }
      const row34 = rows34.length ? JSON.parse(rows34[0]) : null;
      assert(
        row34 && row34.cacheHitPct === 10 && row34.cacheMissTokens === 9000 &&
        Array.isArray(row34.payload && row34.payload.messages) && row34.payload.messages.some((m) => m.content === "z34 absolute probe"),
        "z34b) absolute missCapture.file is used as-is: capture appended to that exact path (rows=" + rows34.length + ")"
      );
      let joined34 = [];
      try { joined34 = (await readFile(join(dirZ34, "router-misses.jsonl"), "utf8")).split("\n").filter(Boolean); } catch { /* absent -> 0 rows */ }
      assert(joined34.length === 0, "z34c) nothing is written next to the config for an absolute file (rows=" + joined34.length + ")");
    } finally {
      childZ34.kill();
      srv34.close();
      await rm(dirZ34, { recursive: true, force: true });
      await rm(absDir34, { recursive: true, force: true });
    }
  }

  // z30) regression: a STREAMING upstream that reports ONLY cached_tokens
  //      (prompt_tokens_details, OpenAI style) must yield the cached/prompt
  //      percentage, not 100% - the collector folds cached into promptCacheHit
  //      while the miss field stays unset, and the ratio must treat that as
  //      "no miss signal" rather than "zero misses".
  {
    const srv = http.createServer((req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write("data: " + JSON.stringify({ id: "m", choices: [{ delta: { content: "ok" } }] }) + "\n\n");
      res.write("data: " + JSON.stringify({ id: "m", choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 1000, completion_tokens: 5, prompt_tokens_details: { cached_tokens: 500 } } }) + "\n\n");
      res.write("data: [DONE]\n\n");
      res.end();
    });
    const port30 = await listen(srv);
    const cfgZ30 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z30", baseURL: `http://${HOST}:${port30}`, apiKeyEnv: "KEY_Z30" }],
      models: { "m30": { providers: [{ backend: "z30", upstream: "u30" }], affinityPool: 1 } },
      presets: { "p30": { strategy: "affinity", models: ["m30"] } },
      backoff: BO,
    };
    const { child: childZ30, base: baseZ30, dir: dirZ30 } = await startRouterCfg(cfgZ30, "KEY_Z30=k\n");
    try {
      const rr = await api(baseZ30, "/v1/chat/completions", { body: { model: "p30", messages: [], stream: true } });
      await rr.text();
      const hist = await (await api(baseZ30, "/api/history?limit=5", { method: "GET" })).json();
      const e = hist.entries.find((x) => x.model === "p30");
      assert(
        e && e.cacheHitPct === 50 && e.cacheHit === true,
        "z30) cached-only streaming usage: pct=cached/prompt=50, cacheHit=true (pct=" + (e && e.cacheHitPct) + ", hit=" + (e && e.cacheHit) + ")"
      );
    } finally {
      childZ30.kill();
      srv.close();
      await rm(dirZ30, { recursive: true, force: true });
    }
  }

  // z17) timeout failover: backend with a tiny timeoutMs never responds; the
  //      request fails over to the next healthy backend; the slow backend gets
  //      marked failed (fails incremented / cooling after repeated timeouts).
  {
    let hangRequests = 0;
    const hangSrv = http.createServer(() => { hangRequests++; /* never respond */ });
    const okSrv = mockBackend("z17ok", {});
    const ports17 = await Promise.all([listen(hangSrv), listen(okSrv.srv)]);
    okSrv.port = ports17[1];
    const cfgZ17 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z17hang", baseURL: `http://${HOST}:${ports17[0]}`, apiKeyEnv: "KEY_Z17HANG", timeoutMs: 80 },
        { id: "z17ok", baseURL: `http://${HOST}:${okSrv.port}`, apiKeyEnv: "KEY_Z17OK" },
      ],
      models: {
        "m17": { providers: [{ backend: "z17hang", upstream: "u17" }, { backend: "z17ok", upstream: "u17" }], affinityPool: 1 },
      },
      presets: { "p17": { strategy: "affinity", models: ["m17"] } },
      // A long server backoff keeps the timed-out route cooling for both probes.
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const { child: childZ17, base: baseZ17, dir: dirZ17 } = await startRouterCfg(cfgZ17, "KEY_Z17HANG=k\nKEY_Z17OK=k\n");
    try {
      const once = async () => {
        const rr = await fetch(baseZ17 + "/v1/chat/completions", {
          method: "POST",
          headers: { "content-type": "application/json", authorization: "Bearer local-master" },
          body: JSON.stringify({ model: "p17", messages: [] }),
          signal: AbortSignal.timeout(8000),
        });
        const backend = rr.headers.get("x-router-backend");
        await rr.text();
        return backend;
      };
      const backend = await once();
      const hang = (await (await api(baseZ17, "/health", { method: "GET" })).json()).backends.find((b) => b.id === "z17hang");
      assert(
        backend === "z17ok" && hang && hang.state === "healthy",
        "z17) a timeout is charged to z17hang's ROUTE, not its account (backend=" + backend + ", hangState=" + (hang ? hang.state : "?") + ")"
      );
      // The cooling route is skipped, so the hanging upstream is not contacted again.
      const hangRequestsAfterFirst = hangRequests;
      const backend2 = await once();
      assert(
        backend2 === "z17ok" && hangRequests === hangRequestsAfterFirst,
        "z17b) the timed-out route is not retried while cooling (backend2=" + backend2 + ", hangRequests=" + hangRequests + ")"
      );
    } finally {
      childZ17.kill();
      hangSrv.close(); okSrv.srv.close();
      await rm(dirZ17, { recursive: true, force: true });
    }
  }

  // z18) reasoning-key normalization: upstreams that emit OpenAI-Reasoning-API
  // style delta.reasoning (+ reasoning_details) must expose reasoning_content
  // to the client, because OpenCode's parser reads reasoning_content only.
  {
    const reasonSrv = http.createServer((req, res) => {
      const chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        const wantStream = body.includes('"stream":true');
        if (wantStream) {
          res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
          res.write("data: " + JSON.stringify({ id: "r1", choices: [{ delta: { role: "assistant" } }] }) + "\n\n");
          res.write("data: " + JSON.stringify({ id: "r1", choices: [{ delta: { reasoning: "Let", reasoning_details: [{ type: "reasoning.text", text: "Let me think", format: "unknown", index: 0 }] } }] }) + "\n\n");
          res.write("data: " + JSON.stringify({ id: "r1", choices: [{ delta: { reasoning: " me think" } }] }) + "\n\n");
          res.write("data: " + JSON.stringify({ id: "r1", choices: [{ delta: { content: "Answer: 391." } }] }) + "\n\n");
          res.write("data: " + JSON.stringify({ id: "r1", choices: [{ delta: {} }], usage: { prompt_tokens: 10, completion_tokens: 12, completion_tokens_details: { reasoning_tokens: 8 } } }) + "\n\n");
          res.write("data: [DONE]\n\n");
          res.end();
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          id: "r1",
          choices: [{ message: { role: "assistant", content: "Answer: 391.", reasoning: "Let me think", reasoning_details: [{ type: "reasoning.text", text: "Let me think", format: "unknown", index: 0 }] } }],
          usage: { prompt_tokens: 10, completion_tokens: 12, completion_tokens_details: { reasoning_tokens: 8 } },
        }));
      });
    });
    const reasonPort = await listen(reasonSrv);
    const cfgZ18 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "r18", baseURL: `http://${HOST}:${reasonPort}`, apiKeyEnv: "KEY_R18" }],
      models: { "m18": { providers: [{ backend: "r18", upstream: "reason-model" }], affinityPool: 1 } },
      presets: { "m18": { strategy: "affinity", models: ["m18"] } },
      backoff: BO,
    };
    const { child: childZ18, base: baseZ18, dir: dirZ18 } = await startRouterCfg(cfgZ18, "KEY_R18=k\n");
    try {
      const rrStream = await api(baseZ18, "/v1/chat/completions", { body: { model: "m18", messages: [], stream: true } });
      const sse = await rrStream.text();
      const hasReasoning = sse.includes('"reasoning"');
      const hasReasoningContent = sse.includes('"reasoning_content"');
      const mergedDetail = sse.includes('"reasoning_content":"Let me think"');
      const hasContent = sse.includes('Answer: 391.');
      assert(
        rrStream.status === 200 && hasReasoning && hasReasoningContent && hasContent,
        "z18a) stream: delta.reasoning AND delta.reasoning_content both present (" +
          "reasoning=" + hasReasoning + ", reasoning_content=" + hasReasoningContent + ")"
      );
      assert(mergedDetail, "z18b) stream: reasoning_details text merged into reasoning_content");

      const rrMsg = await api(baseZ18, "/v1/chat/completions", { body: { model: "m18", messages: [] } });
      const msg = await rrMsg.json();
      const m = msg.choices && msg.choices[0] && msg.choices[0].message;
      assert(
        rrMsg.status === 200 && m && m.reasoning === "Let me think" && m.reasoning_content === "Let me think",
        "z18c) non-stream: message.reasoning AND message.reasoning_content both present"
      );
    } finally {
      childZ18.kill();
      reasonSrv.close();
      await rm(dirZ18, { recursive: true, force: true });
    }
  }

  // z19) retryable 503 then success: same provider is retried (not failed over),
  //      request returns 200, history records retries=1.
  {
    let hits = 0;
    const flakySrv = http.createServer((req, res) => {
      hits++;
      if (hits <= 2) {
        res.writeHead(503, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "upstream temporarily unavailable", type: "overloaded_error" } }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "mock", object: "chat.completion", choices: [{ message: { role: "assistant", content: "mock:z19" } }], usage: { prompt_tokens: 5, completion_tokens: 5 } }));
    });
    const z19port = await listen(flakySrv);
    const cfgZ19 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z19", baseURL: `http://${HOST}:${z19port}`, apiKeyEnv: "KEY_Z19" }],
      models: {
        "m19": {
          providers: [{ backend: "z19", upstream: "u19" }], affinityPool: 1,
          retry: { maxRetries: 3, baseMs: 10, maxMs: 50, multiplier: 2 },
        },
      },
      presets: { "p19": { strategy: "affinity", models: ["m19"] } },
      backoff: BO,
    };
    const { child: childZ19, base: baseZ19, dir: dirZ19 } = await startRouterCfg(cfgZ19, "KEY_Z19=k\n");
    try {
      const rr = await fetch(baseZ19 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p19", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const body = await rr.json();
      const hist = await (await api(baseZ19, "/api/history", { method: "GET" })).json();
      const latest = hist.entries[0];
      // Mock fails on hits 1 and 2, succeeds on hit 3 -> 1 initial + 2 retries.
      assert(
        rr.status === 200 && hits === 3 && latest && latest.retries === 2,
        "z19) retryable 503 then success: same provider retried (hits=" + hits + ", status=" + rr.status + ", retries=" + (latest ? latest.retries : "?") + ")"
      );
    } finally {
      childZ19.kill();
      flakySrv.close();
      await rm(dirZ19, { recursive: true, force: true });
    }
  }

  // z19) preset-of-presets: a preset whose models list contains another preset
  //      id expands to the nested preset's models (recursively), preserving
  //      declared order, with the TOP-LEVEL strategy governing the ordering.
  {
    const z23a = mockBackend("z23a", {});
    const z23b = mockBackend("z23b", {});
    const z23c = mockBackend("z23c", {});
    const z23d = mockBackend("z23d", {});
    const portsZ19 = await Promise.all([listen(z23a.srv), listen(z23b.srv), listen(z23c.srv), listen(z23d.srv)]);
    z23a.port = portsZ19[0]; z23b.port = portsZ19[1]; z23c.port = portsZ19[2]; z23d.port = portsZ19[3];
    const cfgZ23 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z23a", baseURL: `http://${HOST}:${z23a.port}`, apiKeyEnv: "KEY_Z19A" },
        { id: "z23b", baseURL: `http://${HOST}:${z23b.port}`, apiKeyEnv: "KEY_Z19B" },
        { id: "z23c", baseURL: `http://${HOST}:${z23c.port}`, apiKeyEnv: "KEY_Z19C" },
        { id: "z23d", baseURL: `http://${HOST}:${z23d.port}`, apiKeyEnv: "KEY_Z19D" },
      ],
      models: {
        "m19a": { providers: [{ backend: "z23a", upstream: "u19a" }], affinityPool: 1 },
        "m19b": { providers: [{ backend: "z23b", upstream: "u19b" }], affinityPool: 1 },
        "m19c": { providers: [{ backend: "z23c", upstream: "u19c" }], affinityPool: 1 },
        "m19d": { providers: [{ backend: "z23d", upstream: "u19d" }], affinityPool: 1 },
      },
      presets: {
        "glm": { strategy: "failover", models: ["m19a", "m19b"] },   // nested preset
        "os": { strategy: "affinity", models: ["m19c", "m19d"] },    // nested preset (own strategy ignored)
        "super": { strategy: "failover", models: ["glm", "os"] },    // tier aggregating two presets
      },
      backoff: BO,
    };
    const { child: childZ23, base: baseZ23, dir: dirZ23 } = await startRouterCfg(cfgZ23, "KEY_Z19A=k\nKEY_Z19B=k\nKEY_Z19C=k\nKEY_Z19D=k\n");
    try {
      // /v1/models: presets first (config order), models not already listed.
      const mods = await (await api(baseZ23, "/v1/models", { method: "GET" })).json();
      const ids = (mods.data || []).map((m) => m.id);
      const presetFirst = ids[0] === "glm" && ids[1] === "os" && ids[2] === "super" && ids.includes("m19a");
      // failover through nested presets: m19a serves (declared order, own
      // strategies ignored for ordering).
      const rr = await api(baseZ23, "/v1/chat/completions", { body: { model: "super", messages: [] } });
      const bb = await rr.json();
      const served = rr.headers.get("x-router-backend");
      const routedModel = (await (await api(baseZ23, "/api/history?limit=1", { method: "GET" })).json()).entries[0].routedModel;
      assert(
        rr.status === 200 && /^mock:z23a/.test(bb.choices[0].message.content) && served === "z23a" && routedModel === "m19a",
        "z23a) preset-of-presets: nested preset expands, failover serves first expanded model (status=" + rr.status + ", served=" + served + ", routedModel=" + routedModel + ")"
      );
      assert(presetFirst, "z23b) /v1/models unchanged by nesting: presets first, then models (" + JSON.stringify(ids) + ")");
      // /api/config derived members include nested presets' backends.
      const cfgBody = await (await api(baseZ23, "/api/config", { method: "GET" })).json();
      const superMembers = cfgBody.presets.super.members.map((m) => m.backend);
      assert(
        JSON.stringify(superMembers) === JSON.stringify(["z23a", "z23b", "z23c", "z23d"]),
        "z23c) derived members expand nested presets (" + JSON.stringify(superMembers) + ")"
      );
    } finally {
      childZ23.kill();
      z23a.srv.close(); z23b.srv.close(); z23c.srv.close(); z23d.srv.close();
      await rm(dirZ23, { recursive: true, force: true });
    }
  }

  // z20) non-retryable client error does NOT retry: one hit only, request fails.
  {
    let hits = 0;
    const clientSrv = http.createServer((req, res) => {
      hits++;
      res.writeHead(422, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "bad payload", type: "invalid_request_error" } }));
    });
    const z20port = await listen(clientSrv);
    // A SECOND, healthy provider is what gives this test teeth: with a
    // single-provider pool the pool merely exhausts and the status assertion still
    // passes even if an explained client error stopped being terminal. With a
    // healthy fallback present, any failover shows up as z20b hits.
    const z20b = mockBackend("z20b", {});
    z20b.port = await listen(z20b.srv);
    // Live ntfy sink. It exists to PROVE the alert wiring works (zOpaque3 asserts a
    // POST arrives on an equivalent sink), which is what makes this test's "no
    // alert" assertion meaningful rather than vacuous. cooldownMs 0 leaves the
    // path unthrottled.
    const z20Ntfy = mockBackend("z20ntfy", {});
    z20Ntfy.port = await listen(z20Ntfy.srv);
    const cfgZ20 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      alerts: { ntfy: { baseUrl: `http://${HOST}:${z20Ntfy.port}`, topic: "t", cooldownMs: 0 } },
      backends: [
        { id: "z20", baseURL: `http://${HOST}:${z20port}`, apiKeyEnv: "KEY_Z20" },
        { id: "z20b", baseURL: `http://${HOST}:${z20b.port}`, apiKeyEnv: "KEY_Z20B" },
      ],
      models: {
        "m20": {
          // affinityPool 1 pins declared order, so z20 is always attempted first.
          providers: [{ backend: "z20", upstream: "u20" }, { backend: "z20b", upstream: "u20b" }], affinityPool: 1,
          retry: { maxRetries: 3, baseMs: 10, maxMs: 50 },
        },
      },
      presets: { "p20": { strategy: "affinity", models: ["m20"] } },
      backoff: BO,
    };
    const { child: childZ20, base: baseZ20, dir: dirZ20 } = await startRouterCfg(cfgZ20, "KEY_Z20=k\nKEY_Z20B=k\n");
    try {
      const rr = await fetch(baseZ20 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p20", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      // A genuine client error is EXPLAINED (it carries a message), so it is the
      // caller's payload that is wrong: no retry, no failover, and the router now
      // surfaces the actual status with the upstream's explanation instead of
      // masking it as a 503. Only an unexplained/opaque rejection fails over.
      const z20body = await rr.json();
      assert(
        rr.status === 422 && hits === 1,
        "z20) explained client error does NOT retry and keeps its status (hits=" + hits + ", status=" + rr.status + ")"
      );
      assert(
        /bad payload/.test(JSON.stringify(z20body)),
        "z20b) the 422 carries the upstream explanation (body=" + JSON.stringify(z20body).slice(0, 120) + ")"
      );
      assert(
        z20b.hits.length === 0 && rr.headers.get("x-router-backend") === "z20",
        "z20c) the explained client error never reached the healthy fallback (otherHits=" + z20b.hits.length + ", backend=" + rr.headers.get("x-router-backend") + ")"
      );
      // request_exhausted must NOT fire for a caller-side payload error. The
      // sibling assertion in zOpaque3 proves this sink would have caught it.
      await new Promise((r) => setTimeout(r, 250));
      assert(
        z20Ntfy.hits.length === 0,
        "z20d) an explained client error raises no exhaustion alert (alerts=" + z20Ntfy.hits.length + ")"
      );
    } finally {
      childZ20.kill();
      clientSrv.close(); z20b.srv.close(); z20Ntfy.srv.close();
      await rm(dirZ20, { recursive: true, force: true });
    }
  }

  // zOpaque1) An EMPTY-bodied 400 is indistinguishable from a payload bug, so it
  //   must never abort the whole request: the router fails over to the next
  //   provider. Motivating incident (2026-10-07): OpenCode Go answered 400 with
  //   no body and every affected session got a 503 after a single attempt.
  {
    let aHits = 0;
    const aSrv = http.createServer((req, res) => {
      aHits++;
      res.writeHead(400, { "content-type": "application/json" });
      res.end();
    });
    const aPort = await listen(aSrv);
    const bMock = mockBackend("opqB", {});
    bMock.port = await listen(bMock.srv);
    const cfgOpq = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "opqA", baseURL: `http://${HOST}:${aPort}`, apiKeyEnv: "KEY_OPQA" },
        { id: "opqB", baseURL: `http://${HOST}:${bMock.port}`, apiKeyEnv: "KEY_OPQB" },
      ],
      // affinityPool 1 degenerates to declared order, so opqA* is always tried first
      // and the failover path is actually exercised (a larger pool lets the session
      // hash start at opqB*, which passes without ever attempting a failover).
      models: { mOpq: { providers: [{ backend: "opqA", upstream: "uA" }, { backend: "opqB", upstream: "uB" }], affinityPool: 1 } },
      presets: { pOpq: { strategy: "failover", models: ["mOpq"] } },
      backoff: BO,
    };
    const h = await startRouterCfg(cfgOpq, "KEY_OPQA=k\nKEY_OPQB=k\n");
    try {
      const rr = await api(h.base, "/v1/chat/completions", { body: { model: "pOpq", messages: [] } });
      const rb = await rr.json();
      assert(
        rr.status === 200 && rb.choices && rb.choices[0].message.content === "mock:opqB",
        "zOpaque1) empty-bodied 400 fails over to a working provider (status=" + rr.status + ", body=" + JSON.stringify(rb).slice(0, 100) + ")"
      );
      assert(aHits === 1, "zOpaque1b) the opaque provider was tried exactly once (hits=" + aHits + ")");
      const hh = await (await api(h.base, "/health", { method: "GET" })).json();
      const aState = (hh.backends.find((b) => b.id === "opqA") || {}).state;
      assert(aState === "healthy", "zOpaque1c) an opaque rejection does not cool the account (state=" + aState + ")");
    } finally {
      h.child.kill(); aSrv.close(); bMock.srv.close();
      await rm(h.dir, { recursive: true, force: true });
    }
  }

  // zOpaque2) Opacity is a property of the BODY, not the status code: whitespace,
  //   an empty JSON object, and a message-less JSON error are all unexplained and
  //   must fail over rather than abort the request.
  for (const [label, opqStatus, opqBody] of [
    ["whitespace", 422, "   \n  "],
    ["empty-object", 400, "{}"],
    ["message-less-json", 400, JSON.stringify({ error: { type: "invalid_request_error" } })],
    ["terse-model-only-json", 400, JSON.stringify({ model: "some-model" })],
    ["empty-messages", 422, JSON.stringify({ messages: [] })],
  ]) {
    let aHits = 0;
    const aSrv = http.createServer((req, res) => {
      aHits++;
      res.writeHead(opqStatus, { "content-type": "application/json" });
      res.end(opqBody);
    });
    const aPort = await listen(aSrv);
    const bMock = mockBackend("opqB2", {});
    bMock.port = await listen(bMock.srv);
    const cfgO2 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "opqA2", baseURL: `http://${HOST}:${aPort}`, apiKeyEnv: "KEY_OPQA2" },
        { id: "opqB2", baseURL: `http://${HOST}:${bMock.port}`, apiKeyEnv: "KEY_OPQB2" },
      ],
      // affinityPool 1 degenerates to declared order, so opqA* is always tried first
      // and the failover path is actually exercised (a larger pool lets the session
      // hash start at opqB*, which passes without ever attempting a failover).
      models: { mO2: { providers: [{ backend: "opqA2", upstream: "uA" }, { backend: "opqB2", upstream: "uB" }], affinityPool: 1 } },
      presets: { pO2: { strategy: "failover", models: ["mO2"] } },
      backoff: BO,
    };
    const h2 = await startRouterCfg(cfgO2, "KEY_OPQA2=k\nKEY_OPQB2=k\n");
    try {
      const rr = await api(h2.base, "/v1/chat/completions", { body: { model: "pO2", messages: [] } });
      assert(
        rr.status === 200 && aHits === 1,
        "zOpaque2-" + label + ") unexplained " + opqStatus + " fails over (status=" + rr.status + ", aHits=" + aHits + ")"
      );
    } finally {
      h2.child.kill(); aSrv.close(); bMock.srv.close();
      await rm(h2.dir, { recursive: true, force: true });
    }
  }

  // zExplained1) The other half of the same contract, and the direction a
  //   regression would take: a 400/422 that DOES explain itself is the caller's
  //   payload, so it stops at the first provider with its real status. It never
  //   fails over to a healthy provider, never becomes a 503, and never retries.
  for (const [label, exStatus, exBody] of [
    ["json-string-error", 400, JSON.stringify("the payload is malformed")],
    ["error-message", 422, JSON.stringify({ error: { message: "messages[0].role is invalid" } })],
    ["top-level-message", 400, JSON.stringify({ message: "unsupported parameter: logit_bias" })],
    ["plain-text", 422, "context length exceeded for this model"],
  ]) {
    let aHits = 0;
    const aSrv = http.createServer((req, res) => {
      aHits++;
      res.writeHead(exStatus, { "content-type": "application/json" });
      res.end(exBody);
    });
    const aPort = await listen(aSrv);
    const bMock = mockBackend("exB", {});
    bMock.port = await listen(bMock.srv);
    const cfgEx = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "exA", baseURL: `http://${HOST}:${aPort}`, apiKeyEnv: "KEY_EXA" },
        { id: "exB", baseURL: `http://${HOST}:${bMock.port}`, apiKeyEnv: "KEY_EXB" },
      ],
      // affinityPool 1 pins declared order, so exA is always tried first. retry is
      // configured deliberately: an explained error must ALSO decline the
      // same-provider retry, not just the failover.
      models: { mEx: { providers: [{ backend: "exA", upstream: "uA" }, { backend: "exB", upstream: "uB" }], affinityPool: 1, retry: { maxRetries: 3, baseMs: 10, maxMs: 50 } } },
      presets: { pEx: { strategy: "affinity", models: ["mEx"] } },
      backoff: BO,
    };
    const hEx = await startRouterCfg(cfgEx, "KEY_EXA=k\nKEY_EXB=k\n");
    try {
      const rr = await api(hEx.base, "/v1/chat/completions", { body: { model: "pEx", messages: [] } });
      assert(
        rr.status === exStatus && aHits === 1 && bMock.hits.length === 0,
        "zExplained1-" + label + ") explained " + exStatus + " stops with no failover and no retry (status=" + rr.status + ", aHits=" + aHits + ", otherHits=" + bMock.hits.length + ")"
      );
    } finally {
      hEx.child.kill(); aSrv.close(); bMock.srv.close();
      await rm(hEx.dir, { recursive: true, force: true });
    }
  }

  // zOpaque3) When EVERY provider returns an unexplained rejection the request
  //   still cannot be answered, but the router must exhaust the pool (one attempt
  //   each) instead of aborting on the first one, and report it as provider
  //   exhaustion rather than a payload verdict.
  {
    let aHits = 0;
    let bHits = 0;
    const srvA = http.createServer((req, res) => { aHits++; res.writeHead(400, { "content-type": "application/json" }); res.end(); });
    const srvB = http.createServer((req, res) => { bHits++; res.writeHead(400, { "content-type": "application/json" }); res.end(); });
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    // Alert sink. Its assertion below is the CONTROL that makes z20d's "no alert"
    // meaningful: it proves the wiring delivers a request_exhausted POST at all.
    const o3Ntfy = mockBackend("o3ntfy", {});
    o3Ntfy.port = await listen(o3Ntfy.srv);
    const cfgO3 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      alerts: { ntfy: { baseUrl: `http://${HOST}:${o3Ntfy.port}`, topic: "t", cooldownMs: 0 } },
      backends: [
        { id: "opqA3", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_OPQA3" },
        { id: "opqB3", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_OPQB3" },
      ],
      // affinityPool 1 degenerates to declared order, so opqA* is always tried first
      // and the failover path is actually exercised (a larger pool lets the session
      // hash start at opqB*, which passes without ever attempting a failover).
      models: { mO3: { providers: [{ backend: "opqA3", upstream: "uA" }, { backend: "opqB3", upstream: "uB" }], affinityPool: 1 } },
      presets: { pO3: { strategy: "failover", models: ["mO3"] } },
      backoff: BO,
    };
    const h3 = await startRouterCfg(cfgO3, "KEY_OPQA3=k\nKEY_OPQB3=k\n");
    try {
      const rr = await api(h3.base, "/v1/chat/completions", { body: { model: "pO3", messages: [] } });
      const o3body = await rr.json();
      assert(
        rr.status === 503 && aHits === 1 && bHits === 1,
        "zOpaque3) all-opaque exhausts the pool once per provider and returns 503 (status=" + rr.status + ", hits=" + aHits + "/" + bHits + ")"
      );
      // The 503 must blame the unexplained rejection, not a cooling state the
      // router never entered: a regression to the old wording passes the status
      // assertion above and is caught only here.
      assert(
        /without explaining why/.test(JSON.stringify(o3body)),
        "zOpaque3b) the 503 blames the unexplained rejection, not cooling (body=" + JSON.stringify(o3body).slice(0, 150) + ")"
      );
      await new Promise((r) => setTimeout(r, 250));
      assert(
        o3Ntfy.hits.length >= 1,
        "zOpaque3c) provider exhaustion DOES raise an exhaustion alert (alerts=" + o3Ntfy.hits.length + ")"
      );
    } finally {
      h3.child.kill(); srvA.close(); srvB.close(); o3Ntfy.srv.close();
      await rm(h3.dir, { recursive: true, force: true });
    }
  }

  // zScope1) Cooldown is scoped to the PHYSICAL ROUTE, not the whole account. One
  //   model's upstream failing must not disable a DIFFERENT upstream served by the
  //   same backend: two logical models on one account are independent routes.
  {
    const hits = [];
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      hits.push(parsed.model);
      if (parsed.model === "u-bad") {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "upstream exploded" } }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:" + parsed.model } }] }));
    });
    const port = await listen(srv);
    const cfgSc1 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "ra", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_RA" }],
      models: {
        mGood: { providers: [{ backend: "ra", upstream: "u-good" }], affinityPool: 1 },
        mBad: { providers: [{ backend: "ra", upstream: "u-bad" }], affinityPool: 1 },
      },
      presets: { pGood: { strategy: "affinity", models: ["mGood"] }, pBad: { strategy: "affinity", models: ["mBad"] } },
      // A long server cooldown keeps the failure active for the whole test.
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 60000, serverMaxMs: 300000, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const hSc1 = await startRouterCfg(cfgSc1, "KEY_RA=k\n");
    try {
      await (await api(hSc1.base, "/v1/chat/completions", { body: { model: "pBad", messages: [] } })).text();
      const good = await api(hSc1.base, "/v1/chat/completions", { body: { model: "pGood", messages: [] } });
      const goodBody = await good.json();
      assert(
        good.status === 200 && goodBody.choices && goodBody.choices[0].message.content === "ok:u-good",
        "zScope1) a failing route does not disable a sibling route on the same backend (status=" + good.status + ", hits=" + JSON.stringify(hits) + ")"
      );
    } finally {
      hSc1.child.kill(); srv.close();
      await rm(hSc1.dir, { recursive: true, force: true });
    }
  }

  // zScope2) The inverse guarantee, so route scoping cannot over-isolate: two
  //   logical models resolving to the SAME physical route (backend + upstream)
  //   share one cooldown.
  {
    const hits = [];
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      hits.push(JSON.parse(Buffer.concat(chunks).toString("utf8")).model);
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "always down" } }));
    });
    const port = await listen(srv);
    const cfgSc2 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "rb", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_RB" }],
      models: {
        aliasA: { providers: [{ backend: "rb", upstream: "u-shared" }], affinityPool: 1 },
        aliasB: { providers: [{ backend: "rb", upstream: "u-shared" }], affinityPool: 1 },
      },
      presets: { pAliasA: { strategy: "affinity", models: ["aliasA"] }, pAliasB: { strategy: "affinity", models: ["aliasB"] } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 60000, serverMaxMs: 300000, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const hSc2 = await startRouterCfg(cfgSc2, "KEY_RB=k\n");
    try {
      await (await api(hSc2.base, "/v1/chat/completions", { body: { model: "pAliasA", messages: [] } })).text();
      const second = await api(hSc2.base, "/v1/chat/completions", { body: { model: "pAliasB", messages: [] } });
      const sBody = await second.text();
      assert(
        hits.length === 1 && second.status === 503,
        "zScope2) two logical models on the same physical route share one cooldown (hits=" + hits.length + ", status=" + second.status + ", body=" + sBody.slice(0, 80) + ")"
      );
    } finally {
      hSc2.child.kill(); srv.close();
      await rm(hSc2.dir, { recursive: true, force: true });
    }
  }

  // zScope3) Strict eligibility: when every candidate is cooling the router must NOT
  //   force an attempt at one anyway (the old all-cooling fallback sent the request
  //   to a known-broken provider). It admits nothing, reports backend "none", and
  //   tells the caller when to come back.
  {
    let aHits = 0;
    let bHits = 0;
    const mkDown = (bump) => http.createServer((req, res) => { bump(); res.writeHead(500, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "down" } })); });
    const srvA = mkDown(() => aHits++);
    const srvB = mkDown(() => bHits++);
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    const cfgSc3 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "sa", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_SA" },
        { id: "sb", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_SB" },
      ],
      models: { mBoth: { providers: [{ backend: "sa", upstream: "u1" }, { backend: "sb", upstream: "u1" }], affinityPool: 1 } },
      presets: { pBoth: { strategy: "affinity", models: ["mBoth"] } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 60000, serverMaxMs: 300000, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const hSc3 = await startRouterCfg(cfgSc3, "KEY_SA=k\nKEY_SB=k\n");
    try {
      await (await api(hSc3.base, "/v1/chat/completions", { body: { model: "pBoth", messages: [] } })).text();
      const firstHits = aHits + "/" + bHits;
      const second = await api(hSc3.base, "/v1/chat/completions", { body: { model: "pBoth", messages: [] } });
      const retryAfter = second.headers.get("retry-after");
      const backendHeader = second.headers.get("x-router-backend");
      assert(
        second.status === 503 && aHits === 1 && bHits === 1 && backendHeader === "none" && !!retryAfter,
        "zScope3) no attempt is forced at a cooled provider (firstHits=" + firstHits + ", after=" + aHits + "/" + bHits + ", status=" + second.status + ", backend=" + backendHeader + ", retryAfter=" + retryAfter + ")"
      );
    } finally {
      hSc3.child.kill(); srvA.close(); srvB.close();
      await rm(hSc3.dir, { recursive: true, force: true });
    }
  }

  // zScope4) An opaque rejection excludes that route for the CALLER that saw it, not
  //   for every client of the account. B returns an EXPLAINED 400 here on purpose:
  //   with a successful B the session would be PINNED to B, and a pinned session
  //   skips A whether or not any exclusion exists - so the earlier fixture passed
  //   even with exclusions broken. Terminal-and-unsuccessful B leaves the exclusion
  //   as the only thing that can keep A out.
  {
    let aHits = 0;
    let bHits = 0;
    const srvA = http.createServer((req, res) => { aHits++; res.writeHead(400, { "content-type": "application/json" }); res.end(); });
    const srvB = http.createServer((req, res) => {
      bHits++;
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "explained refusal from B" } }));
    });
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    const cfgSc4 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "oa", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_OA" },
        { id: "ob", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_OB" },
      ],
      models: { mS: { providers: [{ backend: "oa", upstream: "u1" }, { backend: "ob", upstream: "u1b" }], affinityPool: 1 } },
      presets: { pS: { strategy: "affinity", models: ["mS"] } },
      backoff: BO,
    };
    const hSc4 = await startRouterCfg(cfgSc4, "KEY_OA=k\nKEY_OB=k\n");
    try {
      const s1 = { "x-session-affinity": "scope-sess-1" };
      await (await api(hSc4.base, "/v1/chat/completions", { body: { model: "pS", messages: [] }, headers: s1 })).text();
      const afterFirst = aHits;
      await (await api(hSc4.base, "/v1/chat/completions", { body: { model: "pS", messages: [] }, headers: s1 })).text();
      const afterSecond = aHits;
      await (await api(hSc4.base, "/v1/chat/completions", { body: { model: "pS", messages: [] }, headers: { "x-session-affinity": "scope-sess-2" } })).text();
      assert(
        afterFirst === 1 && afterSecond === 1 && aHits === 2 && bHits === 3,
        "zScope4) an opaque rejection is excluded for that session only, and B's terminal 400 creates no pin that could hide the exclusion (first=" + afterFirst + ", sameSessionAgain=" + afterSecond + ", otherSession=" + aHits + ", bHits=" + bHits + ")"
      );
    } finally {
      hSc4.child.kill(); srvA.close(); srvB.close();
      await rm(hSc4.dir, { recursive: true, force: true });
    }
  }

  // zScope5) The opaque-rejection scope is caller + session + RESOLVED MODEL + route,
  //   and it exists for requests with no session id too. Three narrowings are load
  //   bearing and each was missing: without the credential, one client's blank 400
  //   blocks every other client that happens to send the same x-session-affinity;
  //   without the resolved model, a blank 400 for model A blocks model B that merely
  //   aliases the same physical route; and with no bucket at all, a no-session caller
  //   re-offers the refusing provider on every single call. Route HEALTH stays shared
  //   across models (zScope1/zScope2) - only this caller-scoped exclusion narrows.
  {
    let aHits = 0;
    const srvA = http.createServer((req, res) => { aHits++; res.writeHead(400, { "content-type": "application/json" }); res.end(); });
    const srvB = http.createServer((req, res) => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "explained refusal from B" } }));
    });
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    const cfgSc5 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "oa", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_OA" },
        { id: "ob", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_OB" },
      ],
      models: {
        mS: { providers: [{ backend: "oa", upstream: "u1" }, { backend: "ob", upstream: "u1b" }], affinityPool: 1 },
        // Same physical route as mS but a different logical model: the exclusion
        // must not leak across the two.
        mS2: { providers: [{ backend: "oa", upstream: "u1" }, { backend: "ob", upstream: "u1b" }], affinityPool: 1 },
        // One route only, so an excluded route leaves nothing eligible and the
        // 503's recovery hint becomes observable without sleeping 30 seconds.
        mOnly: { providers: [{ backend: "oa", upstream: "u1" }], affinityPool: 1 },
      },
      presets: {
        pS: { strategy: "affinity", models: ["mS"] },
        pS2: { strategy: "affinity", models: ["mS2"] },
        pOnly: { strategy: "affinity", models: ["mOnly"] },
      },
      backoff: BO,
    };
    const hSc5 = await startRouterCfg(cfgSc5, "KEY_OA=k\nKEY_OB=k\n");
    try {
      const s1 = { "x-session-affinity": "scope5-sess-1" };
      const otherKey = { "x-session-affinity": "scope5-sess-1", authorization: "Bearer another-client" };
      const call = (b, headers) => api(hSc5.base, "/v1/chat/completions", { body: b, headers }).then((x) => x.text());

      await call({ model: "pS", messages: [] }, s1);
      const after1 = aHits;
      await call({ model: "pS", messages: [] }, s1);
      const after2 = aHits;
      await call({ model: "pS", messages: [] }, otherKey);
      const after3 = aHits;
      await call({ model: "pS2", messages: [] }, s1);
      const after4 = aHits;
      await call({ model: "pS", messages: [] }, {});
      const after5 = aHits;
      await call({ model: "pS", messages: [] }, {});
      const after6 = aHits;
      await call({ model: "pS", messages: [] }, { authorization: "Bearer another-client" });
      const after7 = aHits;

      // The refused route is the ONLY candidate here, so the second call has
      // nothing eligible: the 503 must name the blocker and its finite remainder.
      await call({ model: "pOnly", messages: [] }, s1);
      const third = await api(hSc5.base, "/v1/chat/completions", { body: { model: "pOnly", messages: [] }, headers: s1 });
      const thirdBody = await third.json();
      const retryAfter = third.headers.get("retry-after");
      const thirdCode = thirdBody && thirdBody.error && thirdBody.error.code;

      assert(
        after1 === 1 && after2 === 1,
        "zScope5a) an opaque rejection is remembered for the caller/session/model that saw it (first=" + after1 + ", repeat=" + after2 + ")"
      );
      assert(
        after3 === 2 && after4 === 3,
        "zScope5b) the exclusion is scoped to the credential and to the RESOLVED model, so a different key sharing the session id and a different model aliasing the same route both still reach the refusing provider (sameSessionOtherKey=" + after3 + ", sameCredentialOtherModel=" + after4 + ")"
      );
      assert(
        after5 === 4 && after6 === 4 && after7 === 5,
        "zScope5c) requests without a session id get a client-scoped no-session bucket, so the refusing provider is not re-offered to that client and is not blacklisted for other clients (firstNoSession=" + after5 + ", repeatNoSession=" + after6 + ", otherClientNoSession=" + after7 + ")"
      );
      assert(
        third.status === 503 && thirdCode === "session_excluded" && retryAfter === "30",
        "zScope5d) the exclusion is a finite 30-second block, reported to the caller as the blocking scope rather than an account outage (status=" + third.status + ", code=" + thirdCode + ", retryAfter=" + retryAfter + ")"
      );
      const healthBody = await (await api(hSc5.base, "/health", { method: "GET" })).text();
      assert(
        healthBody.indexOf("scope5-sess-1") === -1 && healthBody.indexOf("another-client") === -1,
        "zScope5e) /health exposes route state without naming sessions or credentials"
      );
    } finally {
      hSc5.child.kill(); srvA.close(); srvB.close();
      await rm(hSc5.dir, { recursive: true, force: true });
    }
  }

  // zSess1) A session served by a fallback STAYS on it, even after the original
  //   provider becomes eligible again, and the pin is per session: another session
  //   still starts at the declared primary. Hit counts, not just answer text.
  {
    const mkSrv = (tag, st) => http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      st.hits++;
      if (st.fail > 0) {
        st.fail--;
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "boom" } }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: tag, choices: [{ message: { role: "assistant", content: "from-" + tag } }] }));
    });
    const A = { hits: 0, fail: 1 }, B = { hits: 0, fail: 0 }, C = { hits: 0, fail: 0 };
    const srvA = mkSrv("a", A), srvB = mkSrv("b", B), srvC = mkSrv("c", C);
    const pp = await Promise.all([listen(srvA), listen(srvB), listen(srvC)]);
    const cfgS = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "sa", baseURL: `http://${HOST}:${pp[0]}`, apiKeyEnv: "KEY_SA" },
        { id: "sb", baseURL: `http://${HOST}:${pp[1]}`, apiKeyEnv: "KEY_SB" },
        { id: "sc", baseURL: `http://${HOST}:${pp[2]}`, apiKeyEnv: "KEY_SC" },
      ],
      models: { mS: { providers: [{ backend: "sa", upstream: "uS" }, { backend: "sb", upstream: "uS" }, { backend: "sc", upstream: "uS" }], affinityPool: 1 } },
      presets: { pS: { strategy: "affinity", models: ["mS"] } },
      backoff: { ...BO, serverBaseMs: 150, serverMaxMs: 400 },
    };
    const hS = await startRouterCfg(cfgS, "KEY_SA=k\nKEY_SB=k\nKEY_SC=k\n");
    const turn = async (sess) => {
      const r = await api(hS.base, "/v1/chat/completions", { body: { model: "pS", messages: [] }, headers: { "x-session-affinity": sess } });
      const j = await r.json();
      return { backend: r.headers.get("x-router-backend"), content: j.choices[0].message.content };
    };
    try {
      const t1 = await turn("sess-1");
      assert(
        A.hits === 1 && B.hits === 1 && t1.backend === "sb" && t1.content === "from-b",
        "zSess1a) a failing primary fails over and the fallback answers (A=" + A.hits + ", B=" + B.hits + ", backend=" + t1.backend + ")"
      );
      await new Promise((r) => setTimeout(r, 240)); // let sa's route cooldown lapse: it is eligible again
      const t2 = await turn("sess-1");
      assert(
        t2.backend === "sb" && A.hits === 1 && B.hits === 2,
        "zSess1b) the session stays on its fallback instead of re-probing the recovered primary (backend=" + t2.backend + ", A=" + A.hits + ", B=" + B.hits + ")"
      );
      const t3 = await turn("sess-2");
      assert(
        t3.backend === "sa" && A.hits === 2,
        "zSess1c) another session still starts at the declared primary (backend=" + t3.backend + ", A=" + A.hits + ")"
      );
    } finally {
      hS.child.kill(); srvA.close(); srvB.close(); srvC.close();
      await rm(hS.dir, { recursive: true, force: true });
    }
  }

  // zSess2) When the fallback itself starts failing the session moves further down
  //   and STAYS there: it is not dragged back to a provider that is merely eligible.
  {
    const mkSrv2 = (tag, st) => http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      st.hits++;
      if (st.fail) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "boom" } }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: tag, choices: [{ message: { role: "assistant", content: "from-" + tag } }] }));
    });
    const A = { hits: 0, fail: true }, B = { hits: 0, fail: false }, C = { hits: 0, fail: false };
    const srvA = mkSrv2("a", A), srvB = mkSrv2("b", B), srvC = mkSrv2("c", C);
    const pp = await Promise.all([listen(srvA), listen(srvB), listen(srvC)]);
    const cfgS2 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "s2a", baseURL: `http://${HOST}:${pp[0]}`, apiKeyEnv: "KEY_S2A" },
        { id: "s2b", baseURL: `http://${HOST}:${pp[1]}`, apiKeyEnv: "KEY_S2B" },
        { id: "s2c", baseURL: `http://${HOST}:${pp[2]}`, apiKeyEnv: "KEY_S2C" },
      ],
      models: { mS2: { providers: [{ backend: "s2a", upstream: "u2" }, { backend: "s2b", upstream: "u2" }, { backend: "s2c", upstream: "u2" }], affinityPool: 1 } },
      presets: { pS2: { strategy: "affinity", models: ["mS2"] } },
      backoff: { ...BO, serverBaseMs: 150, serverMaxMs: 400 },
    };
    const hS2 = await startRouterCfg(cfgS2, "KEY_S2A=k\nKEY_S2B=k\nKEY_S2C=k\n");
    const turn2 = async () => {
      const r = await api(hS2.base, "/v1/chat/completions", { body: { model: "pS2", messages: [] }, headers: { "x-session-affinity": "sess-chain" } });
      await r.json();
      return r.headers.get("x-router-backend");
    };
    try {
      const b1 = await turn2(); // a fails -> b answers, and b becomes the session's provider
      assert(b1 === "s2b" && B.hits === 1, "zSess2a) the first fallback answers (backend=" + b1 + ", B=" + B.hits + ")");
      B.fail = true; // the fallback starts failing too
      const b2 = await turn2();
      assert(b2 === "s2c" && C.hits === 1, "zSess2b) a failing fallback hands the session to the next provider (backend=" + b2 + ", C=" + C.hits + ")");
      const b3 = await turn2();
      assert(b3 === "s2c" && C.hits === 2 && A.hits === 1, "zSess2c) the session stays on the provider that works (backend=" + b3 + ", C=" + C.hits + ", A=" + A.hits + ")");
    } finally {
      hS2.child.kill(); srvA.close(); srvB.close(); srvC.close();
      await rm(hS2.dir, { recursive: true, force: true });
    }
  }

  // zSess3) A 200 that carried nothing usable is an upstream failure wearing a
  //   success status: the response is delivered as-is (never replayed), the ROUTE is
  //   charged for later requests, and the account is not blamed.
  {
    let hits = 0;
    const srvE = http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      hits++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "empty", choices: [{ message: { role: "assistant", content: "" } }] }));
    });
    const goodE = mockBackend("goode", {});
    const pp = await Promise.all([listen(srvE), listen(goodE.srv)]);
    goodE.port = pp[1];
    const cfgE = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "se", baseURL: `http://${HOST}:${pp[0]}`, apiKeyEnv: "KEY_SE" },
        { id: "goode", baseURL: `http://${HOST}:${goodE.port}`, apiKeyEnv: "KEY_GOODE" },
      ],
      models: { mE: { providers: [{ backend: "se", upstream: "uE" }, { backend: "goode", upstream: "uE" }], affinityPool: 1 } },
      presets: { pE: { strategy: "affinity", models: ["mE"] } },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hE = await startRouterCfg(cfgE, "KEY_SE=k\nKEY_GOODE=k\n");
    try {
      const rr = await api(hE.base, "/v1/chat/completions", { body: { model: "pE", messages: [] }, headers: { "x-session-affinity": "sess-empty" } });
      const body = await rr.json();
      const hh = await (await api(hE.base, "/health", { method: "GET" })).json();
      const row = (hh.routes || []).find((x) => x.backend === "se" && x.upstream === "uE");
      const acct = hh.backends.find((b) => b.id === "se");
      assert(
        rr.status === 200 && body.choices[0].message.content === "" && rr.headers.get("x-router-backend") === "se",
        "zSess3a) the empty 200 is delivered unchanged rather than replayed (status=" + rr.status + ", backend=" + rr.headers.get("x-router-backend") + ")"
      );
      assert(
        !!row && row.fails >= 1 && row.state === "cooling" && acct.state === "healthy",
        "zSess3b) an unusable 200 is charged to the route, not the account (row=" + JSON.stringify(row && { fails: row.fails, state: row.state }) + ", account=" + acct.state + ")"
      );
    } finally {
      hE.child.kill(); srvE.close(); goodE.srv.close();
      await rm(hE.dir, { recursive: true, force: true });
    }
  }

  // zSess4) Preference keys and bounds, unit level: credential scope, model scope,
  //   no-session, the stale-sequence guard, no idle TTL, and the LRU cap.
  {
    SRV.clearSessionPreferences();
    assert(
      SRV.sessionPreferenceKey("scopeA", "s1", "m") !== SRV.sessionPreferenceKey("scopeB", "s1", "m"),
      "zSess4a) two credentials with the same session id do not share a preference"
    );
    assert(
      SRV.sessionPreferenceKey("scopeA", "s1", "m") !== SRV.sessionPreferenceKey("scopeA", "s1", "m2"),
      "zSess4b) the logical model is part of the key"
    );
    assert(SRV.sessionPreferenceKey("scopeA", "no-session", "m") === null, "zSess4c) no-session callers get no pin");
    assert(
      SRV.authScopeOf({ headers: { authorization: "Bearer abc" } }) !== SRV.authScopeOf({ headers: { authorization: "Bearer def" } }),
      "zSess4d) different credentials get different scopes"
    );
    assert(SRV.authScopeOf({ headers: {} }) === "anonymous", "zSess4e) no credential is its own scope");
    const tok1 = SRV.beginSessionRequest("scopeA", "s1", "m");
    const tok2 = SRV.beginSessionRequest("scopeA", "s1", "m");
    assert(tok2.sequence > tok1.sequence, "zSess4f) request sequences are monotonic");
    assert(SRV.rememberProvider(tok2, { backend: "zz-b", upstream: "u" }, { usable: true }) === true, "zSess4g) a usable completion publishes a pin");
    assert(
      SRV.rememberProvider(tok1, { backend: "zz-a", upstream: "u" }, { usable: true }) === false &&
        SRV.sessionPreferenceStats().rejectedStale >= 1,
      "zSess4h) an older response cannot overwrite a newer pin (rejectedStale=" + SRV.sessionPreferenceStats().rejectedStale + ")"
    );
    const ordered = [{ model: "m", backend: "zz-a", upstream: "u" }, { model: "m", backend: "zz-b", upstream: "u" }];
    assert(
      SRV.orderSessionProviders(ordered, tok2, Date.now() + 30 * 24 * 3600 * 1000)[0].backend === "zz-b",
      "zSess4i) a healthy pin has no idle TTL (promoted a month later)"
    );
    assert(
      SRV.orderSessionProviders([{ model: "other", backend: "zz-b", upstream: "u" }, ...ordered], tok2, Date.now())[0].model === "other",
      "zSess4j) a pin never reorders logical models"
    );
    assert(SRV.rememberProvider(tok2, { backend: "zz-a", upstream: "u" }, { usable: false }) === false, "zSess4k) an unusable completion never publishes a pin");
    for (let i = 0; i < 10001; i++) {
      SRV.rememberProvider({ key: "cap-" + i, sequence: 1000000 + i, sessionKey: "sb" }, { backend: "zz-b", upstream: "u" }, { usable: true });
    }
    const st = SRV.sessionPreferenceStats();
    assert(st.size <= st.cap && st.evictions >= 1, "zSess4l) the preference map is bounded (size=" + st.size + "/" + st.cap + ", evictions=" + st.evictions + ")");
    SRV.clearSessionPreferences();
    assert(SRV.sessionPreferenceStats().size === 0, "zSess4m) an operator action clears learned preferences");
  }

  // zStage1) WHERE a request failed decides WHAT it may charge. A connection that dies
  //   before any headers is the ENDPOINT being unreachable, which is true of every route
  //   on that account, so it must cool the account. Charging only the route leaves each
  //   sibling route to rediscover the same dead endpoint one request at a time.
  {
    let conns = 0;
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:uD" } }] }));
    });
    srv.on("connection", (sock) => { conns++; if (conns === 1) sock.destroy(); });
    const port = await listen(srv);
    const cfgSt = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "oa", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_OA" }],
      models: {
        mU: { providers: [{ backend: "oa", upstream: "u1" }], affinityPool: 1 },
        mD: { providers: [{ backend: "oa", upstream: "uD" }], affinityPool: 1 },
      },
      presets: {
        pU: { strategy: "affinity", models: ["mU"] },
        pD: { strategy: "affinity", models: ["mD"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hSt = await startRouterCfg(cfgSt, "KEY_OA=k\n");
    try {
      await (await api(hSt.base, "/v1/chat/completions", { body: { model: "pU", messages: [] } })).text();
      const after = await api(hSt.base, "/v1/chat/completions", { body: { model: "pD", messages: [] } });
      const afterBody = await after.json();
      const msg = afterBody && afterBody.error && String(afterBody.error.message);
      assert(
        after.status === 503 && /account/i.test(msg),
        "zStage1) a connection that dies before headers cools the ACCOUNT, not just the one route (status=" + after.status + ", message=" + msg + ")"
      );
    } finally {
      hSt.child.kill(); srv.close();
      await rm(hSt.dir, { recursive: true, force: true });
    }
  }

  // zStage2) The mirror image: headers and a chunk already delivered, THEN the relay dies.
  //   That is a property of the ROUTE, so the route cools and the account stays healthy,
  //   and because the caller already holds part of the response the router must NOT
  //   replay it on the model\u2019s next provider.
  {
    // Event-held cut: the MOCK does not decide when to die, the test does, so the cut is
    // ordered after the first chunk is provably in the caller's hands (no timing sleep).
    let releaseCut = null;
    const cutGate = new Promise((r) => { releaseCut = r; });
    const srvCut = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks).toString("utf8");
      if (body.includes(String.fromCharCode(34) + "stream" + String.fromCharCode(34) + ":true")) {
        res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
        res.write("data: " + JSON.stringify({ id: "m", choices: [{ delta: { content: "first-chunk" } }] }) + "\n\n");
        await cutGate;
        res.destroy();
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:uD" } }] }));
    });
    const srvAlt = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:uR2" } }] }));
    });
    const portCut = await listen(srvCut);
    const portAlt = await listen(srvAlt);
    const cfgRc = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "oa", baseURL: `http://${HOST}:${portCut}`, apiKeyEnv: "KEY_OA" },
        { id: "oa2", baseURL: `http://${HOST}:${portAlt}`, apiKeyEnv: "KEY_OA2" },
      ],
      models: {
        mR: { providers: [{ backend: "oa", upstream: "uR" }, { backend: "oa2", upstream: "uR2" }], affinityPool: 1 },
        mD: { providers: [{ backend: "oa", upstream: "uD" }], affinityPool: 1 },
      },
      presets: {
        pR: { strategy: "affinity", models: ["mR"] },
        pD: { strategy: "affinity", models: ["mD"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hRc = await startRouterCfg(cfgRc, "KEY_OA=k\nKEY_OA2=k\n");
    try {
      // Read incrementally: a cut stream aborts the client connection, so a single
      // await text() would throw and discard the bytes that DID arrive before the cut.
      let text = "";
      let readErr = null;
      try {
        const rr = await fetch(hRc.base + "/v1/chat/completions", {
          method: "POST",
          headers: { "content-type": "application/json", authorization: "Bearer local-master" },
          body: JSON.stringify({ model: "pR", messages: [], stream: true }),
          signal: AbortSignal.timeout(10000),
        });
        const reader = rr.body.getReader();
        const dec = new TextDecoder();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          text += dec.decode(value, { stream: true });
          if (releaseCut && text.includes("first-chunk")) { releaseCut(); releaseCut = null; }
        }
      } catch (e) { readErr = String(e && e.message); }
      const after = await api(hRc.base, "/v1/chat/completions", { body: { model: "pD", messages: [] } });
      const afterBody = await after.json();
      // The cut must be CHARGED to the route. A one-way "the caller left" latch would excuse it
      // as a cancellation and charge nothing, which is how a genuinely dying relay keeps getting
      // picked: the route has to be the thing that cools.
      const hhR = await (await api(hRc.base, "/health", { method: "GET" })).json();
      const cutRow = (hhR.routes || []).find((x) => x.backend === "oa" && x.upstream === "uR");
      assert(
        text.includes("first-chunk") && !text.includes("ok:uR2") &&
          after.status === 200 && afterBody.choices && afterBody.choices[0].message.content === "ok:uD" &&
          !!cutRow && cutRow.state === "cooling",
        "zStage2) a relay cut after delivered chunks cools only that route, never replays it on the next provider, and leaves the account serving (delivered=" + text.includes("first-chunk") + ", replayed=" + text.includes("ok:uR2") + ", siblingStatus=" + after.status + ", readErr=" + readErr + ", routeState=" + (cutRow ? cutRow.state : "none") + ")"
      );
    } finally {
      hRc.child.kill(); srvCut.close(); srvAlt.close();
      await rm(hRc.dir, { recursive: true, force: true });
    }
  }

  // zCancel1) A caller who leaves BEFORE any headers is the case nothing observed: the router
  //   kept waiting on an upstream request for a client that was already gone, and when that
  //   upstream finally failed the failure was charged to a healthy provider. The upstream
  //   request must be released at the socket, and no failover attempted, because there is
  //   nobody left to serve. Held on events, not sleeps: the test waits until the upstream
  //   provably holds the request before walking away.
  {
    let aGot = null;
    const aGotRequest = new Promise((r) => { aGot = r; });
    let aClosed = null;
    const aClosedUpstream = new Promise((r) => { aClosed = r; });
    let bHits = 0;
    const srvA = http.createServer((req, res) => {
      aGot();
      res.on("close", () => aClosed());
      req.on("aborted", () => aClosed());
      // never respond: the router is waiting for headers when the caller walks away
    });
    const srvB = http.createServer(async (req, res) => {
      bHits++;
      for await (const c of req) { /* drain */ }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:cb" } }] }));
    });
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    const cfgCn = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "ca", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_CA" },
        { id: "cb", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_CB" },
      ],
      models: { mCn: { providers: [{ backend: "ca", upstream: "ua" }, { backend: "cb", upstream: "ub" }], affinityPool: 1 } },
      presets: { pCn: { strategy: "affinity", models: ["mCn"] } },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hCn = await startRouterCfg(cfgCn, "KEY_CA=k\nKEY_CB=k\n");
    try {
      const ac = new AbortController();
      const inflight = fetch(hCn.base + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "pCn", messages: [] }),
        signal: ac.signal,
      });
      inflight.catch(() => {}); // the caller's own abort rejects; that is the point
      await aGotRequest;        // the upstream provably holds the request
      ac.abort();               // now the caller walks away, before any header
      let upstreamReleased = false;
      await Promise.race([aClosedUpstream.then(() => { upstreamReleased = true; }), new Promise((r) => setTimeout(r, 5000))]);
      const hh = await (await api(hCn.base, "/health", { method: "GET" })).json();
      const acct = hh.backends.find((x) => x.id === "ca");
      const rows = (hh.routes || []).filter((x) => x.backend === "ca");
      assert(
        upstreamReleased && bHits === 0 && acct && acct.state === "healthy" && rows.length === 0,
        "zCancel1) a caller who leaves before any headers releases the upstream request, fails over nowhere, and charges no health (upstreamReleased=" + upstreamReleased + ", bHits=" + bHits + ", state=" + (acct ? acct.state : "?") + ", routes=" + JSON.stringify(rows) + ")"
      );
    } finally {
      hCn.child.kill(); srvA.close(); srvB.close();
      await rm(hCn.dir, { recursive: true, force: true });
    }
  }

  // zAdmit1) Probe admission is owned by the LEASE, not by "I hold some probe somewhere".
  //   A lease may dispatch through the probes IT holds at the generation it claimed; any
  //   other probe - another request's, or a newer one on the same row - still blocks it.
  {
    const att = { model: "mAdm", backend: "adm-oa", upstream: "adm-u1", dispatchSeq: null };
    const rk = SRV.routeKey(att);
    const now = Date.now();
    const prevH = SRV.health.get("adm-oa");
    const prevR = SRV.ROUTES.get(rk);
    const row = (generation, probeInFlight) => ({ generation, fails: 2, nextAvailableAt: now - 1000, probeInFlight, lastErrorKind: "server", lastStatus: 503, lastError: null });
    try {
      SRV.health.set("adm-oa", { state: "cooling", nextAvailableAt: now - 1000, fails: 2, backoffMs: 0, generation: 900, probeInFlight: true, lastError: null });
      SRV.ROUTES.set(rk, row(901, true));
      const mine = { routeKey: rk, backend: "adm-oa", routeGeneration: 901, routeClaim: true, accountGeneration: 900, accountClaim: true };
      const whyMine = SRV.ineligibility(att, "scope-adm", now, mine);
      assert(
        whyMine === null,
        "zAdmit1a) a lease is admitted through the probes it holds at the generation it claimed (why=" + whyMine + ")"
      );
      // The same rows, a NEWER probe: ownership is gone, so the stale lease must be blocked.
      SRV.health.set("adm-oa", { state: "cooling", nextAvailableAt: now - 1000, fails: 2, backoffMs: 0, generation: 950, probeInFlight: true, lastError: null });
      const whyStale = SRV.ineligibility(att, "scope-adm", now, mine);
      assert(
        whyStale === "probe_in_flight",
        "zAdmit1b) a probe newer than the one the lease claimed still blocks it (why=" + whyStale + ")"
      );
      // The plan's explicit requirement: owning the ROUTE probe is not permission to enter a
      // cooling account that some OTHER request is already probing.
      SRV.health.set("adm-oa", { state: "cooling", nextAvailableAt: now - 1000, fails: 2, backoffMs: 0, generation: 900, probeInFlight: true, lastError: null });
      SRV.ROUTES.set(rk, row(901, true));
      const routeOnly = { routeKey: rk, backend: "adm-oa", routeGeneration: 901, routeClaim: true, accountGeneration: null, accountClaim: false };
      const whyRouteOnly = SRV.ineligibility(att, "scope-adm", now, routeOnly);
      assert(
        whyRouteOnly === "probe_in_flight",
        "zAdmit1c) a foreign ACCOUNT probe blocks an attempt that owns only the ROUTE probe (why=" + whyRouteOnly + ")"
      );
      // The same rule the other way round: owning the ACCOUNT probe is not permission to enter
      // a route another request is already probing.
      SRV.health.set("adm-oa", { state: "healthy", nextAvailableAt: 0, fails: 0, backoffMs: 0, generation: 900, probeInFlight: false, lastError: null });
      SRV.ROUTES.set(rk, row(901, true));
      const accountOnly = { routeKey: rk, backend: "adm-oa", routeGeneration: null, routeClaim: false, accountGeneration: 900, accountClaim: true };
      const whyAccountOnly = SRV.ineligibility(att, "scope-adm", now, accountOnly);
      assert(
        whyAccountOnly === "probe_in_flight",
        "zAdmit1d) a foreign ROUTE probe blocks an attempt that owns only the ACCOUNT probe (why=" + whyAccountOnly + ")"
      );
      // Ownership is bound to the generation that was claimed: a lease whose route booking has
      // been superseded must not carry a request into a probe that is no longer its own.
      const staleRoute = { routeKey: rk, backend: "adm-oa", routeGeneration: 900, routeClaim: true, accountGeneration: null, accountClaim: false };
      const whyStaleRoute = SRV.ineligibility(att, "scope-adm", now, staleRoute);
      assert(
        whyStaleRoute === "probe_in_flight",
        "zAdmit1e) a lease whose claimed route generation is stale is still blocked (why=" + whyStaleRoute + ")"
      );
    } finally {
      if (prevH === undefined) SRV.health.delete("adm-oa"); else SRV.health.set("adm-oa", prevH);
      if (prevR === undefined) SRV.ROUTES.delete(rk); else SRV.ROUTES.set(rk, prevR);
    }
  }

  // zAdmit2) The lease lifecycle: a refused route claim hands the account probe back, a stale
  //   release leaves newer state alone, and a lease can only ever be released ONCE.
  {
    const att = () => ({ model: "mAdm", backend: "adm-oa", upstream: "adm-u1", dispatchSeq: null });
    const rk = SRV.routeKey(att());
    const now = Date.now();
    const prevH = SRV.health.get("adm-oa");
    const prevR = SRV.ROUTES.get(rk);
    try {
      // Partial claim: the account probe is taken, then the route probe is refused. Leaving
      // the account probe set would lock the account out of recovery for good.
      SRV.health.set("adm-oa", { state: "cooling", nextAvailableAt: now - 1000, fails: 2, backoffMs: 0, generation: 900, probeInFlight: false, lastError: null });
      SRV.ROUTES.set(rk, { generation: 901, fails: 2, nextAvailableAt: now - 1000, probeInFlight: true, lastErrorKind: "server", lastStatus: 503, lastError: null });
      const refused = SRV.claimAttempt(att(), now);
      assert(
        refused === null && SRV.health.get("adm-oa").probeInFlight === false,
        "zAdmit2a) a refused route claim rolls the account claim back (token=" + JSON.stringify(refused) + ", accountProbe=" + SRV.health.get("adm-oa").probeInFlight + ")"
      );
      // A release whose generation no longer matches must not clear the newer probe.
      SRV.ROUTES.set(rk, { generation: 902, fails: 2, nextAvailableAt: now - 1000, probeInFlight: true, lastErrorKind: "server", lastStatus: 503, lastError: null });
      const staleToken = { routeKey: rk, backend: "adm-oa", routeGeneration: 901, routeClaim: true, accountGeneration: null, accountClaim: false };
      SRV.releaseAttempt(staleToken);
      assert(
        SRV.ROUTES.get(rk).probeInFlight === true,
        "zAdmit2b) a generation-stale release does not free a probe it no longer owns (probeInFlight=" + SRV.ROUTES.get(rk).probeInFlight + ")"
      );
      // Claiming a probe does NOT move the row generation, so generation checking alone cannot
      // make release idempotent: a repeated release of the same token would free the probe that
      // a SECOND request has since claimed, putting two probes on one route.
      SRV.ROUTES.set(rk, { generation: 903, fails: 2, nextAvailableAt: now - 1000, probeInFlight: true, lastErrorKind: "server", lastStatus: 503, lastError: null });
      const token = { routeKey: rk, backend: "adm-oa", routeGeneration: 903, routeClaim: true, accountGeneration: null, accountClaim: false };
      SRV.releaseAttempt(token);
      const freedByOwner = SRV.ROUTES.get(rk).probeInFlight === false;
      SRV.ROUTES.get(rk).probeInFlight = true; // a later request claims the probe, same generation
      SRV.releaseAttempt(token);             // the same token releases a second time
      assert(
        freedByOwner && SRV.ROUTES.get(rk).probeInFlight === true,
        "zAdmit2c) releasing the same lease twice leaves a later claim’s probe alone (freedByOwner=" + freedByOwner + ", afterSecond=" + SRV.ROUTES.get(rk).probeInFlight + ")"
      );
    } finally {
      if (prevH === undefined) SRV.health.delete("adm-oa"); else SRV.health.set("adm-oa", prevH);
      if (prevR === undefined) SRV.ROUTES.delete(rk); else SRV.ROUTES.set(rk, prevR);
    }
  }

  // zAdmit3) One probe at a time, held on events rather than on timing. Two DIFFERENT sessions
  //   are used so session affinity or a published pin cannot be what keeps the second request
  //   away: while the first request is inside the route probe, the second must be refused and
  //   must never reach the upstream at all.
  {
    let hits = 0;
    let releaseHang = null;
    const hangGate = new Promise((r) => { releaseHang = r; });
    let probeArrived = null;
    const probeInFlight = new Promise((r) => { probeArrived = r; });
    const srvAdm = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks).toString("utf8");
      hits++;
      if (body.includes("priming-outage")) {
        // The priming request is what CREATES the route cooldown the probe then recovers.
        res.writeHead(503, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "priming outage" } }));
        return;
      }
      probeArrived();
      await hangGate; // hold the probe open until the test says so
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:adm" } }] }));
    });
    const portAdm = await listen(srvAdm);
    const cfgAdm = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "adm2", baseURL: `http://${HOST}:${portAdm}`, apiKeyEnv: "KEY_ADM2" }],
      models: { mAdm2: { providers: [{ backend: "adm2", upstream: "uAdm" }], affinityPool: 1 } },
      presets: { pAdm2: { strategy: "affinity", models: ["mAdm2"] } },
      backoff: { ...BO, serverBaseMs: 40, serverMaxMs: 120 },
    };
    const hAdm = await startRouterCfg(cfgAdm, "KEY_ADM2=k\n");
    const call = (session, text) => fetch(hAdm.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master", "x-session-affinity": session },
      body: JSON.stringify({ model: "pAdm2", messages: [{ role: "user", content: text }] }),
      signal: AbortSignal.timeout(15000),
    });
    try {
      const primed = await call("admPrime", "priming-outage");
      await primed.text();
      // Readiness by observation, not by sleeping: wait until the recorded cooldown has elapsed.
      let row = null;
      for (let i = 0; i < 200; i++) {
        const hh = await (await api(hAdm.base, "/health", { method: "GET" })).json();
        row = (hh.routes || []).find((x) => x.backend === "adm2");
        if (row && row.fails > 0 && row.retryInMs <= 0) break;
        await new Promise((r) => setTimeout(r, 10));
      }
      const probe = call("admA", "hold it open");
      probe.catch(() => {});
      // Event-held, but BOUNDED: a mutant that refuses the probe must fail this assertion
      // rather than leave the suite waiting on upstream traffic that never happens.
      const probeLanded = await Promise.race([
        probeInFlight.then(() => true),
        new Promise((r) => setTimeout(() => r(false), 10000)),
      ]);
      const refused = await call("admB", "someone else wants in");
      const refusedStatus = refused.status;
      await refused.text();
      const hitsAtRefusal = hits;
      releaseHang();
      const probeStatus = probeLanded ? (await probe).status : 0;
      assert(
        probeLanded && row !== null && row.fails > 0 && probeStatus === 200 && refusedStatus === 503 && hitsAtRefusal === 2,
        "zAdmit3) while one request holds the route probe, another session is refused without touching the upstream (probeLanded=" + probeLanded + ", row=" + JSON.stringify(row) + ", probeStatus=" + probeStatus + ", refusedStatus=" + refusedStatus + ", hits=" + hitsAtRefusal + ")"
      );
    } finally {
      releaseHang && releaseHang();
      hAdm.child.kill(); srvAdm.close();
      await rm(hAdm.dir, { recursive: true, force: true });
    }
  }

  // zOutage1) Distinct-route escalation is EVIDENCE-FENCED. A success retires the
  //   account's outage evidence only when it is current: a slow response that started
  //   before a later failure proves nothing about the state that failure recorded, and
  //   accepting it would silently abolish the very escalation it was meant to keep.
  {
    const cfgOut = { backoff: BO, backends: [{ id: "zz-outage" }] };
    // Dispatch sequences sit far above this process's own generation counter so the
    // EVIDENCE fence - not the row-ownership guard - is what decides both cases.
    const seq = 1e12;
    SRV.ACCOUNT_OUTAGE_ROUTES.clear();
    SRV.noteAccountOutage(cfgOut, { backend: "zz-outage", upstream: "u1", dispatchSeq: seq });
    SRV.noteAccountOutage(cfgOut, { backend: "zz-outage", upstream: "u2", dispatchSeq: seq + 1 });
    const afterFails = (SRV.ACCOUNT_OUTAGE_ROUTES.get("zz-outage") || []).length;
    SRV.markSuccess("zz-outage", { backend: "zz-outage", upstream: "u1", dispatchSeq: seq - 1 });
    const afterStaleSuccess = (SRV.ACCOUNT_OUTAGE_ROUTES.get("zz-outage") || []).length;
    SRV.markSuccess("zz-outage", { backend: "zz-outage", upstream: "u1", dispatchSeq: seq + 5 });
    const afterFreshSuccess = (SRV.ACCOUNT_OUTAGE_ROUTES.get("zz-outage") || []).length;
    SRV.ACCOUNT_OUTAGE_ROUTES.clear();
    assert(
      afterFails === 2 && afterStaleSuccess === 2 && afterFreshSuccess === 0,
      "zOutage1) an account success retires outage evidence only when no newer failure is retained (afterFails=" + afterFails + ", afterStaleSuccess=" + afterStaleSuccess + ", afterFreshSuccess=" + afterFreshSuccess + ")"
    );
  }

  // zOutage2) A 200 with nothing usable in it is a ROUTE fault, not an account fault.
  //   The relay really is unfit and its route cools, but two such routes say nothing
  //   about the account, so the account must stay eligible and a third route on it must
  //   still serve. Folding status 0 into the distinct-route heuristic declares an
  //   account-wide outage on evidence that never described the account.
  {
    let un1 = 0;
    let un2 = 0;
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const ch of req) chunks.push(ch);
      const up = JSON.parse(Buffer.concat(chunks).toString("utf8")).model;
      if (up === "u-good") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:u-good" } }] }));
        return;
      }
      if (up === "u1") un1++;
      if (up === "u2") un2++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end("");
    });
    const port = await listen(srv);
    const cfgUn = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "oa", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_OA" }],
      models: {
        mUn1: { providers: [{ backend: "oa", upstream: "u1" }], affinityPool: 1 },
        mUn2: { providers: [{ backend: "oa", upstream: "u2" }], affinityPool: 1 },
        mGood: { providers: [{ backend: "oa", upstream: "u-good" }], affinityPool: 1 },
      },
      presets: {
        pUn1: { strategy: "affinity", models: ["mUn1"] },
        pUn2: { strategy: "affinity", models: ["mUn2"] },
        pGood: { strategy: "affinity", models: ["mGood"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hUn = await startRouterCfg(cfgUn, "KEY_OA=k\n");
    try {
      await (await api(hUn.base, "/v1/chat/completions", { body: { model: "pUn1", messages: [] } })).text();
      await (await api(hUn.base, "/v1/chat/completions", { body: { model: "pUn2", messages: [] } })).text();
      const good = await api(hUn.base, "/v1/chat/completions", { body: { model: "pGood", messages: [] } });
      const goodBody = await good.json();
      assert(
        good.status === 200 && goodBody.choices && goodBody.choices[0].message.content === "ok:u-good" && un1 === 1 && un2 === 1,
        "zOutage2) unusable 200s cool their own routes without declaring an account outage (u1=" + un1 + ", u2=" + un2 + ", status=" + good.status + ", body=" + JSON.stringify(goodBody).slice(0, 120) + ")"
      );
    } finally {
      hUn.child.kill(); srv.close();
      await rm(hUn.dir, { recursive: true, force: true });
    }
  }

  // zOutage3a) One failing route is a ROUTE problem: the rest of the account keeps
  //   serving, so a single 5xx must not touch account health.
  {
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const ch of req) chunks.push(ch);
      const up = JSON.parse(Buffer.concat(chunks).toString("utf8")).model;
      if (up === "uD") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:uD" } }] }));
        return;
      }
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "upstream exploded" } }));
    });
    const port = await listen(srv);
    const cfgA1 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "oa", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_OA" }],
      models: {
        mA: { providers: [{ backend: "oa", upstream: "uA" }], affinityPool: 1 },
        mD: { providers: [{ backend: "oa", upstream: "uD" }], affinityPool: 1 },
      },
      presets: {
        pA: { strategy: "affinity", models: ["mA"] },
        pD: { strategy: "affinity", models: ["mD"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hA1 = await startRouterCfg(cfgA1, "KEY_OA=k\n");
    try {
      await (await api(hA1.base, "/v1/chat/completions", { body: { model: "pA", messages: [] } })).text();
      const oneRoute = await api(hA1.base, "/v1/chat/completions", { body: { model: "pD", messages: [] } });
      const oneRouteBody = await oneRoute.json();
      assert(
        oneRoute.status === 200 && oneRouteBody.choices && oneRouteBody.choices[0].message.content === "ok:uD",
        "zOutage3a) one failing route leaves the rest of its account usable (status=" + oneRoute.status + ", body=" + JSON.stringify(oneRouteBody).slice(0, 120) + ")"
      );
    } finally {
      hA1.child.kill(); srv.close();
      await rm(hA1.dir, { recursive: true, force: true });
    }
  }

  // zOutage3b) The escalation itself must survive this fix: TWO DIFFERENT 5xx routes of
  //   one account cool the ACCOUNT. No successful call may intervene - a usable completion
  //   retires the evidence by design (zOutage4), so the two failures are consecutive here.
  {
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const ch of req) chunks.push(ch);
      const up = JSON.parse(Buffer.concat(chunks).toString("utf8")).model;
      if (up === "uD") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:uD" } }] }));
        return;
      }
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "upstream exploded" } }));
    });
    const port = await listen(srv);
    const cfgA2 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "oa", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_OA" }],
      models: {
        mA: { providers: [{ backend: "oa", upstream: "uA" }], affinityPool: 1 },
        mB: { providers: [{ backend: "oa", upstream: "uB" }], affinityPool: 1 },
        mD: { providers: [{ backend: "oa", upstream: "uD" }], affinityPool: 1 },
      },
      presets: {
        pA: { strategy: "affinity", models: ["mA"] },
        pB: { strategy: "affinity", models: ["mB"] },
        pD: { strategy: "affinity", models: ["mD"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hA2 = await startRouterCfg(cfgA2, "KEY_OA=k\n");
    try {
      await (await api(hA2.base, "/v1/chat/completions", { body: { model: "pA", messages: [] } })).text();
      await (await api(hA2.base, "/v1/chat/completions", { body: { model: "pB", messages: [] } })).text();
      const escalated = await api(hA2.base, "/v1/chat/completions", { body: { model: "pD", messages: [] } });
      const escalatedBody = await escalated.json();
      const escMsg = escalatedBody && escalatedBody.error && String(escalatedBody.error.message);
      assert(
        escalated.status === 503 && /account/i.test(escMsg),
        "zOutage3b) two different failing routes of one account escalate to an account outage (status=" + escalated.status + ", message=" + escMsg + ")"
      );
    } finally {
      hA2.child.kill(); srv.close();
      await rm(hA2.dir, { recursive: true, force: true });
    }
  }

  // zOutage4) A usable completion on a SIBLING route retires the outage evidence, so a
  //   later single failure has to start the count again. Without it, one old failure
  //   kept every later unrelated failure one step from escalating.
  {
    const srv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const ch of req) chunks.push(ch);
      const up = JSON.parse(Buffer.concat(chunks).toString("utf8")).model;
      if (up === "uB" || up === "uD") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:" + up } }] }));
        return;
      }
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "upstream exploded" } }));
    });
    const port = await listen(srv);
    const cfgWin = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "oa", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_OA" }],
      models: {
        mA: { providers: [{ backend: "oa", upstream: "uA" }], affinityPool: 1 },
        mB: { providers: [{ backend: "oa", upstream: "uB" }], affinityPool: 1 },
        mC: { providers: [{ backend: "oa", upstream: "uC" }], affinityPool: 1 },
        mD: { providers: [{ backend: "oa", upstream: "uD" }], affinityPool: 1 },
      },
      presets: {
        pA: { strategy: "affinity", models: ["mA"] },
        pB: { strategy: "affinity", models: ["mB"] },
        pC: { strategy: "affinity", models: ["mC"] },
        pD: { strategy: "affinity", models: ["mD"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hWin = await startRouterCfg(cfgWin, "KEY_OA=k\n");
    try {
      await (await api(hWin.base, "/v1/chat/completions", { body: { model: "pA", messages: [] } })).text();
      const sibling = await api(hWin.base, "/v1/chat/completions", { body: { model: "pB", messages: [] } });
      const siblingBody = await sibling.json();
      await (await api(hWin.base, "/v1/chat/completions", { body: { model: "pC", messages: [] } })).text();
      const after = await api(hWin.base, "/v1/chat/completions", { body: { model: "pD", messages: [] } });
      const afterBody = await after.json();
      assert(
        sibling.status === 200 && siblingBody.choices && siblingBody.choices[0].message.content === "ok:uB" &&
          after.status === 200 && afterBody.choices && afterBody.choices[0].message.content === "ok:uD",
        "zOutage4) a usable completion on a sibling route retires the outage evidence, so the next single failure does not escalate (sibling=" + sibling.status + ", after=" + after.status + ", body=" + JSON.stringify(afterBody).slice(0, 120) + ")"
      );
    } finally {
      hWin.child.kill(); srv.close();
      await rm(hWin.dir, { recursive: true, force: true });
    }
  }

  // zAlias1) The pin is keyed by the RESOLVED logical model, not by the id the caller
  //   asked for. A preset, another preset expanding to the same model, and the bare
  //   model id are three names for ONE routing decision: keyed by the requested id, a
  //   session that abandoned a failing provider through one alias walks straight back
  //   to it through the next. oa's route cooldown is deliberately short (BO) and each
  //   alias call waits it out, so every call really is free to choose oa again - the
  //   fresh session at the end proves that by still reaching oa.
  {
    const wait = (ms) => new Promise((r) => setTimeout(r, ms));
    let aHits = 0;
    let bHits = 0;
    const srvA = http.createServer((req, res) => { aHits++; res.writeHead(500, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "down" } })); });
    const srvB = http.createServer((req, res) => { bHits++; res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:ob" } }] })); });
    const portA = await listen(srvA);
    const portB = await listen(srvB);
    const cfgAl = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "oa", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_OA" },
        { id: "ob", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_OB" },
      ],
      models: { m: { providers: [{ backend: "oa", upstream: "u1" }, { backend: "ob", upstream: "u1b" }], affinityPool: 1 } },
      presets: {
        p1: { strategy: "affinity", models: ["m"] },
        p2: { strategy: "affinity", models: ["m"] },
      },
      backoff: BO,
    };
    const hAl = await startRouterCfg(cfgAl, "KEY_OA=k\nKEY_OB=k\n");
    try {
      const s = { "x-session-affinity": "alias-sess" };
      const ask = (model, headers) => api(hAl.base, "/v1/chat/completions", { body: { model, messages: [] }, headers });
      const c1 = await (await ask("p1", s)).json();
      const after1 = aHits;
      await wait(300); // outlast oa's route cooldown, so the pin is the only thing keeping us off it
      const c2 = await (await ask("p2", s)).json();
      const after2 = aHits;
      await wait(300);
      const c3 = await (await ask("m", s)).json();
      const after3 = aHits;
      await wait(300);
      const c4 = await (await ask("m", { "x-session-affinity": "alias-sess-fresh" })).json();
      const after4 = aHits;
      assert(
        after1 === 1 && after2 === 1 && after3 === 1 && after4 === 2 && bHits === 4 &&
          c1.choices && c2.choices && c3.choices && c4.choices &&
          c2.choices[0].message.content === "ok:ob" && c3.choices[0].message.content === "ok:ob",
        "zAlias1) one resolved model means one pin, shared by every name that resolves to it: after p1 failed over to ob, the other preset and the bare model id stay on ob until a fresh session proves oa is eligible again (p1=" + after1 + ", aliasPreset=" + after2 + ", bareModel=" + after3 + ", freshSession=" + after4 + ", bHits=" + bHits + ")"
      );
    } finally {
      hAl.child.kill(); srvA.close(); srvB.close();
      await rm(hAl.dir, { recursive: true, force: true });
    }
  }

  // zSess5) A caller that hangs up mid-stream is NOT an upstream outage: the route is
  //   not cooled and no preference is published, so the provider keeps serving others.
  {
    let hits = 0;
    const srvC = http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      hits++;
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write("data: " + JSON.stringify({ id: "cancel", choices: [{ delta: { content: "hello" } }] }) + "\n\n");
      // then hold forever: the caller walks away
    });
    const portC = await listen(srvC);
    const cfgC = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "sc5", baseURL: `http://${HOST}:${portC}`, apiKeyEnv: "KEY_SC5", idleTimeoutMs: 60000 }],
      models: { mC5: { providers: [{ backend: "sc5", upstream: "u5" }], affinityPool: 1 } },
      presets: { pC5: { strategy: "affinity", models: ["mC5"] } },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hC = await startRouterCfg(cfgC, "KEY_SC5=k\n");
    try {
      const ac = new AbortController();
      const rr = await fetch(hC.base + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "pC5", messages: [], stream: true }),
        signal: ac.signal,
      });
      const reader = rr.body.getReader();
      await reader.read(); // the first chunk arrives
      ac.abort(); // the caller hangs up
      await new Promise((r) => setTimeout(r, 500));
      const hh = await (await api(hC.base, "/health", { method: "GET" })).json();
      const b = hh.backends.find((x) => x.id === "sc5");
      const rows = (hh.routes || []).filter((x) => x.backend === "sc5");
      assert(
        hits === 1 && b.state === "healthy" && rows.length === 0,
        "zSess5) a caller abort is not charged to the provider (hits=" + hits + ", state=" + b.state + ", routes=" + JSON.stringify(rows) + ")"
      );
    } finally {
      hC.child.kill(); srvC.close();
      await rm(hC.dir, { recursive: true, force: true });
    }
  }

  // zT4a) Bounded error-body reads, unit level: every state, the exact-limit
  //   boundary, multichunk truncation, and a multi-byte character split by a chunk.
  {
    const mkRes = (chunks, mode) => ({
      body: {
        getReader() {
          let i = 0;
          return {
            read: async () => {
              if (mode === "reset") throw new Error("ECONNRESET");
              if (mode === "hang") return new Promise(() => {});
              if (i < chunks.length) return { done: false, value: Buffer.from(chunks[i++]) };
              return { done: true };
            },
            cancel: async () => {},
          };
        },
      },
    });
    const text = '{"error":{"message":"nope"}}';
    const present = await SRV.readUpstreamError(mkRes([text]));
    assert(
      present.errorBodyState === "present" && present.errorBodyBytes === Buffer.byteLength(text) && present.body === text,
      "zT4a1) a complete error body reads as present with its captured byte count (state=" + present.errorBodyState + ", bytes=" + present.errorBodyBytes + ")"
    );
    const empty = await SRV.readUpstreamError(mkRes([]));
    assert(empty.errorBodyState === "empty" && empty.body === "" && empty.errorBodyBytes === 0, "zT4a2) a never-written body is empty, not a failure (state=" + empty.errorBodyState + ")");
    const noBody = await SRV.readUpstreamError({ body: null });
    assert(noBody.errorBodyState === "empty", "zT4a3) a body-less response is empty (state=" + noBody.errorBodyState + ")");
    const exact = await SRV.readUpstreamError(mkRes(["a".repeat(16384)]), { maxBytes: 16384 });
    assert(exact.errorBodyState === "present" && exact.errorBodyBytes === 16384, "zT4a4) a body exactly at the limit is present, not truncated (state=" + exact.errorBodyState + ", bytes=" + exact.errorBodyBytes + ")");
    const over = await SRV.readUpstreamError(mkRes(["b".repeat(8192), "b".repeat(8192), "b".repeat(4096)]), { maxBytes: 16384 });
    assert(over.errorBodyState === "truncated" && over.errorBodyBytes === 16384, "zT4a5) a multichunk body past the limit is truncated AT the limit (state=" + over.errorBodyState + ", bytes=" + over.errorBodyBytes + ")");
    const multi = Buffer.from("h\u00e9llo w\u00f6rld");
    const split = await SRV.readUpstreamError(mkRes([multi.subarray(0, 2), multi.subarray(2)]));
    assert(split.body === "h\u00e9llo w\u00f6rld", "zT4a6) a multi-byte character split across chunks survives the read (body=" + JSON.stringify(split.body) + ")");
    const t0 = Date.now();
    const hung = await SRV.readUpstreamError(mkRes([], "hang"), { timeoutMs: 80 });
    assert(hung.errorBodyState === "timeout" && Date.now() - t0 < 2000, "zT4a7) a body that never ends hits the deadline instead of holding the request open (state=" + hung.errorBodyState + ", ms=" + (Date.now() - t0) + ")");
    const reset = await SRV.readUpstreamError(mkRes([], "reset"));
    assert(reset.errorBodyState === "read_error", "zT4a8) a reset socket is a read_error, never an empty body (state=" + reset.errorBodyState + ")");
  }

  // zT4b) Redaction and the one terminal envelope, unit level.
  {
    process.env.T4_SECRET_KEY = "sk-t4-sentinel-0123456789abcdef";
    process.env.T4_MASTER_KEY = "master-t4-sentinel-0123456789";
    process.env.T4_SHORT_KEY = "k";
    const cfgT = { backends: [{ id: "x", apiKeyEnv: "T4_SECRET_KEY" }, { id: "y", apiKeyEnv: "T4_SHORT_KEY" }], masterKeyEnv: "T4_MASTER_KEY" };
    const reqT = { headers: { authorization: "Bearer sk-t4-sentinel-0123456789abcdef" } };
    const red = SRV.redactSecrets("leak sk-t4-sentinel-0123456789abcdef and master-t4-sentinel-0123456789", cfgT, reqT);
    assert(
      !/sentinel/.test(red) && (red.match(/\[redacted\]/g) || []).length === 2,
      "zT4b1) configured credential values never survive redaction (out=" + JSON.stringify(red) + ")"
    );
    assert(SRV.redactSecrets("k", cfgT, null) === "k", "zT4b2) a too-short value is left alone: redacting it would mangle ordinary text");
    const env = JSON.parse(SRV.terminalChatError({ message: "", type: "router_unavailable", callId: "c1", backend: "b1", upstreamStatus: 503, nextAvailableAt: 123, cfg: cfgT, req: reqT }));
    assert(
      typeof env.error.message === "string" && env.error.message.length > 0 && env.error.type === "router_unavailable" && env.error.callId === "c1" && env.error.backend === "b1" && env.error.upstreamStatus === 503 && env.error.nextAvailableAt === 123,
      "zT4b3) a terminal chat error is always nonempty valid JSON with the full envelope (env=" + JSON.stringify(env) + ")"
    );
    const capped = JSON.parse(SRV.terminalChatError({ message: "x".repeat(900) }));
    assert(capped.error.message.length === 512, "zT4b4) terminal messages are capped at 512 characters (len=" + capped.error.message.length + ")");
    const fromUp = JSON.parse(SRV.terminalChatError({
      upstream: JSON.stringify({ error: { message: "upstream said sk-t4-sentinel-0123456789abcdef", code: "nope", nested: { a: 1 }, list: [1, 2] } }),
      message: "fallback wording", cfg: cfgT, req: reqT,
    }));
    assert(
      fromUp.error.message === "upstream said [redacted]" && fromUp.error.code === "nope" && fromUp.error.nested === undefined && fromUp.error.list === undefined,
      "zT4b5) the provider's own explanation is kept, redacted, and stripped of nested payloads (env=" + JSON.stringify(fromUp) + ")"
    );
  }

  // zT4c) The durable outcome cap keeps the story at both ends, unit level.
  {
    const many = Array.from({ length: 40 }, (_, i) => SRV.attemptOutcome({ attempt: { backend: "b" + i }, status: 500, kind: "server", decision: "failover", latencyMs: i }));
    const capped = SRV.capAttemptsSummary(many);
    assert(
      capped.attemptsSummary.length === 32 && capped.attemptsOmitted === 8 && capped.attemptsSummary[0].backend === "b0" && capped.attemptsSummary[30].backend === "b30" && capped.attemptsSummary[31].backend === "b39",
      "zT4c1) the outcome cap keeps the first 31 and the last, and counts what it dropped (len=" + capped.attemptsSummary.length + ", omitted=" + capped.attemptsOmitted + ")"
    );
    const short = SRV.capAttemptsSummary(many.slice(0, 2));
    assert(short.attemptsSummary.length === 2 && short.attemptsOmitted === 0, "zT4c2) a short outcome list is kept whole");
    assert(SRV.attemptOutcome({ attempt: { backend: "b" }, status: 500 }).requestIds === null, "zT4c3) an outcome with no correlation ids records null, not an empty object");
  }

  // zT4d) A 402 is a dry account: it classifies as billing (fail over + cool the
  //   account) instead of falling through to the server class, and "insufficient
  //   balance" is recognised wherever it appears.
  {
    assert(SRV.classify(402, "Insufficient Balance") === "billing", "zT4d1) HTTP 402 is billing, whatever its body says");
    assert(SRV.classify(402, "") === "billing", "zT4d2) a body-less 402 is still billing");
    assert(SRV.classify(400, '{"error":{"message":"Insufficient Balance"}}') === "billing", "zT4d3) 'insufficient balance' is billing on a 400 too");
    assert(SRV.classify(422, "insufficient_balance") === "billing", "zT4d4) the snake_case spelling is billing");
    assert(SRV.classify(400, '{"error":{"message":"bad temperature"}}') === "client", "zT4d5) an explained payload error is still a client error");
    assert(SRV.classify(500, "") === "server", "zT4d6) a 5xx is still a server failure");
  }

  // zT4e) A never-ended error body cannot hold a request open, and the durable
  //   summary explains what happened WITHOUT carrying the request or the key.
  {
    const hangSrv = http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      res.writeHead(400, { "content-type": "application/json", "content-length": "500" });
      res.write('{"error":{"message":"cut off before the end');
      // never ends: the router has to give up on it
    });
    const goodT4 = mockBackend("t4good", {});
    const pp = await Promise.all([listen(hangSrv), listen(goodT4.srv)]);
    goodT4.port = pp[1];
    const tmpBase = process.env.TEMP || process.env.TMPDIR || "/tmp";
    const hPath = tmpBase + "/t4hist-" + Date.now() + "-" + Math.random().toString(36).slice(2) + ".jsonl";
    const cfgT4 = {
      port: 0, prefix: "/v1", masterKeyEnv: null, errorBodyTimeoutMs: 200,
      backends: [
        { id: "t4hang", baseURL: `http://${HOST}:${pp[0]}`, apiKeyEnv: "KEY_T4HANG" },
        { id: "t4good", baseURL: `http://${HOST}:${goodT4.port}`, apiKeyEnv: "KEY_T4GOOD" },
      ],
      models: {
        mT4: { providers: [{ backend: "t4hang", upstream: "uT4" }, { backend: "t4good", upstream: "uT4" }], affinityPool: 1 },
        mT4only: { providers: [{ backend: "t4hang", upstream: "uT4" }], affinityPool: 1 },
      },
      presets: { pT4: { strategy: "affinity", models: ["mT4"] }, pT4only: { strategy: "affinity", models: ["mT4only"] } },
      backoff: BO,
    };
    const hT4 = await startRouterCfg(cfgT4, "KEY_T4HANG=t4-key-sentinel-0123456789\nKEY_T4GOOD=k\n", { ROUTER_HISTORY: hPath });
    const call = (model) => fetch(hT4.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master" },
      body: JSON.stringify({ model, messages: [{ role: "user", content: "SENTINEL_PROMPT_T4" }] }),
      signal: AbortSignal.timeout(8000),
    });
    try {
      const t0 = Date.now();
      const rr = await call("pT4");
      await rr.text();
      const elapsed = Date.now() - t0;
      assert(
        rr.status === 200 && rr.headers.get("x-router-backend") === "t4good" && elapsed < 3000,
        "zT4e1) a stalled error body is abandoned at the deadline and the request fails over (status=" + rr.status + ", backend=" + rr.headers.get("x-router-backend") + ", ms=" + elapsed + ")"
      );
      const hist = await (await api(hT4.base, "/api/history?limit=5", { method: "GET" })).json();
      const e = hist.entries.find((x) => x.callId === rr.headers.get("x-router-call"));
      const su = e && Array.isArray(e.attemptsSummary) ? e.attemptsSummary : [];
      assert(
        !!e && su.length === 2 && su[0].errorBodyState === "timeout" && su[0].status === 400 && su[0].kind === "upstream_rejection" && su[0].decision === "failover" && su[0].backend === "t4hang" && su[1].backend === "t4good" && su[1].decision === "success",
        "zT4e2) the durable summary names both attempts, the body-read state and each decision (" + JSON.stringify(su.map((x) => [x.backend, x.status, x.decision, x.errorBodyState])) + ")"
      );
      assert(
        e.fallbackCount === 1 && e.selectionReason === "configured" && e.cacheMayBeCold === true,
        "zT4e3) a provider switch is counted once and says the cache MAY be cold (fallbackCount=" + e.fallbackCount + ", reason=" + e.selectionReason + ", cacheMayBeCold=" + e.cacheMayBeCold + ")"
      );
      const blob = JSON.stringify(e);
      assert(!blob.includes("SENTINEL_PROMPT_T4") && !blob.includes("t4-key-sentinel"), "zT4e4) a durable summary carries no prompt and no credential (" + blob.length + " bytes)");
      const fsp = await import("node:fs/promises");
      let fileLine = "";
      for (let i = 0; i < 30 && !fileLine; i++) {
        const txt = await fsp.readFile(hPath, "utf8").catch(() => "");
        fileLine = txt.split("\n").find((l) => l.includes(e.callId)) || "";
        if (!fileLine) await new Promise((r) => setTimeout(r, 50));
      }
      assert(fileLine.includes("attemptsSummary") && fileLine.includes("timeout"), "zT4e5) the summary is durable in the history file, not only in the ring buffer (" + fileLine.length + " bytes)");
      const rr2 = await call("pT4only");
      const b2 = await rr2.json();
      const hist2 = await (await api(hT4.base, "/api/history?limit=5", { method: "GET" })).json();
      const e2 = hist2.entries.find((x) => x.callId === rr2.headers.get("x-router-call"));
      const detail2 = await (await api(hT4.base, "/api/history/detail?call=" + rr2.headers.get("x-router-call"), { method: "GET" })).json();
      assert(
        rr2.status === 503 && b2.error && typeof b2.error.message === "string" && b2.error.message.length > 0 && /without explaining why/.test(b2.error.message),
        "zT4e6) a fully unexplained exhaustion is a 503 with real wording, never an empty body (status=" + rr2.status + ", message=" + JSON.stringify(b2.error && b2.error.message) + ")"
      );
      assert(
        !!e2 && e2.status === rr2.status && (e2.attemptsSummary || []).length === 1 &&
        detail2.detail && detail2.detail.response && detail2.detail.response.status === rr2.status && (detail2.detail.response.attemptsSummary || []).length === 1,
        "zT4e7) the terminal status agrees across the response, history row and detail, each carrying the attempt (http=" + rr2.status + ", history=" + (e2 && e2.status) + ", detail=" + (detail2.detail && detail2.detail.response && detail2.detail.response.status) + ", deAttempts=" + ((detail2.detail && detail2.detail.response && detail2.detail.response.attemptsSummary) || []).length + ", hiAttempts=" + ((e2 && e2.attemptsSummary) || []).length + ")"
      );
    } finally {
      hT4.child.kill(); hangSrv.close(); goodT4.srv.close();
      await rm(hT4.dir, { recursive: true, force: true });
      await (await import("node:fs/promises")).rm(hPath, { force: true }).catch(() => {});
    }
  }

  // zT4f) /admin/reset-health really does reset health: a cooled ROUTE comes
  //   back, not just the account row. A reset that leaves routes cooling would
  //   report "healthy" while still refusing to route.
  {
    const r1 = mockBackend("r1", { default: { status: 500, body: { error: { message: "boom" } } } });
    const r2 = mockBackend("r2", {});
    const pR = await Promise.all([listen(r1.srv), listen(r2.srv)]);
    r1.port = pR[0]; r2.port = pR[1];
    const cfgR = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "r1", baseURL: `http://${HOST}:${r1.port}`, apiKeyEnv: "KEY_R1" },
        { id: "r2", baseURL: `http://${HOST}:${r2.port}`, apiKeyEnv: "KEY_R2" },
      ],
      models: { mR: { providers: [{ backend: "r1", upstream: "uR" }, { backend: "r2", upstream: "uR" }], affinityPool: 1 } },
      presets: { pR: { strategy: "affinity", models: ["mR"] } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 60000, serverMaxMs: 300000, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    const hR = await startRouterCfg(cfgR, "KEY_R1=k\nKEY_R2=k\n");
    const hit = () => api(hR.base, "/v1/chat/completions", { body: { model: "pR", messages: [] } });
    const routesFor = (hb, id) => (hb.routes || []).filter((x) => x.backend === id).length;
    try {
      const a = await hit(); await a.text();
      const hb = await (await api(hR.base, "/health", { method: "GET" })).json();
      assert(
        a.headers.get("x-router-backend") === "r2" && r1.hits.length === 1 && routesFor(hb, "r1") === 1,
        "zT4f1) a 500 cools the route and the next provider serves (backend=" + a.headers.get("x-router-backend") + ", r1routes=" + routesFor(hb, "r1") + ")"
      );
      const rst = await api(hR.base, "/admin/reset-health", { method: "POST" });
      await rst.text();
      const hb2 = await (await api(hR.base, "/health", { method: "GET" })).json();
      assert(routesFor(hb2, "r1") === 0, "zT4f2) reset-health clears the ROUTE recovery, not just the account row (r1routes=" + routesFor(hb2, "r1") + ")");
      const b = await hit(); await b.text();
      assert(
        r1.hits.length === 2,
        "zT4f3) the previously cooled provider is tried again after a reset (r1hits=" + r1.hits.length + ", backend=" + b.headers.get("x-router-backend") + ")"
      );
    } finally {
      hR.child.kill(); r1.srv.close(); r2.srv.close();
      await rm(hR.dir, { recursive: true, force: true });
    }
  }

  // zProbe1) Exactly one request may probe a recovering route. Once the cooldown
  //   lapses, the first request takes the probe and hangs on the upstream; a second
  //   request arriving while that probe is still open must NOT pile onto the same
  //   route - it fails over instead.
  {
    let paHits = 0;
    const pa = http.createServer(async (req, res) => {
      for await (const c of req) { /* drain */ }
      paHits++;
      // Deliberately slow, so a rival request arrives while the probe is open.
      setTimeout(() => {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "probe failed" } }));
      }, 500);
    });
    const pb = mockBackend("pb", {});
    const portPa = await listen(pa);
    pb.port = await listen(pb.srv);
    const cfgPr = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "pa", baseURL: `http://${HOST}:${portPa}`, apiKeyEnv: "KEY_PA" },
        { id: "pb", baseURL: `http://${HOST}:${pb.port}`, apiKeyEnv: "KEY_PB" },
      ],
      models: { mPr: { providers: [{ backend: "pa", upstream: "u1" }, { backend: "pb", upstream: "u1" }], affinityPool: 1 } },
      presets: { pPr: { strategy: "affinity", models: ["mPr"] } },
      backoff: { ...BO, serverBaseMs: 120, serverMaxMs: 300 },
    };
    const hPr = await startRouterCfg(cfgPr, "KEY_PA=k\nKEY_PB=k\n");
    try {
      await (await api(hPr.base, "/v1/chat/completions", { body: { model: "pPr", messages: [] } })).text();
      assert(paHits === 1 && pb.hits.length === 1, "zProbe1a) the first failure cools the route and fails over (paHits=" + paHits + ", pbHits=" + pb.hits.length + ")");
      await new Promise((r) => setTimeout(r, 200)); // let the route cooldown lapse
      const openProbe = api(hPr.base, "/v1/chat/completions", { body: { model: "pPr", messages: [] } });
      await new Promise((r) => setTimeout(r, 80)); // let it claim the probe
      const probeHits = paHits;
      const rival = await api(hPr.base, "/v1/chat/completions", { body: { model: "pPr", messages: [] } });
      const rivalBody = await rival.json();
      assert(
        probeHits === 2 && rival.status === 200 && /^mock:pb/.test(rivalBody.choices[0].message.content) && paHits === 2,
        "zProbe1b) a rival request does not join an open probe (probeHits=" + probeHits + ", status=" + rival.status + ", paHits=" + paHits + ")"
      );
      await (await openProbe).text(); // the probe settles (its 500 cools the route again)
      assert(paHits === 2, "zProbe1c) the recovering route saw exactly one probe (paHits=" + paHits + ")");
    } finally {
      hPr.child.kill(); pa.close(); pb.srv.close();
      await rm(hPr.dir, { recursive: true, force: true });
    }
  }

  // zEsc1) One failing route is a route problem. Two DIFFERENT routes of the same
  //   account failing inside a minute is an account-level outage. Counterexample: a
  //   sibling route succeeding must NOT escalate.
  {
    const srvEsc = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const model = JSON.parse(Buffer.concat(chunks).toString("utf8")).model;
      if (model === "good-upstream") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", choices: [{ message: { role: "assistant", content: "ok" } }] }));
        return;
      }
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "route down" } }));
    });
    const portEsc = await listen(srvEsc);
    const baseEsc = `http://${HOST}:${portEsc}`;
    const cfgEsc = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "ea", baseURL: baseEsc, apiKeyEnv: "KEY_EA" },
        { id: "eb", baseURL: baseEsc, apiKeyEnv: "KEY_EB" },
      ],
      models: {
        mEscBad1: { providers: [{ backend: "ea", upstream: "bad-1" }], affinityPool: 1 },
        mEscBad2: { providers: [{ backend: "ea", upstream: "bad-2" }], affinityPool: 1 },
        mEscEbBad: { providers: [{ backend: "eb", upstream: "bad-1" }], affinityPool: 1 },
        mEscEbGood: { providers: [{ backend: "eb", upstream: "good-upstream" }], affinityPool: 1 },
      },
      presets: {
        pEsc1: { strategy: "affinity", models: ["mEscBad1"] },
        pEsc2: { strategy: "affinity", models: ["mEscBad2"] },
        pEscEbBad: { strategy: "affinity", models: ["mEscEbBad"] },
        pEscEbGood: { strategy: "affinity", models: ["mEscEbGood"] },
      },
      backoff: { ...BO, serverBaseMs: 60000, serverMaxMs: 300000 },
    };
    const hEsc = await startRouterCfg(cfgEsc, "KEY_EA=k\nKEY_EB=k\n");
    const hitEsc = async (m) => { const r = await api(hEsc.base, "/v1/chat/completions", { body: { model: m, messages: [] } }); await r.text(); };
    const acct = async (id) => (await (await api(hEsc.base, "/health", { method: "GET" })).json()).backends.find((b) => b.id === id);
    try {
      await hitEsc("pEsc1");
      const ea1 = await acct("ea");
      assert(ea1.state === "healthy", "zEsc1a) one failing route does not blame the account (" + ea1.state + ")");
      await hitEsc("pEsc2");
      const ea2 = await acct("ea");
      assert(
        ea2.state === "cooling" && ea2.lastError && ea2.lastError.kind === "server",
        "zEsc1b) two distinct failing routes escalate to an account outage (" + JSON.stringify({ state: ea2.state, kind: ea2.lastError && ea2.lastError.kind }) + ")"
      );
      await hitEsc("pEscEbBad");
      const eb1 = await acct("eb");
      assert(eb1.state === "healthy", "zEsc1c) one failing route on another account stays healthy");
      await hitEsc("pEscEbGood");
      const eb2 = await acct("eb");
      assert(eb2.state === "healthy", "zEsc1d) a succeeding sibling route prevents escalation (" + eb2.state + ")");
    } finally {
      hEsc.child.kill(); srvEsc.close();
      await rm(hEsc.dir, { recursive: true, force: true });
    }
  }

  // zRel1) /health exposes route-scoped rows; a failure streak survives its timer
  //   expiring (only a real success resets it); and a reload prunes removed routes
  //   WITHOUT forgiving the routes that remain.
  {
    let raHits = 0;
    const ra = http.createServer((req, res) => {
      raHits++;
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "down" } }));
    });
    const rc = mockBackend("rc", {});
    const portRa = await listen(ra);
    rc.port = await listen(rc.srv);
    const cfgRel = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "ra", baseURL: `http://${HOST}:${portRa}`, apiKeyEnv: "KEY_RA" },
        { id: "rc", baseURL: `http://${HOST}:${rc.port}`, apiKeyEnv: "KEY_RC" },
      ],
      models: { mA: { providers: [{ backend: "ra", upstream: "u1" }, { backend: "rc", upstream: "u3" }], affinityPool: 1 } },
      presets: { pA: { strategy: "affinity", models: ["mA"] } },
      backoff: { ...BO, serverBaseMs: 150, serverMaxMs: 3000 },
    };
    const hRel = await startRouterCfg(cfgRel, "KEY_RA=k\nKEY_RC=k\n");
    const routeRows = async () => (await (await api(hRel.base, "/health", { method: "GET" })).json()).routes || [];
    const pA = async () => { const r = await api(hRel.base, "/v1/chat/completions", { body: { model: "pA", messages: [] } }); await r.text(); };
    try {
      await pA();
      const row1 = (await routeRows()).find((x) => x.backend === "ra" && x.upstream === "u1");
      assert(
        row1 && row1.state === "cooling" && row1.fails === 1 && row1.models.includes("mA") && row1.retryInMs > 0,
        "zRel1a) /health lists the cooling route with its logical model and retry time (" + JSON.stringify(row1) + ")"
      );
      await new Promise((r) => setTimeout(r, 250)); // let the 150ms cooldown lapse
      await pA(); // a probe: fails again, so the streak must grow rather than restart
      const row2 = (await routeRows()).find((x) => x.backend === "ra" && x.upstream === "u1");
      assert(
        row2 && row2.fails === 2 && row2.retryInMs > 150,
        "zRel1b) the failure streak survives timer expiry (fails=" + (row2 && row2.fails) + ", retryInMs=" + (row2 && row2.retryInMs) + ")"
      );
      // Reload with the ra/u1 provider removed: that route is pruned.
      const cfgAfter = JSON.parse(JSON.stringify(cfgRel));
      cfgAfter.models.mA.providers = [{ backend: "rc", upstream: "u3" }];
      await writeFile(join(hRel.dir, "config.json"), JSON.stringify(cfgAfter, null, 2));
      let pruned = false;
      for (let i = 0; i < 20 && !pruned; i++) {
        await new Promise((r) => setTimeout(r, 200));
        pruned = !(await routeRows()).some((x) => x.backend === "ra" && x.upstream === "u1");
      }
      const raHitsBefore = raHits;
      await pA();
      assert(
        pruned && raHits === raHitsBefore,
        "zRel1c) a reload prunes the removed route and keeps routing to the survivor (pruned=" + pruned + ", raHits=" + raHits + ")"
      );
    } finally {
      hRel.child.kill(); ra.close(); rc.srv.close();
      await rm(hRel.dir, { recursive: true, force: true });
    }
  }

  // zStale) A settlement that is no longer the newest one must not write health. Review
  //   2026-10-08 verified a held success clearing a cooldown a newer request had recorded
  //   (route fails 1 -> 0). Upstream `sa` answers the FIRST request slowly with a usable
  //   200 and the SECOND immediately with a 500, which is that race seen from outside.
  {
    let saHits = 0;
    const sa = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      saHits++;
      if (saHits === 1) {
        await new Promise((r) => setTimeout(r, 700));
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "slow but usable" }, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 3 } }));
        return;
      }
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "boom", type: "server_error" } }));
    });
    const portSa = await listen(sa);
    const cfgStale = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "sa", baseURL: `http://${HOST}:${portSa}`, apiKeyEnv: "KEY_SA", timeoutMs: 20000 }],
      models: { mS: { providers: [{ backend: "sa", upstream: "u1" }], affinityPool: 1 } },
      presets: { pS: { strategy: "failover", models: ["mS"] } },
      backoff: { ...BO, serverBaseMs: 30000, serverMaxMs: 300000 },
    };
    const hStale = await startRouterCfg(cfgStale, "KEY_SA=k\n");
    const saRow = async () => ((await (await api(hStale.base, "/health", { method: "GET" })).json()).routes || []).find((x) => x.backend === "sa" && x.upstream === "u1");
    const call = () => api(hStale.base, "/v1/chat/completions", { body: { model: "pS", messages: [{ role: "user", content: "hi" }] } });
    try {
      const slow = call();
      await new Promise((r) => setTimeout(r, 150)); // the slow attempt is now in flight
      const fast = await call();
      await fast.text();
      const cooled = await saRow();
      assert(
        saHits === 2 && cooled && cooled.state === "cooling" && cooled.fails === 1,
        "zStale1) a newer failure cools the route while an older attempt is still in flight (hits=" + saHits + ", row=" + JSON.stringify(cooled && { state: cooled.state, fails: cooled.fails }) + ")"
      );
      const slowRes = await slow;
      await slowRes.text();
      const after = await saRow();
      assert(
        after && after.state === "cooling" && after.fails === 1,
        "zStale2) an older usable completion does NOT clear the cooldown a newer request recorded (row=" + JSON.stringify(after && { state: after.state, fails: after.fails }) + ")"
      );
    } finally {
      hStale.child.kill(); sa.close();
      await rm(hStale.dir, { recursive: true, force: true });
    }
  }

  // zStale3) A candidate that cooled while an earlier candidate was still working must not
  //   be dispatched: the candidate list is built once, so without a dispatch-time recheck a
  //   manually cooled provider still receives the request (review finding 1).
  {
    let aHits = 0;
    const sa = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      aHits++;
      await new Promise((r) => setTimeout(r, 600));
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "boom", type: "server_error" } }));
    });
    const sb = mockBackend("sb", {});
    const portSa = await listen(sa);
    sb.port = await listen(sb.srv);
    const cfgDisp = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "sa", baseURL: `http://${HOST}:${portSa}`, apiKeyEnv: "KEY_SA", timeoutMs: 20000 },
        { id: "sb", baseURL: `http://${HOST}:${sb.port}`, apiKeyEnv: "KEY_SB" },
      ],
      models: { mD: { providers: [{ backend: "sa", upstream: "u1" }, { backend: "sb", upstream: "u1" }], affinityPool: 1 } },
      presets: { pD: { strategy: "failover", models: ["mD"] } },
      backoff: { ...BO, serverBaseMs: 30000, serverMaxMs: 300000 },
    };
    const hDisp = await startRouterCfg(cfgDisp, "KEY_SA=k\nKEY_SB=k\n");
    try {
      const pending = api(hDisp.base, "/v1/chat/completions", { body: { model: "pD", messages: [{ role: "user", content: "hi" }] } });
      await new Promise((r) => setTimeout(r, 200)); // the first candidate is in flight
      await api(hDisp.base, "/admin/backend", { body: { id: "sb", action: "cool" } });
      const res = await pending;
      await res.text();
      assert(
        aHits === 1 && sb.hits.length === 0 && res.status >= 500,
        "zStale3) a provider cooled while the previous candidate was in flight is never dispatched (aHits=" + aHits + ", bHits=" + sb.hits.length + ", status=" + res.status + ")"
      );
    } finally {
      hDisp.child.kill(); sa.close(); sb.srv.close();
      await rm(hDisp.dir, { recursive: true, force: true });
    }
  }

  // zSec) A credential echoed back by an upstream must not survive anywhere durable or
  //   served, and an upstream must not be able to forge the router's own facts about the
  //   call (review findings 3 and 11, 2026-10-08).
  {
    const SHORT = "ab12c"; // 5 characters: under the old floor of 8, so it used to leak
    process.env.T4F_SHORT = SHORT;
    const cfgR = { backends: [{ id: "x", apiKeyEnv: "T4F_SHORT" }] };
    const red = SRV.redactSecrets("key=" + SHORT + " done", cfgR, null);
    assert(
      !red.includes(SHORT) && red.includes("[redacted]"),
      "zSec1) a real but short credential value is redacted too (out=" + JSON.stringify(red) + ")"
    );
    assert(
      SRV.redactSecrets("k", cfgR, null) === "k" && SRV.redactSecrets("x" + "k" + "y", cfgR, null) === "x" + "k" + "y",
      "zSec2) a value below the floor is still left alone"
    );
    const forged = JSON.parse(SRV.terminalChatError({
      upstream: JSON.stringify({ error: { message: "no", backend: "caller-said-so", upstreamStatus: 418, callId: "C".repeat(300), type: "T".repeat(200) } }),
      message: "fallback", type: "router_error", callId: "real-call", backend: "real-backend", upstreamStatus: 503, cfg: cfgR, req: null,
    }));
    assert(
      forged.error.backend === "real-backend" && forged.error.upstreamStatus === 503 && forged.error.callId === "real-call" && forged.error.type.length <= 64,
      "zSec3) an upstream cannot forge backend/upstreamStatus/callId, and its strings stay bounded (env=" + JSON.stringify(forged.error) + ")"
    );
    const upCall = JSON.parse(SRV.terminalChatError({
      upstream: JSON.stringify({ error: { message: "no", callId: "C".repeat(300) } }), message: "fallback", cfg: cfgR, req: null,
    }));
    assert(upCall.error.callId === null || upCall.error.callId.length <= 160, "zSec4) an upstream-supplied callId is capped at 160 (len=" + (upCall.error.callId || "").length + ")");

    // Live path: the upstream echoes the Authorization value in BOTH its message and a
    // correlation header, which is what makes this reach the durable summary and detail.
    const sc = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(500, { "content-type": "application/json", "x-request-id": SHORT });
      res.end(JSON.stringify({ error: { message: "bad credential " + SHORT + " for this account" } }));
    });
    const portSc = await listen(sc);
    const cfgLeak = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "sc", baseURL: `http://${HOST}:${portSc}`, apiKeyEnv: "KEY_SC" }],
      models: { mL: { providers: [{ backend: "sc", upstream: "u1" }], affinityPool: 1 } },
      presets: { pL: { strategy: "failover", models: ["mL"] } },
      backoff: { ...BO, serverBaseMs: 30000, serverMaxMs: 300000 },
    };
    const hLeak = await startRouterCfg(cfgLeak, "KEY_SC=" + SHORT + "\n");
    try {
      const res = await api(hLeak.base, "/v1/chat/completions", { body: { model: "pL", messages: [{ role: "user", content: "hi" }] } });
      const body = await res.text();
      const callId = res.headers.get("x-router-call");
      const hist = await (await api(hLeak.base, "/api/history?limit=5", { method: "GET" })).json();
      const entry = (hist.entries || []).find((e) => e.callId === callId);
      const det = await (await api(hLeak.base, "/api/history/detail?call=" + callId, { method: "GET" })).json();
      const sums = JSON.stringify((entry && entry.attemptsSummary) || []);
      const deta = JSON.stringify((det.detail && det.detail.attempts) || []);
      assert(
        !body.includes(SHORT) && body.includes("[redacted]"),
        "zSec5) the terminal error never echoes the credential (body=" + body.slice(0, 160) + ")"
      );
      assert(
        !sums.includes(SHORT) && deta.includes("[redacted]") && !deta.includes(SHORT),
        "zSec6) the durable outcome and the detail attempt are redacted at record time (sums=" + sums.slice(0, 140) + ")"
      );
      assert(
        !JSON.stringify(det).includes(SHORT),
        "zSec7) nothing in the detail response carries the credential"
      );
    } finally {
      hLeak.child.kill(); sc.close();
      await rm(hLeak.dir, { recursive: true, force: true });
    }
  }

  // zPref) The preference cap evicts the least recently USED pin, and /health exposes the
  //   table as counts only (review finding 13, plan checkbox 204).
  {
    const haveSeams = typeof SRV.touchSessionPreference === "function" && typeof SRV.evictSessionPreferences === "function" && SRV.SESSION_PREFS instanceof Map;
    assert(haveSeams, "zPref1) the preference table exposes its LRU seams for deterministic testing");
    if (haveSeams) {
      const M = SRV.SESSION_PREFS;
      const saved = new Map(M);
      try {
        M.clear();
        for (const k of ["a", "b", "c"]) M.set(k, { routeKey: "r", latestRequestSequence: 1, lastUsedAt: 1 });
        SRV.touchSessionPreference("a", Date.now()); // 'a' is oldest written, newest used
        SRV.evictSessionPreferences(2);
        assert(
          M.has("a") && M.has("c") && !M.has("b"),
          "zPref2) the cap evicts the least recently used pin, not the oldest written one (order=" + JSON.stringify([...M.keys()]) + ")"
        );
      } finally { M.clear(); for (const [k, v] of saved) M.set(k, v); }
    }
  }

  // zUncool) An operator uncool clears the backend's ROUTES as well as its account;
  //   otherwise /health reports healthy while every route stays refused (finding 12).
  {
    let ucHits = 0;
    const ua = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      ucHits++;
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "boom", type: "server_error" } }));
    });
    const portUa = await listen(ua);
    const cfgUc = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "ua", baseURL: `http://${HOST}:${portUa}`, apiKeyEnv: "KEY_UA" }],
      models: { mU: { providers: [{ backend: "ua", upstream: "u1" }], affinityPool: 1 } },
      presets: { pU: { strategy: "failover", models: ["mU"] } },
      backoff: { ...BO, serverBaseMs: 30000, serverMaxMs: 300000 },
    };
    const hUc = await startRouterCfg(cfgUc, "KEY_UA=k\n");
    const uHealth = async () => (await (await api(hUc.base, "/health", { method: "GET" })).json());
    const uCall = () => api(hUc.base, "/v1/chat/completions", { body: { model: "pU", messages: [{ role: "user", content: "hi" }] } });
    try {
      await (await uCall()).text();
      const before = await uHealth();
      const r1 = (before.routes || []).find((x) => x.backend === "ua");
      assert(r1 && r1.state === "cooling", "zUncool1) the failing route is cooling first (state=" + (r1 && r1.state) + ")");
      assert(
        before.preferences && typeof before.preferences.size === "number" && typeof before.preferences.evictions === "number",
        "zUncool2) /health reports the preference table as counts only (" + JSON.stringify(before.preferences) + ")"
      );
      await api(hUc.base, "/admin/backend", { body: { id: "ua", action: "uncool" } });
      const after = await uHealth();
      const r2 = (after.routes || []).find((x) => x.backend === "ua");
      const hitsBefore = ucHits;
      await (await uCall()).text();
      assert(
        (!r2 || r2.state === "healthy") && ucHits === hitsBefore + 1,
        "zUncool3) uncool clears the route cooldown and the route is dispatched to again (state=" + (r2 && r2.state) + ", hits=" + ucHits + ")"
      );
    } finally {
      hUc.child.kill(); ua.close();
      await rm(hUc.dir, { recursive: true, force: true });
    }
  }

  // zUnusable) A 200 whose body carries no output at all -- not even a non-empty reasoning
  //   string -- must not count as usable. It fails over instead of reporting success on a
  //   provider that served nothing (review finding 9).
  {
    let uzHits = 0;
    const uz = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      uzHits++;
      if (Buffer.concat(chunks).toString("utf8").includes('"stream":true')) {
        // A real SSE reply whose only delta carries an EMPTY reasoning string: without a
        // genuine event stream the collector's delta parser never runs at all, which is
        // how an earlier version of this test passed against the unfixed code.
        res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
        res.write("data: " + JSON.stringify({ id: "m", choices: [{ index: 0, delta: { role: "assistant", reasoning_content: "" } }] }) + "\n\n");
        res.write("data: [DONE]\n\n");
        res.end();
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "", reasoning_content: "" }, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 0 } }));
    });
    const portUz = await listen(uz);
    const okb = mockBackend("ok", {});
    okb.port = await listen(okb.srv);
    const cfgUn = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "uz", baseURL: `http://${HOST}:${portUz}`, apiKeyEnv: "KEY_UZ", timeoutMs: 20000 },
        { id: "ok", baseURL: `http://${HOST}:${okb.port}`, apiKeyEnv: "KEY_OK" },
      ],
      models: { mE: { providers: [{ backend: "uz", upstream: "u1" }, { backend: "ok", upstream: "u1" }], affinityPool: 1 } },
      presets: { pE: { strategy: "failover", models: ["mE"] } },
      backoff: { ...BO, serverBaseMs: 30000, serverMaxMs: 300000 },
    };
    const hUn = await startRouterCfg(cfgUn, "KEY_UZ=k\nKEY_OK=k\n");
    try {
      const res = await api(hUn.base, "/v1/chat/completions", { body: { model: "pE", messages: [{ role: "user", content: "hi" }] } });
      await res.text();
      const callId = res.headers.get("x-router-call");
      const entries = async () => ((await (await api(hUn.base, "/api/history?limit=5", { method: "GET" })).json()).entries || []);
      const entry = (await entries()).find((e) => e.callId === callId);
      const sum0 = entry && (entry.attemptsSummary || [])[0];
      // The response is already on the wire by then, so it is charged rather than
      // replayed (see the settlement comment in server.mjs): what matters is that it is
      // never a success, never a pin, and that the route pays for it.
      assert(
        uzHits === 1 && entry && entry.backend === "uz" && entry.errorKind === "server" && entry.relayError === "upstream returned no usable output" && sum0 && sum0.decision === "unusable" && sum0.cooldownScope === "route",
        "zUnusable1) an empty 200 is charged as unusable, never as a success (uzHits=" + uzHits + ", backend=" + (entry && entry.backend) + ", kind=" + (entry && entry.errorKind) + ", decision=" + (sum0 && sum0.decision) + ")"
      );
      const res2 = await api(hUn.base, "/v1/chat/completions", { body: { model: "pE", messages: [{ role: "user", content: "hi" }] } });
      const txt2 = await res2.text();
      const entry2 = (await entries()).find((e) => e.callId === res2.headers.get("x-router-call"));
      assert(
        txt2.includes("mock:ok") && entry2 && entry2.backend === "ok" && uzHits === 1,
        "zUnusable2) the route an empty 200 cooled is not chosen for the next request (backend=" + (entry2 && entry2.backend) + ", uzHits=" + uzHits + ")"
      );
      // The same rule on the STREAMING collector, whose flags come from the delta slots
      // rather than from a final message. Uncools first (which also re-checks that uncool
      // clears the route) so the empty provider is reachable again.
      await api(hUn.base, "/admin/backend", { body: { id: "uz", action: "uncool" } });
      const sres = await api(hUn.base, "/v1/chat/completions", { body: { model: "pE", messages: [{ role: "user", content: "hi" }], stream: true } });
      await sres.text();
      const sentry = (await entries()).find((e) => e.callId === sres.headers.get("x-router-call"));
      assert(
        sentry && sentry.stream === true && sentry.errorKind === "server" && sentry.relayError === "upstream returned no usable output" && ((sentry.attemptsSummary || [])[0] || {}).decision === "unusable",
        "zUnusable3) an empty streaming delta is charged as unusable too (stream=" + (sentry && sentry.stream) + ", decision=" + (sentry && (sentry.attemptsSummary || [])[0] && sentry.attemptsSummary[0].decision) + ")"
      );
    } finally {
      hUn.child.kill(); uz.close(); okb.srv.close();
      await rm(hUn.dir, { recursive: true, force: true });
    }
  }

  // zUi) The dashboard renders recovery state from payloads captured off a real router, and
  //   survives both hostile upstream text and legacy records that predate these fields
  //   (plan checkbox 201). The renderers run in a stub DOM: no browser, no external assets.
  {
    const FIX = join(import.meta.dirname, "test-fixtures", "ui");
    const load = async (f) => JSON.parse(await readFile(join(FIX, f), "utf8"));
    const baseHealth = await load("health.json");
    const baseHist = await load("api_history_limit_10.json");
    const baseDetail = await load("detail_c06gQZM-rM.json");
    const baseDash = await load("api_dashboard.json");
    const CALL = baseDetail.detail.callId;
    const HTML = await readFile(join(import.meta.dirname, "index.html"), "utf8");
    const SCRIPTS = [...HTML.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]).sort((a, b) => b.length - a.length);
    const IDS = [...HTML.matchAll(/id="([^"]+)"/g)].map((m) => m[1]);
    function makeEl(id) {
      const el = {
        id, dataset: {}, children: [], attrs: {}, title: "", value: "", checked: false, disabled: false, hidden: false,
        style: {}, _html: "", _text: "",
        classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
        appendChild(c) { this.children.push(c); return c; }, removeChild() {}, insertBefore(c) { this.children.push(c); return c; },
        querySelector() { return null; }, querySelectorAll() { return []; },
        addEventListener() {}, removeEventListener() {}, dispatchEvent() {},
        setAttribute(k, v) { this.attrs[k] = String(v); }, getAttribute(k) { return this.attrs[k] ?? null; }, removeAttribute(k) { delete this.attrs[k]; },
        focus() {}, blur() {}, click() {}, remove() {}, closest() { return null; }, contains() { return false; },
        getBoundingClientRect() { return { top: 0, left: 0, width: 0, height: 0, bottom: 0, right: 0 }; }, getContext() { return null; },
      };
      Object.defineProperty(el, "innerHTML", { get() { return this._html; }, set(v) { this._html = String(v); } });
      Object.defineProperty(el, "textContent", { get() { return this._text; }, set(v) { this._text = String(v); } });
      return el;
    }
    async function drive(fx, openCall) {
      const els = new Map();
      for (const id of IDS) if (!els.has(id)) els.set(id, makeEl(id));
      const doc = {
        getElementById(id) { if (!els.has(id)) els.set(id, makeEl(id)); return els.get(id); },
        querySelector() { return null; }, querySelectorAll() { return []; },
        createElement(tag) { return makeEl("#" + tag); }, createTextNode(t) { return { nodeValue: String(t), textContent: String(t) }; },
        addEventListener() {}, removeEventListener() {}, readyState: "complete", title: "", cookie: "",
        documentElement: makeEl("html"), body: makeEl("body"), head: makeEl("head"), visibilityState: "visible", hidden: false,
      };
      const pick = (url) => {
        const p = String(url).replace(/^https?:\/\/[^/]+/, "").split("?")[0];
        if (p === "/health") return fx.health;
        if (p === "/api/dashboard") return fx.dash;
        if (p === "/api/history") return { entries: fx.hist.entries, limit: fx.hist.limit };
        if (p === "/api/history/detail") return fx.detail;
        return {};
      };
      const sandbox = {
        console: { log() {}, warn() {}, error() {}, info() {}, debug() {} }, document: doc,
        localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
        matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
        getComputedStyle: () => ({ getPropertyValue: () => "" }),
        requestAnimationFrame: () => 0, cancelAnimationFrame() {}, setInterval: () => 0, clearInterval() {}, setTimeout: () => 0, clearTimeout() {},
        fetch: async (url) => { const body = pick(url); return { ok: true, status: 200, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) }; },
        alert() {}, location: { href: "http://127.0.0.1:1/", hash: "", search: "", pathname: "/", protocol: "http:", host: "127.0.0.1:1" },
        navigator: { userAgent: "node", clipboard: { writeText: async () => {} } },
        URL: { createObjectURL: () => "", revokeObjectURL() {} }, addEventListener() {}, removeEventListener() {},
        Event: class { constructor(t) { this.type = t; } }, Blob: class {}, FileReader: class {},
      };
      sandbox.window = sandbox; sandbox.globalThis = sandbox; sandbox.self = sandbox;
      const ctx = vm.createContext(sandbox);
      vm.runInContext(SCRIPTS[0], ctx, { filename: "index-inline.js" });
      const run = async (expr) => { const p = vm.runInContext(expr, ctx); if (p && typeof p.then === "function") await p; };
      const wait = () => new Promise((r) => setTimeout(r, 25));
      await run("poll()");
      await wait();
      if (openCall) { await run("openDetail(" + JSON.stringify(openCall) + ")"); await wait(); }
      return [...els.values()].map((e) => e._html + " " + e._text).join("\n");
    }
    const fx = () => ({ health: structuredClone(baseHealth), hist: structuredClone(baseHist), detail: structuredClone(baseDetail), dash: structuredClone(baseDash) });
    const plain = await drive(fx());
    assert(/recovery scope/i.test(plain) && /model route/i.test(plain) && /cool/i.test(plain), "zUi1) the dashboard separates account recovery from provider routes");
    assert(/first pick/i.test(plain) && /cache may be cold/i.test(plain), "zUi2) the history row names the first pick and marks a fallback as possibly cold");
    const detail = await drive(fx(), CALL);
    assert(
      /attempt outcome/i.test(detail) && /cooldown scope/i.test(detail) && /next recovery/i.test(detail) && /session exclusion/i.test(detail) && /body present/i.test(detail),
      "zUi3) the detail panel shows attempt outcomes, cooldown scope, next recovery, body state and this call's session exclusions"
    );
    const H = "<img src=x onerror=alert(1)>";
    const host = fx();
    host.hist.entries[0].relayError = H;
    host.hist.entries[0].attemptsSummary = [{ backend: H, upstream: "mock-upstream", model: "m", status: 429, kind: "rate", decision: H, latencyMs: 4, retries: 0, errorBodyState: "present", errorBodyBytes: 9, cooldownScope: "account", nextAvailableAt: 0, requestIds: { "x-request-id": H } }];
    if (host.detail.detail.attempts && host.detail.detail.attempts[0]) host.detail.detail.attempts[0].errorBody = H;
    if (host.detail.detail.response) { host.detail.detail.response.preview = H; host.detail.detail.response.relayError = H; }
    const hostile = await drive(host, CALL);
    assert(!/<img src=x/.test(hostile) && /&lt;img src=x/.test(hostile), "zUi4) upstream text renders as escaped text, never as markup");
    const old = fx();
    for (const e of old.hist.entries) { delete e.selectionReason; delete e.attemptsSummary; delete e.cacheMayBeCold; delete e.attemptsOmitted; }
    for (const a of (old.detail.detail.attempts || [])) { delete a.errorBody; delete a.retry; }
    for (const k of ["attemptsSummary", "selectionReason", "fallbackCount", "errorKind", "relayError", "cacheMayBeCold", "attemptsOmitted"]) delete old.detail.detail.response[k];
    const legacy = await drive(old, CALL);
    const bad = (legacy.replace(/\s+/g, " ").match(/.{0,70}(undefined|NaN).{0,70}/) || [])[0] || "";
    assert(
      !/undefined/.test(legacy) && !/NaN/.test(legacy) && legacy.includes(old.hist.entries[0].backend) && legacy.includes(old.hist.entries[0].model),
      "zUi5) a record predating these fields still renders its backend and model, with no undefined or NaN (" + JSON.stringify(bad) + ")"
    );
  }

  // rUnit) Route-health helpers: the 30/60/120-second progression to the
  //   five-minute cap, computed from the failure streak. Pure functions, so the
  //   clock never has to advance to test them.
  {
    const cfgB = { backoff: { serverBaseMs: 30000, serverMaxMs: 300000 } };
    const haveHelpers = typeof SRV.routeBackoffMs === "function" && typeof SRV.routeKey === "function";
    assert(haveHelpers, "rUnit1) routeKey and routeBackoffMs are exported for deterministic testing");
    if (haveHelpers) {
      const seq = [1, 2, 3, 5, 9].map((f) => SRV.routeBackoffMs(f, cfgB));
      assert(
        JSON.stringify(seq) === JSON.stringify([30000, 60000, 120000, 300000, 300000]),
        "rUnit2) route backoff progresses 30/60/120s then caps at five minutes (got " + JSON.stringify(seq) + ")"
      );
      const k1 = SRV.routeKey({ backend: "b1", upstream: "u1" });
      assert(
        k1 === SRV.routeKey({ backend: "b1", upstream: "u1" }) &&
          k1 !== SRV.routeKey({ backend: "b1", upstream: "u2" }) &&
          k1 !== SRV.routeKey({ backend: "b2", upstream: "u1" }),
        "rUnit3) routeKey identifies backend+upstream and separates both axes (" + k1 + ")"
      );
    }
  }

  // z21) billing error (400 insufficient credits) FAILS OVER to the next
  //      provider and cools the dry account; an all-dry pool exhausts to 503.
  {
    const billingBody = { error: { message: "You have insufficient credits to make this request. Please purchase more credits to continue using the service.", type: "invalid_request_error", code: "BAD_REQUEST" } };
    const drySrv = http.createServer((req, res) => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify(billingBody));
    });
    const drySrv2 = http.createServer((req, res) => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify(billingBody));
    });
    const okSrv = mockBackend("z21ok", {});
    const [dryPort, dry2Port, okPort] = await Promise.all([listen(drySrv), listen(drySrv2), listen(okSrv.srv)]);
    okSrv.port = okPort;
    const cfgZ21 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z21dry", baseURL: `http://${HOST}:${dryPort}`, apiKeyEnv: "KEY_Z21DRY" },
        { id: "z21dry2", baseURL: `http://${HOST}:${dry2Port}`, apiKeyEnv: "KEY_Z21DRY2" },
        { id: "z21ok", baseURL: `http://${HOST}:${okSrv.port}`, apiKeyEnv: "KEY_Z21OK" },
      ],
      models: {
        "m21a": {
          providers: [{ backend: "z21dry", upstream: "u21" }, { backend: "z21ok", upstream: "u21" }],
          affinityPool: 1,
        },
        "m21b": {
          providers: [{ backend: "z21dry", upstream: "u21" }, { backend: "z21dry2", upstream: "u21" }],
          affinityPool: 1,
        },
      },
      presets: {
        "p21a": { strategy: "failover", models: ["m21a"] },
        "p21b": { strategy: "failover", models: ["m21b"] },
      },
      backoff: BO,
    };
    const { child: childZ21, base: baseZ21, dir: dirZ21 } = await startRouterCfg(cfgZ21, "KEY_Z21DRY=k\nKEY_Z21DRY2=k\nKEY_Z21OK=k\n");
    try {
      // Dry first, healthy second: the request must succeed via failover, and
      // the dry backend must be cooled out of rotation.
      const rr = await fetch(baseZ21 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p21a", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const bb = await rr.json();
      assert(
        rr.status === 200 && bb.choices && /^mock:z21ok/.test(bb.choices[0].message.content),
        "z21a) billing 400 fails over to the next provider (status=" + rr.status + ")"
      );
      const hb = await (await api(baseZ21, "/health", { method: "GET" })).json();
      const dry = hb.backends.find((b) => b.id === "z21dry");
      assert(dry && dry.state === "cooling", "z21b) billing 400 cools the dry backend (state=" + (dry && dry.state) + ")");
      // All-dry pool: the loop exhausts and surfaces a 503 with errorKind billing.
      const rr2 = await fetch(baseZ21 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p21b", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      assert(rr2.status === 503, "z21c) all-dry pool exhausts to 503 (status=" + rr2.status + ")");
      const hh = await (await api(baseZ21, "/api/history?limit=10", { method: "GET" })).json();
      const last = hh.entries && hh.entries.find((e) => e.model === "p21b");
      assert(last && last.errorKind === "billing", "z21d) exhausted billing failure records errorKind=billing (got " + (last && last.errorKind) + ")");
    } finally {
      childZ21.kill();
      drySrv.close();
      drySrv2.close();
      okSrv.srv.close();
      await rm(dirZ21, { recursive: true, force: true });
    }
  }

  // z20) preset-of-presets cycle A->B->A terminates with a clean response (no
  //      hang, no crash): the cycle is cut, valid siblings still route.
  {
    const z20a = mockBackend("z20a", {});
    const z20b = mockBackend("z20b", {});
    const portsZ20 = await Promise.all([listen(z20a.srv), listen(z20b.srv)]);
    z20a.port = portsZ20[0]; z20b.port = portsZ20[1];
    const cfgZ24 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z20a", baseURL: `http://${HOST}:${z20a.port}`, apiKeyEnv: "KEY_Z20A" },
        { id: "z20b", baseURL: `http://${HOST}:${z20b.port}`, apiKeyEnv: "KEY_Z20B" },
      ],
      models: {
        "m20a": { providers: [{ backend: "z20a", upstream: "u20a" }], affinityPool: 1 },
        "m20b": { providers: [{ backend: "z20b", upstream: "u20b" }], affinityPool: 1 },
      },
      presets: {
        "a": { strategy: "affinity", models: ["b"] },          // a -> b -> a cycle
        "b": { strategy: "affinity", models: ["a", "m20b"] },  // cycle cut at 'a', m20b survives
      },
      backoff: BO,
    };
    const { child: childZ24, base: baseZ24, dir: dirZ24 } = await startRouterCfg(cfgZ24, "KEY_Z20A=k\nKEY_Z20B=k\n");
    try {
      const rr = await api(baseZ24, "/v1/chat/completions", { body: { model: "a", messages: [] } });
      const bb = await rr.json();
      const served = rr.headers.get("x-router-backend");
      const routedModel = (await (await api(baseZ24, "/api/history?limit=1", { method: "GET" })).json()).entries[0].routedModel;
      assert(
        rr.status === 200 && /^mock:z20b/.test(bb.choices[0].message.content) && served === "z20b" && routedModel === "m20b",
        "z20a) cycle a->b->a terminates, valid sibling model routes (status=" + rr.status + ", served=" + served + ", routedModel=" + routedModel + ")"
      );
    } finally {
      childZ24.kill();
      z20a.srv.close(); z20b.srv.close();
      await rm(dirZ24, { recursive: true, force: true });
    }
  }

  // z21) failover FIRST: a retryable upstream failure with another provider still
  //      eligible leaves for that provider immediately instead of burning the whole
  //      retry budget against the same failing one. badSrv is hit exactly once.
  {
    let badHits = 0;
    const badSrv = http.createServer((req, res) => {
      badHits++;
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "overloaded", type: "overloaded_error" } }));
    });
    const goodSrv = mockBackend("z21good", {});
    const z21ports = await Promise.all([listen(badSrv), listen(goodSrv.srv)]);
    goodSrv.port = z21ports[1];
    const cfgZ21 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z21bad", baseURL: `http://${HOST}:${z21ports[0]}`, apiKeyEnv: "KEY_Z21BAD" },
        { id: "z21good", baseURL: `http://${HOST}:${goodSrv.port}`, apiKeyEnv: "KEY_Z21GOOD" },
      ],
      models: {
        "m21": {
          providers: [{ backend: "z21bad", upstream: "u21" }, { backend: "z21good", upstream: "u21" }], affinityPool: 1,
          retry: { maxRetries: 2, baseMs: 10, maxMs: 30, multiplier: 2 },
        },
      },
      presets: { "p21": { strategy: "affinity", models: ["m21"] } },
      backoff: BO,
    };
    const { child: childZ21, base: baseZ21, dir: dirZ21 } = await startRouterCfg(cfgZ21, "KEY_Z21BAD=k\nKEY_Z21GOOD=k\n");
    try {
      const rr = await fetch(baseZ21 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p21", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const backend = rr.headers.get("x-router-backend");
      const body = await rr.json();
      assert(
        rr.status === 200 && backend === "z21good" && badHits === 1,
        "z21) failover-first: a retryable failure with an eligible alternative is not retried (backend=" + backend + ", badHits=" + badHits + ")"
      );
    } finally {
      childZ21.kill();
      badSrv.close(); goodSrv.srv.close();
      await rm(dirZ21, { recursive: true, force: true });
    }
  }

  // z21b) the same-provider retry still exists, but only when NOTHING else is
  //      eligible: with a single configured provider there is no alternative to
  //      fail over to, so a retryable 503 is retried up to maxRetries before the
  //      failure is finally charged.
  {
    let badHits = 0;
    const badSrv = http.createServer((req, res) => {
      badHits++;
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "overloaded", type: "overloaded_error" } }));
    });
    const z21bPort = await listen(badSrv);
    const cfgZ21b = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z21bbad", baseURL: `http://${HOST}:${z21bPort}`, apiKeyEnv: "KEY_Z21BBAD" }],
      models: {
        "m21b": {
          providers: [{ backend: "z21bbad", upstream: "u21b" }], affinityPool: 1,
          retry: { maxRetries: 2, baseMs: 10, maxMs: 30, multiplier: 2 },
        },
      },
      presets: { "p21b": { strategy: "affinity", models: ["m21b"] } },
      backoff: BO,
    };
    const { child: childZ21b, base: baseZ21b, dir: dirZ21b } = await startRouterCfg(cfgZ21b, "KEY_Z21BBAD=k\n");
    try {
      const rr = await fetch(baseZ21b + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p21b", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      await rr.text();
      assert(
        rr.status === 503 && badHits === 3,
        "z21h) the last eligible provider is still retried (1 + 2 retries) before cooling (status=" + rr.status + ", badHits=" + badHits + ")"
      );
    } finally {
      childZ21b.kill();
      badSrv.close();
      await rm(dirZ21b, { recursive: true, force: true });
    }
  }

  // z22) totalMs budget stops same-provider retries mid-way: with baseMs large
  //      enough that the second retry would exceed the budget, only 2 attempts
  //      happen (1 initial + 1 retry) and the failure is charged.
  {
    let badHits = 0;
    const badSrv = http.createServer((req, res) => {
      badHits++;
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "overloaded", type: "overloaded_error" } }));
    });
    const goodSrv = mockBackend("z22good", {});
    const z22ports = await Promise.all([listen(badSrv), listen(goodSrv.srv)]);
    goodSrv.port = z22ports[1];
    const cfgZ22 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z22bad", baseURL: `http://${HOST}:${z22ports[0]}`, apiKeyEnv: "KEY_Z22BAD" },
        { id: "z22good", baseURL: `http://${HOST}:${goodSrv.port}`, apiKeyEnv: "KEY_Z22GOOD" },
      ],
      models: {
        "m22": {
          providers: [{ backend: "z22bad", upstream: "u22" }], affinityPool: 1,
          // maxRetries high, but totalMs budget tiny: after first retry (wait 40ms)
          // the second retry's wait would blow the 60ms budget -> stop retrying.
          retry: { maxRetries: 5, baseMs: 40, maxMs: 200, multiplier: 2, totalMs: 60 },
        },
      },
      presets: { "p22": { strategy: "affinity", models: ["m22"] } },
      backoff: BO,
    };
    const { child: childZ22, base: baseZ22, dir: dirZ22 } = await startRouterCfg(cfgZ22, "KEY_Z22BAD=k\nKEY_Z22GOOD=k\n");
    try {
      const rr = await fetch(baseZ22 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p22", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const backend = rr.headers.get("x-router-backend");
      assert(
        rr.status === 503 && badHits === 2,
        "z22) totalMs budget stops retries: badSrv hit 2x (1 + 1 retry) then the failure is charged (status=" + rr.status + ", badHits=" + badHits + ")"
      );
    } finally {
      childZ22.kill();
      badSrv.close(); goodSrv.srv.close();
      await rm(dirZ22, { recursive: true, force: true });
    }
  }

  // z23) wall-clock kill removed: a slow-but-alive SSE stream that runs LONGER
  //      than timeoutMs is no longer cut mid-body. The old AbortSignal.timeout
  //      aborted the whole fetch (headers + body) at the wall clock, so a 150ms
  //      timeout killed a ~480ms stream after ~2 chunks. Now timeoutMs bounds
  //      only the HEADER wait (the stream's headers arrive fast) and the body
  //      is guarded by an idle/stall watchdog, so all chunks + [DONE] arrive.
  {
    const slowSrv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks).toString("utf8");
      if (!body.includes('"stream":true')) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "slow", choices: [{ message: { role: "assistant", content: "slow-ok" } }] }));
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      for (let i = 0; i < 6; i++) {
        res.write("data: " + JSON.stringify({ id: "slow", choices: [{ delta: { content: "c" + i } }] }) + "\n\n");
        await new Promise((r) => setTimeout(r, 80));
      }
      res.write("data: [DONE]\n\n");
      res.end();
    });
    const slowPort = await listen(slowSrv);
    const cfgZ23 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z23slow", baseURL: `http://${HOST}:${slowPort}`, apiKeyEnv: "KEY_Z23SLOW", timeoutMs: 150 },
      ],
      models: {
        "m23": { providers: [{ backend: "z23slow", upstream: "u23" }], affinityPool: 1 },
      },
      presets: { "p23": { strategy: "affinity", models: ["m23"] } },
      backoff: BO,
    };
    const { child: childZ23, base: baseZ23, dir: dirZ23 } = await startRouterCfg(cfgZ23, "KEY_Z23SLOW=k\n");
    try {
      const rr = await fetch(baseZ23 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p23", messages: [], stream: true }),
        signal: AbortSignal.timeout(8000),
      });
      const text = await rr.text();
      const gotDone = text.includes("[DONE]");
      const gotAll = ["c0", "c1", "c2", "c3", "c4", "c5"].every((c) => text.includes(c));
      assert(
        rr.status === 200 && gotDone && gotAll,
        "z23) slow-alive stream completes past wall clock (status=" + rr.status + ", done=" + gotDone + ", allChunks=" + gotAll + ")"
      );
    } finally {
      childZ23.kill();
      slowSrv.close();
      await rm(dirZ23, { recursive: true, force: true });
    }
  }

  // z24) genuine upstream stall is still cut: headers + one chunk then silence.
  //      The idle watchdog aborts the stuck body after idleTimeoutMs, so the
  //      client gets a short body with NO [DONE] and the request settles fast
  //      (it does not hang until the harness-side 5s cap).
  {
    const stallSrv = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const body = Buffer.concat(chunks).toString("utf8");
      if (!body.includes('"stream":true')) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "stall", choices: [{ message: { role: "assistant", content: "stall-ok" } }] }));
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write("data: " + JSON.stringify({ id: "stall", choices: [{ delta: { content: "start" } }] }) + "\n\n");
      // deliberately never end
    });
    const stallPort = await listen(stallSrv);
    const cfgZ24 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z24stall", baseURL: `http://${HOST}:${stallPort}`, apiKeyEnv: "KEY_Z24STALL", idleTimeoutMs: 200 },
      ],
      models: {
        "m24": { providers: [{ backend: "z24stall", upstream: "u24" }], affinityPool: 1 },
      },
      presets: { "p24": { strategy: "affinity", models: ["m24"] } },
      backoff: BO,
    };
    const { child: childZ24, base: baseZ24, dir: dirZ24 } = await startRouterCfg(cfgZ24, "KEY_Z24STALL=k\n");
    try {
      const t0 = Date.now();
      let cut = false;
      try {
        const rr = await fetch(baseZ24 + "/v1/chat/completions", {
          method: "POST",
          headers: { "content-type": "application/json", authorization: "Bearer local-master" },
          body: JSON.stringify({ model: "p24", messages: [], stream: true }),
          signal: AbortSignal.timeout(5000),
        });
        const text = await rr.text();
        cut = !text.includes("[DONE]");
      } catch {
        cut = true; // client read aborted -> the relay cut the stuck stream
      }
      const elapsed = Date.now() - t0;
      // Telemetry: the cut row must surface relayError in /api/history (a 200
      // with relayError=idle timeout), so relays that were cut are traceable.
      let relayErr = "no-history";
      try {
        // The summary is recorded after the relay settles, so read it with a short
        // bounded poll instead of racing the write (a bare read flaked once).
        for (let i = 0; i < 40 && relayErr === "no-history"; i++) {
          const hist = await (await api(baseZ24, "/api/history?limit=1", { method: "GET" })).json();
          if (Array.isArray(hist.entries) && hist.entries[0]) relayErr = hist.entries[0].relayError || null;
          else await new Promise((r) => setTimeout(r, 25));
        }
      } catch { /* history read best-effort */ }
      const telOk = typeof relayErr === "string" && /idle timeout/.test(relayErr || "");
      assert(
        cut && elapsed < 4000 && telOk,
        "z24) stalled upstream cut by idle watchdog + relayError telemetry (cut=" + cut + ", elapsed=" + elapsed + "ms, relayError=" + JSON.stringify(relayErr) + ")"
      );
    } finally {
      childZ24.kill();
      stallSrv.close();
      await rm(dirZ24, { recursive: true, force: true });
    }
  }

  // z21e-z21g) unit-level: top-level strategy governs ordering over the EXPANDED
  //      list. failover through a nested preset's models in declared order;
  //      weighted keeps the top-level weight of the nested preset position.
  {
    const wcfg = {
      backends: [
        { id: "b1", model: "u1" }, { id: "b2", model: "u2" },
        { id: "b3", model: "u3" }, { id: "b4", model: "u4" },
      ],
      models: {
        "m1": { providers: [{ backend: "b1", upstream: "u1" }] },
        "m2": { providers: [{ backend: "b2", upstream: "u2" }] },
        "m3": { providers: [{ backend: "b3", upstream: "u3" }] },
        "m4": { providers: [{ backend: "b4", upstream: "u4" }] },
      },
      presets: {
        "glm": { strategy: "failover", models: ["m1", "m2"] },
        "os": { strategy: "affinity", models: ["m3", "m4"] },
        "super": { strategy: "failover", models: ["glm", "os"] },
        "superW": { strategy: "weighted", models: [{ model: "glm", weight: 3 }, { model: "os", weight: 1 }] },
      },
    };
    normalizeConfig(wcfg);
    const fo = candidates(wcfg, "super", "sess");
    assert(
      JSON.stringify(fo.models) === JSON.stringify(["m1", "m2", "m3", "m4"]) &&
      JSON.stringify(fo.ordered.map((o) => o.model)) === JSON.stringify(["m1", "m2", "m3", "m4"]),
      "z21e) failover: nested presets expand in declared order, top-level strategy governs (" + JSON.stringify(fo.ordered.map((o) => o.model)) + ")"
    );
    // weighted: rng 0.01 -> glm band (weight 3) -> first model m1; rest weight
    // desc with declared-order tiebreak: m2 (glm's second), then os models.
    const cw = candidates(wcfg, "superW", "sess", () => 0.01);
    assert(
      JSON.stringify(cw.ordered.map((o) => o.model)) === JSON.stringify(["m1", "m2", "m3", "m4"]),
      "z21f) weighted: nested preset refs carry top-level weight (" + JSON.stringify(cw.ordered.map((o) => o.model)) + ")"
    );
    // nested preset's own strategy is ignored for ordering (os is affinity but
    // inside a failover parent it contributes declared order).
    assert(
      JSON.stringify(candidates(wcfg, "super", "sess").ordered.map((o) => o.backend)) === JSON.stringify(["b1", "b2", "b3", "b4"]),
      "z21g) nested preset own strategy ignored: failover parent, declared order (" + JSON.stringify(candidates(wcfg, "super", "sess").ordered.map((o) => o.backend)) + ")"
    );
  }

  // z22) unknown id inside a preset-of-presets context is dropped (with warn),
  //      valid sibling models still route.
  {
    const z22a = mockBackend("z22a", {});
    const z22b = mockBackend("z22b", {});
    const portsZ22 = await Promise.all([listen(z22a.srv), listen(z22b.srv)]);
    z22a.port = portsZ22[0]; z22b.port = portsZ22[1];
    const cfgZ26 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "z22a", baseURL: `http://${HOST}:${z22a.port}`, apiKeyEnv: "KEY_Z22A" },
        { id: "z22b", baseURL: `http://${HOST}:${z22b.port}`, apiKeyEnv: "KEY_Z22B" },
      ],
      models: {
        "m22a": { providers: [{ backend: "z22a", upstream: "u22a" }], affinityPool: 1 },
        "m22b": { providers: [{ backend: "z22b", upstream: "u22b" }], affinityPool: 1 },
      },
      presets: {
        "ghost": { strategy: "affinity", models: ["nope-model"] },   // unknown -> dropped at normalize
        "base": { strategy: "failover", models: ["m22a"] },
        "top": { strategy: "failover", models: ["base", "ghost", "m22b"] }, // ghost has 0 valid models; m22b survives
      },
      backoff: BO,
    };
    const { child: childZ26, base: baseZ26, dir: dirZ26 } = await startRouterCfg(cfgZ26, "KEY_Z22A=k\nKEY_Z22B=k\n");
    try {
      // "top" expands: base -> [m22a], ghost -> [] (unknown dropped at
      // normalize), m22b. failover declared order -> m22a serves.
      const rr = await api(baseZ26, "/v1/chat/completions", { body: { model: "top", messages: [] } });
      const bb = await rr.json();
      const served = rr.headers.get("x-router-backend");
      assert(
        rr.status === 200 && /^mock:z22a/.test(bb.choices[0].message.content) && served === "z22a",
        "z22a) unknown id inside nested preset dropped; valid siblings route (status=" + rr.status + ", served=" + served + ")"
      );
    } finally {
      childZ26.kill();
      z22a.srv.close(); z22b.srv.close();
      await rm(dirZ26, { recursive: true, force: true });
    }
  }

  // y10-y12) session affinity from request body session fields (session_id / sessionId);
  //     x-session-id header still takes precedence over the body.
  {
    const srvA = mockBackend("sya", {});
    const srvB = mockBackend("syb", {});
    const portA = await listen(srvA.srv);
    const portB = await listen(srvB.srv);
    srvA.port = portA; srvB.port = portB;
    const cfgSY = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "sya", baseURL: `http://${HOST}:${portA}`, apiKeyEnv: "KEY_SYA" },
        { id: "syb", baseURL: `http://${HOST}:${portB}`, apiKeyEnv: "KEY_SYB" },
      ],
      models: { "msy": { providers: [{ backend: "sya", upstream: "usy" }, { backend: "syb", upstream: "usy" }], affinityPool: 2 } },
      presets: { "psy": { strategy: "affinity", models: ["msy"] } },
      backoff: BO,
    };
    const { child: childSY, base: baseSY, dir: dirSY } = await startRouterCfg(cfgSY, "KEY_SYA=k\nKEY_SYB=k\n");
    try {
      const backendOf = async (opts) => {
        const rr = await api(baseSY, "/v1/chat/completions", opts);
        const jj = await rr.json();
        const m = /^mock:([a-z0-9_-]+)/.exec(jj && jj.choices && jj.choices[0] && jj.choices[0].message && jj.choices[0].message.content);
        return m ? m[1] : null;
      };
      const baseOpts = { model: "psy", messages: [] };
      const b1a = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" } });
      const b1b = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" } });
      const b1c = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" } });
      assert(b1a && b1a === b1b && b1b === b1c, "y10) body session_id binds one session to one backend (" + JSON.stringify([b1a, b1b, b1c]) + ")");
      const b2a = await backendOf({ body: { ...baseOpts, sessionId: "sy-case-1" } });
      const b2b = await backendOf({ body: { ...baseOpts, sessionId: "sy-case-1" } });
      assert(b2a && b2a === b2b, "y11) body sessionId (camelCase) binds one session to one backend (" + JSON.stringify([b2a, b2b]) + ")");
      const h1 = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" }, headers: { "x-session-id": "sy-head-1" } });
      const h2 = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" }, headers: { "x-session-id": "sy-head-1" } });
      const h3 = await backendOf({ body: { ...baseOpts }, headers: { "x-session-id": "sy-head-1" } });
      assert(h1 && h1 === h2 && h2 === h3, "y12) x-session-id header wins over body session_id (" + JSON.stringify([h1, h2, h3]) + ")");
    } finally {
      childSY.kill();
      srvA.srv.close(); srvB.srv.close();
      await rm(dirSY, { recursive: true, force: true });
    }
  }

  // zAuth) inbound Bearer auth via top-level masterKeyEnv: gated endpoints
  //  (/v1/chat/completions, /admin/*) require the env-referenced token, the
  //  scheme matches case-insensitively, read-only endpoints stay open, an
  //  empty expected value fails closed, and unset/empty masterKeyEnv keeps
  //  the exact open posture as before.
  {
    const chatBody = { model: "pauth", messages: [] };
    // Fresh mock + router child per case: each child owns its own health
    // registry, so nothing leaks between the auth states below.
    const bringUp = async (masterKeyEnv, envLines) => {
      const srv = mockBackend("auth", {});
      const port = await listen(srv.srv);
      const cfg = {
        port: 0, prefix: "/v1", masterKeyEnv,
        backends: [{ id: "auth", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_AUTH" }],
        models: { "mauth": { providers: [{ backend: "auth", upstream: "u" }], affinityPool: 1 } },
        presets: { "pauth": { strategy: "affinity", models: ["mauth"] } },
        backoff: BO,
      };
      return { ...(await startRouterCfg(cfg, envLines)), srv };
    };
    const closeUp = async ({ child, srv, dir }) => {
      child.kill();
      srv.srv.close();
      await rm(dir, { recursive: true, force: true });
    };

    // Set master key with a real env value.
    let h = await bringUp("LMR_MASTER", "KEY_AUTH=k\nLMR_MASTER=master-secret-123\n");
    try {
      let rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer master-secret-123" } });
      let bb = await rr.json();
      assert(rr.status === 200 && /^mock:auth/.test(bb.choices[0].message.content), "zA1) correct bearer token -> 200 (status=" + rr.status + ")");

      const hitsBefore = h.srv.hits.length;
      rr = await api(h.base, "/v1/chat/completions", { body: chatBody }); // api default: Bearer local-master (wrong)
      assert(rr.status === 401 && h.srv.hits.length === hitsBefore, "zA2) wrong bearer token -> 401, backend never hit (status=" + rr.status + ")");

      rr = await fetch(h.base + "/v1/chat/completions", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(chatBody) });
      assert(rr.status === 401, "zA3) no Authorization header -> 401 (status=" + rr.status + ")");

      rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "bearer master-secret-123" } });
      assert(rr.status === 200, "zA4) case-insensitive 'bearer' scheme -> 200 (status=" + rr.status + ")");

      // Read-only endpoints stay open even without a valid token. /api/config
      // is deliberately NOT in this list: the config-editing routes live under
      // the same `/api/config` prefix and the gate matches on the prefix, so
      // the plain viewer is gated too. That is covered by zCE12b, and the
      // dashboard config viewer sends credentials once a master key is set.
      const openChecks = [
        ["/health", 200], ["/v1/models", 200], ["/api/stats", 200],
        ["/api/history", 200],
      ];
      for (const [path, want] of openChecks) {
        const rrr = await api(h.base, path, { method: "GET" });
        assert(rrr.status === want, "zA5) read-only " + path + " open without valid token (status=" + rrr.status + ")");
      }
      const rCfgNoAuth = await fetch(h.base + "/api/config");
      assert(rCfgNoAuth.status === 401, "zA5b) /api/config follows the config-editor gate (401 without a token, got " + rCfgNoAuth.status + ")");
      const rCfgAuth = await api(h.base, "/api/config", { method: "GET", headers: { authorization: "Bearer master-secret-123" } });
      assert(rCfgAuth.status === 200, "zA5c) /api/config with a token -> 200 (got " + rCfgAuth.status + ")");
      const rrDetail = await api(h.base, "/api/history/detail", { method: "GET" });
      assert(rrDetail.status !== 401, "zA6) /api/history/detail open (not 401, status=" + rrDetail.status + ")");

      rr = await api(h.base, "/admin/reset-health", { method: "POST", headers: { authorization: "Bearer master-secret-123" } });
      assert(rr.status === 200, "zA7) /admin/reset-health with token -> 200 (status=" + rr.status + ")");
      rr = await api(h.base, "/admin/reset-health", { method: "POST" });
      assert(rr.status === 401, "zA8) /admin/reset-health without token -> 401 (status=" + rr.status + ")");

      rr = await api(h.base, "/admin/backend", { method: "POST", body: { id: "auth", action: "cool" }, headers: { authorization: "Bearer master-secret-123" } });
      assert(rr.status === 200, "zA9) /admin/backend with token -> 200 (status=" + rr.status + ")");
      rr = await api(h.base, "/admin/backend", { method: "POST", body: { id: "auth", action: "cool" } });
      assert(rr.status === 401, "zA10) /admin/backend without token -> 401 (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }

    // Fail closed: masterKeyEnv names an env var with no value anywhere.
    h = await bringUp("LMR_UNSET", "KEY_AUTH=k\n");
    try {
      let rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer whatever" } });
      assert(rr.status === 401, "zB1) masterKeyEnv set but env value empty -> chat 401 even with a token (fail closed, status=" + rr.status + ")");
      rr = await api(h.base, "/admin/reset-health", { method: "POST" });
      assert(rr.status === 401, "zB2) fail-closed 401 on /admin/reset-health (status=" + rr.status + ")");
      rr = await api(h.base, "/health", { method: "GET" });
      assert(rr.status === 200, "zB3) /health still open under fail-closed config (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }

    // Unset / empty masterKeyEnv keeps the open posture (no auth anywhere).
    for (const [envVal, tag] of [[null, "zC1"], ["", "zC2"]]) {
      h = await bringUp(envVal, "KEY_AUTH=k\n");
      try {
        const rr = await fetch(h.base + "/v1/chat/completions", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(chatBody) });
        assert(rr.status === 200, tag + ") masterKeyEnv=" + JSON.stringify(envVal) + " -> /v1/chat/completions open without token (status=" + rr.status + ")");
      } finally {
        await closeUp(h);
      }
    }
  }

  // zW) wildcard host: a config with host "0.0.0.0" must bind ONLY the
  //  wildcard socket (no loopback double-bind) - the exact startup regression
  //  that crashed Linux containers with EADDRINUSE. Expect one and only one
  //  listening banner (the wildcard, via port 0), the child stays up, and
  //  /health is reachable through loopback.
  {
    const dir = await mkdtemp(join(tmpdir(), "lmr-test-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const cfgW = {
      port: 0, prefix: "/v1", host: "0.0.0.0", masterKeyEnv: null,
      backends: [{ id: "w", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_W" }],
      models: { "mw": { providers: [{ backend: "w", upstream: "u" }], affinityPool: 1 } },
      presets: { "pw": { strategy: "affinity", models: ["mw"] } },
      backoff: BO,
    };
    await writeFile(cfgPath, JSON.stringify(cfgW, null, 2));
    await writeFile(envPath, "KEY_W=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const banners = [];
    let exited = false;
    const onData = (d) => {
      for (const m of d.toString().matchAll(/listening on http:\/\/([^:]+):(\d+)/g)) {
        banners.push({ host: m[1], port: parseInt(m[2], 10) });
      }
    };
    child.on("exit", () => { exited = true; });
    child.stderr.on("data", onData);
    let port = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("wildcard child did not start")), 8000);
      const check = () => {
        const w = banners.find((b) => b.host === "0.0.0.0");
        if (w) { port = w.port; clearTimeout(timer); resolve(); }
      };
      child.on("exit", (code) => { clearTimeout(timer); reject(new Error("wildcard child exited early, code=" + code)); });
      child.stderr.on("data", check);
      check();
    });
    // Grace window: the buggy double-bind emits a second banner immediately.
    await new Promise((r) => setTimeout(r, 250));
    try {
      assert(!exited, "zW1) host 0.0.0.0 child stays up (no EADDRINUSE crash)");
      assert(banners.length === 1, "zW2) host 0.0.0.0 binds exactly one socket, got " + banners.length + " (" + JSON.stringify(banners) + ")");
      assert(banners[0].host === "0.0.0.0", "zW3) the single bind is the wildcard (got " + JSON.stringify(banners[0]) + ")");
      const h = await api("http://127.0.0.1:" + port, "/health", { method: "GET" });
      assert(h.status === 200, "zW4) /health reachable on the wildcard bind via loopback (status=" + h.status + ")");
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  // zU) web UI playground carries the API-key input: when masterKeyEnv is set,
  //  the served dashboard must still load (200) and include the key field so
  //  users can chat against an authenticated router from the browser.
  {
    const dir = await mkdtemp(join(tmpdir(), "lmr-test-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const cfgU = {
      port: 0, prefix: "/v1", masterKeyEnv: "LMR_UI_KEY",
      backends: [{ id: "u", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_U" }],
      models: { "mu": { providers: [{ backend: "u", upstream: "u" }], affinityPool: 1 } },
      presets: { "pu": { strategy: "affinity", models: ["mu"] } },
      backoff: BO,
    };
    await writeFile(cfgPath, JSON.stringify(cfgU, null, 2));
    await writeFile(envPath, "KEY_U=k\nLMR_UI_KEY=dummy-ui-key\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let port = null;
    const bannerRe = /listening on http:\/\/([^:]+):(\d+)/g;
    let exited = false;
    const grab = (d) => {
      for (const m of d.toString().matchAll(bannerRe)) {
        if (m[1] === "127.0.0.1") port = parseInt(m[2], 10);
      }
    };
    child.on("exit", () => { exited = true; });
    child.stderr.on("data", grab);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("ui child did not start")), 8000);
      const check = () => { if (port) { clearTimeout(timer); resolve(); } };
      child.on("exit", (code) => { clearTimeout(timer); reject(new Error("ui child exited early, code=" + code)); });
      child.stderr.on("data", check);
      check();
    });
    try {
      const rr = await api("http://127.0.0.1:" + port, "/", { method: "GET" });
      const html = await rr.text();
      assert(rr.status === 200 && html.includes('id="pgKey"'), "zU1) dashboard loads with playground API key field under auth (status=" + rr.status + ")");
      assert(html.includes("API key"), "zU2) dashboard key field has a visible label (status=" + rr.status + ")");
      assert(!exited, "zU3) ui child stays up after serving dashboard");
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  // zK) dashboard UI login: /api/auth/status + /api/auth/login + logout.
  //  No uiPasswordEnv -> open UI; with it -> login gates, cookie session,
  //  wrong password 401 with no cookie, per-IP brute-force throttle (5 per
  //  10s -> 429 on the 6th), logout clears the session.
  {
    const chatBody = { model: "pauth", messages: [] };
    const bringUp = async (uiPasswordEnv, envLines) => {
      const srv = mockBackend("auth", {});
      const port = await listen(srv.srv);
      const cfg = {
        port: 0, prefix: "/v1", masterKeyEnv: null, uiPasswordEnv,
        backends: [{ id: "auth", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_AUTH" }],
        models: { "mauth": { providers: [{ backend: "auth", upstream: "u" }], affinityPool: 1 } },
        presets: { "pauth": { strategy: "affinity", models: ["mauth"] } },
        backoff: BO,
      };
      // Temp keys file (separate dir) so these children never touch a real
      // api-keys.json; closeUp removes both dirs.
      const keysDir = await mkdtemp(join(tmpdir(), "lmr-k-"));
      const keysPath = join(keysDir, "keys.json");
      return { ...(await startRouterCfg(cfg, envLines, { LMR_KEYS_FILE: keysPath })), srv, keysDir };
    };
    const closeUp = async ({ child, srv, dir, keysDir }) => {
      child.kill();
      srv.srv.close();
      await rm(dir, { recursive: true, force: true });
      await rm(keysDir, { recursive: true, force: true });
    };

    // zK1) no uiPasswordEnv -> open UI.
    let h = await bringUp(null, "KEY_AUTH=k\n");
    try {
      let rr = await api(h.base, "/api/auth/status", { method: "GET" });
      let bb = await rr.json();
      assert(rr.status === 200 && bb.passwordSet === false && bb.uiAuthed === true, "zK1a) no uiPasswordEnv -> passwordSet:false, uiAuthed:true (status=" + rr.status + ")");
      rr = await api(h.base, "/", { method: "GET" });
      assert(rr.status === 200, "zK1b) / serves the dashboard without login (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }

    // zK2) password set: status false -> login with the correct password sets
    //      the cookie -> status true with the cookie.
    h = await bringUp("LMR_UI_PASS", "KEY_AUTH=k\nLMR_UI_PASS=secret-pass-1\n");
    try {
      let rr = await api(h.base, "/api/auth/status", { method: "GET" });
      let bb = await rr.json();
      assert(rr.status === 200 && bb.passwordSet === true && bb.uiAuthed === false, "zK2a) passwordSet:true, uiAuthed:false before login (status=" + rr.status + ")");

      rr = await api(h.base, "/api/auth/login", { body: { password: "secret-pass-1" } });
      bb = await rr.json();
      const setCookie = rr.headers.get("set-cookie") || "";
      const cookieMatch = /lmr_ui=([^;]+)/.exec(setCookie);
      assert(rr.status === 200 && bb.ok === true && bb.uiAuthed === true && !!cookieMatch, "zK2b) correct password -> 200 uiAuthed:true with session cookie (status=" + rr.status + ")");
      const cookieHeader = cookieMatch ? "lmr_ui=" + cookieMatch[1] : "";

      rr = await api(h.base, "/api/auth/status", { method: "GET", headers: { cookie: cookieHeader } });
      bb = await rr.json();
      assert(rr.status === 200 && bb.uiAuthed === true, "zK2c) /api/auth/status with session cookie -> uiAuthed:true (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }

    // zK3) wrong password -> 401, no cookie set.
    h = await bringUp("LMR_UI_PASS", "KEY_AUTH=k\nLMR_UI_PASS=secret-pass-1\n");
    try {
      const rr = await api(h.base, "/api/auth/login", { body: { password: "wrong-pass" } });
      const bb = await rr.json();
      assert(rr.status === 401 && bb.ok === false && !(rr.headers.get("set-cookie") || "").includes("lmr_ui="), "zK3) wrong password -> 401, no cookie set (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }

    // zK4) throttle: 5 wrong attempts allowed, 6th within the 10s window -> 429.
    h = await bringUp("LMR_UI_PASS", "KEY_AUTH=k\nLMR_UI_PASS=secret-pass-1\n");
    try {
      let all401 = true;
      for (let i = 0; i < 5; i++) {
        const rr = await api(h.base, "/api/auth/login", { body: { password: "wrong-pass" } });
        if (rr.status !== 401) all401 = false;
      }
      assert(all401, "zK4a) first 5 wrong attempts -> 401 each");
      const rr6 = await api(h.base, "/api/auth/login", { body: { password: "wrong-pass" } });
      assert(rr6.status === 429, "zK4b) 6th wrong attempt within window -> 429 (status=" + rr6.status + ")");
    } finally {
      await closeUp(h);
    }

    // zK5) logout clears the session -> status back to uiAuthed:false.
    h = await bringUp("LMR_UI_PASS", "KEY_AUTH=k\nLMR_UI_PASS=secret-pass-1\n");
    try {
      let rr = await api(h.base, "/api/auth/login", { body: { password: "secret-pass-1" } });
      const setCookie = rr.headers.get("set-cookie") || "";
      const cookieHeader = "lmr_ui=" + (/lmr_ui=([^;]+)/.exec(setCookie) || [])[1];
      rr = await api(h.base, "/api/auth/logout", { method: "POST", headers: { cookie: cookieHeader } });
      const bbLogout = await rr.json();
      assert(rr.status === 200 && bbLogout.ok === true, "zK5a) logout -> 200 ok:true (status=" + rr.status + ")");
      rr = await api(h.base, "/api/auth/status", { method: "GET", headers: { cookie: cookieHeader } });
      const bb = await rr.json();
      assert(rr.status === 200 && bb.uiAuthed === false, "zK5b) status after logout with old cookie -> uiAuthed:false (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
    }
  }

  // zL) issued chat-only API keys: /api/keys management is master-gated,
  //  issued keys unlock only /v1/chat/completions (never /admin/*), revoke
  //  kills the key, GET never leaks hashes/raw, and a missing keys file is
  //  tolerated (empty list).
  {
    const chatBody = { model: "pkeys", messages: [] };
    const master = "master-secret-123";
    const bringUp = async (keysPath) => {
      const srv = mockBackend("keys", {});
      const port = await listen(srv.srv);
      const cfg = {
        port: 0, prefix: "/v1", masterKeyEnv: "LMR_MASTER",
        backends: [{ id: "keys", baseURL: `http://${HOST}:${port}`, apiKeyEnv: "KEY_KEYS" }],
        models: { "mkeys": { providers: [{ backend: "keys", upstream: "u" }], affinityPool: 1 } },
        presets: { "pkeys": { strategy: "affinity", models: ["mkeys"] } },
        backoff: BO,
      };
      return { ...(await startRouterCfg(cfg, "KEY_KEYS=k\nLMR_MASTER=" + master + "\n", { LMR_KEYS_FILE: keysPath })), srv };
    };
    const closeUp = async ({ child, srv, dir }) => {
      child.kill();
      srv.srv.close();
      await rm(dir, { recursive: true, force: true });
    };
    const newKeysDir = async () => {
      const d = await mkdtemp(join(tmpdir(), "lmr-l-"));
      return { dir: d, path: join(d, "keys.json") };
    };

    // zL1) /api/keys without the master bearer -> 401 (gated).
    let kd = await newKeysDir();
    let h = await bringUp(kd.path);
    try {
      const rr = await api(h.base, "/api/keys"); // api default: Bearer local-master (wrong)
      assert(rr.status === 401, "zL1) POST /api/keys without master bearer -> 401 (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL2) with master: create a key -> raw sk-lmr-...; the raw key works on chat.
    kd = await newKeysDir();
    h = await bringUp(kd.path);
    try {
      let rr = await api(h.base, "/api/keys", { body: { name: "test-key" }, headers: { authorization: "Bearer " + master } });
      let bb = await rr.json();
      assert(rr.status === 200 && bb.key && typeof bb.key.raw === "string" && bb.key.raw.startsWith("sk-lmr-"), "zL2a) POST /api/keys with master -> 200, raw starts sk-lmr- (status=" + rr.status + ")");
      const raw = bb.key.raw;
      const hitsBefore = h.srv.hits.length;
      rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer " + raw } });
      bb = await rr.json();
      assert(rr.status === 200 && /^mock:keys/.test(bb.choices[0].message.content), "zL2b) issued raw key works on /v1/chat/completions (status=" + rr.status + ")");
      assert(h.srv.hits.length === hitsBefore + 1, "zL2c) the issued-key chat hit the backend");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL3) GET /api/keys never exposes hash or raw.
    kd = await newKeysDir();
    h = await bringUp(kd.path);
    try {
      let rr = await api(h.base, "/api/keys", { body: { name: "leak-check" }, headers: { authorization: "Bearer " + master } });
      const created = await rr.json();
      rr = await api(h.base, "/api/keys", { method: "GET", headers: { authorization: "Bearer " + master } });
      const bb = await rr.json();
      const leak = bb.keys.some((k) => "hash" in k || "raw" in k);
      assert(rr.status === 200 && Array.isArray(bb.keys) && bb.keys.length === 1 && !leak, "zL3) GET /api/keys -> 200, one key, no hash/raw leaked (status=" + rr.status + ")");
      assert(bb.keys[0].id === created.key.id && bb.keys[0].name === "leak-check" && bb.keys[0].revoked === false, "zL3b) listing carries id/name/createdAt/revoked metadata only");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL4) revoke -> raw key 401s on chat; master still works.
    kd = await newKeysDir();
    h = await bringUp(kd.path);
    try {
      let rr = await api(h.base, "/api/keys", { body: { name: "revoke-me" }, headers: { authorization: "Bearer " + master } });
      const { key } = await rr.json();
      rr = await api(h.base, "/api/keys?id=" + key.id, { method: "DELETE", headers: { authorization: "Bearer " + master } });
      const bbDel = await rr.json();
      assert(rr.status === 200 && bbDel.ok === true, "zL4a) DELETE /api/keys?id= -> 200 ok:true (status=" + rr.status + ")");
      rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer " + key.raw } });
      assert(rr.status === 401, "zL4b) revoked key -> 401 on chat (status=" + rr.status + ")");
      rr = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer " + master } });
      assert(rr.status === 200, "zL4c) master key still works after revoke (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL5) issued key accepted on chat while masterKeyEnv is set (zL2b covers
    //      the same path; a second fresh key proves it independently).
    kd = await newKeysDir();
    h = await bringUp(kd.path);
    try {
      const rr = await api(h.base, "/api/keys", { body: { name: "chat-only" }, headers: { authorization: "Bearer " + master } });
      const { key } = await rr.json();
      const rrChat = await api(h.base, "/v1/chat/completions", { body: chatBody, headers: { authorization: "Bearer " + key.raw } });
      assert(rrChat.status === 200, "zL5) chat with a valid issued key while masterKeyEnv set -> 200 (status=" + rrChat.status + ")");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL6) admin endpoint with an issued key -> 401 (master-only).
    kd = await newKeysDir();
    h = await bringUp(kd.path);
    try {
      const rr = await api(h.base, "/api/keys", { body: { name: "admin-try" }, headers: { authorization: "Bearer " + master } });
      const { key } = await rr.json();
      const rrAdmin = await api(h.base, "/admin/reset-health", { method: "POST", headers: { authorization: "Bearer " + key.raw } });
      assert(rrAdmin.status === 401, "zL6) /admin/reset-health with issued key -> 401 (status=" + rrAdmin.status + ")");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }

    // zL7) missing api-keys.json file -> endpoints still work (empty list).
    kd = await newKeysDir();
    h = await bringUp(join(kd.dir, "missing-keys.json")); // file never created
    try {
      let rr = await api(h.base, "/api/keys", { method: "GET", headers: { authorization: "Bearer " + master } });
      let bb = await rr.json();
      assert(rr.status === 200 && Array.isArray(bb.keys) && bb.keys.length === 0, "zL7a) GET /api/keys with missing keys file -> 200 empty list (status=" + rr.status + ")");
      rr = await api(h.base, "/api/keys", { body: { name: "first" }, headers: { authorization: "Bearer " + master } });
      bb = await rr.json();
      assert(rr.status === 200 && bb.key && bb.key.raw.startsWith("sk-lmr-"), "zL7b) creating a key works when the file was missing (status=" + rr.status + ")");
    } finally {
      await closeUp(h);
      await rm(kd.dir, { recursive: true, force: true });
    }
  }

  // zM) analytics dashboard: aggregate store, cooling ring, JSONL seeding,
  //      pricing echo, health join. GET /api/dashboard returns the exact
  //      contract shape; field names are load-bearing for the UI.
  {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    // zM1) seed continuity: a temp router-history.jsonl (new-style rows with
    //      cost in the current hour + a day 3 days ago, plus one legacy row)
    //      rolls into kpis/hourly/daily/byModel/byBackend before any traffic.
    {
      const seedDir = await mkdtemp(join(tmpdir(), "lmr-dash-seed-"));
      const histPath = join(seedDir, "router-history.jsonl");
      const now = Date.now();
      const curHour = Math.floor(now / 3600000) * 3600000;
      const curDay = Math.floor(now / 86400000) * 86400000;
      const day3 = curDay - 3 * 86400000;
      const rows = [
        JSON.stringify({ t: curHour + 1000, callId: "s1", model: "sd1", backend: "b1", stream: false, status: 200, latencyMs: 120, promptTokens: 1000, completionTokens: 50, cacheHit: true, promptCacheHitTokens: 400, promptCacheMissTokens: 600, cost: 0.00011975, cacheSavings: 0.000084, offPeakSavings: 0.00011975, offPeak: true }),
        JSON.stringify({ t: curHour + 2000, callId: "s2", model: "sd1", backend: "b1", stream: false, status: 200, latencyMs: 200, promptTokens: 200, completionTokens: 10, cacheHit: false, promptCacheHitTokens: 0, promptCacheMissTokens: 200, cost: 0.00002, cacheSavings: 0, offPeakSavings: 0.00002, offPeak: true }),
        JSON.stringify({ t: day3 + 12 * 3600000, callId: "s3", model: "sd2", backend: "b2", stream: false, status: 503, latencyMs: 50, promptTokens: 0, completionTokens: 0, cost: 0, errorKind: "rate" }),
        JSON.stringify({ t: day3 + 13 * 3600000, callId: "s4", model: "sd2", backend: "b2", stream: false, status: 200, latencyMs: 90, promptTokens: 500, completionTokens: 25, cacheHit: true, promptCacheHitTokens: 500, promptCacheMissTokens: 0, cost: 0.000005, cacheSavings: 0.0001, offPeakSavings: 0.000005 }),
        JSON.stringify({ t: curHour + 3000, model: "sd1", backend: "b1", stream: false, status: 200, latencyMs: 60, session: "legacy" }), // legacy shape: no tokens/cost/cache fields
      ];
      await writeFile(histPath, rows.join("\n") + "\n");
      const cfgSeed = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [
          { id: "b1", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_B1" },
          { id: "b2", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_B2" },
        ],
        models: {
          "sd1": { providers: [{ backend: "b1", upstream: "u1" }], meta: { label: "Seeded Model One" } },
          "sd2": { providers: [{ backend: "b2", upstream: "u2" }], meta: { label: "Seeded Model Two" } },
        },
        backoff: BO,
      };
      const { child: childM1, base: baseM1, dir: dirM1 } = await startRouterCfg(cfgSeed, "KEY_B1=k\nKEY_B2=k\n", { ROUTER_HISTORY: histPath });
      try {
        const d = await (await api(baseM1, "/api/dashboard", { method: "GET" })).json();
        assert(d.kpis.requests === 5 && d.kpis.ok === 4 && d.kpis.errors === 1, "zM1a) seed: kpis.requests counts all 5 rows incl. legacy, ok/errors split (got " + d.kpis.requests + "/" + d.kpis.ok + "/" + d.kpis.errors + ")");
        assert(d.kpis.activeModelCount === 2 && d.kpis.promptTokens === 1700 && d.kpis.completionTokens === 85, "zM1b) seed: activeModelCount + token sums (got models " + d.kpis.activeModelCount + ")");
        assert(Math.abs(d.kpis.cost - 0.00014475) < 1e-12 && Math.abs(d.kpis.cacheHitRate - 900 / 1700) < 1e-9, "zM1c) seed: kpis.cost sums priced rows, cacheHitRate 900/1700 (got " + d.kpis.cost + ", " + d.kpis.cacheHitRate + ")");
        const hb = d.hourly.find((x) => x.t === curHour);
        assert(hb && hb.requests === 3 && hb.promptTokens === 1200 && hb.completionTokens === 60 && hb.cacheHitTokens === 400 && hb.cacheMissTokens === 800, "zM1d) seed: current-hour bucket holds the 3 rows of this hour (req " + (hb && hb.requests) + ")");
        assert(hb && Math.abs(hb.cost - 0.00013975) < 1e-12 && hb.latencyAvgMs > 0 && hb.latencyMaxMs === 200, "zM1e) seed: hour bucket cost = priced sum, latencies aggregated (cost " + (hb && hb.cost) + ")");
        assert(d.hourly.length === 24 && d.hourly[0].t === curHour - 23 * 3600000, "zM1f) seed: hourly is exactly 24 slots oldest first (got " + d.hourly.length + ")");
        const db3 = d.daily.find((x) => x.t === day3);
        assert(db3 && db3.requests === 2 && db3.tokens === 525 && db3.errors === 1 && Math.abs(db3.cost - 0.000005) < 1e-12, "zM1g) seed: daily includes the 3-days-ago slot with seeded totals (req " + (db3 && db3.requests) + ")");
        assert(d.daily.length === 30 && d.daily[29].t === curDay, "zM1h) seed: daily is exactly 30 slots ending today (got " + d.daily.length + ")");
        const m1 = d.byModel.find((x) => x.id === "sd1");
        const m2 = d.byModel.find((x) => x.id === "sd2");
        assert(m1 && m1.label === "Seeded Model One" && m1.requests === 3 && m1.promptTokens === 1200 && m1.ok === 3 && m1.errorRate === 0, "zM1i) seed: byModel label from meta, legacy row counted (got " + JSON.stringify(m1 && { l: m1.label, r: m1.requests }) + ")");
        assert(m2 && m2.label === "Seeded Model Two" && m2.requests === 2 && m2.errors === 1 && m2.cost === 0.000005, "zM1j) seed: byModel error/ok split (got " + JSON.stringify(m2 && { r: m2.requests, e: m2.errors }) + ")");
        assert(d.byBackend.length === 2 && d.byBackend.every((x) => x.state === "healthy" && x.requests > 0), "zM1k) seed: byBackend joins health for both known backends (got " + d.byBackend.length + " rows)");
      } finally {
        childM1.kill();
        await rm(dirM1, { recursive: true, force: true });
        await rm(seedDir, { recursive: true, force: true });
      }
    }

    // zM2) live aggregation: a priced request (cache split, off-peak) flows
    //      into kpis, byModel/byBackend rows, the current-hour bucket and the
    //      current-day bucket with EXACT cost numbers. Pristine dashboard
    //      shape: nulls where no data, zeros elsewhere.
    {
      const srvM2 = http.createServer(async (req, res) => {
        for await (const c of req) { /* drain */ }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          id: "zM2", object: "chat.completion",
          choices: [{ message: { role: "assistant", content: "ok" } }],
          usage: { prompt_tokens: 1000, completion_tokens: 50, prompt_cache_hit_tokens: 400, prompt_cache_miss_tokens: 600 },
        }));
      });
      const portM2 = await listen(srvM2);
      const cfgM2 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "zb", baseURL: `http://${HOST}:${portM2}`, apiKeyEnv: "KEY_ZB" }],
        models: {
          "mc": { providers: [{ backend: "zb", upstream: "u" }], meta: { label: "Cost Model", pricing: { inputPerM: 0.28, cacheHitInputPerM: 0.07, outputPerM: 0.87, offPeak: true } } },
        },
        pricing: { peakWindows: [] },
        backoff: BO,
      };
      const { child: childM2, base: baseM2, dir: dirM2 } = await startRouterCfg(cfgM2, "KEY_ZB=k\n");
      try {
        const d0 = await (await api(baseM2, "/api/dashboard", { method: "GET" })).json();
        assert(d0.kpis.requests === 0 && d0.kpis.errorRate === null && d0.kpis.cacheHitRate === null && d0.kpis.cost === null && d0.kpis.offPeakShare === null, "zM2a) pristine dashboard: requests 0, rates/cost null (got req " + d0.kpis.requests + ", er " + d0.kpis.errorRate + ")");
        assert(d0.byBackend.length === 1 && d0.byBackend[0].id === "zb" && d0.byBackend[0].requests === 0 && d0.byBackend[0].cost === null && d0.byBackend[0].state === "healthy", "zM2b) byBackend joins health: single zeroed row for the known backend (got " + d0.byBackend.length + ")");
        assert(d0.byModel.length === 0 && d0.hourly.length === 24 && d0.daily.length === 30 && d0.cooling.length === 0, "zM2c) windows materialized, cooling ring empty (hourly " + d0.hourly.length + "/daily " + d0.daily.length + ")");
        const rrM2 = await api(baseM2, "/v1/chat/completions", { body: { model: "mc", messages: [] } });
        assert(rrM2.status === 200, "zM2d) priced request succeeds (status=" + rrM2.status + ")");
        const d = await (await api(baseM2, "/api/dashboard", { method: "GET" })).json();
        assert(d.kpis.requests === 1 && d.kpis.ok === 1 && d.kpis.errors === 0 && d.kpis.errorRate === 0, "zM2e) kpis: 1 ok request, errorRate 0 (got " + JSON.stringify([d.kpis.requests, d.kpis.ok, d.kpis.errors, d.kpis.errorRate]) + ")");
        assert(d.kpis.promptTokens === 1000 && d.kpis.completionTokens === 50 && d.kpis.cacheHitTokens === 400 && d.kpis.cacheMissTokens === 600 && d.kpis.cacheHitRate === 0.4, "zM2f) kpis tokens + cacheHitRate 400/1000 (got rate " + d.kpis.cacheHitRate + ")");
        assert(d.kpis.cost === 0.00011975 && d.kpis.cacheSavings === 0.000084 && d.kpis.offPeakSavings === 0.00011975 && d.kpis.offPeakRequests === 1 && d.kpis.offPeakShare === 1 && d.kpis.cooledBackends === 0, "zM2g) kpis cost fields exact (got cost " + d.kpis.cost + ")");
        const bm = d.byModel.find((x) => x.id === "mc");
        assert(bm && bm.label === "Cost Model" && bm.requests === 1 && bm.ok === 1 && bm.promptTokens === 1000 && bm.completionTokens === 50 && bm.cost === 0.00011975 && bm.cacheHitRate === 0.4, "zM2h) byModel row: label from meta + exact tokens/cost (got " + JSON.stringify(bm && { r: bm.requests, c: bm.cost }) + ")");
        const bb = d.byBackend.find((x) => x.id === "zb");
        assert(bb && bb.requests === 1 && bb.ok === 1 && bb.errors === 0 && bb.errorRate === 0 && bb.cost === 0.00011975 && bb.cacheHits === 1 && bb.cacheKnown === 1 && bb.cacheHitRate === 0.4 && bb.state === "healthy" && bb.lastErrorKind === null && bb.latencyAvgMs > 0 && bb.latencyMaxMs >= bb.latencyAvgMs, "zM2i) byBackend row: traffic merged onto the health join (got " + JSON.stringify(bb && { r: bb.requests, c: bb.cost, s: bb.state }) + ")");
        const histM2 = await (await api(baseM2, "/api/history?limit=5", { method: "GET" })).json();
        const eM2 = (histM2.entries || []).find((x) => x.model === "mc");
        const slotM2 = Math.floor((eM2 ? eM2.t : Date.now()) / 3600000) * 3600000;
        const hbM2 = d.hourly.find((x) => x.t === slotM2);
        assert(hbM2 && hbM2.requests === 1 && hbM2.cost === 0.00011975 && hbM2.promptTokens === 1000 && hbM2.latencyAvgMs > 0 && hbM2.latencyMaxMs >= hbM2.latencyAvgMs, "zM2j) current-hour bucket holds the live request (slot " + slotM2 + ", req " + (hbM2 && hbM2.requests) + ")");
        const daySlotM2 = Math.floor((eM2 ? eM2.t : Date.now()) / 86400000) * 86400000;
        const dbM2 = d.daily.find((x) => x.t === daySlotM2);
        assert(dbM2 && dbM2.requests === 1 && dbM2.tokens === 1050 && dbM2.cost === 0.00011975 && dbM2.cacheHitRate === 0.4 && dbM2.errors === 0, "zM2k) current-day bucket holds the live request (tokens " + (dbM2 && dbM2.tokens) + ")");
      } finally {
        childM2.kill();
        srvM2.close();
        await rm(dirM2, { recursive: true, force: true });
      }
    }

    // zM3) error counting + cache denominators: an exhausted 5xx flow bumps
    //      kpis.errors/errorRate; a signal request sets cacheHitRate 0.4 and a
    //      signal-less request (no usage at all) must NOT corrupt it. A 5xx is used
    //      because its backoff is genuinely configurable (see backoffFor: the 429
    //      class reads "rateBaseMs" while configs set "rateLimitBaseMs", so a 429
    //      would pin for the 30s default and this fixture could not wait it out).
    {
      let callsM3 = 0;
      const srvM3 = http.createServer(async (req, res) => {
        for await (const c of req) { /* drain */ }
        callsM3++;
        if (callsM3 === 1) {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: { message: "upstream exploded" } }));
          return;
        }
        if (callsM3 === 2) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ id: "zM3", choices: [{ message: { role: "assistant", content: "hit" } }], usage: { prompt_tokens: 1000, completion_tokens: 50, prompt_cache_hit_tokens: 400, prompt_cache_miss_tokens: 600 } }));
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ id: "zM3b", choices: [{ message: { role: "assistant", content: "nosig" } }] })); // no usage -> no cache signal
      });
      const portM3 = await listen(srvM3);
      const cfgM3 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "zb", baseURL: `http://${HOST}:${portM3}`, apiKeyEnv: "KEY_ZB" }],
        models: { "mf": { providers: [{ backend: "zb", upstream: "u" }] } },
        backoff: BO,
      };
      const { child: childM3, base: baseM3, dir: dirM3 } = await startRouterCfg(cfgM3, "KEY_ZB=k\n");
      try {
        await api(baseM3, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // 503 exhausted
        await new Promise((res) => setTimeout(res, 120)); // let the route backoff lapse
        await api(baseM3, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // 200 with cache split
        await api(baseM3, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // 200 without any usage
        const d = await (await api(baseM3, "/api/dashboard", { method: "GET" })).json();
        assert(d.kpis.requests === 3 && d.kpis.ok === 2 && d.kpis.errors === 1 && d.kpis.errorRate === 1 / 3, "zM3a) error counting: 503 exhaust bumps errors, errorRate 1/3 (got " + JSON.stringify([d.kpis.requests, d.kpis.ok, d.kpis.errors, d.kpis.errorRate]) + ")");
        assert(d.kpis.cacheHitRate === 0.4 && d.kpis.cacheHitTokens === 400 && d.kpis.cacheMissTokens === 600, "zM3b) cache denominator: signal request sets 0.4, no-signal request leaves it untouched (got rate " + d.kpis.cacheHitRate + ", hit " + d.kpis.cacheHitTokens + ")");
        assert(d.kpis.cost === null && d.kpis.cacheSavings === null && d.kpis.offPeakSavings === null, "zM3c) unpriced traffic keeps cost null (got " + d.kpis.cost + ")");
        const bb = d.byBackend.find((x) => x.id === "zb");
        assert(bb && bb.requests === 3 && bb.ok === 2 && bb.errors === 1 && bb.errorRate === 1 / 3 && bb.cacheHits === 1 && bb.cacheKnown === 1, "zM3d) byBackend mirrors the error/cache split (got " + JSON.stringify(bb && { r: bb.requests, e: bb.errors }) + ")");
      } finally {
        childM3.kill();
        srvM3.close();
        await rm(dirM3, { recursive: true, force: true });
      }
    }

    // zM4) cooling ring: 429 rate-limit cools -> ring {action cool, kind rate};
    //      after the short backoff elapses the next request observes the lazy
    //      expiry (expire event) and re-cools; admin uncool records the uncool
    //      event newest-first. Events keep {t, backend, action, kind, manual}.
    {
      const srvM4 = http.createServer(async (req, res) => {
        for await (const c of req) { /* drain */ }
        res.writeHead(429, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "rate limited" } }));
      });
      const portM4 = await listen(srvM4);
      const cfgM4 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "zb", baseURL: `http://${HOST}:${portM4}`, apiKeyEnv: "KEY_ZB" }],
        models: { "mf": { providers: [{ backend: "zb", upstream: "u" }] } },
        // rateBaseMs/rateMaxMs are the keys backoffFor actually reads (the
        // suite-wide BO's rateLimit* names are silently ignored - a pre-existing
        // mismatch) so the cool here really is short and the lazy expiry is
        // observable within the test.
        backoff: { rateBaseMs: 50, rateMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
      };
      const { child: childM4, base: baseM4, dir: dirM4 } = await startRouterCfg(cfgM4, "KEY_ZB=k\n");
      try {
        await api(baseM4, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // 503 exhausted -> cool
        await sleep(300); // outlast the 50ms rate backoff so the next request sees it expired
        await api(baseM4, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // expired -> expire -> retried -> re-cooled -> 503
        const d = await (await api(baseM4, "/api/dashboard", { method: "GET" })).json();
        const evts = d.cooling;
        assert(evts.length === 3, "zM4a) cooling ring: cool + expire + recool recorded (got " + evts.length + ")");
        assert(evts[0] && evts[0].action === "cool" && evts[0].kind === "rate" && evts[0].backend === "zb" && evts[0].manual === false && typeof evts[0].t === "number", "zM4b) newest first: the re-cool heads the ring (got " + JSON.stringify(evts[0]) + ")");
        assert(evts[1] && evts[1].action === "expire" && evts[1].backend === "zb" && evts[1].kind === "rate" && evts[1].manual === false, "zM4c) lazy expiry lands between the cools (got " + JSON.stringify(evts[1]) + ")");
        assert(evts[2] && evts[2].action === "cool" && evts[2].kind === "rate", "zM4d) the first cool trails (got " + JSON.stringify(evts[2]) + ")");
        const ru = await api(baseM4, "/admin/backend", { method: "POST", body: { id: "zb", action: "uncool" } });
        assert(ru.status === 200, "zM4e) admin uncool ok (status=" + ru.status + ")");
        const d2 = await (await api(baseM4, "/api/dashboard", { method: "GET" })).json();
        const ev2 = d2.cooling;
        assert(ev2[0] && ev2[0].action === "uncool" && ev2[0].backend === "zb" && ev2[0].kind === "rate" && ev2[0].manual === false, "zM4f) uncool event newest-first with kind from the original failure (got " + JSON.stringify(ev2[0]) + ")");
        assert(ev2.length === evts.length + 1 && ev2.slice(1).every((x, i) => x.action === evts[i].action && x.backend === evts[i].backend && x.kind === evts[i].kind), "zM4g) ring order preserved, exactly one new event (got " + ev2.length + ")");
      } finally {
        childM4.kill();
        srvM4.close();
        await rm(dirM4, { recursive: true, force: true });
      }
    }

    // zM5) pricing echo: offPeakMultiplier + peakWindows mirrored from the
    //      config, modelsWithOffPeak = ids whose meta.pricing.offPeak is true.
    {
      const cfgM5 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "pb", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_PB" }],
        models: {
          "pm1": { providers: [{ backend: "pb", upstream: "u1" }], meta: { pricing: { inputPerM: 1, outputPerM: 1, offPeak: true } } },
          "pm2": { providers: [{ backend: "pb", upstream: "u2" }], meta: { pricing: { inputPerM: 1, outputPerM: 1 } } },
          "pm3": { providers: [{ backend: "pb", upstream: "u3" }] },
        },
        pricing: { offPeakMultiplier: 0.35, peakWindows: [{ days: "1-5", start: "02:00", end: "05:00" }] },
        backoff: BO,
      };
      const { child: childM5, base: baseM5, dir: dirM5 } = await startRouterCfg(cfgM5, "KEY_PB=k\n");
      try {
        const d = await (await api(baseM5, "/api/dashboard", { method: "GET" })).json();
        assert(d.pricing.offPeakMultiplier === 0.35, "zM5a) pricing.offPeakMultiplier echoes config (got " + d.pricing.offPeakMultiplier + ")");
        assert(Array.isArray(d.pricing.peakWindows) && d.pricing.peakWindows.length === 1 && d.pricing.peakWindows[0].start === "02:00" && d.pricing.peakWindows[0].end === "05:00", "zM5b) pricing.peakWindows echoes config (got " + JSON.stringify(d.pricing.peakWindows) + ")");
        assert(Array.isArray(d.pricing.modelsWithOffPeak) && d.pricing.modelsWithOffPeak.length === 1 && d.pricing.modelsWithOffPeak[0] === "pm1", "zM5c) modelsWithOffPeak = ids with meta.pricing.offPeak true (got " + JSON.stringify(d.pricing.modelsWithOffPeak) + ")");
      } finally {
        childM5.kill();
        await rm(dirM5, { recursive: true, force: true });
      }
    }

    // zM6) byBackend health join: a long-cooling backend (429, 60s backoff)
    //      renders state cooling / fails 1 / lastErrorKind rate, and
    //      kpis.cooledBackends counts it.
    {
      const srvM6 = http.createServer(async (req, res) => {
        for await (const c of req) { /* drain */ }
        res.writeHead(429, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "rate limited" } }));
      });
      const portM6 = await listen(srvM6);
      const cfgM6 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "zb", baseURL: `http://${HOST}:${portM6}`, apiKeyEnv: "KEY_ZB" }],
        models: { "mf": { providers: [{ backend: "zb", upstream: "u" }] } },
        backoff: { rateBaseMs: 60000, rateMaxMs: 120000, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
      };
      const { child: childM6, base: baseM6, dir: dirM6 } = await startRouterCfg(cfgM6, "KEY_ZB=k\n");
      try {
        await api(baseM6, "/v1/chat/completions", { body: { model: "mf", messages: [] } }); // 503 -> cools for 60s
        const d = await (await api(baseM6, "/api/dashboard", { method: "GET" })).json();
        const row = d.byBackend.find((x) => x.id === "zb");
        assert(row && row.state === "cooling" && row.fails === 1 && row.lastErrorKind === "rate" && row.manual === false && row.requests === 1 && row.errors === 1 && row.errorRate === 1, "zM6a) byBackend joins health: 429-cooled backend renders cooling (got " + JSON.stringify(row && { s: row.state, f: row.fails, lk: row.lastErrorKind }) + ")");
        assert(d.kpis.cooledBackends === 1, "zM6b) kpis.cooledBackends counts the cooling backend (got " + d.kpis.cooledBackends + ")");
      } finally {
        childM6.kill();
        srvM6.close();
        await rm(dirM6, { recursive: true, force: true });
      }
    }

    // zM7) Console Go compliance: upstream requests carry x-opencode-session
    //      (client header wins, else the resolved session key) and an
    //      identifiable user-agent instead of Node's default.
    {
      const srvM7 = mockBackend("sess-p", {});
      const portM7 = await listen(srvM7.srv);
      const cfgM7 = {
        port: 0, prefix: "/v1", masterKeyEnv: null,
        backends: [{ id: "sess-p", baseURL: `http://${HOST}:${portM7}`, apiKeyEnv: "KEY_SESS_P" }],
        models: { "mf": { providers: [{ backend: "sess-p", upstream: "u" }] } },
        backoff: BO,
      };
      const { child: childM7, base: baseM7, dir: dirM7 } = await startRouterCfg(cfgM7, "KEY_SESS_P=k\n");
      try {
        await api(baseM7, "/v1/chat/completions", { body: { model: "mf", messages: [] }, headers: { "x-session-affinity": "sess-X" } });
        let rec = srvM7.hits[srvM7.hits.length - 1];
        assert(rec.ocSession === "sess-X", "zM7a) x-opencode-session = session key (x-session-affinity) (got " + rec.ocSession + ")");
        assert(rec.ua && !rec.ua.toLowerCase().startsWith("node"), "zM7b) user-agent is identifiable, not Node default (got " + rec.ua + ")");

        await api(baseM7, "/v1/chat/completions", { body: { model: "mf", messages: [] } });
        rec = srvM7.hits[srvM7.hits.length - 1];
        assert(rec.ocSession === "no-session", "zM7c) no client session -> x-opencode-session no-session (got " + rec.ocSession + ")");

        await api(baseM7, "/v1/chat/completions", { body: { model: "mf", messages: [] }, headers: { "x-opencode-session": "client-oc" } });
        rec = srvM7.hits[srvM7.hits.length - 1];
        assert(rec.ocSession === "client-oc", "zM7d) client x-opencode-session header passes through (got " + rec.ocSession + ")");
      } finally {
        childM7.kill();
        srvM7.srv.close();
        await rm(dirM7, { recursive: true, force: true });
      }
    }
  }

  // ---- config editor (web UI write path) ------------------------------------
  // Exercises /api/config/raw, POST /api/config and POST /api/config/reset
  // against a real child server on a temp config. The invariant under test is
  // that a REJECTED save leaves the file byte-identical.
  {
    const dirCE = await mkdtemp(join(tmpdir(), "lmr-cfg-"));
    const cfgPathCE = join(dirCE, "config.json");
    const cfgCE = {
      port: 0,
      prefix: "/v1",
      masterKeyEnv: null,
      uiPasswordEnv: null,
      backends: [
        { id: "ce-a", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_CE_A" },
        { id: "ce-b", baseURL: "http://127.0.0.1:2", apiKeyEnv: "KEY_CE_B" },
      ],
      models: {
        "ce-model": { providers: [{ backend: "ce-a", upstream: "u1" }], affinityPool: 1 },
      },
      presets: {
        "ce-preset": { strategy: "affinity", models: ["ce-model"] },
      },
      // Blocks that the NORMALIZED /api/config does not model. Their survival
      // through a save is the regression guard for the raw-load rule.
      pricing: { offPeakMultiplier: 0.5, peakWindows: [{ days: [1, 2, 3, 4, 5], startHour: 0, endHour: 8 }] },
      missCapture: { enabled: true, maxMissPct: 50, minMissTokens: 1000, file: "router-misses.jsonl", maxFileBytes: 25000000 },
      alerts: { ntfy: { baseUrl: "https://ntfy.sh", topic: "ce-topic", minMissPct: 50, minMissTokens: 20000, cooldownMs: 600000 } },
      backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
    };
    await writeFile(cfgPathCE, JSON.stringify(cfgCE, null, 2) + "\n");
    await writeFile(join(dirCE, ".env"), "KEY_CE_A=secret-value-a\nKEY_CE_B=secret-value-b\n");
    const childCE = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPathCE, ROUTER_ENV: join(dirCE, ".env") },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let baseCE = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("config-editor server did not start")), 8000);
      childCE.stderr.on("data", (d) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { baseCE = `http://${HOST}:${parseInt(m[1], 10)}`; clearTimeout(timer); resolve(); }
      });
    });

    const readCfg = async () => JSON.parse(await readFile(cfgPathCE, "utf8"));
    const post = (path, body) => api(baseCE, path, { body });
    const rawOnDisk = async () => await readFile(cfgPathCE, "utf8");

    try {
      // CE1) raw read is byte-faithful for the blocks /api/config drops
      let r1 = await fetch(baseCE + "/api/config/raw");
      const raw1 = await r1.json();
      assert(r1.status === 200 && raw1.config, "zCE1a) GET /api/config/raw returns the config");
      assert(
        JSON.stringify(raw1.config.pricing) === JSON.stringify(cfgCE.pricing) &&
          JSON.stringify(raw1.config.missCapture) === JSON.stringify(cfgCE.missCapture) &&
          JSON.stringify(raw1.config.alerts) === JSON.stringify(cfgCE.alerts),
        "zCE1b) raw read preserves pricing/missCapture/alerts that /api/config drops"
      );
      assert(typeof raw1.revision === "number" && raw1.revision > 0, "zCE1c) raw read returns a revision (mtime)");
      assert(Array.isArray(raw1.envNames) && raw1.envNames.includes("KEY_CE_A"), "zCE1d) raw read lists env var names for the key picker");

      // CE2) a valid save lands on disk and keeps the unmodeled blocks
      const draft = JSON.parse(JSON.stringify(raw1.config));
      draft.backends.push({ id: "ce-c", baseURL: "http://127.0.0.1:3", apiKeyEnv: "KEY_CE_A" });
      draft.models["ce-model2"] = { providers: [{ backend: "ce-c", upstream: "u3" }] };
      let r2 = await post("/api/config", { config: draft, baseRevision: raw1.revision });
      const b2 = await r2.json();
      assert(r2.status === 200 && b2.ok, "zCE2a) valid save returns 200 ok (" + r2.status + ")");
      const onDisk2 = await readCfg();
      assert(onDisk2.backends.some((b) => b.id === "ce-c"), "zCE2b) new backend is on disk after save");
      assert(!!onDisk2.models["ce-model2"], "zCE2c) new model is on disk after save");
      assert(JSON.stringify(onDisk2.pricing) === JSON.stringify(cfgCE.pricing), "zCE2d) pricing survives the save");
      assert(JSON.stringify(onDisk2.missCapture) === JSON.stringify(cfgCE.missCapture), "zCE2e) missCapture survives the save");
      assert(!!b2.snapshot, "zCE2f) a snapshot is taken on save");

      // CE3) the saved config is picked up by hot-reload (/api/config reflects it)
      await new Promise((r) => setTimeout(r, 700));
      const live = await (await fetch(baseCE + "/api/config")).json();
      assert(live.backends.some((b) => b.id === "ce-c"), "zCE3a) hot-reload picks up the saved backend");

      // CE4) unset apiKeyEnv -> 400 AND the file is byte-identical
      const before4 = await rawOnDisk();
      const draft4 = JSON.parse(JSON.stringify(onDisk2));
      draft4.backends.push({ id: "ce-nokey", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_DOES_NOT_EXIST" });
      const r4 = await post("/api/config", { config: draft4, force: true });
      const b4 = await r4.json();
      assert(r4.status === 400 && b4.ok === false, "zCE4a) backend with an unset apiKeyEnv is rejected (400)");
      assert((b4.errors || []).some((e) => /apiKeyEnv/.test(e.path)), "zCE4b) the error points at apiKeyEnv");
      assert((await rawOnDisk()) === before4, "zCE4c) rejected save left the file byte-identical");

      // CE5) dangling references are rejected
      const mkBad = async (mutate, label) => {
        const b4raw = await rawOnDisk();
        const dd = JSON.parse(b4raw);
        mutate(dd);
        const rr = await post("/api/config", { config: dd, force: true });
        assert(rr.status === 400, "zCE5 " + label + " rejected (400, got " + rr.status + ")");
        assert((await rawOnDisk()) === b4raw, "zCE5 " + label + " left the file unchanged");
      };
      await mkBad((d) => { d.models["ce-model"].providers = [{ backend: "ghost", upstream: "u" }]; }, "a) unknown provider backend");
      await mkBad((d) => { d.presets["ce-preset"].models = ["does-not-exist"]; }, "b) preset with an unknown model");
      await mkBad((d) => { d.missCapture.file = "../escape.jsonl"; }, "c) missCapture.file traversal");
      await mkBad((d) => { d.port = 9999; }, "d) restart-only port change");
      await mkBad((d) => { d.backends.push({ id: "ce-c", baseURL: "http://x", apiKeyEnv: "KEY_CE_A" }); }, "e) duplicate backend id");
      await mkBad((d) => { d.missCapture.file = "sub/x.jsonl"; }, "f) missCapture.file with a separator in a relative name");
      await mkBad((d) => { d.missCapture.file = "/data/../etc/router-misses.jsonl"; }, "g) missCapture.file with '..' inside an absolute path");
      // zCE5h) an ABSOLUTE missCapture.file is legal. The live homelab config
      // points at /data/history/router-misses.jsonl on a durable named volume;
      // rejecting it made EVERY config-editor save return 400 in production.
      const dAbsCE = JSON.parse(await rawOnDisk());
      dAbsCE.missCapture.file = "/data/history/router-misses.jsonl";
      const rAbsCE = await post("/api/config", { config: dAbsCE, dryRun: true });
      const bAbsCE = await rAbsCE.json();
      assert(
        rAbsCE.status === 200 && bAbsCE.ok === true,
        "zCE5 h) absolute missCapture.file accepted (" + rAbsCE.status + ", errors=" + JSON.stringify((bAbsCE.errors || []).map((e) => e.path)) + ")"
      );

      // CE6) stale revision -> 409 with the current config, no write
      const before6 = await rawOnDisk();
      const draft6 = JSON.parse(before6);
      draft6.prefix = "/v9";
      const r6 = await post("/api/config", { config: draft6, baseRevision: 1 });
      const b6 = await r6.json();
      assert(r6.status === 409 && b6.error && b6.error.type === "conflict", "zCE6a) stale baseRevision -> 409 conflict");
      assert(b6.current && typeof b6.current === "object", "zCE6b) conflict response carries the current config");
      assert((await rawOnDisk()) === before6, "zCE6c) conflicted save left the file unchanged");
      const r6b = await post("/api/config", { config: draft6, baseRevision: 1, force: true });
      assert(r6b.status === 200, "zCE6d) the same save with force:true succeeds");

      // CE7) dryRun validates without writing
      const before7 = await rawOnDisk();
      const draft7 = JSON.parse(before7);
      draft7.presets["ce-preset"].models = ["nope"];
      const r7 = await post("/api/config", { config: draft7, dryRun: true });
      const b7 = await r7.json();
      assert(r7.status === 400 && b7.ok === false, "zCE7a) dryRun reports validation errors");
      const draft7b = JSON.parse(before7);
      const r7b = await post("/api/config", { config: draft7b, dryRun: true });
      assert(r7b.status === 200, "zCE7b) dryRun on a valid config returns 200");
      assert((await rawOnDisk()) === before7, "zCE7c) dryRun never writes");

      // CE8) warnings are advisory, not fatal (empty preset models + unknown key)
      const draft8 = JSON.parse(await rawOnDisk());
      draft8.presets["ce-empty"] = { strategy: "affinity", models: [] };
      draft8.someUnknownKey = 1;
      const r8 = await post("/api/config", { config: draft8, force: true });
      const b8 = await r8.json();
      assert(r8.status === 200, "zCE8a) warnings do not block a save");
      assert((b8.warnings || []).length >= 2, "zCE8b) warnings are reported back to the UI");
      assert((await readCfg()).someUnknownKey === 1, "zCE8c) the warned-about key was still saved (it is advisory)");

      // CE9) snapshot restore returns the previous bytes
      const snaps = await (await fetch(baseCE + "/api/config/raw")).json();
      assert(Array.isArray(snaps.snapshots) && snaps.snapshots.length > 0, "zCE9a) snapshots are listed, newest first");
      const targetSnap = snaps.snapshots[snaps.snapshots.length - 1];
      const r9 = await post("/api/config/reset", { snapshot: targetSnap });
      const b9 = await r9.json();
      assert(r9.status === 200 && b9.ok, "zCE9b) restore returns 200 (" + r9.status + ")");
      const after9 = await readCfg();
      assert(!after9.someUnknownKey, "zCE9c) restore reverted the later save (unknown key gone)");
      assert(Array.isArray(after9.backends) && after9.backends.some((b) => b.id === "ce-a"), "zCE9d) restored config still has the base backends");

      // CE10) path traversal in the snapshot name is refused
      const r10 = await post("/api/config/reset", { snapshot: "../../config.json" });
      assert(r10.status === 400, "zCE10a) snapshot path traversal -> 400 (got " + r10.status + ")");
      const r10b = await post("/api/config/reset", { snapshot: "nope.json" });
      assert(r10b.status === 404, "zCE10b) unknown snapshot -> 404 (got " + r10b.status + ")");

      // reset hardening: the restore path is a SAVE-class write to the live
      // file, so it runs the same validator against what is actually on disk
      // and enforces the same lost-update guard. A snapshot is a config the
      // browser asked us to restore, not an escape from the save contract.
      const snaps35 = await (await fetch(baseCE + "/api/config/raw")).json();
      // The newest snapshot is the pre-reset state zCE9 replaced, so its bytes
      // DIFFER from the live file: any write on the refused path is visible.
      const newerSnap = snaps35.snapshots[0];
      const before35 = await rawOnDisk();
      const r35 = await post("/api/config/reset", { snapshot: newerSnap, baseRevision: 1 });
      const b35 = await r35.json();
      assert(
        r35.status === 409 && b35.error && b35.error.type === "conflict",
        "v35a) stale baseRevision on reset -> 409 conflict (got " + r35.status + ")"
      );
      assert((await rawOnDisk()) === before35, "v35a2) a conflicted reset did not touch the live file");
      const r35b = await post("/api/config/reset", { snapshot: newerSnap, baseRevision: 1, force: true });
      assert(r35b.status === 200, "v35b) forced reset with guards passes (got " + r35b.status + ")");

      // the startup-only key guard is part of the reset contract too: a snapshot
      // that changes `port` is refused with the field path instead of being
      // applied (validateConfig(restored, restored) made this vacuous).
      const snapPort = JSON.parse(await rawOnDisk());
      snapPort.port = 9999;
      await writeFile(join(dirCE, "config.history", "v35-port.json"), JSON.stringify(snapPort, null, 2) + "\n");
      const before35c = await rawOnDisk();
      const r35c = await post("/api/config/reset", { snapshot: "v35-port.json", force: true });
      const b35c = await r35c.json();
      assert(
        r35c.status === 400 && (b35c.errors || []).some((e) => e.path === "port"),
        "v35c) a snapshot that changes a startup-only key is refused (400, got " + r35c.status + ", errors=" + JSON.stringify((b35c.errors || []).map((e) => e.path)) + ")"
      );
      assert((await rawOnDisk()) === before35c, "v35c2) a refused reset left the live file byte-identical");

      // CE11) secret hygiene: no apiKeyEnv VALUE appears in any response body
      const bodies = [
        JSON.stringify(raw1), JSON.stringify(b2), JSON.stringify(b4),
        JSON.stringify(b6), JSON.stringify(b8), JSON.stringify(b9), JSON.stringify(snaps),
      ].join("\n");
      assert(!bodies.includes("secret-value-a") && !bodies.includes("secret-value-b"), "zCE11a) no apiKeyEnv value leaks into any config response");
      assert(bodies.includes("KEY_CE_A"), "zCE11b) the env var NAME is still served (the UI needs it)");

      // v39a)-v39f) bridge the WRITE half: saving and restoring go through the
      // same conversion the loader already runs, so an old-shape payload can
      // never land on disk as `providers` again. The two ways an old-shape
      // write arrives are covered separately - a browser save (v39a/v39b) and a
      // snapshot restore (v39e/v39f) - because they are independent entry
      // points onto the same helper.
      const draft39 = JSON.parse(await rawOnDisk());
      draft39.models["bridge-probe"] = { providers: [{ backend: "ce-a", upstream: "u-probe" }] };
      const r39 = await post("/api/config", { config: draft39, force: true });
      const b39 = await r39.json();
      assert(r39.status === 200, "v39a) old-shape save is accepted through the bridge (got " + r39.status + ", " + JSON.stringify(b39.errors || b39.error) + ")");
      const disk39 = JSON.parse(await rawOnDisk());
      assert(
        !!disk39.models["bridge-probe"].deployments &&
          disk39.models["bridge-probe"].providers === undefined &&
          Object.values(disk39.models).every((m) => m.providers === undefined),
        "v39b) a save writes v3, never a persisted `providers` block (" + JSON.stringify(disk39.models["bridge-probe"]) + ")"
      );

      // The bridge's own warnings reach the CALLER, not just the console: the
      // Config tab can only tell the operator what it rewrote if the save
      // response carries it. A repair nobody is told about is a silent edit of
      // the operator's file. The payload is the block's ORIGINAL old-shape
      // config - a clone of disk is already v3, so the bridge would not run and
      // this would prove nothing - carrying a real error-class empty string in
      // an object-typed position (`models[*].retry`), the exact editor artifact
      // the production config shipped with: left alone it would 400 the save,
      // and repaired silently it would rewrite the operator's file.
      const draft39c = JSON.parse(JSON.stringify(cfgCE));
      draft39c.models["bridge-probe"] = { providers: [{ backend: "ce-a", upstream: "u-probe" }], retry: "" };
      const expectBridge39c = convertLegacyConfig(draft39c).warnings;
      assert(expectBridge39c.length > 0, "v39c0) the repair fixture really makes the converter warn (" + JSON.stringify(expectBridge39c) + ")");
      const r39c = await post("/api/config", { config: draft39c, force: true });
      const b39c = await r39c.json();
      assert(r39c.status === 200, "v39c1) the repaired save itself succeeds (got " + r39c.status + ", " + JSON.stringify(b39c.errors || b39c.error) + ")");
      const gotBridge39c = (b39c.warnings || []).filter((w) => w.path === "(bridge)");
      assert(
        gotBridge39c.length === expectBridge39c.length && gotBridge39c.some((w) => String(w.message).includes("was an empty string")),
        "v39c2) the save response reports EVERY bridge warning, the repair included (got " + JSON.stringify(gotBridge39c.map((w) => w.message)) + ", expected " + expectBridge39c.length + ")"
      );
      const disk39c = JSON.parse(await rawOnDisk());
      assert(disk39c.models["bridge-probe"].retry === undefined, "v39c3) the repaired value is not on disk (" + JSON.stringify(disk39c.models["bridge-probe"]) + ")");
      assert(disk39c.models["bridge-probe"].deployments !== undefined, "v39c4) the repair and the conversion land together, not one without the other");

      // The one refusal the converter STILL has: a converted config whose id is
      // both a model and a preset cannot be resolved by rule. The fixture is
      // built from the block's ORIGINAL old-shape config - not from disk, which
      // is v3 by now - and is deliberately free of every OTHER validator error,
      // so the 400 below can only come from the bridge: at base an old-shape
      // preset/model id collision passes as a warning and the snapshot is
      // written straight through, which is the silent bypass this pair exists
      // to catch.
      const before39 = await rawOnDisk();
      const collisionSnap = JSON.parse(JSON.stringify(cfgCE));
      collisionSnap.presets["ce-model"] = { strategy: "failover", models: ["ce-model", "ce-preset"] };
      collisionSnap.x13Marker = "refused-snapshot-marker";
      await writeFile(join(dirCE, "config.history", "t13-collision.json"), JSON.stringify(collisionSnap, null, 2) + "\n");
      // Anti-vacuity: if the fixture is not old-shape (a clone of disk would
      // not be) the bridge never runs and the 400 below would prove nothing.
      assert(
        Object.values(collisionSnap.models).some((m) => Array.isArray(m.providers)),
        "v39d0) the refusal fixture really is an old-shape document, so the bridge is what judges it"
      );
      const r39d = await post("/api/config/reset", { snapshot: "t13-collision.json", force: true });
      const b39d = await r39d.json();
      assert(
        r39d.status === 400 && (b39d.errors || []).some((e) => String(e.message).includes("both a model and a preset")),
        "v39d) a snapshot the conversion refuses is refused (400, got " + r39d.status + ", " + JSON.stringify(b39d.errors || b39d.error) + ")"
      );
      const after39 = await rawOnDisk();
      assert(after39 === before39, "v39e) the refused restore left the live file byte-identical");
      assert(after39.indexOf("refused-snapshot-marker") === -1, "v39f) no part of the refused snapshot reached the file");

      // ...and the POSITIVE half, which is the actual gap: at base the restore
      // wrote the snapshot's `providers` verbatim and the preset kept the
      // v2-era `affinity` name, so a restored config was silently a shape the
      // v3 branch would then reject. Cloned from the block's ORIGINAL old-shape
      // fixture rather than from disk, because by now disk is already v3 and a
      // clone of it could not fail these assertions.
      const plainOld = JSON.parse(JSON.stringify(cfgCE));
      plainOld.models["ce-extra"] = { providers: [{ backend: "ce-b", upstream: "u-extra" }] };
      // `ce-preset` is an identity preset named exactly like a model, so the
      // conversion drops it. That makes the warning count a property of the
      // FIXTURE rather than of whatever the converter happens to say today,
      // while the assertion still counts against an INDEPENDENT conversion
      // instead of a hardcoded number that would drift.
      plainOld.presets["ce-model"] = { strategy: "failover", models: ["ce-model"] };
      await writeFile(join(dirCE, "config.history", "t13-plain-old.json"), JSON.stringify(plainOld, null, 2) + "\n");
      const expectBridge39g = convertLegacyConfig(plainOld).warnings;
      assert(expectBridge39g.length > 0, "v39j0) the restore fixture really makes the bridge warn (" + JSON.stringify(expectBridge39g) + ")");
      const r39g = await post("/api/config/reset", { snapshot: "t13-plain-old.json", force: true });
      const b39g = await r39g.json();
      assert(r39g.status === 200, "v39g) a valid old-shape snapshot is restored through the bridge (got " + r39g.status + ", " + JSON.stringify(b39g.errors || b39g.error) + ")");
      const restored39 = await readCfg();
      assert(
        !!restored39.models["ce-extra"].deployments && restored39.models["ce-extra"].providers === undefined,
        "v39h) the restored file is v3, not the snapshot's own shape (" + JSON.stringify(restored39.models["ce-extra"]) + ")"
      );
      assert(restored39.presets["ce-preset"].strategy === "fallback", "v39i) the restored preset carries the v3 strategy name (" + JSON.stringify(restored39.presets["ce-preset"]) + ")");
      assert(
        (b39g.warnings || []).filter((w) => w.path === "(bridge)").length === expectBridge39g.length,
        "v39j) every bridge warning the restore's conversion emits is reported to the caller (got " + JSON.stringify((b39g.warnings || []).map((w) => w.path + ":" + w.message)) + ", expected " + expectBridge39g.length + ")"
      );
    } finally {
      childCE.kill();
      await rm(dirCE, { recursive: true, force: true });
    }
  }

  // ---- legacy -> v3 bridge converter ----------------------------------------
  // The converter is the only bridge between today's two-layer config and the
  // three-layer shape, so its warnings are a contract: warn exactly when the
  // conversion changes routing behavior, stay silent when it does not. The
  // fixture also carries the three empty-string values the pre-fix config
  // editor used to write, because no config in the repo carries them and the
  // repair sweep would otherwise be unpinned.
  {
    // a served id is addressable when either layer still answers for it
    const addressable = (cfg, id) => Boolean(cfg.models[id]) || Boolean(cfg.presets[id]);
    const old = {
      backends: [
        { id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1" },
        { id: "b2", baseURL: "http://127.0.0.1:2", apiKeyEnv: "KEY_B2", params: "" },
        { id: "b3", baseURL: "http://127.0.0.1:3", apiKeyEnv: "KEY_B3" }
      ],
      models: {
        m1: { providers: [{ backend: "b1", upstream: "u" }, { backend: "b2", upstream: "u", params: "" }, { backend: "b3", upstream: "u" }], affinityPool: 2 },
        "kimi-k3": { providers: [{ backend: "b1", upstream: "u" }] },
        "glm-5.2": { providers: [{ backend: "b1", upstream: "u" }], retry: "" },
        "qwen3.7-plus": { providers: [{ backend: "b1", upstream: "u" }] }
      },
      presets: {
        "kimi-k3": { strategy: "affinity", models: [{ model: "kimi-k3", weight: 1 }] },
        w: { strategy: "weighted", models: ["m1"] },
        tier: { strategy: "failover", models: ["m1", "kimi-k3"] },
        amulti: { strategy: "affinity", models: ["m1", "kimi-k3"] },
        "muse-spark-1.3-contributor": { strategy: "affinity", models: ["glm-5.2"], params: "" },
        // the live config's actual shape: the empty-string preset params sits on
        // an IDENTITY preset, which step 1 deletes. Only the repair pass BEFORE
        // the mapping step can report it - the mapping step never sees it.
        "qwen3.7-plus": { strategy: "affinity", models: [{ model: "qwen3.7-plus", weight: 1 }], params: "" }
      },
      params: { temperature: 0.2 },
      backoff: { serverBaseMs: 50 }
    };
    const r = convertLegacyConfig(old);
    assert(r.config.models.m1.deployments.length === 3, "v36a) providers map 1:1 to deployments");
    assert(r.config.models.m1.deployments[2].backupOnly === true, "v36b) affinityPool 2 of 3 marks providers[2..] backupOnly");
    assert(!r.config.presets["kimi-k3"], "v36c) identity preset dropped, model stays addressable");
    assert(r.config.presets.w.strategy === "fallback", "v36d) weighted preset maps to fallback silently (order preserved; per-ref weights warned separately)");
    assert(r.warnings.some((w) => w.includes("kimi-k3")), "v36e) identity drop warned first");
    assert(r.config.params.temperature === 0.2 && r.config.backoff.serverBaseMs === 50, "v36f) untouched blocks carried through");
    // Warn ONLY on behavior changes: an affinity preset with MORE THAN ONE ref
    // loses its session spread under ordered fallback. The live mirror's five
    // affinity presets are each single-ref, so none is a behavior change (the
    // three that are identity presets fell to step 1, and the two aliases keep
    // behaving exactly as before). failover->fallback and weighted->fallback
    // preserve declared order, so they stay silent.
    assert(r.warnings.some((w) => w.includes("amulti")), "v36g) affinity multi-model preset warns (session-spread lost)");
    assert(!r.warnings.some((w) => w.includes("'tier'")), "v36h) failover->fallback does not warn (order preserved)");
    assert(!r.warnings.some((w) => w.includes("'w'") && w.includes("strategy")), "v36i) weighted->fallback does not warn (order preserved)");
    // the empty-string repair sweep: the pre-fix editor wrote "" for any JSON
    // textarea it rendered empty, and a model-level retry/params "" is a
    // save-mode ERROR that blocks EVERY save (the whole candidate is
    // validated), so the sweep has to drop every position it can reach.
    assert(r.config.models["glm-5.2"].retry === undefined, "v36j) a model-level empty-string retry is dropped");
    assert(old.backends[1].params === "", "v36k2) the sweep does not mutate its input");
    assert(r.config.backends[1].params === undefined, "v36k) a backend-level empty-string params is dropped");
    assert(r.config.models.m1.deployments[1].params === undefined, "v36k3) a DEPLOYMENT-level empty-string params is dropped (the provider position maps into the deployment)");
    assert(r.config.presets["muse-spark-1.3-contributor"].params === undefined, "v36l) a preset-level empty-string params is dropped");
    assert(
      r.warnings.filter((w) => w.includes("was an empty string")).length === 5,
      "v36m) every repaired value warns, all five from one shared sentence (" + JSON.stringify(r.warnings.filter((w) => w.includes("was an empty string"))) + ")"
    );
    // the repair pass runs BEFORE the identity drop and before the mapping step.
    // The live config's shape is exactly this: an empty-string preset params on
    // a preset that is ALSO an identity preset, so the mapping step never sees
    // it. Only this assertion makes the early pass load-bearing - with the
    // mapping step's own silent delete in place, the drop is otherwise invisible.
    const idRepair = r.warnings.filter((w) => w.includes("presets.qwen3.7-plus.params"));
    assert(
      idRepair.length === 1 && idRepair[0].endsWith("was an empty string (an older config editor wrote it); dropped - it was already ignored at runtime"),
      "v36m2) an empty-string params on an IDENTITY preset is still repaired and reported (" + JSON.stringify(idRepair) + ")"
    );
    assert(
      !r.config.presets["qwen3.7-plus"] && r.config.models["qwen3.7-plus"],
      "v36m3) and that preset is still dropped as an identity, its id served by the model"
    );
    assert(
      r.warnings.filter((w) => w.includes("presets.muse-spark-1.3-contributor.params")).length === 1,
      "v36m4) a surviving preset's repair is reported exactly once (no double warn)"
    );
    // a non-object preset params (e.g. the number 5, which validateConfig
    // rejects outright) must not survive conversion either: the repair pass
    // owns empty strings and the mapping step owns everything else present.
    const oddPreset = JSON.parse(JSON.stringify(old));
    oddPreset.presets.tier.params = 5;
    const r3 = convertLegacyConfig(oddPreset);
    assert(r3.config.presets.tier.params === undefined, "v36m5) a non-object preset params is dropped by the mapping step");
    // step 5: a collision that survives the identity drop is refused, not guessed.
    let refused = null;
    try {
      const clash = JSON.parse(JSON.stringify(old));
      clash.presets.tier.models = ["m1", "tier"];
      clash.models.tier = { providers: [{ backend: "b1", upstream: "u" }] };
      convertLegacyConfig(clash);
    } catch (e) {
      refused = e;
    }
    assert(refused && /conversion refused/.test(refused.message) && refused.message.includes("'tier'"), "v36n) a residual model/preset collision is refused");
    assert(r.config.models.m1.exposed === true && r.config.presets.w.exposed === true, "v36o) converted entries default to exposed");
    // Structural completeness: the conversion FINISHED rather than merely ran.
    // v3-only validator checks are Task 10b's, so what this block can pin today
    // is that no legacy key survived anywhere and no empty-string artifact did
    // either. Scope note: the sweep is defined over the value positions the
    // converter owns (backend, model, deployment, preset), NOT over arbitrary
    // nested UI data - a nested `meta.pricing.offPeak: ""` in the live file is
    // deliberately left alone (it is only ever read as === true), so this
    // assertion is about the fixture's value positions, not a global property.
    {
      const emptyAt = [];
      const walk = (obj, at) => {
        if (!obj || typeof obj !== "object") return;
        for (const [k, v] of Object.entries(obj)) {
          if (v === "") emptyAt.push(at + "." + k);
          else if (v && typeof v === "object") walk(v, at + "." + k);
        }
      };
      walk(r.config, "config");
      assert(emptyAt.length === 0, "v36p) no empty-string value survives at any value position of the fixture (" + JSON.stringify(emptyAt) + ")");
      const legacy = [];
      for (const [id, m] of Object.entries(r.config.models)) {
        if (m.providers !== undefined) legacy.push("models." + id + ".providers");
        if (m.affinityPool !== undefined) legacy.push("models." + id + ".affinityPool");
        if (!Array.isArray(m.deployments) || !m.deployments.length) legacy.push("models." + id + ".deployments");
        else {
          m.deployments.forEach((d, i) => {
            if (typeof d.backend !== "string" || !d.backend) legacy.push("models." + id + ".deployments[" + i + "].backend");
            if (typeof d.upstream !== "string" || !d.upstream) legacy.push("models." + id + ".deployments[" + i + "].upstream");
          });
        }
      }
      for (const [id, p] of Object.entries(r.config.presets)) {
        if (p.strategy !== "fallback") legacy.push("presets." + id + ".strategy=" + p.strategy);
        if (p.members !== undefined) legacy.push("presets." + id + ".members");
        if (p.models.some((e) => typeof e !== "string")) legacy.push("presets." + id + ".models[non-string]");
      }
      assert(legacy.length === 0, "v36q) every model is v3-shaped and every preset is fallback with string refs (" + JSON.stringify(legacy) + ")");
      // exposed defaults where absent and is PRESERVED where already set.
      const withFlag = JSON.parse(JSON.stringify(old));
      withFlag.models.m1.exposed = false;
      withFlag.presets.w.exposed = false;
      const r2 = convertLegacyConfig(withFlag);
      assert(r2.config.models.m1.exposed === false && r2.config.presets.w.exposed === false, "v36r) an existing exposed:false is preserved, not overwritten");
    }
    // Step 1 is what makes a 20-collision production config convertible, so its
    // arithmetic is pinned directly: every identity preset goes, and every
    // model-level id it used to shadow stays addressable.
    assert(Object.keys(r.config.presets).length === 4, "v36s) exactly the identity preset was dropped (" + Object.keys(r.config.presets).join(",") + ")");
    assert(
      Object.keys(old.presets).every((id) => addressable(r.config, id)),
      "v36t) every preset id dropped as an identity stays served by the same-named model"
    );
  }

  // ---- v3 typed validation ---------------------------------------------------
  // A config whose shape is v3 (detected by at least one model carrying
  // `deployments`) is checked under v3 rules. The pre-v3 path is untouched: its
  // strategy allowlist, its collision WARN and its `providers` requirement all
  // still apply to a config that has not migrated, which the last four
  // assertions here characterize.
  {
    const savedKeyB1 = process.env.KEY_B1;
    process.env.KEY_B1 = "x";
    try {
      const base = () => ({
        backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1", idleTimeoutMs: 5000, connectTimeoutMs: 2000, model: "bridge-era-upstream" }],
        models: { m1: { deployments: [{ backend: "b1", upstream: "u" }], exposed: true } },
        presets: { p1: { strategy: "fallback", models: ["m1"], exposed: true } }
      });
      const bad1 = base(); bad1.models.m1.deployments = [];
      assert(validateConfig(bad1, bad1).errors.some((e) => e.path === "models.m1.deployments"), "v36xa) empty deployments is an error");
      const bad2 = base(); bad2.models.m1.deployments = [{ backend: "ghost", upstream: "u" }];
      assert(validateConfig(bad2, bad2).errors.some((e) => e.path === "models.m1.deployments[0].backend"), "v36xb) unknown deployment backend ref is an error");
      const bad3 = base(); bad3.models.m1.deployments = [{ backend: "b1", upstream: "" }];
      assert(validateConfig(bad3, bad3).errors.some((e) => e.path === "models.m1.deployments[0].upstream"), "v36xc) empty upstream is an error");
      const bad4 = base(); bad4.models.m1.deployments = [{ backend: "b1", upstream: "u", weight: "abc" }];
      assert(validateConfig(bad4, bad4).errors.some((e) => e.path === "models.m1.deployments[0].weight"), "v36xd) non-integer weight is an error");
      const bad5 = base(); bad5.models.m1.deployments = [{ backend: "b1", upstream: "u", backupOnly: "yes" }];
      assert(validateConfig(bad5, bad5).errors.some((e) => e.path === "models.m1.deployments[0].backupOnly"), "v36xe) non-boolean backupOnly is an error");
      const bad5e = base(); bad5e.models.m1.exposed = "yes";
      assert(validateConfig(bad5e, bad5e).errors.some((e) => e.path === "models.m1.exposed"), "v36xk) non-boolean model exposed is an error");
      const bad6 = base(); bad6.presets.p1.strategy = "affinity";
      const v6 = validateConfig(bad6, bad6);
      assert(v6.errors.length === 0 && v6.warnings.some((w) => w.path === "presets.p1.strategy"), "v36xf) v3 non-fallback strategy warns only (the bridge already mapped the legacy shapes)");
      const good = base();
      const vg = validateConfig(good, good);
      assert(vg.errors.length === 0 && vg.warnings.length === 0, "v36xg) known backend fields (idleTimeoutMs/connectTimeoutMs + bridge-era model) never warn (" + JSON.stringify(vg.warnings) + ")");
      const coll = base(); coll.presets.m1 = { strategy: "fallback", models: ["m1"], exposed: true };
      assert(validateConfig(coll, coll).errors.some((e) => e.path === "presets.m1"), "v36xh) v3-shaped collision is an error (G1 XOR)");
      // Pre-v3 input must NOT hit the v3 branch: affinity/failover/weighted stay
      // silent here, and nothing the v3 branch adds may appear at a strategy path.
      const old = { backends: base().backends, models: { "m1": { providers: [{ backend: "b1", upstream: "u" }] } }, presets: { "p1": { strategy: "affinity", models: ["m1"] } } };
      assert(!validateConfig(old, old).warnings.some((w) => w.path === "presets.p1.strategy"), "v36xi) old-shape strategy untouched by the v3 warn set");
      // The other half of the warn-set narrowing, stated truthfully: what the
      // pre-v3 path keeps silent is affinity/failover/weighted. A pre-v3 preset
      // spelled `strategy: "fallback"` warns TODAY, because `fallback` is the v3
      // name and the old allowlist does not know it - and that pre-existing
      // verdict must not change (old shapes keep their behaviour exactly).
      const legacy4 = {
        backends: base().backends,
        models: { "m1": { providers: [{ backend: "b1", upstream: "u" }] } },
        presets: {
          "la": { strategy: "affinity", models: ["m1"] },
          "lf": { strategy: "failover", models: ["m1"] },
          "lw": { strategy: "weighted", models: ["m1"] },
          "lb": { strategy: "fallback", models: ["m1"] }
        }
      };
      const vj = validateConfig(legacy4, legacy4);
      const stratPaths = vj.warnings.filter((w) => /\.strategy$/.test(w.path)).map((w) => w.path);
      assert(
        !stratPaths.some((p) => p !== "presets.lb.strategy"),
        "v36xj) old-shape input keeps affinity/failover/weighted silent and no v3 strategy warn leaks into the pre-v3 path (" + JSON.stringify(stratPaths) + ")"
      );
      assert(
        stratPaths.length === 1 && stratPaths[0] === "presets.lb.strategy",
        "v36xj2) characterization: 'fallback' on a PRE-v3 preset keeps exactly its one pre-existing v2 'unknown strategy' warning (" + JSON.stringify(stratPaths) + ")"
      );
      // v3 parity for the typed positions the pre-v3 path checks on a provider:
      // a deployment now holds them, so the same values are errors there.
      const badp = base(); badp.models.m1.deployments = [{ backend: "b1", upstream: "u", params: 5 }];
      assert(validateConfig(badp, badp).errors.some((e) => e.path === "models.m1.deployments[0].params"), "v36xl2) a non-object deployment params is an error (parity with the pre-v3 provider check)");
      const badpe = base(); badpe.presets.p1.exposed = "yes";
      assert(validateConfig(badpe, badpe).errors.some((e) => e.path === "presets.p1.exposed"), "v36xl) non-boolean preset exposed is an error");
      // The detection boundary, pinned rather than accidental: at least one model
      // with `deployments` puts the WHOLE config under v3 rules. For a
      // hand-written mixture the honest verdict is the error below, not a silent
      // pass - the pre-migration entry has no deployments, so a v3 router cannot
      // route it, and validating each entry under its own shape would accept a
      // file that only half works. A converted payload is purely v3 (the bridge
      // converts or refuses), so this only bites input that skipped the bridge.
      const mixed = () => ({
        backends: base().backends,
        models: {
          "v3m": { deployments: [{ backend: "b1", upstream: "u" }], exposed: true },
          "oldm": { providers: [{ backend: "b1", upstream: "u" }] }
        },
        presets: { "pm": { strategy: "fallback", models: ["v3m", "oldm"], exposed: true } }
      });
      const mixedVerdict = validateConfig(mixed(), mixed());
      assert(
        mixedVerdict.errors.some((e) => e.path === "models.oldm.deployments") &&
          !mixedVerdict.errors.some((e) => e.path.startsWith("models.v3m") || e.path === "models.oldm.providers"),
        "v36xm) a MIXED config is validated config-wide as v3: the unmigrated model errors at models.oldm.deployments, the v3 model is clean, and the pre-v3 `providers` requirement is NOT applied (" + JSON.stringify(mixedVerdict.errors.map((e) => e.path)) + ")"
      );
      const m2 = mixed();
      const swapped = { backends: m2.backends, models: { "oldm": m2.models.oldm, "v3m": m2.models.v3m }, presets: m2.presets };
      const swappedVerdict = validateConfig(swapped, swapped);
      assert(
        JSON.stringify(swappedVerdict.errors.map((e) => e.path)) === JSON.stringify(mixedVerdict.errors.map((e) => e.path)),
        "v36xm2) detection is order-independent: the same mixture in the other order gives the same verdict (" + JSON.stringify(swappedVerdict.errors.map((e) => e.path)) + ")"
      );
      // *** THE DEFERRED ZERO-ERROR CRITERION (Task 10's acceptance criterion, closed here). ***
      // The bridge and the validator must AGREE end to end: a pre-v3 config runs
      // through convertLegacyConfig and is then validated in SAVE mode - the mode
      // POST /api/config uses - and must report ZERO errors, including the shape
      // and env classes that were the only ones left when this criterion was
      // deferred. The apiKeyEnv name stands in the process above. The fixture
      // carries the pre-fix editor's empty-string artifacts on purpose, since
      // their repair is what keeps every later save from being blocked.
      const legacy = {
        backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_B1", idleTimeoutMs: 5000, connectTimeoutMs: 2000, model: "bridge-era-upstream" }],
        models: {
          "m1": { providers: [{ backend: "b1", upstream: "u" }, { backend: "b1", upstream: "u2" }], affinityPool: 1 },
          "m2": { providers: [{ backend: "b1", upstream: "u" }], retry: "" }
        },
        presets: {
          "m1": { strategy: "affinity", models: [{ model: "m1", weight: 1 }] },
          "p1": { strategy: "failover", models: ["m1", "m2"], params: "" }
        },
        params: { temperature: 0.2 }
      };
      const converted = convertLegacyConfig(legacy);
      const vc = validateConfig(converted.config, converted.config);
      assert(
        vc.errors.length === 0,
        "v36xn) a converted pre-v3 config validates in SAVE mode with ZERO errors - the bridge and the validator agree (" + JSON.stringify(vc.errors) + "; validator warnings " + JSON.stringify(vc.warnings) + ")"
      );
    } finally {
      if (savedKeyB1 === undefined) delete process.env.KEY_B1;
      else process.env.KEY_B1 = savedKeyB1;
    }
  }

  // ---- v37) the DUAL-SHAPE load pipeline -----------------------------------
  // loadConfig must convert an old-shape file in memory BEFORE validating and
  // normalizing it, so one loader serves both eras. Every assertion below runs
  // on a config THIS BLOCK builds and hands to a child process or calls
  // directly; none of them reads config.json, so the working-tree mirror and
  // the committed mirror cannot change a result.
  {
    // v37a)-v37d) the real load path: an old-shape temp config is served as v3
    // by /api/config, which is what proves `cfg = effective` (the router, not
    // just the converter, holds the converted form).
    const dir = await mkdtemp(join(tmpdir(), "lmr-v37-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const old = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "v37a", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37" },
        { id: "v37b", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37" },
        { id: "v37c", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37" }
      ],
      models: {
        v37m: {
          providers: [
            { backend: "v37a", upstream: "u1", params: { temperature: 0.5 }, dropParams: ["logit_bias"], paramMap: { max_tokens: "max_output_tokens" } },
            { backend: "v37b", upstream: "u2", weight: 3 },
            { backend: "v37c", upstream: "u3" }
          ],
          affinityPool: 2
        }
      },
      presets: { v37p: { strategy: "affinity", models: ["v37m"] } }
    };
    await writeFile(cfgPath, JSON.stringify(old, null, 2));
    await writeFile(envPath, "KEY_V37=k\n");
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let base = null;
    let childErr = "";
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("v37 child did not start (stderr tail: " + JSON.stringify(childErr.slice(-400)) + ")")), 8000);
      child.stderr.on("data", (d) => {
        const s = d.toString();
        childErr += s;
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
        if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
      });
    });
    try {
      const live = await (await fetch(base + "/api/config")).json();
      const deps = live.models && live.models.v37m ? live.models.v37m.deployments : null;
      assert(
        Array.isArray(deps) && deps.length === 3 && deps.map((d) => d.backend).join(",") === "v37a,v37b,v37c" &&
          deps[0].upstream === "u1" && deps[1].upstream === "u2" && deps[2].upstream === "u3",
        "v37a) the ROUTER holds the converted v3 form: /api/config serves models.v37m as 3 deployments in declared order (" + JSON.stringify(live.models && live.models.v37m) + ")"
      );
      assert(
        live.models.v37m.providers === undefined && live.models.v37m.affinityPool === undefined,
        "v37b) the legacy keys are gone from the served model, not merged alongside the deployments"
      );
      assert(
        deps && deps[2].backupOnly === true && deps[0].backupOnly === undefined && deps[1].backupOnly === undefined,
        "v37c) affinityPool 2 marks only the providers past the pool as backupOnly (" + JSON.stringify(deps && deps.map((d) => d.backupOnly)) + ")"
      );
      assert(
        deps && deps[1].weight === 3 && deps[0].weight === undefined &&
          JSON.stringify(deps[0].params) === "{\"temperature\":0.5}" &&
          JSON.stringify(deps[0].dropParams) === "[\"logit_bias\"]" &&
          JSON.stringify(deps[0].paramMap) === "{\"max_tokens\":\"max_output_tokens\"}",
        "v37d) per-provider weight/params/dropParams/paramMap carry onto the deployments (" + JSON.stringify(deps && deps[0]) + ")"
      );
      assert(
        live.presets.v37p.strategy === "fallback",
        "v37e) the served preset strategy is the v3 'fallback' AFTER normalizeConfig, not rewritten to a legacy value (" + JSON.stringify(live.presets.v37p) + ")"
      );
      assert(
        live.presets.v37p.models.length === 1 && live.presets.v37p.models[0].model === "v37m",
        "v37f) the served preset still resolves to the same model id (" + JSON.stringify(live.presets.v37p.models) + ")"
      );
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v37g) R8(b): `providers: []` becomes `deployments: []`, which the v3
    // validator refuses in SAVE mode (the pre-v3 path only WARNED). The bridge
    // therefore moves this case across the reload-rejection boundary, and the
    // boot path must stay NON-FATAL for it (path models.<id>.deployments is not
    // in the loader's fatal set), so the router still starts.
    const dir = await mkdtemp(join(tmpdir(), "lmr-v37e-"));
    const cfgPath = join(dir, "config.json");
    const envPath = join(dir, ".env");
    const empty = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "v37e", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37E" }],
      models: { v37em: { providers: [] } },
      presets: { v37ep: { strategy: "failover", models: ["v37em"] } }
    };
    await writeFile(cfgPath, JSON.stringify(empty, null, 2));
    await writeFile(envPath, "KEY_V37E=k\n");
    const conv = convertLegacyConfig(empty);
    const saveVerdict = validateConfig(conv.config, conv.config, { mode: "save" });
    assert(
      Array.isArray(conv.config.models.v37em.deployments) && conv.config.models.v37em.deployments.length === 0 &&
        saveVerdict.errors.some((e) => e.path === "models.v37em.deployments"),
      "v37g) an empty providers array converts to an empty deployments array that SAVE mode refuses (" + JSON.stringify(saveVerdict.errors.map((e) => e.path)) + ")"
    );
    const child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPath, ROUTER_ENV: envPath },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let base = null;
    let childErr = "";
    try {
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("v37g child did not start (stderr tail: " + JSON.stringify(childErr.slice(-400)) + ")")), 8000);
        child.stderr.on("data", (d) => {
          const s = d.toString();
          childErr += s;
          const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
          if (m) { base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
        });
      });
      assert(
        /not fatal at boot\): models\.v37em\.deployments: /.test(childErr),
        "v37g2) BOOT stays non-fatal for the empty-deployments error and names its path (stderr tail: " + JSON.stringify(childErr.slice(-300)) + ")"
      );
      const live = await (await fetch(base + "/api/config")).json();
      assert(!!live.backends.find((b) => b.id === "v37e"), "v37g3) the router really is up and serving the config after that non-fatal error");
    } finally {
      child.kill();
      await rm(dir, { recursive: true, force: true });
    }
  }

  {
    // v37h)-v37j) R6: normalizeConfig is DUAL-SHAPE. A v3 model must not be
    // synthesized into `providers`, must not acquire `affinityPool`, and its
    // preset must keep the v3 'fallback' strategy; a legacy model in the same
    // config keeps its exact current treatment.
    const v3 = {
      backends: [{ id: "v37n" }],
      models: {
        v37nm: { deployments: [{ backend: "v37n", upstream: "u", weight: 2 }], exposed: false },
        v37lm: { providers: [{ backend: "v37n", upstream: "u" }], affinityPool: 2 }
      },
      presets: {
        v37np: { strategy: "fallback", models: ["v37nm"], exposed: true },
        v37lp: { strategy: "failover", models: ["v37lm"] }
      }
    };
    normalizeConfig(v3);
    assert(
      v3.models.v37nm.deployments.length === 1 && v3.models.v37nm.deployments[0].weight === 2 &&
        v3.models.v37nm.deployments[0].backend === "v37n" && v3.models.v37nm.exposed === false,
      "v37h) the v3 model survives normalize byte-for-byte (" + JSON.stringify(v3.models.v37nm) + ")"
    );
    assert(
      v3.models.v37nm.providers === undefined && v3.models.v37nm.affinityPool === undefined,
      "v37i) normalize writes NO legacy key onto a v3 model (" + JSON.stringify(Object.keys(v3.models.v37nm)) + ")"
    );
    assert(
      v3.presets.v37np.strategy === "fallback" && v3.presets.v37np.models.length === 1 &&
        v3.presets.v37np.models[0].model === "v37nm" && v3.presets.v37np.models[0].weight === 1 &&
        v3.presets.v37np.exposed === true,
      "v37j) the v3 preset keeps 'fallback' and its ref, with the ref normalized to {model, weight} (" + JSON.stringify(v3.presets.v37np) + ")"
    );
    assert(
      Array.isArray(v3.models.v37lm.providers) && v3.models.v37lm.providers.length === 1 && v3.models.v37lm.providers[0].backend === "v37n" &&
        v3.models.v37lm.affinityPool === 2 && v3.models.v37lm.deployments === undefined &&
        v3.presets.v37lp.strategy === "failover",
      "v37j2) the LEGACY entry in the same config keeps its current treatment (dual-shape, not v3-only) (" + JSON.stringify(v3.models.v37lm) + ")"
    );
  }

  {
    // v37k)-v37m) R5, the M-4 obligation: three structurally malformed entries
    // used to raise a RAW TypeError out of the converter, which the new load
    // path would turn into a router crash instead of a refusal. Each must PASS
    // THROUGH UNTOUCHED so the VALIDATOR names it, and the load verdict (which
    // is what loadConfig logs and what decides boot-vs-reject) must carry that
    // path.
    const malformed = (models, presets) => ({
      backends: [{ id: "v37k", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37K" }],
      models,
      presets
    });
    const cases = [
      ["presets: {x: null}", malformed({ v37km: { providers: [{ backend: "v37k", upstream: "u" }] } }, { x: null }), "presets.x"],
      ["presets: {x: 5}", malformed({ v37km: { providers: [{ backend: "v37k", upstream: "u" }] } }, { x: 5 }), "presets.x"],
      ["providers: [null]", malformed({ v37km: { providers: [null] } }, { v37kp: { strategy: "affinity", models: ["v37km"] } }), "models.v37km.deployments[0]"]
    ];
    for (const [label, raw, path] of cases) {
      let conv = null;
      let threw = null;
      try {
        conv = convertLegacyConfig(raw);
      } catch (e) {
        threw = e;
      }
      const verdict = conv ? validateConfig(conv.config, conv.config, { mode: "load" }) : { errors: [] };
      assert(
        threw === null && verdict.errors.some((e) => e.path === path),
        "v37k) " + label + " converts without throwing and the LOAD verdict names " + path +
          " (threw " + (threw ? threw.constructor.name + ": " + threw.message : "no") + "; errors " + JSON.stringify(verdict.errors.map((e) => e.path)) + ")"
      );
    }
    // The malformed entry is not DROPPED either: the operator's bytes survive
    // into the payload the validator judges, and the boot abort set (a non-object
    // root, or a missing/non-array backends) does not include any of these paths.
    const kept = convertLegacyConfig(malformed({ v37km: { providers: [{ backend: "v37k", upstream: "u" }] } }, { x: null }));
    const nonFatal = validateConfig(kept.config, kept.config, { mode: "load" }).errors.every((e) => e.path !== "" && e.path !== "backends");
    assert(
      kept.config.presets.x === null && nonFatal,
      "v37l) the malformed preset survives the converter untouched and its error is outside the boot-abort set (" + JSON.stringify(kept.config.presets.x) + ")"
    );
    const keptProviders = convertLegacyConfig(malformed({ v37km: { providers: [null] } }, {}));
    const nonFatal2 = validateConfig(keptProviders.config, keptProviders.config, { mode: "load" }).errors.every((e) => e.path !== "" && e.path !== "backends");
    assert(
      keptProviders.config.models.v37km.deployments.length === 1 && keptProviders.config.models.v37km.deployments[0] === null && nonFatal2,
      "v37m) the null provider survives as a null deployment (not dropped, not thrown on) and its error is outside the boot-abort set (" + JSON.stringify(keptProviders.config.models.v37km.deployments) + ")"
    );
  }

  {
    // v37n)-v37o) R8(a): a model carrying BOTH providers and deployments loses
    // the hand-written v3 deployments to converter step 4. The behaviour is kept
    // (it cannot arise from production, where the file is old-shape throughout);
    // the goal here is that the loss is VISIBLE, naming the model.
    const both = convertLegacyConfig({
      backends: [{ id: "v37n", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37N" }],
      models: {
        v37bm: {
          providers: [{ backend: "v37n", upstream: "from-providers" }],
          deployments: [{ backend: "v37n", upstream: "from-deployments", backupOnly: true }]
        }
      },
      presets: { v37bp: { strategy: "fallback", models: ["v37bm"], exposed: true } }
    });
    assert(
      both.config.models.v37bm.deployments.length === 1 &&
        both.config.models.v37bm.deployments[0].upstream === "from-providers" &&
        both.config.models.v37bm.deployments[0].backupOnly === undefined,
      "v37n) providers still win and the hand-written deployments are discarded (" + JSON.stringify(both.config.models.v37bm.deployments) + ")"
    );
    assert(
      both.warnings.some((w) => w.includes("v37bm") && /deployments/.test(w) && /dropped|discard/i.test(w)),
      "v37o) the discard is VISIBLE: a warning names the model and says the deployments were dropped (" + JSON.stringify(both.warnings) + ")"
    );
    // R7: a MIXTURE converts model by model - the already-v3 entry keeps its own
    // deployments while the old-shape entry is converted, so one config-wide
    // predicate in the pipeline is enough and no per-model predicate is needed.
    const mixed = convertLegacyConfig({
      backends: [{ id: "v37n", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_V37N" }],
      models: {
        v37mm: { deployments: [{ backend: "v37n", upstream: "already-v3" }], exposed: true },
        v37om: { providers: [{ backend: "v37n", upstream: "was-old-shape" }] }
      },
      presets: { v37mp: { strategy: "fallback", models: ["v37mm", "v37om"], exposed: true } }
    });
    assert(
      mixed.config.models.v37mm.deployments[0].upstream === "already-v3" &&
        mixed.config.models.v37om.deployments[0].upstream === "was-old-shape" &&
        mixed.config.models.v37om.providers === undefined,
      "v37p) a MIXED config converts the old-shape entry and leaves the v3 entry's deployments alone (" + JSON.stringify(mixed.config.models) + ")"
    );
  }

  {
    // v38a)-v38d) the v3 routing read. `candidates()` reads `deployments` with
    // backupOnly ordering and sticky weighted primaries, and it must do so from
    // the same code path the router actually runs for a converted config: a v3
    // model is dispatched to the v3 branch PER ENTRY, while the legacy
    // `providers`/`affinityPool` read stays reachable for the pre-bridge blocks
    // that call `candidates()` directly with old-shape fixtures.
    const cfg = {
      backends: [{ id: "b1" }, { id: "b2" }, { id: "b3" }],
      models: {
        m1: {
          deployments: [
            { backend: "b1", upstream: "u" },
            { backend: "b2", upstream: "u", weight: 2 },
            { backend: "b3", upstream: "u", backupOnly: true },
          ],
          exposed: true,
        },
        hid: { deployments: [{ backend: "b1", upstream: "uh" }], exposed: false },
      },
      presets: {
        tier: { strategy: "fallback", models: ["m1", "hid"] },
        secret: { strategy: "fallback", models: ["m1"], exposed: false },
      },
    };
    normalizeConfig(cfg);
    const c = candidates(cfg, "m1", "sess-a");
    // This block does NOT pin "the base got backupOnly wrong". Measured against
    // 5ec0549: the base shim's `modelPool` already EXCLUDED `backupOnly` (pool =
    // 2 over {b1, b2}), `sess-1`'s base primary is `b2`, and `b3` is primary for
    // 0 of 5000 base keys. What these four pin is the CONVERTED-config path
    // holding the same exclusion - the head routes by `deployments` rather than
    // `providers`/`affinityPool`, and they fail if that read loses the
    // `backupOnly` tail. The ordering rule itself is carried by `z8`, `v38d`
    // and `v38d3`, which an ordering-bypass mutation DOES flip.
    assert(
      c.ordered[0].backend !== "b3",
      "v38a) backupOnly never primary (" + JSON.stringify(c.ordered) + ")"
    );
    const sweep = [];
    for (let i = 0; i < 300; i++) sweep.push(candidates(cfg, "m1", "sess-" + i).ordered.map((a) => a.backend));
    assert(
      sweep.every((o) => o[0] !== "b3"),
      "v38a2) backupOnly is never the PRIMARY for any of 300 session keys (" + JSON.stringify(sweep.slice(0, 6)) + ")"
    );
    assert(
      c.ordered[c.ordered.length - 1].backend === "b3",
      "v38b) backupOnly tried last (" + JSON.stringify(c.ordered) + ")"
    );
    assert(
      sweep.every((o) => o[o.length - 1] === "b3"),
      "v38b2) backupOnly is LAST for every session key, not just the favourably-hashed ones (" + JSON.stringify(sweep.slice(0, 6)) + ")"
    );
    assert(
      candidates(cfg, "m1", "sess-a").ordered.map((a) => a.backend).join(",") ===
        candidates(cfg, "m1", "sess-a").ordered.map((a) => a.backend).join(","),
      "v38c) two SEPARATE calls with the same session key pick the same primary, so a later call cannot drift"
    );
    // The fixture's weights are b1:1, b2:2 over a total of 3, so the pick is the
    // session hash modulo 3: buckets 0 -> b1, 1 and 2 -> b2. Spread, ratio and
    // the backupOnly exclusion are three properties the same 300-key sweep
    // measures, and every one of them can fail.
    const picks = { b1: 0, b2: 0, b3: 0 };
    for (let i = 0; i < 300; i++) {
      const first = candidates(cfg, "m1", "sess-" + i).ordered[0].backend;
      picks[first] = (picks[first] || 0) + 1;
    }
    assert(
      picks.b1 > 0 && picks.b2 > 0,
      "v38d) both weighted primaries are picked at least once (" + JSON.stringify(picks) + ")"
    );
    assert(
      picks.b3 === 0,
      "v38d2) backupOnly never wins the pick for ANY session key (" + JSON.stringify(picks) + ")"
    );
    const b1Share = picks.b1 / 300;
    assert(
      b1Share >= 0.2 && b1Share <= 0.45 && picks.b2 === 300 - picks.b1,
      "v38d3) the picks land in the fixture's 1:2 weight band (" + JSON.stringify(picks) + ")"
    );
    const t = candidates(cfg, "tier", "sess-a");
    assert(t.models.includes("hid"), "v38d4) hidden model still routable through preset (" + JSON.stringify(t.models) + ")");
  }

  {
    // v38e)-v38f) T3: exposure is a LISTING rule, not a routing rule. An
    // `exposed: false` preset and model stay routable while being absent from
    // /v1/models, and the list is exposed presets first, then exposed models
    // (the order the plan sanctions; the legacy collision rule is gone).
    const hid = mockBackend("hidB", {});
    hid.port = await listen(hid.srv);
    const hcfg = {
      port: 0,
      prefix: "/v1",
      masterKeyEnv: null,
      backends: [{ id: "hidB", baseURL: "http://127.0.0.1:" + hid.port, apiKeyEnv: "KEY_HID" }],
      models: {
        "shown-m": { deployments: [{ backend: "hidB", upstream: "u" }], exposed: true },
        "hidden-m": { deployments: [{ backend: "hidB", upstream: "u" }], exposed: false },
      },
      presets: {
        "shown-p": { strategy: "fallback", models: ["shown-m"], exposed: true },
        "hidden-p": { strategy: "fallback", models: ["hidden-m"], exposed: false },
      },
    };
    const { child: hidChild, base: hidBase, dir: hidDir } = await startRouterCfg(hcfg, "KEY_HID=k\n");
    try {
      const served = (await (await api(hidBase, "/v1/models", { method: "GET" })).json()).data.map((x) => x.id);
      assert(
        served.join(",") === "shown-p,shown-m",
        "v38e) exposed:false model and preset ids are ABSENT from /v1/models (exposed presets first, then exposed models; got " +
          JSON.stringify(served) + ")"
      );
      const rh = await api(hidBase, "/v1/chat/completions", { body: { model: "hidden-p", messages: [{ role: "user", content: "hi" }] } });
      const rm = await api(hidBase, "/v1/chat/completions", { body: { model: "hidden-m", messages: [{ role: "user", content: "hi" }] } });
      assert(
        rh.status === 200 && rm.status === 200,
        "v38f) an exposed:false preset and an exposed:false model stay routable (preset=" + rh.status + ", model=" + rm.status + ")"
      );
    } finally {
      hidChild.kill();
      hid.srv.close();
      await rm(hidDir, { recursive: true, force: true });
    }
  }

  {
    // v38g)-v38h) R6: the READ path must not USE a deployment it cannot route.
    // The bridge-era pass-through hands a structurally intact but semantically
    // unusable provider into `deployments` so the VALIDATOR can name it; that is
    // not licence for the router to build an attempt carrying `model: undefined`.
    // Four unusable entries are mixed with two good ones: an unknown backend, a
    // missing upstream on a backend with no default model, a non-object entry,
    // and an entry with no backend at all. Each good deployment uses its OWN
    // backend, because the attempt list dedups by backend id - a rule this block
    // deliberately stays clear of so it measures the drop and nothing else.
    const cfg = {
      backends: [{ id: "b1" }, { id: "b2" }],
      models: {
        m1: {
          deployments: [
            { backend: "b1", upstream: "u1" },
            { backend: "ghost", upstream: "u2" },
            { backend: "b1" },
            null,
            { upstream: "u3" },
            { backend: "b2", upstream: "u4", backupOnly: true },
          ],
        },
      },
      presets: { p1: { strategy: "fallback", models: ["m1"] } },
    };
    const seen = new Set();
    const realWarn = console.warn;
    console.warn = (msg) => seen.add(String(msg));
    let c = null;
    try {
      normalizeConfig(cfg);
      c = candidates(cfg, "m1", "sess-a");
    } finally {
      console.warn = realWarn;
    }
    assert(
      c.ordered.map((a) => a.backend + "/" + a.upstream).join(",") === "b1/u1,b2/u4",
      "v38g) only resolvable deployments reach the attempt list (" + JSON.stringify(c.ordered) + ")"
    );
    assert(
      c.ordered.every((a) => a.backend && a.upstream),
      "v38g2) no attempt carries an undefined backend or upstream (" + JSON.stringify(c.ordered) + ")"
    );
    // Each unusable entry is dropped for ITS OWN reason, and the v2 wording is
    // pinned down to the entry so a silent drop cannot hide behind a count.
    const expectWarn = [
      "model 'm1': entry at deployments[1] references unknown backend 'ghost', dropped",
      "model 'm1': entry at deployments[2] (backend 'b1') has no resolvable upstream, dropped",
      "model 'm1': entry at deployments[3] is not an object, dropped",
      "model 'm1': entry at deployments[4] names no backend, dropped",
    ];
    assert(
      [...seen].filter((m) => m.includes("model 'm1'")).length === 4 && expectWarn.every((w) => [...seen].some((m) => m.includes(w))),
      "v38h) each unusable deployment is dropped with its OWN v2-wording warning naming the model and the entry (" + JSON.stringify(expectWarn) + " vs " + JSON.stringify([...seen]) + ")"
    );
    let sessB = null;
    const realWarn2 = console.warn;
    console.warn = () => {};
    try {
      sessB = candidates(cfg, "m1", "sess-b").ordered.map((a) => a.backend + "/" + a.upstream).join(",");
    } finally {
      console.warn = realWarn2;
    }
    assert(
      sessB === "b1/u1,b2/u4",
      "v38h3) the drop is not session-dependent (" + sessB + ")"
    );
  }

  {
    // v38i)-v38j) R3: a MISSPELLED key in a v3 model or deployment is the hazard
    // this validator branch exists to catch - `backuponly: true` silently makes a
    // deployment the operator intended as a backup into a PRIMARY that receives
    // live traffic. The backend layer already allowlists its keys; the same
    // pattern now covers models and deployments, as a WARNING (never an error).
    const v3cfg = (model) => ({
      port: 0,
      prefix: "/v1",
      masterKeyEnv: null,
      backends: [{ id: "b1", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_R3" }],
      models: { m1: model },
      presets: { p1: { strategy: "fallback", models: ["m1"], exposed: true } },
    });
    // The validator also reports an unset apiKeyEnv as an error; these fixtures
    // are about the KEY axis, so the env name is provided and the assertions
    // name the exact paths they care about instead of a bare error count.
    const prevKeyR3 = process.env.KEY_R3;
    process.env.KEY_R3 = "k";
    let misspelled = null;
    let modelTypo = null;
    let clean = null;
    try {
      misspelled = validateConfig(
        v3cfg({ deployments: [{ backend: "b1", upstream: "u", backuponly: true, wieght: 2 }] }),
        null,
        {}
      );
      modelTypo = validateConfig(v3cfg({ deployments: [{ backend: "b1", upstream: "u" }], expossed: false }), null, {});
      // The allowlist must be complete for the shape the converter PRODUCES and
      // for the fields the live config really carries, or every boot logs noise.
      clean = validateConfig(
        v3cfg({
          deployments: [
            { backend: "b1", upstream: "u", weight: 2, params: { temperature: 1 }, dropParams: ["logit_bias"], paramMap: { a: "b" } },
            { backend: "b1", upstream: "u2", backupOnly: true },
          ],
          params: { temperature: 0.5 },
          retry: { maxRetries: 1 },
          meta: { label: "l", contextWindow: 1000, pricing: { inputPerM: 1 } },
          exposed: true,
        }),
        null,
        {}
      );
    } finally {
      if (prevKeyR3 === undefined) delete process.env.KEY_R3;
      else process.env.KEY_R3 = prevKeyR3;
    }
    assert(
      misspelled.warnings.some((w) => w.path === "models.m1.deployments[0]" && /backuponly/.test(w.message)) &&
        misspelled.warnings.some((w) => w.path === "models.m1.deployments[0]" && /wieght/.test(w.message)),
      "v38i) an unknown v3 DEPLOYMENT key warns with its real path (" + JSON.stringify(misspelled.warnings) + ")"
    );
    assert(
      misspelled.errors.filter((e) => e.path.startsWith("models.m1")).length === 0,
      "v38i2) an unknown key is a WARNING, never an error (" + JSON.stringify(misspelled.errors) + ")"
    );
    assert(
      modelTypo.warnings.some((w) => w.path === "models.m1" && /expossed/.test(w.message)) &&
        modelTypo.errors.filter((e) => e.path.startsWith("models.m1")).length === 0,
      "v38j) an unknown v3 MODEL key warns at the model path and is not an error (" + JSON.stringify(modelTypo.warnings) + ")"
    );
    assert(
      clean.warnings.length === 0,
      "v38j2) every key the converter produces is known: no spurious warning (" + JSON.stringify(clean.warnings) + ")"
    );
    // The warning must reach the OPERATOR at boot, not only the save verdict -
    // that is the whole point of closing the misspelled-key hazard. The child is
    // spawned inline so its stderr is captured and assertable.
    const typoDir = await mkdtemp(join(tmpdir(), "lmr-v38j-"));
    const typoCfgPath = join(typoDir, "config.json");
    await writeFile(typoCfgPath, JSON.stringify(v3cfg({ deployments: [{ backend: "b1", upstream: "u", backuponly: true }] }), null, 2));
    await writeFile(join(typoDir, ".env"), "KEY_R3=k\n");
    const typoChild = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: typoCfgPath, ROUTER_ENV: join(typoDir, ".env") },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let typoBase = null;
    let typoErr = "";
    try {
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("v38j4 child did not start (stderr tail: " + JSON.stringify(typoErr.slice(-400)) + ")")), 8000);
        typoChild.stderr.on("data", (d) => {
          const s = d.toString();
          typoErr += s;
          const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
          if (m) { typoBase = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
        });
      });
      const live = await (await api(typoBase, "/api/config", { method: "GET" })).json();
      assert(
        !!live && !!live.models && !!live.models.m1,
        "v38j3) the router boots and serves a config carrying the misspelling (sanity)"
      );
      assert(
        /backuponly/.test(typoErr) && /models\.m1/.test(typoErr),
        "v38j4) the misspelling reaches the OPERATOR at boot, naming the model path (stderr tail: " + JSON.stringify(typoErr.slice(-400)) + ")"
      );
    } finally {
      typoChild.kill();
      await rm(typoDir, { recursive: true, force: true });
    }
  }

  {
    // v38k) R5: `backends: [null]` is a hand-edited config that used to take the
    // router DOWN at boot - an unguarded `.id` dereference in normalizeConfig's
    // backendById map (base 197824a line 1522, and HEAD's converter sweep wrapped
    // by loadConfig as "config conversion failed"). A non-object entry must pass
    // through untouched so the validator names `backends.0` and boot stays
    // tolerant: a warning, not death.
    const r5cfg = {
      port: 0,
      prefix: "/v1",
      masterKeyEnv: null,
      backends: [null, { id: "r5b", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_R5" }],
      models: { r5m: { deployments: [{ backend: "r5b", upstream: "u" }], exposed: true } },
      presets: { r5p: { strategy: "fallback", models: ["r5m"], exposed: true } },
    };
    // The warning path is asserted the same way: the point of the fix is BOTH
    // that boot survives AND that the operator is told which entry is junk. The
    // child is spawned inline so its stderr stays in scope for the assertion,
    // which runs BEFORE the finally-block cleanup so it can still read it.
    const r5Dir = await mkdtemp(join(tmpdir(), "lmr-v38k-"));
    const r5CfgPath = join(r5Dir, "config.json");
    await writeFile(r5CfgPath, JSON.stringify(r5cfg, null, 2));
    await writeFile(join(r5Dir, ".env"), "KEY_R5=k\n");
    const r5Child = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: r5CfgPath, ROUTER_ENV: join(r5Dir, ".env") },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let r5Base = null;
    let r5Err = "";
    try {
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("v38k child did not start (stderr tail: " + JSON.stringify(r5Err.slice(-400)) + ")")), 8000);
        r5Child.stderr.on("data", (d) => {
          const s = d.toString();
          r5Err += s;
          const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(s);
          if (m) { r5Base = "http://127.0.0.1:" + parseInt(m[1], 10); clearTimeout(t); resolve(); }
        });
      });
      // The child prints its listen banner from the listen callback, so it is
      // accepting connections already; a first-request ECONNRESET here would be
      // a real defect, and the failure is carried into the assertion rather than
      // crashing the whole run.
      const r5Get = async (path) => {
        let lastErr = null;
        for (let i = 0; i < 5; i++) {
          try {
            return await (await api(r5Base, path, { method: "GET" })).json();
          } catch (e) {
            lastErr = e;
            await new Promise((r) => setTimeout(r, 100));
          }
        }
        throw lastErr;
      };
      const health = await r5Get("/health");
      const r5served = (await r5Get("/v1/models")).data.map((x) => x.id);
      assert(
        health.ok === true && r5served.join(",") === "r5p,r5m",
        "v38k) a config carrying backends:[null] BOOTS and serves instead of dying (health=" +
          JSON.stringify(health.ok) + ", served=" + JSON.stringify(r5served) + ")"
      );
      assert(
        /backends\[0\][\s\S]*must be an object/.test(r5Err),
        "v38k2) the junk backend entry is NAMED at boot with its validator path (stderr tail: " + JSON.stringify(r5Err.slice(-400)) + ")"
      );
    } finally {
      r5Child.kill();
      await rm(r5Dir, { recursive: true, force: true });
    }
  }

  // ---- config write path: single-file bind mount ----------------------------
  // The live homelab config is a single-file bind mount. Linux refuses to
  // rename() onto a mount point (EBUSY), and even a swap that succeeded would
  // leave the container's mount and its fs.watch pinned to the old inode, so
  // the save must fall back to writing the validated bytes IN PLACE. This block
  // pins that fallback and, just as importantly, the errors it must NOT absorb.
  {
    const dirWA = await mkdtemp(join(tmpdir(), "lmr-write-"));
    const wTarget = join(dirWA, "config.json");
    const wObj = { port: 8765, backends: [{ id: "wa", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_WA" }] };
    const wText = JSON.stringify(wObj, null, 2) + "\n";
    const wSentinel = JSON.stringify({ sentinel: true }) + "\n";
    // readFile returning null (not throwing) keeps a broken implementation
    // failing the assertion instead of crashing the whole suite.
    const readOrNull = async (p) => {
      try { return await readFile(p, "utf8"); } catch { return null; }
    };
    const tmpPresent = async () => (await readdir(dirWA)).includes("config.json.tmp");
    const mountError = (code) => () => {
      throw Object.assign(new Error(code === "EBUSY" ? "resource busy or locked" : "permission denied"), { code });
    };
    try {
      // WA1) EBUSY (the bind-mount case) -> in-place write, temp file removed
      writeConfigAtomic(wObj, wTarget, mountError("EBUSY"));
      assert((await readOrNull(wTarget)) === wText, "zWA1a) an EBUSY rename falls back to writing the config in place");
      assert(!(await tmpPresent()), "zWA1b) the in-place fallback removes the temp file");

      // WA2) a non-mount rename error must propagate with no fallback write
      await writeFile(wTarget, wSentinel);
      let threw2 = null;
      try {
        writeConfigAtomic(wObj, wTarget, mountError("EACCES"));
      } catch (e) {
        threw2 = e;
      }
      assert(!!threw2 && threw2.code === "EACCES", "zWA2a) EACCES propagates instead of being swallowed by the fallback");
      assert((await readOrNull(wTarget)) === wSentinel, "zWA2b) the rejected write left the target byte-unchanged");
      assert(await tmpPresent(), "zWA2c) the non-fallback path leaves the temp file as before (no cleanup on a real error)");

      // WA3) the real renameSync path is unchanged
      writeConfigAtomic(wObj, wTarget);
      assert((await readOrNull(wTarget)) === wText, "zWA3a) the real renameSync path still writes the config");
      assert(!(await tmpPresent()), "zWA3b) the atomic rename path leaves no temp file behind");

      // WA4) the serialization contract: 2-space indent plus ONE trailing newline
      const bytes4 = await readOrNull(wTarget);
      assert(bytes4 === JSON.stringify(wObj, null, 2) + "\n", "zWA4a) bytes are JSON.stringify(obj, null, 2) plus one trailing newline");
      assert(bytes4.endsWith("\n") && !bytes4.endsWith("\n\n"), "zWA4b) exactly one trailing newline");
    } finally {
      await rm(dirWA, { recursive: true, force: true });
    }
  }

  // ---- config editor: auth gate ---------------------------------------------
  // With masterKeyEnv set and no credential, every config-write route must be
  // refused and the file must stay untouched.
  {
    const dirCE2 = await mkdtemp(join(tmpdir(), "lmr-cfg-auth-"));
    const cfgPathCE2 = join(dirCE2, "config.json");
    const cfgCE2 = {
      port: 0, prefix: "/v1", masterKeyEnv: "CE_MASTER", uiPasswordEnv: null,
      backends: [{ id: "ce-a", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_CE_A" }],
      models: { "ce-model": { providers: [{ backend: "ce-a", upstream: "u" }] } },
      presets: { "ce-preset": { strategy: "affinity", models: ["ce-model"] } },
    };
    await writeFile(cfgPathCE2, JSON.stringify(cfgCE2, null, 2) + "\n");
    await writeFile(join(dirCE2, ".env"), "KEY_CE_A=secret-value-a\nCE_MASTER=master-secret\n");
    const childCE2 = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPathCE2, ROUTER_ENV: join(dirCE2, ".env") },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let baseCE2 = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("config-editor auth server did not start")), 8000);
      childCE2.stderr.on("data", (d) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { baseCE2 = `http://${HOST}:${parseInt(m[1], 10)}`; clearTimeout(timer); resolve(); }
      });
    });
    try {
      const before = await readFile(cfgPathCE2, "utf8");
      const noAuth = { "content-type": "application/json" };
      const ra = await fetch(baseCE2 + "/api/config", {
        method: "POST",
        headers: noAuth,
        body: JSON.stringify({ config: JSON.parse(before), force: true }),
      });
      assert(ra.status === 401, "zCE12a) unauthenticated save -> 401 (got " + ra.status + ")");
      const rb = await fetch(baseCE2 + "/api/config/raw");
      assert(rb.status === 401, "zCE12b) unauthenticated raw read -> 401 (got " + rb.status + ")");
      const rc = await fetch(baseCE2 + "/api/config/reset", {
        method: "POST",
        headers: noAuth,
        body: JSON.stringify({ snapshot: "x.json" }),
      });
      assert(rc.status === 401, "zCE12c) unauthenticated restore -> 401 (got " + rc.status + ")");
      assert((await readFile(cfgPathCE2, "utf8")) === before, "zCE12d) unauthenticated attempts left the file unchanged");

      // With the master key the same save is accepted.
      const rd = await fetch(baseCE2 + "/api/config", {
        method: "POST",
        headers: { ...noAuth, authorization: "Bearer master-secret" },
        body: JSON.stringify({ config: JSON.parse(before), force: true }),
      });
      assert(rd.status === 200, "zCE12e) the master key authorizes a save (" + rd.status + ")");
    } finally {
      childCE2.kill();
      await rm(dirCE2, { recursive: true, force: true });
    }
  }

  // ---- env editor (UI-managed secrets store) --------------------------------
  // Exercises the /api/env* routes against a real child server whose
  // ROUTER_SECRETS points into a temp dir, so the repo's real secrets.json is
  // never touched. The load-bearing invariant is that the LIST endpoint never
  // carries a value: values exist only in the deliberate per-name reveal.
  {
    const dirSE = await mkdtemp(join(tmpdir(), "lmr-se-env-"));
    const cfgPathSE = join(dirSE, "config.json");
    const secretsPathSE = join(dirSE, "secrets.json");
    const cfgSE = {
      port: 0, prefix: "/v1", masterKeyEnv: "CE_MASTER", uiPasswordEnv: "CE_UIPASS",
      backends: [{ id: "se-a", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_SE_A" }],
      models: { "se-model": { providers: [{ backend: "se-a", upstream: "u" }] } },
      presets: { "se-preset": { strategy: "affinity", models: ["se-model"] } },
    };
    await writeFile(cfgPathSE, JSON.stringify(cfgSE, null, 2) + "\n");
    await writeFile(join(dirSE, ".env"), "KEY_SE_A=env-file-value-a\nCE_MASTER=master-secret\nCE_UIPASS=ui-pass-value\nROUTER_HISTORY=env-history.jsonl\n");
    // Pre-seeded so `updated` (vs `added`) and the merge behavior have a base.
    await writeFile(secretsPathSE, JSON.stringify({ KEY_SE_A: "secrets-value-a", SE_KEEP: "keep-me" }, null, 2) + "\n");
    const childSE = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPathSE, ROUTER_ENV: join(dirSE, ".env"), ROUTER_SECRETS: secretsPathSE },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let baseSE = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("secrets-editor server did not start")), 8000);
      childSE.stderr.on("data", (d) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { baseSE = `http://${HOST}:${parseInt(m[1], 10)}`; clearTimeout(timer); resolve(); }
      });
    });

    // api() defaults to the suite's local-master header; this fixture's master
    // key is CE_MASTER, so route every call through the real credential.
    const apiSE = (base, path, opts) => api(base, path, { ...opts, headers: { authorization: "Bearer master-secret", ...(opts && opts.headers) } });
    const listEnv = async () => await (await apiSE(baseSE, "/api/env", { method: "GET" })).json();
    const findVar = (body, name) => (body.vars || []).find((v) => v.name === name);
    const postEnv = (body) => apiSE(baseSE, "/api/env", { body });
    const reveal = async (name) => await (await apiSE(baseSE, "/api/env/reveal", { body: { name } })).json();
    const secretsOnDisk = async () => JSON.parse(await readFile(secretsPathSE, "utf8"));
    const sendEnv = (method, name) => apiSE(baseSE, `/api/env?name=${name}`, { method, body: method === "DELETE" ? undefined : {} });
    const snapshots = async () => {
      try { return (await readdir(join(dirSE, "secrets.history"))).filter((f) => f.endsWith(".json")); } catch { return []; }
    };

    try {
      // SE1) the route is auth-gated exactly like /api/config
      const rSE1 = await fetch(baseSE + "/api/env");
      assert(rSE1.status === 401, "zSE1a) unauthenticated GET /api/env -> 401 (got " + rSE1.status + ")");
      const rSE1b = await fetch(baseSE + "/api/env/reveal", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "KEY_SE_A" }),
      });
      assert(rSE1b.status === 401, "zSE1b) unauthenticated POST /api/env/reveal -> 401 (got " + rSE1b.status + ")");
      const rSE1c = await apiSE(baseSE, "/api/env", { method: "GET" });
      const bSE1c = await rSE1c.json();
      assert(rSE1c.status === 200 && Array.isArray(bSE1c.vars), "zSE1c) the master key authorizes GET /api/env (200 + vars)");

      // SE2) a name referenced by a backend apiKeyEnv is listed, and NO value
      //      (from .env or from the secrets store) appears in the response text
      const rSE2 = await apiSE(baseSE, "/api/env", { method: "GET" });
      const textSE2 = await rSE2.text();
      const bSE2 = JSON.parse(textSE2);
      const varSE2 = findVar(bSE2, "KEY_SE_A");
      assert(!!varSE2 && varSE2.set === true && typeof varSE2.source === "string", "zSE2a) a backend apiKeyEnv name is listed with set/source");
      assert(varSE2.source === "secrets", "zSE2b) a name present in both files reports source 'secrets' (" + (varSE2 && varSE2.source) + ")");
      assert(!textSE2.includes("env-file-value-a") && !textSE2.includes("secrets-value-a") && !textSE2.includes("keep-me"), "zSE2c) no secret value leaks into the list response");
      assert(textSE2.includes("KEY_SE_A") && textSE2.includes("SE_KEEP"), "zSE2d) names are still served (the UI needs them)");

      // SE3) add a new var through POST, then confirm it is live and revealable
      const rSE3 = await postEnv({ name: "SE_NEW", value: "new-value-1", baseRevision: bSE2.revision });
      const bSE3 = await rSE3.json();
      assert(rSE3.status === 200 && bSE3.ok === true && bSE3.action === "added" && bSE3.applied === true, "zSE3a) POST /api/env adds a var (ok/added/applied)");
      const afterSE3 = await listEnv();
      const varSE3 = findVar(afterSE3, "SE_NEW");
      assert(!!varSE3 && varSE3.live === true && varSE3.source === "secrets", "zSE3b) the new var reports live:true + source 'secrets'");
      assert(afterSE3.restartRequired === false, "zSE3c) no restart is required after an applied write");
      assert((await reveal("SE_NEW")).value === "new-value-1", "zSE3d) reveal returns the exact written value");

      // SE4) the auth credential itself is not editable from the UI
      const beforeSE4 = await readFile(secretsPathSE, "utf8");
      const rSE4 = await postEnv({ name: "CE_MASTER", value: "hijack", force: true });
      const bSE4 = await rSE4.json();
      assert(rSE4.status === 400 && /authentication credential/.test(JSON.stringify(bSE4)), "zSE4a) writing the masterKeyEnv name -> 400 naming the credential");
      assert((await readFile(secretsPathSE, "utf8")) === beforeSE4, "zSE4b) the rejected protected write left the secrets file unchanged");
      // Reveal must refuse the same names: the list reports editable:false for
      // them, and the master key is a wider-scope credential than the dashboard
      // session that would be asking for it.
      const revSE4 = await fetch(baseSE + "/api/env/reveal", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer master-secret" },
        body: JSON.stringify({ name: "CE_MASTER" }),
      });
      const bRevSE4 = await revSE4.json();
      assert(revSE4.status === 400, "zSE4c) revealing the masterKeyEnv name -> 400 (got " + revSE4.status + ")");
      assert(!JSON.stringify(bRevSE4).includes("master-secret"), "zSE4d) the refused reveal leaks no value");
      const revSE4b = await fetch(baseSE + "/api/env/reveal", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer master-secret" },
        body: JSON.stringify({ name: "CE_UIPASS" }),
      });
      assert(revSE4b.status === 400, "zSE4e) revealing the uiPasswordEnv name -> 400 (got " + revSE4b.status + ")");
      // A non-protected name still reveals, so the refusal is targeted.
      assert((await reveal("SE_NEW")).value === "new-value-1", "zSE4f) an ordinary name still reveals after the protected refusal");
      // The reveal call is scoped to the same known set the list serves, so a
      // name that is only ambient process noise cannot be read out of band.
      const revSE4c = await fetch(baseSE + "/api/env/reveal", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer master-secret" },
        body: JSON.stringify({ name: "PATH" }),
      });
      assert(revSE4c.status === 400, "zSE4g) revealing an out-of-scope ambient name (PATH) -> 400 (got " + revSE4c.status + ")");
      assert((await revSE4c.json()).value === undefined, "zSE4h) the refused out-of-scope reveal carries no value");
      // The scope refusal is targeted: config-referenced names still reveal.
      assert((await reveal("KEY_SE_A")).value === "secrets-value-a", "zSE4i) a config-referenced name still reveals after the scope guard");

      // SE5) a second write merges rather than replaces
      const rSE5 = await postEnv({ name: "SE_SECOND", value: "new-value-2", force: true });
      const bSE5 = await rSE5.json();
      const onDiskSE5 = await secretsOnDisk();
      assert(rSE5.status === 200 && bSE5.action === "added", "zSE5a) a second write is accepted (added)");
      assert(onDiskSE5.SE_NEW === "new-value-1" && onDiskSE5.SE_SECOND === "new-value-2", "zSE5b) the second write merged: both vars are on disk");
      assert(onDiskSE5.SE_KEEP === "keep-me" && onDiskSE5.KEY_SE_A === "secrets-value-a", "zSE5c) unrelated entries survived the merge");

      // SE6) stale baseRevision -> 409 with the live revision, file untouched
      const beforeSE6 = await readFile(secretsPathSE, "utf8");
      const rSE6 = await postEnv({ name: "SE_CONFLICT", value: "nope", baseRevision: 1 });
      const bSE6 = await rSE6.json();
      assert(rSE6.status === 409 && typeof bSE6.revision === "number" && bSE6.revision > 0, "zSE6a) stale baseRevision -> 409 carrying the current revision");
      assert((await readFile(secretsPathSE, "utf8")) === beforeSE6, "zSE6b) the conflicted write left the secrets file unchanged");
      assert(JSON.parse(beforeSE6).SE_CONFLICT === undefined, "zSE6c) the conflicted name never reached disk");
      const rSE6b = await postEnv({ name: "SE_CONFLICT", value: "forced", baseRevision: 1, force: true });
      assert(rSE6b.status === 200, "zSE6d) the same save with force:true succeeds (" + rSE6b.status + ")");

      // SE7) every write leaves a snapshot beside the store
      assert((await snapshots()).length > 0, "zSE7a) a snapshot file appears in secrets.history/ after a write");
      assert((await snapshots()).every((f) => f.endsWith(".json")), "zSE7b) snapshots are JSON files");

      // SE8) validation: bad names, an empty name, and an empty value
      const badSE8 = [
        ["se_lower", "a lowercase name"],
        ["SE-DASH", "a dashed name"],
        ["", "an empty name"],
      ];
      for (const [badName, label] of badSE8) {
        const rr = await postEnv({ name: badName, value: "x", force: true });
        assert(rr.status === 400, "zSE8 " + label + " -> 400 (got " + rr.status + ")");
      }
      const rrSE8 = await postEnv({ name: "SE_EMPTY", value: "", force: true });
      const bbSE8 = await rrSE8.json();
      assert(rrSE8.status === 400 && /empty/.test(JSON.stringify(bbSE8)), "zSE8d) an empty value -> 400 pointing at the remove action");
      assert((await secretsOnDisk()).SE_EMPTY === undefined && (await secretsOnDisk()).se_lower === undefined, "zSE8e) no invalid write reached the file");

      // SE9) delete removes exactly the named entry, and only once
      const rSE9 = await sendEnv("DELETE", "SE_SECOND");
      const bSE9 = await rSE9.json();
      const after9 = await secretsOnDisk();
      assert(rSE9.status === 200 && bSE9.ok === true && bSE9.action === "removed", "zSE9a) DELETE /api/env?name=X removes the entry");
      assert(after9.SE_SECOND === undefined && after9.SE_NEW === "new-value-1", "zSE9b) only the named var was removed; the other survived");
      const rSE9b = await sendEnv("DELETE", "SE_SECOND");
      assert(rSE9b.status === 409, "zSE9c) deleting a name that is not secrets-managed -> 409 (got " + rSE9b.status + ")");
      const rSE9c = await sendEnv("DELETE", "CE_MASTER");
      assert(rSE9c.status === 400, "zSE9d) deleting a protected name -> 400 (got " + rSE9c.status + ")");

      // SE10) reloadable reflects what is read once at startup
      const bSE10 = await listEnv();
      // ROUTER_HISTORY is declared in .env, so the (now config-scoped) list
      // carries it; it is read once at module load, hence not reloadable.
      const histSE10 = findVar(bSE10, "ROUTER_HISTORY");
      assert(!!histSE10 && histSE10.reloadable === false, "zSE10a) ROUTER_HISTORY reports reloadable:false");
      const masterSE10 = findVar(bSE10, "CE_MASTER");
      assert(!!masterSE10 && masterSE10.editable === false, "zSE10b) the masterKeyEnv name reports editable:false");
      assert(findVar(bSE10, "SE_NEW").reloadable === true, "zSE10c) an ordinary var reports reloadable:true");
      // The list is scoped to config-referenced + secrets + .env names, so
      // ambient shell noise must NOT appear.
      assert(!findVar(bSE10, "PATH") && !findVar(bSE10, "COMPUTERNAME"), "zSE10d) unrelated ambient shell vars are not listed");
      // ROUTER_HISTORY is declared only in .env here: it stays editable, since
      // saving it moves the name into the store, which outranks the file.
      const histEditSE10 = findVar(bSE10, "ROUTER_HISTORY");
      assert(!!histEditSE10 && histEditSE10.editable === true, "zSE10e) an .env-declared name stays editable (a save takes it over into the store)");
    } finally {
      childSE.kill();
      await rm(dirSE, { recursive: true, force: true });
    }
  }

  // ---- env editor: boot precedence (shell > secrets.json > .env) -------------
  // SHELL_ENV_NAMES is snapshotted in main() BEFORE loadEnv()/loadSecrets(), so
  // a var passed in the child's env: wins over both files and must be reported
  // as platform-owned rather than silently overwritten.
  {
    const dirP = await mkdtemp(join(tmpdir(), "lmr-se-prec-"));
    const cfgPathP = join(dirP, "config.json");
    const secretsPathP = join(dirP, "secrets.json");
    const cfgP = {
      port: 0, prefix: "/v1", masterKeyEnv: "CE_MASTER", uiPasswordEnv: null,
      backends: [{ id: "se-p", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_SE_P" }],
      models: { "se-model": { providers: [{ backend: "se-p", upstream: "u" }] } },
      presets: { "se-preset": { strategy: "affinity", models: ["se-model"] } },
    };
    await writeFile(cfgPathP, JSON.stringify(cfgP, null, 2) + "\n");
    await writeFile(join(dirP, ".env"), "KEY_SE_P=env-file-value-p\nCE_MASTER=master-secret\n");
    await writeFile(secretsPathP, JSON.stringify({ KEY_SE_P: "secrets-value-p", SE_OTHER: "other-secret-p" }, null, 2) + "\n");
    const childP = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPathP, ROUTER_ENV: join(dirP, ".env"), ROUTER_SECRETS: secretsPathP, KEY_SE_P: "shell-value-p" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let baseP = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("secrets precedence server did not start")), 8000);
      childP.stderr.on("data", (d) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { baseP = `http://${HOST}:${parseInt(m[1], 10)}`; clearTimeout(timer); resolve(); }
      });
    });
    try {
      const apiSE = (base, path, opts) => api(base, path, { ...opts, headers: { authorization: "Bearer master-secret", ...(opts && opts.headers) } });
      const bP = await (await apiSE(baseP, "/api/env", { method: "GET" })).json();
      const varP = (bP.vars || []).find((v) => v.name === "KEY_SE_P");
      assert(!!varP && varP.source === "shell", "zSE11a) a var the shell already owned reports source 'shell'");
      const revP = await (await apiSE(baseP, "/api/env/reveal", { body: { name: "KEY_SE_P" } })).json();
      assert(revP.value === "shell-value-p", "zSE11b) reveal returns the shell value, not the secrets/.env value");
      const otherP = (bP.vars || []).find((v) => v.name === "SE_OTHER");
      assert(!!otherP && otherP.source === "secrets" && otherP.live === true, "zSE11c) a secrets-only name still applies when the shell does not own it");
      assert((await (await apiSE(baseP, "/api/env/reveal", { body: { name: "SE_OTHER" } })).json()).value === "other-secret-p", "zSE11d) the secrets-only value reached process.env");
      // A shell-owned name is display-only in the editor: editable:false, and a
      // write is refused with 400 because the store entry would be silently
      // ignored by applyRuntimeEnv anyway.
      assert(varP.editable === false, "zSE11e) the shell-owned name reports editable:false");
      const beforePE = await readFile(secretsPathP, "utf8");
      const rPostP = await apiSE(baseP, "/api/env", { body: { name: "KEY_SE_P", value: "hijack-from-ui" } });
      const bPostP = await rPostP.json();
      assert(rPostP.status === 400 && JSON.stringify(bPostP).includes("KEY_SE_P") && /platform or shell environment/.test(JSON.stringify(bPostP)), "zSE11f) writing the shell-owned name -> 400 naming the owner");
      assert((await readFile(secretsPathP, "utf8")) === beforePE, "zSE11g) the rejected shell-owned write left the secrets file byte-identical");
      const otherEditP = (bP.vars || []).find((v) => v.name === "SE_OTHER");
      assert(!!otherEditP && otherEditP.editable === true, "zSE11h) a secrets-owned name stays editable");
    } finally {
      childP.kill();
      await rm(dirP, { recursive: true, force: true });
    }
  }

  // ---- env editor: watcher hot-reload ---------------------------------------
  // A hand edit of secrets.json (not a UI write, which applies directly) must
  // reach process.env through the fs.watch path with no restart.
  {
    const dirW = await mkdtemp(join(tmpdir(), "lmr-se-watch-"));
    const cfgPathW = join(dirW, "config.json");
    const secretsPathW = join(dirW, "secrets.json");
    const cfgW = {
      port: 0, prefix: "/v1", masterKeyEnv: "CE_MASTER", uiPasswordEnv: null,
      backends: [{ id: "se-w", baseURL: "http://127.0.0.1:1", apiKeyEnv: "KEY_SE_W" }],
      models: { "se-model": { providers: [{ backend: "se-w", upstream: "u" }] } },
      presets: { "se-preset": { strategy: "affinity", models: ["se-model"] } },
    };
    await writeFile(cfgPathW, JSON.stringify(cfgW, null, 2) + "\n");
    await writeFile(join(dirW, ".env"), "KEY_SE_W=env-file-value-w\nCE_MASTER=master-secret\n");
    await writeFile(secretsPathW, JSON.stringify({}, null, 2) + "\n");
    const childW = spawn(process.execPath, ["server.mjs"], {
      cwd: join(import.meta.dirname),
      env: { ...process.env, ROUTER_CONFIG: cfgPathW, ROUTER_ENV: join(dirW, ".env"), ROUTER_SECRETS: secretsPathW },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let baseW = null;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("secrets watcher server did not start")), 8000);
      childW.stderr.on("data", (d) => {
        const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
        if (m) { baseW = `http://${HOST}:${parseInt(m[1], 10)}`; clearTimeout(timer); resolve(); }
      });
    });
    try {
      const apiSE = (base, path, opts) => api(base, path, { ...opts, headers: { authorization: "Bearer master-secret", ...(opts && opts.headers) } });
      await new Promise((r) => setTimeout(r, 400)); // let the watcher attach
      // Written directly (not via the API) so only the fs.watch path can make
      // it visible; the API write applies to process.env synchronously.
      await writeFile(secretsPathW, JSON.stringify({ SE_WATCHED: "watched-value" }, null, 2) + "\n");
      let liveW = false;
      const deadlineW = Date.now() + 2000;
      while (!liveW && Date.now() < deadlineW) {
        try {
          const bb = await (await apiSE(baseW, "/api/env", { method: "GET" })).json();
          const vv = (bb.vars || []).find((v) => v.name === "SE_WATCHED");
          liveW = !!(vv && vv.live === true && vv.source === "secrets");
        } catch {
          /* a transient fetch failure during reload is "not ready yet" */
        }
        if (!liveW) await new Promise((r) => setTimeout(r, 50));
      }
      assert(liveW === true, "zSE12a) an edit to secrets.json hot-reloads (live:true) with no restart");
      const revW = await (await apiSE(baseW, "/api/env/reveal", { body: { name: "SE_WATCHED" } })).json();
      assert(revW.value === "watched-value", "zSE12b) the hand-edited value reached process.env via the watcher");
    } finally {
      childW.kill();
      await rm(dirW, { recursive: true, force: true });
    }
  }

  // ---- recovery: documented cooldown settings (plan Task 2.1) ---------------
  // The config contract documents rateLimit*/server*/auth* key names, and the
  // pre-v3 docs used rate*/billing* aliases. backoffFor must honour BOTH, with
  // the canonical name winning when a config carries both spellings. A config
  // that sets the documented key but is ignored by the code is worse than one
  // with no key at all: the operator believes a cooldown is 17ms and gets 30s.
  {
    const bfCases = [
      ["recBackoff1", "rate", { rateLimitBaseMs: 17, rateLimitMaxMs: 60 }, 1, 17],
      ["recBackoff2", "rate", { rateLimitBaseMs: 17, rateLimitMaxMs: 60 }, 3, 60],
      ["recBackoff3", "rate", { rateBaseMs: 9, rateMaxMs: 30 }, 2, 18],
      ["recBackoff4", "rate", { rateLimitBaseMs: 17, rateLimitMaxMs: 60, rateBaseMs: 9, rateMaxMs: 30 }, 1, 17],
      ["recBackoff5", "auth", { authBaseMs: 19, authMaxMs: 60 }, 1, 19],
      ["recBackoff6", "auth", {}, 1, 60000],
      ["recBackoff7", "billing", { serverBaseMs: 21, serverMaxMs: 70 }, 1, 21],
      ["recBackoff8", "billing", { billingBaseMs: 11, billingMaxMs: 50 }, 2, 22],
      ["recBackoff9", "server", {}, 1, 30000],
      ["recBackoff10", "rate", {}, 20, 600000],
      ["recBackoff11", "auth", {}, 20, 1800000],
    ];
    for (const [id, kind, backoff, fails, want] of bfCases) {
      const got = SRV.backoffFor(kind, { backoff }, fails);
      assert(
        got === want,
        id + ") backoffFor(" + kind + ", " + JSON.stringify(backoff) + ", fails " + fails + ") must be " + want +
          " - the documented config key names (rateLimit*/server*/auth*) and their legacy aliases (rate*/billing*) both apply, canonical wins, and the per-class default is used when the config sets nothing; got " + got
      );
    }
  }
  // ---- config editor: merge-preserve contract (index.html) -------------------
  // index.html ships its UI as one inline <script>, and the config editor's
  // collect/apply pair are top-level declarations - so they are extracted BY NAME
  // with brace matching and evaluated in a node:vm context against stub globals.
  // That runs the real production editor code headlessly: no browser, no jsdom,
  // no dependency. The extractor is the part that can rot silently, so u6a proves
  // it found every function before any contract assertion runs and fails loudly
  // (naming what was missing) if it did not.
  {
    let htmlSource = "";
    let readError = null;
    try {
      htmlSource = await readFile(join(import.meta.dirname, "index.html"), "utf8");
    } catch (e) {
      readError = e.message;
    }

    // Source of one top-level `function NAME(...) {...}`, located by name. Strings,
    // template literals and comments are skipped so a brace inside one cannot end
    // the body early (none of the extracted bodies holds a regex literal).
    function sliceFunction(source, name) {
      let at = source.indexOf("\nfunction " + name + "(");
      if (at < 0) at = source.indexOf("\nasync function " + name + "(");
      if (at < 0) return null;
      let depth = 0;
      for (let i = source.indexOf("{", at); i >= 0 && i < source.length; i++) {
        const c = source[i];
        if (c === "\"" || c === "'" || c === "`") {
          for (i++; i < source.length && source[i] !== c; i++) if (source[i] === "\\") i++;
          continue;
        }
        if (c === "/" && source[i + 1] === "/") {
          const nl = source.indexOf("\n", i);
          if (nl < 0) break;
          i = nl;
          continue;
        }
        if (c === "/" && source[i + 1] === "*") {
          const end = source.indexOf("*/", i + 2);
          if (end < 0) break;
          i = end + 1;
          continue;
        }
        if (c === "{") depth++;
        else if (c === "}" && --depth === 0) return source.slice(at + 1, i + 1);
      }
      return null;
    }

    const needed = ["cfgDeepCopy", "cfgParseMaybeJson", "cfgJsonText", "cfgMutate", "cfgCloseModal", "deploymentListOf", "renderCfgModal", "renderDeployList", "cfgModalCollect", "cfgApplyModal", "countRefsTo"];
    const bodies = [];
    const broken = [];
    for (const name of needed) {
      const body = sliceFunction(htmlSource, name);
      if (!body || !(() => { try { new vm.Script(body); return true; } catch { return false; } })()) {
        broken.push(name);
        continue;
      }
      bodies.push(body);
    }
    const dollarDecl = (htmlSource.match(/\nconst \$ = [^\n;]*;/) || [])[0];
    if (!dollarDecl || !(() => { try { new vm.Script(dollarDecl); return true; } catch { return false; } })()) broken.push("$");

    // The vm globals: `cfg` is the draft/modal state, getElementById serves form
    // stubs, and the HTML-building helpers stay inert - only the two functions
    // that read the form and write the draft are under test here.
    const fields = new Map();
    const stubField = (v) => ({ value: typeof v === "boolean" ? String(v) : v == null ? "" : String(v), checked: v === true, style: {}, innerHTML: "", textContent: "" });
    const mountFields = (spec) => {
      fields.clear();
      for (const [id, v] of Object.entries(spec)) fields.set(id, stubField(v));
    };
    const confirmLog = [];
    let confirmAnswer = true;
    const cfg = { draft: null, modal: null, dirty: false };
    const sandbox = {
      cfg,
      console,
      confirm: (msg) => { confirmLog.push(msg); return confirmAnswer; },
      renderConfig: () => {},
      ICONS: { warn: "" },
      document: { getElementById: (id) => { if (!fields.has(id)) fields.set(id, stubField("")); return fields.get(id); } },
      esc: (v) => String(v == null ? "" : v),
      cfgField: () => "",
      cfgJsonText: () => "",
      cfgEnvNames: () => [],
      renderDeployList: () => {},
      renderPresetModelList: () => {},
    };
    let api = null;
    let evalError = null;
    if (broken.length === 0) {
      const program = [dollarDecl]
        .concat(bodies, ["({ $, cfgDeepCopy, cfgParseMaybeJson, cfgJsonText, cfgMutate, cfgCloseModal, deploymentListOf, renderCfgModal, renderDeployList, cfgModalCollect, cfgApplyModal, countRefsTo })"])
        .join("\n");
      try {
        api = vm.runInContext(program, vm.createContext(sandbox));
      } catch (e) {
        evalError = e.message;
      }
    }
    const harnessOk = broken.length === 0 && !evalError && !!api && typeof api.$ === "function" && typeof api.cfgModalCollect === "function" && typeof api.cfgApplyModal === "function";
    assert(
      harnessOk,
      "u6a) the index.html editor harness extracted " + bodies.length + "/" + needed.length + " function(s) plus $ and evaluated them in a vm context" +
        (readError ? " [index.html unreadable: " + readError + "]" : "") +
        (broken.length ? " [NOT FOUND OR UNPARSEABLE: " + broken.join(", ") + "]" : "") +
        (evalError ? " [evaluation failed: " + evalError + "]" : "")
    );

    if (!harnessOk) {
      assert(false, "u6a2) the config editor merge-preserve contract could not be exercised: the harness did not come up");
    } else {
      const norm = (v) => JSON.stringify(v, (k, x) => (x && typeof x === "object" && !Array.isArray(x) ? Object.keys(x).sort().reduce((o, kk) => ((o[kk] = x[kk]), o), {}) : x));
      const same = (a, b) => norm(a) === norm(b);
      const open = (kind, key, data, index) => {
        cfg.modal = { kind, key, data: api.cfgDeepCopy(data || {}), index: index == null ? null : index, deploys: [], models: [], exposedOn: !(data && data.exposed === false) };
      };

      // The trickiest input this editor can be handed: BOTH arrays. The rows come
      // from `providers` because that is what the server's own converter reads
      // (v37n), so a form that took the hand-written `deployments` would show a
      // list the router is not honouring.
      const modelEntry = {
        providers: [
          { backend: "go", upstream: "glm-5.3", dropParams: ["temperature"], params: { top_p: 1 }, paramMap: { max_tokens: "max_output_tokens" }, unownedRowKey: 7 },
          { backend: "cc", upstream: "glm-5.3-cc", dropParams: ["top_p"] },
        ],
        affinityPool: 2,
        retry: { maxRetries: 2, baseMs: 500, maxMs: 4000, multiplier: 2, totalMs: 20000 },
        params: { temperature: 0.7 },
        meta: { label: "GLM 5.3", contextWindow: 200000, unownedMeta: "keep", pricing: { inputPerM: 0.1, outputPerM: 0.4, cacheHitInputPerM: 0.01, unownedPricing: "keep" } },
        exposed: false,
        deployments: [{ backend: "go", upstream: "glm-5.3", backupOnly: true }],
      };
      // What the two provider rows become: same order, same dialect keys, and no
      // `affinityPool` (v3 removed the concept, so nothing reads it back).
      const modelDeploys = [
        { backend: "go", upstream: "glm-5.3", dropParams: ["temperature"], params: { top_p: 1 }, paramMap: { max_tokens: "max_output_tokens" }, unownedRowKey: 7 },
        { backend: "cc", upstream: "glm-5.3-cc", dropParams: ["top_p"] },
      ];
      // An entry already in v3, with every default the form must NOT write back.
      const v3Entry = {
        deployments: [
          { backend: "go", upstream: "v3-one" },
          { backend: "cc", upstream: "v3-two", weight: 3, backupOnly: true, params: { top_p: 1 }, dropParams: ["temperature"], paramMap: { max_tokens: "max_output_tokens" } },
        ],
        retry: { maxRetries: 1 },
        params: { temperature: 0.7 },
        meta: { label: "v3", contextWindow: 200000, pricing: { inputPerM: 0.1, outputPerM: 0.4, cacheHitInputPerM: 0.01 } },
      };
      const presetEntry = {
        strategy: "failover",
        models: ["glm-5.3-flash", { model: "z", weight: 3 }],
        affinityPool: 3,
        params: { reasoning_effort: "high" },
        exposed: false,
        strategyV3: { kind: "ordered" },
        meta: { label: "Tier", tier: "balanced", whenToUse: "cheap", unownedMeta: "keep" },
      };
      const backendEntry = { id: "go", baseURL: "http://go", apiKeyEnv: "KEY_GO", translateDeveloperRole: true, model: "deepseek-v4-flash", timeoutMs: 90000, params: { temperature: 0.2 }, dropParams: ["n"], unownedBackendKey: "keep" };
      const freshDraft = () => ({
        port: 8787,
        backends: [{ id: "go", baseURL: "http://go", apiKeyEnv: "KEY_GO" }, { id: "cc", baseURL: "http://cc", apiKeyEnv: "KEY_CC" }],
        models: {
          a: { providers: [{ backend: "go", upstream: "ua" }] },
          "glm-5.3-flash": api.cfgDeepCopy(modelEntry),
          z: { providers: [{ backend: "cc", upstream: "uz" }] },
        },
        presets: {
          "p-one": { strategy: "affinity", models: [{ model: "glm-5.3-flash", weight: 2 }] },
          "p-two": { strategy: "failover", models: ["glm-5.3-flash"] },
        },
      });
      // The deployment-row boxes the real render emits for entry `entry`.
      const jsonText = (v) => (v === undefined ? "" : JSON.stringify(v, null, 2));
      const deployFields = (entry, over) => {
        const list = Array.isArray(entry.providers) ? entry.providers : Array.isArray(entry.deployments) ? entry.deployments : [];
        const spec = {};
        list.forEach((d, i) => {
          spec["f_d" + i + "_backend"] = d.backend || "";
          spec["f_d" + i + "_upstream"] = d.upstream || "";
          spec["f_d" + i + "_weight"] = d.weight == null ? "" : String(d.weight);
          spec["f_d" + i + "_backupOnly"] = !!d.backupOnly;
          spec["f_d" + i + "_params"] = jsonText(d.params);
          spec["f_d" + i + "_dropParams"] = jsonText(d.dropParams);
          spec["f_d" + i + "_paramMap"] = jsonText(d.paramMap);
        });
        return Object.assign(spec, over || {});
      };
      const modelFields = (over) =>
        Object.assign(
          {
            f_id: "glm-5.3-flash",
            f_metaLabel: "GLM 5.3",
            f_metaCtx: "200000",
            f_priceIn: "0.1",
            f_priceOut: "0.4",
            f_priceCache: "0.01",
            f_offPeak: "",
            f_params: '{"temperature":0.7}',
            f_retry: '{"maxRetries":2,"baseMs":500,"maxMs":4000,"multiplier":2,"totalMs":20000}',
          },
          over || {}
        );

      // A model whose providers carry dialect keys, rendered by the REAL
      // renderCfgModal: the row objects it builds are what collect sees, so a row
      // that stops carrying its previous provider fails here.
      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields()));
      api.renderCfgModal();
      assert(
        cfg.modal.deploys.length === 2 && !!cfg.modal.deploys[0].prev && same(cfg.modal.deploys[0].prev.dropParams, ["temperature"]),
        "u6f) the real modal render keeps each deployment's previous object reachable on its row"
      );
      const merged = api.cfgModalCollect();
      assert(same(merged.deployments, modelDeploys), "u6b2) collect keeps dropParams/params/paramMap and unowned row keys on every deployment, converting the provider rows in declared order");
      assert(merged.exposed === false && !("providers" in merged) && !("affinityPool" in merged), "u6b3) collect keeps a model-level unowned key (exposed: false) and REPLACES providers/affinityPool - the server reads a both-keys model as providers-wins, so keeping the legacy key would silently discard this edit");
      assert("deployments" in merged, "u6b3b) a model that arrived with BOTH providers and deployments collects deployments derived from the PROVIDERS (the array the server's converter honours)");
      assert(merged.meta.unownedMeta === "keep" && merged.meta.pricing.unownedPricing === "keep", "u6b4) collect keeps unowned keys inside meta and meta.pricing");

      const mergedSnap = api.cfgDeepCopy(merged);
      delete mergedSnap.id;
      api.cfgApplyModal();
      const storedModel = cfg.draft.models["glm-5.3-flash"];
      assert(
        !!storedModel && same(storedModel, mergedSnap) && !("prev" in storedModel) && !("data" in storedModel),
        "u6c) apply stores the merged model under its own key with no modal bookkeeping keys left behind"
      );

      cfg.draft.presets.mix = api.cfgDeepCopy(presetEntry);
      open("presets", "mix", Object.assign({ id: "mix" }, api.cfgDeepCopy(presetEntry)));
      mountFields({ f_id: "mix", f_strategy: "failover", f_metaLabel: "Tier", f_metaTier: "balanced", f_metaWhen: "cheap", f_params: '{"reasoning_effort":"high"}' });
      api.renderCfgModal();
      const mergedPreset = api.cfgModalCollect();
      assert(
        mergedPreset.exposed === false && same(mergedPreset.strategyV3, { kind: "ordered" }) && mergedPreset.meta.unownedMeta === "keep" && !("affinityPool" in mergedPreset),
        "u6d) a preset keeps unowned keys (exposed, a real v3 field) and unowned meta keys, and drops the affinityPool the form no longer owns (v3 removed the concept)"
      );
      const presetSnap = api.cfgDeepCopy(mergedPreset);
      delete presetSnap.id;
      api.cfgApplyModal();
      assert(
        same(cfg.draft.presets.mix, presetSnap) && same(cfg.draft.presets.mix.models, ["glm-5.3-flash", { model: "z", weight: 3 }]),
        "u6d2) apply stores the merged preset without disturbing its model rows"
      );

      cfg.draft = freshDraft();
      cfg.draft.backends[0] = api.cfgDeepCopy(backendEntry);
      open("backends", "go", api.cfgDeepCopy(backendEntry), 0);
      mountFields({ f_id: "go", f_baseURL: "http://go", f_apiKeyEnv: "KEY_GO", f_timeoutMs: "90000", f_translateDeveloperRole: true, f_model: "deepseek-v4-flash", f_params: '{"temperature":0.2}' });
      api.renderCfgModal();
      assert(same(api.cfgModalCollect(), backendEntry), "u6e) a backend keeps unowned keys (dropParams, backend-level params) through collect");

      // An emptied box is an explicit clear: its key must go, never silently keep
      // the previous value.
      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ f_metaLabel: "", f_metaCtx: "", f_priceIn: "", f_priceOut: "", f_priceCache: "", f_params: "", f_retry: "" })));
      api.renderCfgModal();
      const cleared = api.cfgModalCollect();
      assert(!("params" in cleared) && !("retry" in cleared) && !("affinityPool" in cleared), "u6f1) an emptied model field removes its key instead of silently keeping the previous value (affinityPool is never written at all)");
      assert(
        !!cleared.meta && !("label" in cleared.meta) && !("contextWindow" in cleared.meta) && !("inputPerM" in cleared.meta) && cleared.meta.unownedMeta === "keep" && cleared.meta.pricing.unownedPricing === "keep",
        "u6f2) clearing meta/pricing fields removes only the keys the form owns"
      );
      cfg.draft.models.bare = { providers: [{ backend: "go", upstream: "u" }], meta: { label: "x" } };
      open("models", "bare", { id: "bare", providers: [{ backend: "go", upstream: "u" }], meta: { label: "x" } });
      mountFields({ f_id: "bare" });
      api.renderCfgModal();
      assert(!("meta" in api.cfgModalCollect()), "u6f3) a meta object whose every form-owned key was cleared is dropped, not left as {}");
      cfg.draft = freshDraft();
      cfg.draft.backends[0] = api.cfgDeepCopy(backendEntry);
      open("backends", "go", api.cfgDeepCopy(backendEntry), 0);
      mountFields({ f_id: "go", f_baseURL: "http://go", f_apiKeyEnv: "KEY_GO", f_translateDeveloperRole: false });
      api.renderCfgModal();
      assert(!("translateDeveloperRole" in api.cfgModalCollect()), "u6f4) unchecking the developer-role box clears its key (an unchecked box is an explicit clear)");

      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ f_metaLabel: "Renamed", f_metaCtx: "100000", f_priceIn: "0.2", f_priceOut: "", f_priceCache: "", f_offPeak: '{"inputPerM":0.14}', f_params: '{"temperature":0.9}', f_retry: '{"maxRetries":5}' })));
      api.renderCfgModal();
      const edited = api.cfgModalCollect();
      assert(
        !("affinityPool" in edited) && edited.meta.label === "Renamed" && edited.meta.contextWindow === 100000 && edited.meta.pricing.inputPerM === 0.2 &&
          same(edited.meta.pricing.offPeak, { inputPerM: 0.14 }) && same(edited.params, { temperature: 0.9 }) && same(edited.retry, { maxRetries: 5 }) && !("outputPerM" in edited.meta.pricing),
        "u6g) a non-empty field overrides the previous value and a cleared one is removed, in the same save, and no field writes the removed affinityPool"
      );

      // R9: a value equal to the DEFAULT is omitted, so the form's own defaults
      // never become config the operator did not type.
      const w1 = api.cfgDeepCopy(modelEntry.providers);
      w1[0].weight = 1;
      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields({ providers: w1 }, modelFields()));
      api.renderCfgModal();
      const w1Out = api.cfgModalCollect();
      assert(!("weight" in w1Out.deployments[0]), "u6g2) a deployment whose weight field is blank or 1 collects NO weight key (the default is 1, and writing it would put an explicit weight: 1 on every deployment)");

      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ "f_d0_weight": "2", "f_d0_backupOnly": true, "f_d1_dropParams": "", "f_d0_paramMap": '{"max_tokens":"max_output_tokens"}' })));
      api.renderCfgModal();
      const w2Out = api.cfgModalCollect();
      assert(
        w2Out.deployments[0].weight === 2 && w2Out.deployments[0].backupOnly === true && !("dropParams" in w2Out.deployments[1]) && same(w2Out.deployments[0].paramMap, { max_tokens: "max_output_tokens" }),
        "u6g3) a deployment's weight and backupOnly round-trip from their boxes, and an emptied dialect box clears only its own key"
      );

      // An unchecked exposure box is the one direction that writes; a checked one
      // omits the key, because `exposed: true` IS the default.
      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ f_exposed: false })));
      api.renderCfgModal();
      assert(api.cfgModalCollect().exposed === false, "u6g4) an UNCHECKED Exposed box on a model collects exposed: false");
      cfg.draft.presets.mix = api.cfgDeepCopy(presetEntry);
      open("presets", "mix", Object.assign({ id: "mix" }, api.cfgDeepCopy(presetEntry)));
      mountFields({ f_id: "mix", f_strategy: "failover", f_exposed: false });
      api.renderCfgModal();
      assert(api.cfgModalCollect().exposed === false, "u6g5) an UNCHECKED Exposed box on a preset collects exposed: false too (exposure covers both namespaces)");

      // The v3 round-trip on an entry that is ALREADY v3: nothing may be added,
      // moved or dropped, so the deployment objects come back exactly as given.
      // This fixture carries every box the form has a field for - label, context
      // window, the three prices, the retry budget and the per-deployment dialect
      // keys - and `modelFields` fills exactly those boxes, which is what the real
      // render does for this entry.
      const v3Want = Object.assign({ id: "v3" }, api.cfgDeepCopy(v3Entry));
      const v3Fields = { f_id: "v3", f_metaLabel: "v3", f_metaCtx: "200000", f_priceIn: "0.1", f_priceOut: "0.4", f_priceCache: "0.01", f_params: '{"temperature":0.7}', f_retry: '{"maxRetries":1}' };
      cfg.draft = freshDraft();
      cfg.draft.models.v3 = api.cfgDeepCopy(v3Entry);
      open("models", "v3", Object.assign({ id: "v3" }, api.cfgDeepCopy(v3Entry)));
      api.renderCfgModal();
      mountFields(deployFields(v3Entry, modelFields(v3Fields)));
      cfg.modal.exposedOn = true;
      const v3Out = api.cfgModalCollect();
      assert(
        same(v3Out, v3Want),
        "u6g6) an entry that is ALREADY v3 round-trips byte-identically through the real render: no weight: 1 written on the weight-less deployment, no exposed added, no dialect key moved (" + JSON.stringify(v3Out) + ")"
      );
      // The same entry with its Exposed box UNCHECKED: that is the one direction
      // which writes, and it must change that single key and nothing else.
      open("models", "v3", Object.assign({ id: "v3" }, api.cfgDeepCopy(v3Entry)));
      api.renderCfgModal();
      mountFields(deployFields(v3Entry, modelFields(v3Fields)));
      cfg.modal.exposedOn = false;
      const v3HiddenOut = api.cfgModalCollect();
      assert(
        same(v3HiddenOut, Object.assign({}, v3Want, { exposed: false })),
        "u6g6b) unchecking Exposed on a v3 entry changes that one key and nothing else (" + JSON.stringify(v3HiddenOut) + ")"
      );

      // The XOR, the invariant a future refactor is most likely to break: collect
      // may never emit a model carrying both arrays, from any of the three inputs.
      // A model with both is read by the server as providers-wins, so emitting
      // both would discard the operator's own edit.
      const xorCases = {
        "old shape": { providers: [{ backend: "go", upstream: "u1" }] },
        "new shape": { deployments: [{ backend: "go", upstream: "u1" }] },
        "both shapes": { providers: [{ backend: "go", upstream: "u1" }], deployments: [{ backend: "cc", upstream: "u9" }] },
      };
      const xorBroken = [];
      for (const [label, entry] of Object.entries(xorCases)) {
        cfg.draft = freshDraft();
        open("models", "xorm", Object.assign({ id: "xorm" }, api.cfgDeepCopy(entry)));
        mountFields(deployFields(entry, { f_id: "xorm" }));
        api.renderCfgModal();
        const out = api.cfgModalCollect();
        if ("providers" in out) xorBroken.push(label + ": emitted providers");
        if (!Array.isArray(out.deployments) || out.deployments.length !== 1) xorBroken.push(label + ": deployments " + JSON.stringify(out.deployments));
        if (out.deployments && out.deployments[0] && out.deployments[0].upstream !== "u1") xorBroken.push(label + ": rows came from the wrong array (" + JSON.stringify(out.deployments) + ")");
      }
      assert(
        xorBroken.length === 0,
        "u6g7) collect NEVER emits `providers`, and takes its rows from `providers` when that array is present - for an old-shape input, a v3 input, and an input carrying both (" + (xorBroken.length ? xorBroken.join(" | ") : "all three clean") + ")"
      );

      // Create path: no previous entry exists, so nothing may leak from the keys
      // the modal itself keeps, and a row added with the + button stays clean.
      cfg.draft = freshDraft();
      open("models", null, {});
      mountFields({ f_id: "fresh", f_metaLabel: "Fresh", f_params: '{"a":1}' });
      api.renderCfgModal();
      cfg.modal.deploys.push({ backend: "go", upstream: "go-model", weight: null, backupOnly: false, orig: { params: "", dropParams: "", paramMap: "" } });
      const hOut = api.cfgModalCollect();
      assert(
        same(hOut, { id: "fresh", deployments: [{ backend: "go", upstream: "go-model" }], meta: { label: "Fresh" }, params: { a: 1 } }),
        "u6h) a brand-new model with no previous entry collects a clean object: deployments, a row with no invented weight or backupOnly, and no affinityPool/providers residue (" + JSON.stringify(hOut) + ")"
      );
      open("presets", null, {});
      mountFields({ f_id: "freshp", f_strategy: "fallback" });
      api.renderCfgModal();
      cfg.modal.models.push({ model: "a", weight: null });
      const h2Out = api.cfgModalCollect();
      assert(
        same(h2Out, { id: "freshp", strategy: "fallback", models: ["a"] }),
        "u6h2) a brand-new preset with no previous entry collects a clean object, at the v3 default strategy and with no exposed key (the default is exposed) (" + JSON.stringify(h2Out) + ")"
      );
      open("backends", null, {});
      mountFields({ f_id: "newb", f_baseURL: "http://new", f_apiKeyEnv: "KEY_NEW", f_timeoutMs: "1000" });
      api.renderCfgModal();
      assert(
        same(api.cfgModalCollect(), { id: "newb", baseURL: "http://new", apiKeyEnv: "KEY_NEW", timeoutMs: 1000 }),
        "u6h3) a brand-new backend with no previous entry collects a clean object"
      );

      // The apply half still renames: prompt, reference rewrite, key order.
      cfg.draft = freshDraft();
      confirmLog.length = 0;
      confirmAnswer = true;
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ f_id: "glm-5.4-flash", f_metaLabel: "", f_metaCtx: "", f_priceIn: "", f_priceOut: "", f_priceCache: "", f_offPeak: "", f_params: "", f_retry: "" })));
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(Object.keys(cfg.draft.models).join(",") === "a,glm-5.4-flash,z", "u6i) renaming a model keeps its position in the ordered key rebuild");
      assert(
        confirmLog.length === 1 && /Rename 'glm-5\.3-flash' to 'glm-5\.4-flash'\?/.test(confirmLog[0]) && /2 reference\(s\)/.test(confirmLog[0]),
        "u6i2) the rename prompt still names both ids and the reference count"
      );
      assert(
        cfg.draft.presets["p-one"].models[0].model === "glm-5.4-flash" && cfg.draft.presets["p-two"].models[0] === "glm-5.4-flash",
        "u6i3) a rename rewrites preset references in both row shapes"
      );
      assert(same(cfg.draft.models["glm-5.4-flash"].deployments, modelDeploys), "u6i4) the renamed entry keeps its per-deployment dialect keys");
      cfg.draft.backends[0] = { id: "go", baseURL: "http://go", apiKeyEnv: "KEY_GO", dropParams: ["y"] };
      confirmLog.length = 0;
      open("backends", "go", api.cfgDeepCopy(cfg.draft.backends[0]), 0);
      mountFields({ f_id: "go-2", f_baseURL: "http://go", f_apiKeyEnv: "KEY_GO" });
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(
        cfg.draft.models.a.providers[0].backend === "go-2" && cfg.draft.models["glm-5.4-flash"].deployments[0].backend === "go-2",
        "u6i5) renaming a backend still rewrites the references that point at it, in both the v3 array and the legacy one an unsaved entry still carries"
      );

      // Collect must copy the previous entry: starting from the live draft object
      // and deleting keys on it would edit the draft before the user saves.
      cfg.draft = freshDraft();
      const draftSnap = api.cfgDeepCopy(cfg.draft);
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      mountFields(deployFields(modelEntry, modelFields({ f_metaLabel: "", f_params: "" })));
      api.renderCfgModal();
      api.cfgModalCollect();
      assert(same(cfg.draft, draftSnap), "u6j) collect copies the previous entry instead of aliasing and editing the live draft");

      // Deployment rows fed directly, so the row contract is pinned independently of
      // the modal render: emptied upstream, unowned row key, backend-less row, new row.
      // The render is called FIRST because it rebuilds the row list; the rows are
      // then replaced with what the render would have produced for the same entry,
      // and each dialect box is mounted holding that row's own baseline text.
      cfg.draft = freshDraft();
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)));
      api.renderCfgModal();
      const rowBaseline = (json) => {
        const text = api.cfgJsonText(json);
        return { text, stub: text, parsed: api.cfgParseMaybeJson(text) };
      };
      const b0params = rowBaseline({ top_p: 1 });
      const b0drop = rowBaseline(["temperature"]);
      const b0map = rowBaseline({ max_tokens: "max_output_tokens" });
      cfg.modal.deploys = [
        { backend: "go", upstream: "", prev: Object.assign({ backend: "go", upstream: "glm-5.3", dropParams: ["temperature"], unownedRowKey: 1 }, { params: b0params.parsed, paramMap: b0map.parsed }), weight: null, backupOnly: false, orig: { params: b0params.text, dropParams: b0drop.text, paramMap: b0map.text } },
        { backend: "", upstream: "ghost", prev: { backend: "old", upstream: "ghost" }, weight: null, backupOnly: false, orig: { params: "", dropParams: "", paramMap: "" } },
        { backend: "cc", upstream: "new-upstream", weight: null, backupOnly: false, orig: { params: "", dropParams: "", paramMap: "" } },
      ];
      mountFields({ "f_d0_params": b0params.stub, "f_d0_dropParams": b0drop.stub, "f_d0_paramMap": b0map.stub });
      const rowMerged = api.cfgModalCollect();
      assert(
        rowMerged.deployments.length === 2 && same(rowMerged.deployments[0], { backend: "go", dropParams: ["temperature"], unownedRowKey: 1, params: b0params.parsed, paramMap: b0map.parsed }) && same(rowMerged.deployments[1], { backend: "cc", upstream: "new-upstream" }),
        "u6k) an emptied upstream is deleted, unowned row keys survive, a backend-less row is dropped, and a new row stays clean - each row's dialect box is compared against the baseline text the row was fed, so an unchanged box keeps its key (" + JSON.stringify(rowMerged.deployments) + ")"
      );

      // The blank-box-means-clear rule is expressed against the text the box was
      // RENDERED with. A row that carries no rendered baseline at all therefore
      // cannot be read as a clear: an empty box on such a row would otherwise
      // delete a dialect key the operator never saw, which is silent data loss on
      // any future modal path that builds a row without filling `orig`.
      cfg.draft = freshDraft();
      cfg.draft.models.glm5 = { deployments: [{ backend: "go", upstream: "u1", params: { top_p: 1 }, dropParams: ["temperature"], paramMap: { max_tokens: "max_output_tokens" } }] };
      open("models", "glm5", api.cfgDeepCopy(cfg.draft.models.glm5));
      api.renderCfgModal();
      cfg.modal.deploys = [{ backend: "go", upstream: "u1", weight: null, backupOnly: false, prev: { backend: "go", upstream: "u1", params: { top_p: 1 }, dropParams: ["temperature"], paramMap: { max_tokens: "max_output_tokens" } } }];
      mountFields({ "f_d0_params": "", "f_d0_dropParams": "", "f_d0_paramMap": "" });
      const noBaseline = api.cfgModalCollect();
      assert(
        same(noBaseline.deployments, [{ backend: "go", upstream: "u1", params: { top_p: 1 }, dropParams: ["temperature"], paramMap: { max_tokens: "max_output_tokens" } }]),
        "u6k2) a deployment row with NO rendered baseline keeps every dialect key even though its boxes are empty - a blank box only clears a key the operator was shown (" + JSON.stringify(noBaseline.deployments) + ")"
      );

      // A backend's id lives INSIDE its element (draft.backends is an array), so
      // apply has to leave it there; models and presets are keyed maps and drop it.
      // A config that lost the id does not merely look wrong - it fails validation.
      const ENV_KEY = "U6_BACKEND_ID_KEY";
      process.env[ENV_KEY] = "set-for-validation";
      const vDraft = () => ({
        port: 8787,
        backends: [{ id: "go", baseURL: "http://go", apiKeyEnv: ENV_KEY, idleTimeoutMs: 120000 }, { id: "cc", baseURL: "http://cc", apiKeyEnv: ENV_KEY }],
        models: { m1: { providers: [{ backend: "go", upstream: "u1", dropParams: ["z"] }] } },
        presets: { p1: { strategy: "affinity", models: ["m1"] } },
      });
      const backendFields = (over) =>
        Object.assign({ f_id: "go", f_baseURL: "http://go", f_apiKeyEnv: ENV_KEY, f_timeoutMs: "90000", f_translateDeveloperRole: true, f_model: "deepseek-v4-flash", f_params: '{"temperature":0.2}' }, over || {});
      const saveErrors = () => validateConfig(cfg.draft, null, { mode: "save" }).errors || [];

      cfg.draft = vDraft();
      open("backends", "go", api.cfgDeepCopy(cfg.draft.backends[0]), 0);
      mountFields(backendFields());
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(!!cfg.draft.backends[0] && cfg.draft.backends[0].id === "go", "u6l1) editing a backend stores the element with its id intact");
      assert(saveErrors().length === 0, "u6l2) the config a backend edit produces validates in save mode" + (saveErrors().length ? " - " + saveErrors().map((e) => e.path + ": " + e.message).join("; ") : ""));

      cfg.draft = vDraft();
      open("backends", null, {});
      mountFields(backendFields({ f_id: "fresh-backend" }));
      api.renderCfgModal();
      api.cfgApplyModal();
      const pushedBackend = cfg.draft.backends[cfg.draft.backends.length - 1];
      assert(!!pushedBackend && pushedBackend.id === "fresh-backend", "u6l3) adding a backend stores the pushed element with its id intact");
      assert(saveErrors().length === 0, "u6l4) the config a backend add produces validates in save mode");

      cfg.draft = vDraft();
      confirmLog.length = 0;
      confirmAnswer = true;
      open("backends", "go", api.cfgDeepCopy(cfg.draft.backends[0]), 0);
      mountFields(backendFields({ f_id: "go-renamed" }));
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(!!cfg.draft.backends[0] && cfg.draft.backends[0].id === "go-renamed", "u6l5) accepting a backend rename stores the element under the new id");
      assert(cfg.draft.models.m1.providers[0].backend === "go-renamed", "u6l6) a backend rename still rewrites the reference in a legacy `providers` array (this draft is the pre-v3 shape, which a rename must not silently drop)");
      assert(confirmLog.length === 1 && /Rename 'go' to 'go-renamed'\?/.test(confirmLog[0]), "u6l7) the rename prompt is still offered for a backend id change");
      delete process.env[ENV_KEY];

      // A form value the user never filled in must not become a value: an emptied
      // id has no identity to store under, so apply refuses it with a message
      // instead of offering "Rename 'go' to 'undefined'?" and then a 400.
      cfg.draft = vDraft();
      confirmLog.length = 0;
      open("backends", "go", api.cfgDeepCopy(cfg.draft.backends[0]), 0);
      mountFields(backendFields({ f_id: "" }));
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(
        cfg.draft.backends[0].id === "go" && confirmLog.length === 0 && /id is required/.test(fields.get("cfgModalIssues").innerHTML || ""),
        "u6n1) an emptied backend id is refused with a message instead of a rename to undefined"
      );
      cfg.draft = freshDraft();
      confirmLog.length = 0;
      open("models", "glm-5.3-flash", Object.assign({ id: "glm-5.3-flash" }, api.cfgDeepCopy(modelEntry)), 0);
      mountFields(deployFields(modelEntry, modelFields({ f_id: "" })));
      api.renderCfgModal();
      api.cfgApplyModal();
      assert(
        confirmLog.length === 0 && /id is required/.test(fields.get("cfgModalIssues").innerHTML || ""),
        "u6n2) an emptied model id is refused too (one rule across the three kinds)"
      );

      // ---- real-render mode ----------------------------------------------------
      // The block above hand-feeds every scalar form value, so it cannot see the
      // render -> form-value -> collect half: a field helper that manufactures a
      // value nobody typed (cfgField JSON-encoding an ABSENT textarea into the two
      // characters `""`, say) is invisible to it. This mode reads the HTML that
      // renderCfgModal actually emits, the way a browser would, and then opens and
      // saves every entry of the real mirror config untouched.
      const renderNeeded = ["cfgDeepCopy", "cfgParseMaybeJson", "cfgJsonText", "cfgMutate", "cfgCloseModal", "cfgEnvNames", "cfgField", "deploymentListOf", "renderDeployList", "renderCfgModal", "cfgModalCollect", "cfgApplyModal", "countRefsTo"];
      const rBodies = [];
      const rBroken = [];
      const parses = (src) => { try { new vm.Script(src); return true; } catch { return false; } };
      for (const name of renderNeeded) {
        const body = sliceFunction(htmlSource, name);
        if (!body || !parses(body)) {
          rBroken.push(name);
          continue;
        }
        rBodies.push(body);
      }
      const rDollar = (htmlSource.match(/\nconst \$ = [^\n]*\n/) || [])[0];
      const rEsc = (htmlSource.match(/\nconst esc = [^\n]*\n/) || [])[0];
      for (const pair of [["$", rDollar], ["esc", rEsc]]) if (!pair[1] || !parses(pair[1])) rBroken.push(pair[0]);

      // A fake DOM fed by the emitted HTML: <input value='..'>, <textarea>..</textarea>
      // and the selected <select> option, unescaped through the same five entities
      // esc writes. cfgModalBody.innerHTML is the seam the renderer writes through.
      const rFields = new Map();
      const rField = (id) => {
        if (!rFields.has(id)) {
          // A field can be a HOST the renderer writes into after the body was
          // parsed (the deployment list and the preset model list both are), so
          // its innerHTML setter parses the rows it is handed. That parse MERGES:
          // the body assignment below owns the reset, because it is the one write
          // that starts a fresh modal.
          let inner = "";
          rFields.set(id, {
            value: "",
            checked: false,
            style: {},
            textContent: "",
            get innerHTML() { return inner; },
            set innerHTML(html) { inner = html; rParse(html); },
          });
        }
        return rFields.get(id);
      };
      const rUnesc = (s) => String(s).replace(/&#39;/g, "'").replace(/&quot;/g, "\"").replace(/&gt;/g, ">").replace(/&lt;/g, "<").replace(/&amp;/g, "&");
      const rAttrs = (s) => {
        const out = {};
        for (const m of s.matchAll(/([A-Za-z0-9_-]+)(?:='([^']*)')?/g)) out[m[1]] = m[2] === undefined ? "" : rUnesc(m[2]);
        return out;
      };
      const rParse = (html) => {
        for (const m of html.matchAll(/<input\b([^>]*)>/g)) {
          const a = rAttrs(m[1]);
          if (a.id) rFields.set(a.id, { value: a.value || "", checked: "checked" in a, style: {}, innerHTML: "", textContent: "" });
        }
        for (const m of html.matchAll(/<textarea\b([^>]*)>([\s\S]*?)<\/textarea>/g)) {
          const a = rAttrs(m[1]);
          if (a.id) rFields.set(a.id, { value: rUnesc(m[2]), checked: false, style: {}, innerHTML: "", textContent: "" });
        }
        for (const m of html.matchAll(/<select\b([^>]*)>([\s\S]*?)<\/select>/g)) {
          const a = rAttrs(m[1]);
          if (!a.id) continue;
          const opt = /<option value='([^']*)' selected>/.exec(m[2]);
          rFields.set(a.id, { value: opt ? rUnesc(opt[1]) : "", checked: false, style: {}, innerHTML: "", textContent: "" });
        }
      };
      const rBody = {
        value: "",
        checked: false,
        style: {},
        textContent: "",
        get innerHTML() { return ""; },
        set innerHTML(html) { rFields.clear(); rParse(html); },
      };
      const rCfg = { draft: null, modal: null, dirty: false, envNames: [], loaded: false };
      const rSandbox = {
        cfg: rCfg,
        console,
        confirm: () => true,
        renderConfig: () => {},
        ICONS: { warn: "" },
        document: { getElementById: (id) => (id === "cfgModalBody" ? rBody : rField(id)) },
        renderPresetModelList: () => {},
      };
      let apiR = null;
      let rEvalError = null;
      if (rBroken.length === 0) {
        const program = [rDollar, rEsc]
          .concat(rBodies, ["({ $, esc, cfgField, cfgJsonText, cfgEnvNames, renderCfgModal, cfgModalCollect, cfgApplyModal, cfgDeepCopy })"])
          .join("\n");
        try {
          apiR = vm.runInContext(program, vm.createContext(rSandbox));
        } catch (e) {
          rEvalError = e.message;
        }
      }
      const renderOk = rBroken.length === 0 && !rEvalError && !!apiR && typeof apiR.renderCfgModal === "function" && typeof apiR.cfgApplyModal === "function" && typeof apiR.cfgField === "function";
      assert(
        renderOk,
        "u6m1) the real-render harness extracted " + rBodies.length + "/" + renderNeeded.length + " function(s) plus $ and esc" +
          (rBroken.length ? " [NOT FOUND OR UNPARSEABLE: " + rBroken.join(", ") + "]" : "") +
          (rEvalError ? " [evaluation failed: " + rEvalError + "]" : "")
      );

      let mirror = null;
      let mirrorError = null;
      try {
        mirror = JSON.parse(await readFile(join(import.meta.dirname, "config.json"), "utf8"));
      } catch (e) {
        mirrorError = e.message;
      }
      assert(!!mirror && Array.isArray(mirror.backends) && !!mirror.models && !!mirror.presets, "u6m2) the mirror config.json was read for the round-trip check" + (mirrorError ? " [unreadable: " + mirrorError + "]" : ""));

      const rJson = (v) => JSON.stringify(v);
      const firstDiff = (a, b, path) => {
        const p = path || "";
        if (Array.isArray(a) || Array.isArray(b)) {
          if (!Array.isArray(a) || !Array.isArray(b)) return p + " " + rJson(a) + " -> " + rJson(b);
          for (let i = 0; i < Math.max(a.length, b.length); i++) if (rJson(a[i]) !== rJson(b[i])) return firstDiff(a[i], b[i], p + "[" + i + "]");
          return p;
        }
        if (a && b && typeof a === "object" && typeof b === "object") {
          for (const k of Array.from(new Set(Object.keys(a).concat(Object.keys(b))))) {
            if (!(k in a)) return p + "." + k + " added: " + rJson(b[k]);
            if (!(k in b)) return p + "." + k + " LOST: " + rJson(a[k]);
            if (rJson(a[k]) !== rJson(b[k])) return firstDiff(a[k], b[k], p + "." + k);
          }
          return p;
        }
        return p + " " + rJson(a) + " -> " + rJson(b);
      };

      const rDiffs = [];
      const rErrs = [];
      let rCount = 0;
      let rDiffer = 0;
      let rIdFields = 0;
      let rInvalid = 0;
      let rModelSaves = 0;
      let rModelsMatchingConverter = 0;
      // What a no-op save is EXPECTED to produce. Backends and presets have no v3
      // migration, so they must come back exactly as they went in. A model is the
      // one entry the bridge re-writes: `providers` becomes `deployments` derived
      // by the SERVER's own converter (so the expectation is not a second policy
      // written here), and the editor's own omission rules are applied on top -
      // no `weight` at the default 1, no `exposed` at the default true, dialect
      // keys taken from the former provider rows.
      const modelExpectation = (entry) => {
        const conv = convertLegacyConfig({ models: { m: JSON.parse(JSON.stringify(entry)) }, presets: {} }).config.models.m;
        const out = {};
        for (const k of Object.keys(conv)) {
          if (k === "providers" || k === "affinityPool") continue;
          out[k] = conv[k];
        }
        out.deployments = (conv.deployments || []).map((d) => Object.assign({}, d));
        for (const d of out.deployments) if (d.weight === 1) delete d.weight;
        if (out.exposed === true) delete out.exposed;
        return out;
      };
      // The editor's migration is the SERVER's migration, or the save would write
      // a shape the router reads differently from the one the UI showed.
      const bridgeAgrees = (stored, entry) => {
        const want = modelExpectation(entry);
        return rJson(stored) === rJson(want);
      };
      // POST /api/config runs the same bridge over the whole candidate before it
      // validates or writes, so the draft is measured the way the server would
      // read it. The mirror cannot be clean on its own account: 19 ids are both a
      // model and a preset, which the v3 validator refuses, so the baseline error
      // set is captured once and a save is only counted against what it ADDS.
      const rBridged = (draft) => convertLegacyConfig(JSON.parse(JSON.stringify(draft))).config;
      const rErrKeys = (cfgIn) => validateConfig(cfgIn, null, { mode: "save" }).errors.map((e) => e.path + ": " + e.message);
      const rBaseline = new Set(rErrKeys(rBridged(mirror)));
      const rSaveUntouched = (kind, key, entry, index, expect) => {
        rCount++;
        rCfg.draft = JSON.parse(JSON.stringify(mirror));
        // The tab opens a map entry with its key injected as the id (index.html:5179),
        // which is what puts the id in the ID box; a backend already carries its own.
        const data = kind === "backends" ? entry : Object.assign({ id: key }, entry);
        rCfg.modal = { kind, key, data: JSON.parse(JSON.stringify(data)), index: index == null ? null : index, deploys: [], models: [], exposedOn: !(data && data.exposed === false) };
        apiR.renderCfgModal();
        if (rFields.has("f_id")) rIdFields++;
        apiR.cfgApplyModal();
        const stored = kind === "backends" ? rCfg.draft.backends[index] : rCfg.draft[kind][key];
        if (kind === "models") {
          rModelSaves++;
          if (bridgeAgrees(stored, entry)) rModelsMatchingConverter++;
        }
        const want = expect === undefined ? entry : expect;
        if (rJson(stored) !== rJson(want)) {
          rDiffer++;
          if (rDiffs.length < 8) rDiffs.push(kind + "." + key + ": " + firstDiff(want, stored));
        }
        const fresh = rErrKeys(rBridged(rCfg.draft)).filter((e) => !rBaseline.has(e));
        if (fresh.length) {
          rInvalid++;
          if (rErrs.length < 6) rErrs.push(kind + "." + key + ": " + fresh.join("; "));
        }
      };
      // The exposed box is the one control whose DEFAULT is on, so BOTH directions
      // are pinned here against the HTML the render actually emitted: an entry
      // carrying `exposed: false` must come up UNCHECKED and an entry with the key
      // ABSENT must come up CHECKED. Getting this backwards hides a model from
      // consumers with no visible error anywhere.
      const rExposedCase = (entry) => {
        rCfg.draft = { backends: [], models: { ex: JSON.parse(JSON.stringify(entry)) }, presets: {} };
        rCfg.modal = { kind: "models", key: "ex", data: Object.assign({ id: "ex" }, JSON.parse(JSON.stringify(entry))), index: null, deploys: [], models: [], exposedOn: entry.exposed !== false };
        apiR.renderCfgModal();
        const box = rFields.get("f_exposed");
        const out = apiR.cfgModalCollect();
        return { box: box ? box.checked : null, exposed: out && Object.prototype.hasOwnProperty.call(out, "exposed") ? out.exposed : "absent" };
      };
      const rHidden = rExposedCase({ deployments: [{ backend: "u7b", upstream: "up-1" }], exposed: false });
      const rDefaulted = rExposedCase({ deployments: [{ backend: "u7b", upstream: "up-1" }] });
      assert(
        rHidden.box === false && rHidden.exposed === false,
        "u6m0a) an entry carrying exposed: false renders the Exposed box UNCHECKED and collects that one key back (box " + rHidden.box + ", collected " + rJson(rHidden.exposed) + ")"
      );
      assert(
        rDefaulted.box === true && rDefaulted.exposed === "absent",
        "u6m0b) an entry with NO exposed key renders the box CHECKED and collects nothing for it, because the default IS exposed (box " + rDefaulted.box + ", collected " + rJson(rDefaulted.exposed) + ")"
      );
      if (renderOk && mirror) {
        // The validator requires every referenced apiKeyEnv to exist in THIS
        // process; that is about the machine, not about the payload, so stand the
        // names up for the duration and take them back down.
        const stoodUp = new Set();
        for (const b of mirror.backends) if (b && b.apiKeyEnv && !(b.apiKeyEnv in process.env)) stoodUp.add(b.apiKeyEnv);
        for (const n of stoodUp) process.env[n] = "u6m-placeholder";
        for (const [id, m] of Object.entries(mirror.models)) rSaveUntouched("models", id, m, null, modelExpectation(m));
        for (const [id, p] of Object.entries(mirror.presets)) rSaveUntouched("presets", id, p);
        mirror.backends.forEach((b, i) => rSaveUntouched("backends", b.id, b, i));
        for (const n of stoodUp) delete process.env[n];
      }
      assert(
        rCount > 0 && rIdFields === rCount,
        "u6m3) all " + rCount + " mirror entries rendered a real id field out of the emitted HTML (" + rIdFields + " did) - a silent no-op would pass the round-trip vacuously"
      );
      assert(
        rModelSaves > 0 && rModelsMatchingConverter === rModelSaves,
        "u6m3b) every one of the " + rModelSaves + " mirror model saves derives its deployments from the SERVER's converter output for that same entry - same rows, same order, same backupOnly, same dialect keys (" + rModelsMatchingConverter + " did) - the UI and the router must not hold two different migrations"
      );
      assert(
        rDiffer === 0,
        "u6m4) a no-op open+save of all " + rCount + " mirror entries through the REAL render path produces exactly the expected result: backends and presets byte-identical, and each old-shape model exactly its v3 conversion - no key invented, none dropped, none moved (" + rDiffer + " differ)" + (rDiffs.length ? ": " + rDiffs.join(" | ") : "")
      );
      assert(
        rInvalid === 0,
        "u6m5) every payload the real render path produces validates in save mode (" + rInvalid + " of " + rCount + " entry saves were rejected)" + (rErrs.length ? ": " + rErrs.join(" | ") : "")
      );

      // ---- Task 9: the editor round-trips the REAL mirror config --------------
      // One model's LABEL is the only thing a user changed. Everything else has
      // to come back as it went in - every DEPLOYMENT's dialect keys above all -
      // the payload has to still validate, and the served-id surface must not
      // move. The subject id and its deployment shape are asserted FIRST, so a
      // mirror rename or a deployment-less entry fails here loudly instead of
      // quietly testing some other id, or testing an empty list that cannot fail.
      // The subject is picked from whatever the mirror actually carries, so the
      // block runs against a 17-model and a 20-model mirror alike. The dialect
      // axis cannot ride on it: the committed mirror has no deployment-level
      // dialect key anywhere, so the fixture further down owns that axis and
      // this pick only has to be a real entry the editor can round-trip.
      // Served-surface comparisons must use the shape the router actually runs:
      // the load pipeline converts an old-shape file in memory, so the mirror as
      // written and the mirror as served are two different objects. This is the
      // project's own converter, so the compared form is the effective config,
      // not a hand-written approximation of it. The SUBJECT comes from that
      // served form, because that is the shape the editor now produces.
      const u9MirrorServed = convertLegacyConfig(mirror).config;
      const u9Subject = Object.keys((u9MirrorServed && u9MirrorServed.models) || {}).reduce((best, id) => {
        const n = (u9MirrorServed.models[id] && Array.isArray(u9MirrorServed.models[id].deployments) ? u9MirrorServed.models[id].deployments : []).length;
        return n > (best.n || 0) ? { id, n } : best;
      }, { id: null, n: 0 }).id;
      // servedIds is the rule the Task 7 harness also extracts, through the same
      // helper. The block pulls it again and fails loudly on its own, so the
      // surface comparison below can never run against a rule that silently
      // vanished from index.html.
      const u9Needed = ["servedIds"];
      const u9Bodies = [];
      const u9Broken = [];
      for (const name of u9Needed) {
        const body = sliceFunction(htmlSource, name);
        if (!body || !parses(body)) { u9Broken.push(name); continue; }
        u9Bodies.push(body);
      }
      let u9Api = null;
      let u9EvalError = null;
      if (u9Broken.length === 0) {
        try { u9Api = vm.runInContext(u9Bodies.concat(["({ servedIds })"]).join("\n"), vm.createContext({})); } catch (e) { u9EvalError = e.message; }
      }
      const u9Ok = u9Broken.length === 0 && !u9EvalError && !!u9Api && typeof u9Api.servedIds === "function";
      assert(
        u9Ok,
        "u9a) the served-id rule was extracted from index.html (" + u9Bodies.length + "/" + u9Needed.length + ") and evaluated for the before/after comparison" +
          (u9Broken.length ? " [NOT FOUND OR UNPARSEABLE: " + u9Broken.join(", ") + "]" : "") +
          (u9EvalError ? " [evaluation failed: " + u9EvalError + "]" : "")
      );
      // Served-surface comparisons must use the shape the router actually runs:
      // the load pipeline converts an old-shape file in memory, so the mirror as
      // written and the mirror as served are two different objects. This is the
      // project's own converter, so the compared form is the effective config,
      // not a hand-written approximation of it.
      const u9Entry = u9MirrorServed && u9MirrorServed.models ? u9MirrorServed.models[u9Subject] : null;
      assert(
        !!u9Entry,
        "u9b) the round-trip subject is the served model with the most deployments ('" + u9Subject + "')" +
          (u9Entry
            ? " [picked from the " + Object.keys(u9MirrorServed.models).length + " model(s) this mirror carries]"
            : u9MirrorServed && u9MirrorServed.models
              ? " [the mirror carries " + Object.keys(u9MirrorServed.models).length + " model(s) and the selection found none - the pick is content-dependent, so it has to come from the file]"
              : " [the config has no models object]")
      );
      const u9Provs = u9Entry && Array.isArray(u9Entry.deployments) ? u9Entry.deployments : null;
      const u9Dialect = u9Provs ? u9Provs.filter((p) => p && (Array.isArray(p.dropParams) || p.params || p.paramMap)).length : 0;
      assert(
        !!u9Provs && u9Provs.length > 0,
        // An empty deployment list is what makes the round-trip below vacuous, so
        // that is what this guards. The mirror's own dialect-key count is an
        // ANNOTATION - it is 0 on the committed config and cannot be asserted -
        // and the dialect-key coverage is guaranteed by the fixture round-trip.
        "u9c) the round-trip subject model carries " + (u9Provs ? u9Provs.length : 0) + " deployment(s) (an empty list would make the round-trip assertions below vacuous, which is all this guarantees; this mirror's subject holds " + u9Dialect + " deployment(s) with a dropParams/params/paramMap key, an annotation, not a check - the dialect axis is guaranteed by the fixture round-trip further down)"
      );

      if (!(u9Ok && u9Entry && u9Provs && u9Provs.length > 0)) {
        assert(false, "u9c2) the label-only round-trip could not be exercised: the subject model or the served-id rule is missing");
      } else {
        const u9Label = "GLM 5.3 Flash (task 9 edit)";
        rCfg.draft = JSON.parse(JSON.stringify(mirror));
        rCfg.modal = { kind: "models", key: u9Subject, data: Object.assign({ id: u9Subject }, JSON.parse(JSON.stringify(u9Entry))), index: null, deploys: [], models: [], exposedOn: u9Entry.exposed !== false };
        apiR.renderCfgModal();
        const u9LabelField = rFields.get("f_metaLabel");
        if (u9LabelField) u9LabelField.value = u9Label;
        const u9Collected = apiR.cfgModalCollect();
        apiR.cfgApplyModal();
        const u9Stored = rCfg.draft.models[u9Subject];

        // The whole entry, not just the label: a save that quietly drops a key
        // the form does not own is the defect this pins. `u9Entry` is the SERVER's
        // conversion of the mirror entry, and that conversion spells out
        // `exposed: true` while the editor omits the default (R9) - so the default
        // is removed from the expectation rather than written by the form.
        const u9Expect = JSON.parse(JSON.stringify(u9Entry));
        if (u9Expect.exposed === true) delete u9Expect.exposed;
        u9Expect.meta.label = u9Label;
        const u9Diff = firstDiff(u9Expect, u9Stored);
        assert(
          !!u9LabelField && u9Diff === "" && !!u9Stored && !!u9Stored.meta && u9Stored.meta.label === u9Label,
          "u9d) the real render + collect + apply of a LABEL-ONLY edit on the mirror's '" + u9Subject + "' stores it byte-identical to its input except meta.label ('" + u9Label + "')" +
            (u9LabelField ? "" : " [f_metaLabel was never rendered]") +
            (u9Stored && u9Stored.meta ? "" : " [meta missing after apply]") +
            (u9Diff === "" ? "" : " [first difference: " + u9Diff + "]")
        );

        // Every provider-level dialect key the inputs carry, across both the real
        // mirror subject and the fixture below, compared by value in the collected
        // object and in the stored entry. The pairing is inside the helper because
        // the fixture can be empty and an empty list must count as nothing rather
        // than as a clean result.
        const u9DialectRows = [];
        const u9KeyDiffs = [];
        const u9DialectDiff = (subject, where, list, input) => {
          if (!Array.isArray(list) || list.length !== input.length) {
            u9KeyDiffs.push(subject + " " + where + ": deployment count " + (Array.isArray(list) ? list.length : "none") + " != " + input.length);
            return;
          }
          input.forEach((want, i) => {
            for (const k of ["dropParams", "params", "paramMap"]) {
              if (want && want[k] !== undefined) u9DialectRows.push(subject + "[" + i + "]." + k);
              if (rJson(list[i] && list[i][k]) !== rJson(want && want[k])) {
                u9KeyDiffs.push(subject + " " + where + "[" + i + "]." + k + ": " + rJson(want && want[k]) + " -> " + rJson(list[i] && list[i][k]));
              }
            }
          });
        };
        u9DialectDiff("mirror", "collected", u9Collected && u9Collected.deployments, u9Provs);
        u9DialectDiff("mirror", "stored", u9Stored && u9Stored.deployments, u9Provs);
        assert(
          u9KeyDiffs.length === 0,
          "u9e) every deployment-level dialect key present in the input survives the round-trip unchanged, in the collected object and in the stored entry (" + u9KeyDiffs.length + " key value(s) moved, " + u9DialectRows.length + " dialect key(s) present to check here: " + u9DialectRows.join(", ") + ")"
        );

        // The validator requires every referenced apiKeyEnv to exist in THIS
        // process; that is about the machine, not the payload.
        const u9StoodUp = new Set();
        for (const b of rCfg.draft.backends || []) if (b && b.apiKeyEnv && !(b.apiKeyEnv in process.env)) u9StoodUp.add(b.apiKeyEnv);
        for (const n of u9StoodUp) process.env[n] = "u9-placeholder";
        let u9Verdict = null;
        try { u9Verdict = validateConfig(rBridged(rCfg.draft), null, { mode: "save" }); } finally { for (const n of u9StoodUp) delete process.env[n]; }
        // The draft is the whole MIRROR, so it is measured the way POST /api/config
        // measures it - through the bridge - and against the baseline the untouched
        // mirror already reports: 19 mirror ids are both a model and a preset, which
        // the v3 validator refuses and no edit to one model can repair. A rejected
        // save is therefore one that ADDS an error.
        const u9Errors = u9Verdict ? u9Verdict.errors.map((e) => e.path + ": " + e.message).filter((e) => !rBaseline.has(e)) : null;
        assert(
          !!u9Errors && u9Errors.length === 0,
          "u9f) the payload a label-only edit of the mirror subject produces adds no NEW validation error in SAVE mode, measured on the bridged draft against the untouched mirror's own baseline (" + (u9Errors ? u9Errors.length + " new error(s)" : "no verdict") + ")" +
            (u9Errors && u9Errors.length ? ": " + u9Errors.join("; ") : "")
        );

        // The dialect axis, guaranteed by construction --------------------------

        // The mirror is real data and cannot be made to carry provider-level
        // dialect keys: the committed config has none anywhere, and a future mirror
        // is free to drop them again. So the axis is proven on a config this test
        // builds, through the same REAL render -> form-value -> collect -> apply
        // path the u6 real-render mode established: providers that carry every
        // dialect key, a row whose upstream is emptied, and a model-level
        // `deployments` list the form has no field for, so the merge-preserve claim
        // is exercised on data whose shape is known rather than hoped for.
        // The before/after surface lists are taken from the LIVE mirror here,
        // ahead of the fixture rebuilding a draft of its own.
        const u9BeforeList = u9Api.servedIds(u9MirrorServed);
        const u9FxModel = "u9-fx-dialect-model";
        const u9FxBackend = "u9-fx-backend";
        const u9FxKeyEnv = "U9_FX_KEY";
        const u9FxTail = [
          { backend: u9FxBackend, upstream: "u9-up-2", dropParams: ["thinking"], params: { top_p: 1 }, paramMap: { max_tokens: "max_output_tokens" } },
          { backend: u9FxBackend, upstream: "u9-up-3", dropParams: ["temperature"] },
        ];
        const u9FxInput = {
          providers: [
            { backend: u9FxBackend, upstream: "u9-up-1", dropParams: ["thinking"], params: { reasoning_effort: "high" }, paramMap: { top_p: "top_p" } },
            { backend: u9FxBackend, upstream: "", dropParams: ["presence_penalty"] },
          ].concat(u9FxTail.map((p) => JSON.parse(JSON.stringify(p)))),
          affinityPool: 1,
          params: { temperature: 0.7 },
          retry: { maxRetries: 2, baseMs: 500, maxMs: 4000, multiplier: 2, totalMs: 20000 },
          meta: { label: "U9 dialect fixture", contextWindow: 200000, unweightedMeta: "keep" },
          exposed: false,
          deployments: [{ backend: u9FxBackend, upstream: "u9-up-1", backupOnly: true }],
        };
        // What the form must produce from those rows: the deployment array the
        // server's converter derives (row 2 carries affinityPool 4 > 2, so it is
        // the ordered fallback tail), carrying every dialect key, minus the
        // upstream of the row whose box was left empty, plus the label.
        const u9FxDraft = JSON.parse(JSON.stringify(mirror));
        u9FxDraft.backends = [{ id: u9FxBackend, baseURL: "http://127.0.0.1:9", apiKeyEnv: u9FxKeyEnv, model: "u9-default-upstream" }];
        u9FxDraft.models = {};
        u9FxDraft.models[u9FxModel] = JSON.parse(JSON.stringify(u9FxInput));
        u9FxDraft.presets = { "u9-fx-preset": { strategy: "affinity", models: [u9FxModel], exposed: false } };
        // What the form must produce from those rows: the deployment array the
        // SERVER's converter derives from the same input (pool 1 of 4, so rows 2-4
        // are the ordered fallback tail), carrying every dialect key, minus the
        // upstream of the row whose box was left empty, plus the label. Built from
        // convertLegacyConfig rather than written out, so this test cannot hold a
        // second migration policy.
        const u9FxConverted = convertLegacyConfig({ models: { m: JSON.parse(JSON.stringify(u9FxInput)) }, presets: {} }).config.models.m;
        const u9FxLabel = "U9 dialect fixture (label edit)";
        const u9FxExpect = Object.assign({}, u9FxConverted, {
          deployments: u9FxConverted.deployments.map((d) => Object.assign({}, d)),
          meta: Object.assign({}, u9FxConverted.meta, { label: u9FxLabel }),
        });
        delete u9FxExpect.providers;
        delete u9FxExpect.affinityPool;
        // The one intended difference: the editor deletes the key of a box the
        // operator emptied, where the converter would carry an empty string on.
        for (const d of u9FxExpect.deployments) if (d.upstream === "") delete d.upstream;
        process.env[u9FxKeyEnv] = "u9-fx-placeholder";
        try {
          rCfg.draft = u9FxDraft;
          rCfg.modal = { kind: "models", key: u9FxModel, data: Object.assign({ id: u9FxModel }, JSON.parse(JSON.stringify(u9FxInput))), index: null, deploys: [], models: [] };
          apiR.renderCfgModal();
          const u9FxRowHtml = rField("f_deploylist").innerHTML;
          const u9FxRowCount = (u9FxRowHtml.match(/data-dep-upstream='/g) || []).length;
          const u9FxUpstreams = u9FxInput.providers.filter((p) => p.upstream).filter((p) => u9FxRowHtml.includes("value='" + p.upstream + "'")).length;
          const u9FxLabelField = rFields.get("f_metaLabel");
          assert(
            u9FxRowCount === u9FxInput.providers.length && u9FxUpstreams === u9FxInput.providers.length - 1 && !!u9FxLabelField,
            "u9x1) the dialect fixture rendered every one of its " + u9FxInput.providers.length + " deployment row(s) into the deployment list, carrying the upstreams it was given (" + u9FxRowCount + " upstream input(s) emitted, " + u9FxUpstreams + " upstream value(s) present, label field " + (u9FxLabelField ? "present" : "MISSING") + ") - a render that emitted nothing would make the round-trip below vacuous"
          );
          if (u9FxLabelField) u9FxLabelField.value = u9FxLabel;
          const u9FxCollected = apiR.cfgModalCollect();
          apiR.cfgApplyModal();
          const u9FxStored = rCfg.draft.models[u9FxModel];

          // The whole entry, not just the label. Name the dialect keys explicitly:
          // a helper bug that returns the input object itself would otherwise pass
          // every value comparison here, and so would one that drops a key from
          // both sides at once. u9FxExpect is built above from the input; the one
          // intended difference is the fixture row whose upstream was emptied (the
          // editor drops that key), and the `let` keeps the label edit below
          // explicit rather than folded into the literal.
          const u9FxPending = [];
          const u9FxCheck = (where, list) => {
            if (!Array.isArray(list) || list.length !== u9FxInput.providers.length) {
              u9FxPending.push(where + ": deployment count " + (Array.isArray(list) ? list.length : "none") + " != " + u9FxInput.providers.length);
              return;
            }
            u9FxInput.providers.forEach((want, i) => {
              for (const k of ["dropParams", "params", "paramMap"]) {
                if (want[k] !== undefined && rJson(list[i] && list[i][k]) !== rJson(want[k])) {
                  u9FxPending.push(where + "[" + i + "]." + k + ": " + rJson(want[k]) + " -> " + rJson(list[i] && list[i][k]));
                }
              }
            });
          };
          u9FxCheck("collected", u9FxCollected && u9FxCollected.deployments);
          u9FxCheck("stored", u9FxStored && u9FxStored.deployments);
          assert(
            u9FxPending.length === 0,
            "u9y1) all " + u9FxInput.providers.length + " fixture rows keep the dialect key they were given - dropParams, params and paramMap each present on the ones that carry them - through render -> collect -> apply, collected and stored (" + u9FxPending.length + " missing or changed" + (u9FxPending.length ? ": " + u9FxPending.slice(0, 6).join(" | ") : "") + ")"
          );

          u9DialectDiff("fixture", "collected", u9FxCollected && u9FxCollected.deployments, u9FxExpect.deployments);
          u9DialectDiff("fixture", "stored", u9FxStored && u9FxStored.deployments, u9FxExpect.deployments);
          assert(
            u9KeyDiffs.length === 0 && u9DialectRows.length > 0,
            "u9y2) the fixture carries " + u9DialectRows.length + " deployment-level dialect key(s) in total and every one survives the round-trip by value (" + u9KeyDiffs.length + " moved" + (u9KeyDiffs.length ? ": " + u9KeyDiffs.slice(0, 6).join(" | ") : "") + ")"
          );

          // Key order is NOT compared here: a legacy row keeps the provider's own
          // key order (and any key the form does not own), which is the editor's
          // preservation behavior, while the converter re-emits the canonical
          // order. What must match is the CONTENT of every deployment, so the
          // comparison is made on key-sorted copies and any LOST/added is real.
          const u9FxSorted = (v) => JSON.parse(norm(v));
          const u9FxDiff = firstDiff(u9FxSorted(u9FxExpect), u9FxSorted(u9FxStored));
          assert(
            !!u9FxStored && u9FxDiff === "",
            "u9y3) the fixture entry survives render -> collect -> apply with every deployment equal BY VALUE to the one the SERVER's converter derives from the same provider rows - the label ('" + u9FxLabel + "') and the one provider whose upstream box was left empty are the only intended differences" +
              (u9FxDiff === "" ? "" : " [first difference: " + u9FxDiff + "]")
          );
          const u9FxVerdict = validateConfig(rCfg.draft, null, { mode: "save" });
          const u9FxErrs = u9FxVerdict.errors.map((e) => e.path + ": " + e.message);
          const u9FxBlankIn = u9FxInput.providers.map((p, i) => (p.upstream ? -1 : i)).filter((i) => i >= 0);
          assert(
            u9FxErrs.length === u9FxBlankIn.length &&
              u9FxErrs.every((e, i) => e === "models." + u9FxModel + ".deployments[" + u9FxBlankIn[i] + "].upstream: upstream is required"),
            "u9y4) the fixture payload's ONLY validation error is the one the editor's blank-upstream rule creates: a row whose box was left empty keeps its place and loses its upstream key, which SAVE mode refuses - so the editor can hand the server a payload it will reject, and the row that does it is exactly the row that was emptied (" + u9FxErrs.length + " error(s)" + (u9FxErrs.length ? ": " + u9FxErrs.join("; ") : "") + ")"
          );
        } finally {
          delete process.env[u9FxKeyEnv];
        }

        // Back to the mirror draft the label-only edit produced: the served-surface
        // assertions below are about that edit, not about the fixture. Rebuilt from
        // the mirror rather than aliased, so the children cannot outlive a
        // reference that is about to be replaced.
        rCfg.draft = JSON.parse(JSON.stringify(mirror));
        rCfg.draft.models[u9Subject].meta = Object.assign({}, rCfg.draft.models[u9Subject].meta, { label: u9Label });

        const u9AfterDraft = JSON.parse(JSON.stringify(rCfg.draft));
        u9AfterDraft.models[u9Subject].meta = Object.assign({}, u9AfterDraft.models[u9Subject].meta, { label: u9Label });
        const u9AfterList = u9Api.servedIds(convertLegacyConfig(u9AfterDraft).config);
        let u9FirstMove = -1;
        for (let i = 0; i < Math.max(u9BeforeList.length, u9AfterList.length) && u9FirstMove < 0; i++) if (u9BeforeList[i] !== u9AfterList[i]) u9FirstMove = i;
        assert(
          u9BeforeList.length > 0 && rJson(u9BeforeList) === rJson(u9AfterList),
          "u9g) a models-only edit leaves the served-id surface alone - the same ids in the same order before and after the edit, which is SURFACE stability only: most of these ids are presets shadowing a same-named model, so this does not show that a model entry itself survived (" + u9BeforeList.length + " -> " + u9AfterList.length + " id(s)" +
            (u9FirstMove < 0 ? "" : ", first move at index " + u9FirstMove + ": " + rJson(u9BeforeList[u9FirstMove]) + " -> " + rJson(u9AfterList[u9FirstMove])) + ")"
        );

        // The surface as a REAL router reports it, before and after, and against
        // the UI's own rule - the editor's view and the endpoint's must agree.
        // `port` is the one field overridden: a child has to bind an ephemeral
        // port, and /v1/models does not read it.
        let u9ServedBefore = null;
        let u9ServedAfter = null;
        let u9ServeError = null;
        try {
          const u9BeforeChild = await startRouterCfg(Object.assign({}, u9MirrorServed, { port: 0 }), "");
          try { u9ServedBefore = (await (await fetch(u9BeforeChild.base + "/v1/models")).json()).data.map((x) => x.id); } finally { u9BeforeChild.child.kill(); await rm(u9BeforeChild.dir, { recursive: true, force: true }); }
          const u9AfterChild = await startRouterCfg(Object.assign({}, rCfg.draft, { port: 0 }), "");
          try { u9ServedAfter = (await (await fetch(u9AfterChild.base + "/v1/models")).json()).data.map((x) => x.id); } finally { u9AfterChild.child.kill(); await rm(u9AfterChild.dir, { recursive: true, force: true }); }
        } catch (e) {
          u9ServeError = e.message;
        }
        assert(
          !!u9ServedBefore && !!u9ServedAfter && u9ServedBefore.length > 0 && rJson(u9ServedBefore) === rJson(u9ServedAfter) && rJson(u9ServedBefore) === rJson(u9BeforeList) && rJson(u9ServedAfter) === rJson(u9AfterList),
          "u9h) a real router's /v1/models is identical before and after the edit and matches the UI's served-id rule before and after - the endpoint's surface, which is id and order stability only: the ids are mostly presets shadowing a same-named model, so this never proves a model entry survived (" +
            (u9ServedBefore ? u9ServedBefore.length : "none") + " -> " + (u9ServedAfter ? u9ServedAfter.length : "none") + " id(s), UI rule " + u9AfterList.length + " id(s))" +
            (u9ServeError ? " [child failed: " + u9ServeError + "]" : "")
        );
      }
      // ---- served-id pickers (Task 7) -----------------------------------------
      // The Playground selector, the Compare checkboxes and the persisted-selection
      // restore must all list what the router serves: exposed presets first, then
      // exposed models, no duplicate. The lists are read back OUT of the DOM the
      // real functions wrote - loadConfig owns the pgModel populate - and the rule
      // is cross-checked against a real spawned router's /v1/models, so a picker
      // that diverges from the served rule fails here rather than in a browser.
      const pickNeeded = ["loadConfig", "populateCompareModels", "loadPlaygroundState", "pgSend"];
      const pickBodies = [];
      const pickBroken = [];
      for (const name of pickNeeded) {
        const body = sliceFunction(htmlSource, name);
        if (!body || !parses(body)) { pickBroken.push(name); continue; }
        pickBodies.push(body);
      }
      const servedBody = sliceFunction(htmlSource, "servedIds");
      const servedOk = !!servedBody && parses(servedBody);
      const pickOk = pickBroken.length === 0 && typeof dollarDecl === "string";
      assert(
        pickOk,
        "u7a) the served-id picker harness extracted " + pickBodies.length + "/" + pickNeeded.length + " function(s) plus $ from index.html" +
          (pickBroken.length ? " [NOT FOUND OR UNPARSEABLE: " + pickBroken.join(", ") + "]" : "") +
          (typeof dollarDecl === "string" ? "" : " [NOT FOUND OR UNPARSEABLE: $]")
      );

      if (!pickOk) {
        assert(false, "u7a2) the served-id picker contract could not be exercised: the harness did not come up");
      } else {
        const mkNode = (tag) => ({
          tagName: String(tag || "div").toUpperCase(),
          children: [],
          options: [],
          value: "",
          checked: false,
          disabled: false,
          className: "",
          textContent: "",
          _html: "",
          style: {},
          scrollTop: 0,
          scrollHeight: 0,
          classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
          appendChild(c) {
            this.children.push(c);
            if (this.tagName === "SELECT" && c && c.tagName === "OPTION") this.options.push(c);
            return c;
          },
          get innerHTML() { return this._html; },
          set innerHTML(v) { this._html = String(v); this.children = []; this.options = []; },
          addEventListener() {},
          focus() {},
          querySelectorAll(sel) {
            const out = [];
            const walk = (n) => {
              for (const c of n.children || []) {
                if (sel === "input:checked" && c.tagName === "INPUT" && c.checked) out.push(c);
                walk(c);
              }
            };
            walk(this);
            return out;
          },
        });
        // A <select> whose value follows the browser's rule: it holds a value only
        // while an option with that value exists, so a picker left without a
        // matching option reads "" - the state the send path has to refuse.
        const mkSelect = () => {
          const el = mkNode("select");
          let selected = null;
          Object.defineProperty(el, "value", {
            get: () => (selected ? selected.value : ""),
            set: (v) => { selected = el.options.find((o) => o.value === String(v)) || null; },
          });
          Object.defineProperty(el, "innerHTML", {
            get: () => el._html,
            set: (v) => { el._html = String(v); el.children = []; el.options = []; selected = null; },
          });
          return el;
        };
        const servedRule = (c) => {
          const out = [];
          for (const id of Object.keys((c && c.presets) || {})) {
            if (c.presets[id] && c.presets[id].exposed === false) continue;
            if (!out.includes(id)) out.push(id);
          }
          for (const id of Object.keys((c && c.models) || {})) {
            if (c.models[id] && c.models[id].exposed === false) continue;
            if (!out.includes(id)) out.push(id);
          }
          return out;
        };
        const jw = (v) => JSON.stringify(v);

        // Mount the real loadConfig - which builds the pgModel options, populates
        // the compare list, then applies the persisted selection - against a fake
        // DOM that those functions themselves create.
        const mkPicker = async (cfg, saved) => {
          const els = new Map();
          const el = (id) => {
            if (!els.has(id)) els.set(id, id === "pgModel" ? mkSelect() : mkNode("div"));
            return els.get(id);
          };
          const posted = [];
          const sandbox = {
            console,
            state: { cfg: null },
            PRESET_META: {},
            playgroundStateLoaded: false,
            PG_STORAGE_KEY: "lmr-test-pg",
            pg: { msgs: [], streaming: false, cacheHits: 0, cacheMisses: 0 },
            safeGet: () => JSON.stringify(saved || {}),
            highlightJSON: () => "",
            renderPresets: () => {},
            renderModels: () => {},
            renderThread: () => {},
            restoreDebugState: () => {},
            setDebug: () => {},
            setLatencyMetric: () => {},
            setTokensMetric: () => {},
            setCostMetric: () => {},
            savePlaygroundState: () => {},
            applyServerDebugMetrics: () => {},
            poll: async () => {},
            esc: (v) => String(v == null ? "" : v),
            chatUrl: () => "http://127.0.0.1:9/v1/chat/completions",
            pgAuthHeader: () => null,
            modelUsesThinkingControl: () => false,
            document: {
              getElementById: (id) => el(id),
              createElement: (t) => (t === "select" ? mkSelect() : mkNode(t)),
              createTextNode: (t) => ({ nodeValue: t }),
            },
            fetch: async (url, opts) => {
              if (opts && opts.body) posted.push({ url: String(url), body: opts.body });
              if (String(url).includes("/api/config")) return { json: async () => cfg };
              return { ok: true, headers: { get: () => "u7mock" }, json: async () => ({ choices: [{ message: { content: "ok" } }] }) };
            },
          };
          const exports = "({ $, loadConfig, populateCompareModels, loadPlaygroundState, pgSend" + (servedOk ? ", servedIds" : "") + " })";
          const program = [dollarDecl].concat(pickBodies, servedOk ? [servedBody] : [], [exports]).join("\n");
          const api = vm.runInContext(program, vm.createContext(sandbox));
          await api.loadConfig();
          return {
            api,
            posted,
            el,
            pg: sandbox.pg,
            select: el("pgModel"),
            options: () => el("pgModel").options.map((o) => o.value),
            checkedIds: () => el("pgCompareModels").querySelectorAll("input:checked").map((c) => c.value),
          };
        };

        const pickCfg = {
          presets: {
            "hidden-p": { exposed: false, models: ["m1"] },
            p1: { models: ["m1"] },
            shared: { models: ["m2"] },
          },
          models: {
            m1: {},
            "hidden-m": { exposed: false },
            shared: {},
            m2: {},
          },
        };
        const want = servedRule(pickCfg);
        const A = await mkPicker(pickCfg, { model: "gone-xyz", msgs: [] });
        const servedList = servedOk && typeof A.api.servedIds === "function" ? A.api.servedIds(pickCfg) : null;

        assert(
          servedList !== null && jw(servedList) === jw(want),
          "u7b) servedIds(cfg) lists exposed presets then exposed models, drops duplicates, and lets a preset id win a collision (got " +
            (servedList === null ? "[servedIds NOT FOUND IN index.html]" : jw(servedList)) + ", want " + jw(want) + ")"
        );
        assert(
          jw(A.options()) === jw(want),
          "u7c) the real Playground picker lists exactly the served ids (got " + jw(A.options()) + ", want " + jw(want) + ")"
        );
        assert(
          jw(A.checkedIds()) === jw(want),
          "u7d) the real Compare picker lists exactly the served ids (got " + jw(A.checkedIds()) + ", want " + jw(want) + ")"
        );
        assert(
          A.select.value === want[0] && A.select.value !== "",
          "u7e) restoring a selection whose model no longer exists falls back to the FIRST served id (got " + jw(A.select.value) + ", want " + jw(want[0]) + ")"
        );

        const B = await mkPicker(pickCfg, { model: "m2", msgs: [] });
        assert(
          B.select.value === "m2",
          "u7f) restoring a selection that is still served keeps it, including a served model id (got " + jw(B.select.value) + ", want " + jw("m2") + ")"
        );

        // The rule is pinned against a real router, not only against a locally
        // recomputed expectation: the same ids in the same order as /v1/models.
        const xCfg = {
          port: 0,
          prefix: "/v1",
          masterKeyEnv: null,
          backends: [{ id: "u7b", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_U7", model: "up-1" }],
          models: {
            p1: { providers: [{ backend: "u7b", upstream: "up-1" }] },
            m2: { providers: [{ backend: "u7b", upstream: "up-2" }] },
          },
          presets: { p1: { strategy: "failover", models: ["p1"] }, p2: { strategy: "failover", models: ["m2"] } },
        };
        // The router serves the EFFECTIVE config, and `mkPicker` injects exactly
        // what /api/config returns. Feeding the UI harness the raw old-shape file
        // would compare the UI's rule for a config the router no longer runs, so
        // the injected body is the project's own converted form.
        const xServed = convertLegacyConfig(xCfg).config;
        const X = await mkPicker(xServed, { model: "p1", msgs: [] });
        let servedByServer = null;
        let servedError = null;
        try {
          const x = await startRouterCfg(xCfg, "KEY_U7=test-key\n");
          try {
            const rr = await fetch(x.base + "/v1/models");
            servedByServer = (await rr.json()).data.map((d) => d.id);
          } finally {
            x.child.kill();
          }
        } catch (e) {
          servedError = e.message;
        }
        assert(
          !!servedByServer && servedByServer.length > 0,
          "u7g) a real router's /v1/models list was read for the served-id cross-check" + (servedError ? " [child failed: " + servedError + "]" : "")
        );
        const xList = servedOk && typeof X.api.servedIds === "function" ? X.api.servedIds(xServed) : null;
        assert(
          xList !== null && jw(xList) === jw(servedByServer),
          "u7h) servedIds(cfg) yields the same ids in the same order as a real router's /v1/models (got " +
            (xList === null ? "[servedIds NOT FOUND IN index.html]" : jw(xList)) + ", server " + jw(servedByServer) + ")"
        );
        assert(
          jw(X.options()) === jw(servedByServer),
          "u7i) the Playground picker lists exactly what a real router serves, in the same order (got " + jw(X.options()) + ", server " + jw(servedByServer) + ")"
        );

        // `xCfg` has no `exposed: false` anywhere, so the two assertions above pass
        // even for a rule that ignores exposure entirely. This sibling config hides
        // one preset and one model, and the assertion is two-sided: the hidden ids
        // must be ABSENT from the endpoint (or the comparison below is vacuous) and
        // the UI's rule must still match the endpoint id for id.
        const xHiddenCfg = {
          port: 0,
          masterKeyEnv: null,
          backends: [{ id: "u7b", baseURL: "http://127.0.0.1:9", apiKeyEnv: "KEY_U7", model: "up-1" }],
          models: {
            hvisible: { deployments: [{ backend: "u7b", upstream: "up-1" }] },
            hhidden: { deployments: [{ backend: "u7b", upstream: "up-2" }], exposed: false },
          },
          presets: {
            hshown: { strategy: "fallback", models: ["hvisible"] },
            hhiddenpreset: { strategy: "fallback", models: ["hvisible"], exposed: false },
          },
        };
        const H = await mkPicker(xHiddenCfg, { model: "hshown", msgs: [] });
        let hiddenServed = null;
        let hiddenError = null;
        try {
          const h = await startRouterCfg(xHiddenCfg, "KEY_U7=test-key\n");
          try {
            hiddenServed = (await (await fetch(h.base + "/v1/models")).json()).data.map((d) => d.id);
          } finally {
            h.child.kill();
          }
        } catch (e) {
          hiddenError = e.message;
        }
        const hiddenList = servedOk && typeof H.api.servedIds === "function" ? H.api.servedIds(xHiddenCfg) : null;
        assert(
          !!hiddenServed && hiddenServed.includes("hshown") && hiddenServed.includes("hvisible") && !hiddenServed.includes("hhiddenpreset") && !hiddenServed.includes("hhidden"),
          "u7h2) the exposure fixture really hides something: a real router serves the exposed preset and model and neither id marked exposed: false (" + (hiddenServed ? jw(hiddenServed) : "no list") + ")" + (hiddenError ? " [child failed: " + hiddenError + "]" : "")
        );
        assert(
          hiddenList !== null && jw(hiddenList) === jw(hiddenServed),
          "u7h3) servedIds(cfg) matches a real router's /v1/models for a config that DOES hide a preset and a model, so the UI's served rule is the endpoint's rule including its exposure filter (got " +
            (hiddenList === null ? "[servedIds NOT FOUND IN index.html]" : jw(hiddenList)) + ", server " + jw(hiddenServed) + ")"
        );

        // With nothing served the picker cannot hold a value at all, so the send
        // path must refuse instead of posting a request the router can only reject.
        const C = await mkPicker({ presets: {}, models: {} }, { model: "gone-xyz", msgs: [] });
        C.el("pgInput").value = "hello";
        C.el("pgStream").checked = false;
        await C.api.pgSend();
        assert(
          C.posted.length === 0 && C.pg.msgs.length === 1 && C.pg.msgs[0].error === true,
          "u7j) the send path refuses to post an empty model id (posted " + C.posted.length + ", msgs " + jw(C.pg.msgs.map((m) => m.content)) + ")"
        );

        A.pg.msgs.length = 0;
        A.el("pgInput").value = "hello";
        A.el("pgStream").checked = false;
        await A.api.pgSend();
        const aSent = A.posted.filter((p) => !p.url.includes("/api/config"));
        assert(
          aSent.length === 1 && JSON.parse(aSent[0].body).model === want[0],
          "u7k) a served model is still posted with its own id, so the empty-id guard is not a dead send path (posted " + aSent.length + ")"
        );
      }


      // ---- post-save and post-env-write refresh (Task 8) ----------------------
      // Two pieces of state the page kept from an earlier render: the config viewer
      // (written only by the dashboard loader) and the backend modal's "Key env"
      // select (built once by cfgField and re-rendered by nothing). Both claims are
      // measured through the REAL success paths - cfgSave, envAdd, envSaveEdit -
      // against a stubbed fetch, with the fake DOM's options parsed out of the HTML
      // those functions actually emit, so a render/refresh divergence cannot hide
      // behind a hand-fed form.
      const u8Needed = [
        "cfgDeepCopy", "cfgRawParseError", "cfgCurrentObject", "loadRawConfig", "applyRawPayload",
        "refreshConfigFromDisk", "cfgPost", "cfgSave", "cfgEnvNames", "applyEnvPayload", "loadEnvVars",
        "envAddProblem", "envPost", "envErrText", "envFlagsAuth", "envAdd", "envSaveEdit", "envRemove",
        "cfgField", "renderConfig", "cfgTableHead", "cfgRowActions", "cfgInjectIcons", "cfgSetCount",
        "renderCfgBackends",
      ];
      const u8New = ["renderEnvPickers", "refreshCfgEnvNames"];
      const u8Bodies = [];
      const u8Broken = [];
      const u8NewBroken = [];
      for (const name of u8Needed) {
        const body = sliceFunction(htmlSource, name);
        if (!body || !parses(body)) { u8Broken.push(name); continue; }
        u8Bodies.push(body);
      }
      for (const name of u8New) {
        const body = sliceFunction(htmlSource, name);
        if (!body || !parses(body)) { u8NewBroken.push(name); continue; }
        u8Bodies.push(body);
      }
      if (!dollarDecl || !parses(dollarDecl)) u8Broken.push("$");
      if (!rEsc || !parses(rEsc)) u8Broken.push("esc");

      // A fake DOM fed by the emitted HTML, with a <select> that follows the
      // browser's rules: the option carrying `selected` wins, otherwise the first.
      const u8Dom = new Map();
      const u8Node = () => ({
        value: "", checked: false, disabled: false, hidden: false, className: "", textContent: "",
        style: {}, dataset: {}, children: [],
        classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
        appendChild(c) { this.children.push(c); return c; },
        addEventListener() {}, focus() {}, select() {},
        querySelectorAll() { return []; },
        get innerHTML() { return this._html || ""; },
        set innerHTML(v) { this._html = String(v); },
      });
      const u8Select = () => {
        const el = u8Node();
        let opts = [];
        let picked = null;
        Object.defineProperty(el, "innerHTML", {
          get: () => el._html || "",
          set: (html) => {
            el._html = String(html);
            opts = Array.from(String(html).matchAll(/<option value='([^']*)'([^>]*)>([\s\S]*?)<\/option>/g)).map((m) => ({
              value: rUnesc(m[1]),
              selected: /(^|\s)selected(\s|$)/.test(m[2]),
              label: rUnesc(m[3]),
            }));
            picked = opts.find((o) => o.selected) || opts[0] || null;
          },
        });
        Object.defineProperty(el, "value", {
          get: () => (picked ? picked.value : ""),
          set: (v) => { picked = opts.find((o) => o.value === String(v)) || null; },
        });
        el.optionValues = () => opts.map((o) => o.value);
        return el;
      };
      const u8El = (id) => {
        if (!u8Dom.has(id)) u8Dom.set(id, id === "f_apiKeyEnv" ? u8Select() : u8Node());
        return u8Dom.get(id);
      };

      // The server mock: /api/env serves a name list that gains whatever was just
      // written, and /api/config/raw serves the file the last save stored - with a
      // field the server added itself, so "the viewer shows the current draft" is
      // distinguishable from "the viewer shows the object the page posted".
      let u8Served = [];
      let u8Raw = null;
      const u8Fetched = [];
      const u8Fetch = async (url, opts) => {
        const u = String(url);
        const method = (opts && opts.method) || "GET";
        u8Fetched.push(method + " " + u);
        if (u === "/api/env" && method === "GET") {
          return { status: 200, ok: true, json: async () => ({ vars: u8Served.map((n) => ({ name: n, live: true, editable: true })), revision: "env-rev-1" }) };
        }
        if (u === "/api/env" && method === "POST") {
          const body = JSON.parse(opts.body);
          if (!u8Served.includes(body.name)) u8Served = u8Served.concat([body.name]);
          return { status: 200, ok: true, json: async () => ({ action: "written", live: true, applied: true }) };
        }
        if (u.startsWith("/api/env?name=") && method === "DELETE") {
          const gone = decodeURIComponent(u.slice("/api/env?name=".length));
          u8Served = u8Served.filter((n) => n !== gone);
          return { status: 200, ok: true, json: async () => ({ removed: gone, live: true }) };
        }
        if (u === "/api/config" && method === "POST") {
          const sent = JSON.parse(opts.body).config;
          u8Raw = Object.assign({}, u8Raw, { config: Object.assign({}, sent, { u8ServerNormalized: true }), revision: "rev-2" });
          return { status: 200, ok: true, json: async () => ({ warnings: [], revision: "rev-2" }) };
        }
        if (u === "/api/config/raw") return { status: 200, ok: true, json: async () => u8Raw };
        return { status: 404, ok: false, json: async () => ({}) };
      };

      const u8Cfg = { draft: null, disk: null, modal: null, mode: "forms", dirty: false, loaded: false, text: null, revision: null, envNames: [], snapshots: [], errors: [], warnings: [] };
      const u8Env = { vars: [], revision: null, restartRequired: false, loaded: false, issue: null, revealed: {}, editing: null, draftValue: "", busy: {}, copied: null, status: {} };
      const u8Calls = { renderEnv: 0, hl: 0 };
      const u8HlArgs = [];
      const u8Sandbox = {
        cfg: u8Cfg,
        env: u8Env,
        console,
        confirm: () => true,
        renderCfgModels: () => {},
        renderCfgPresets: () => {},
        renderCfgSettings: () => {},
        renderCfgSnapshots: () => {},
        renderEnv: () => { u8Calls.renderEnv++; },
        showAuthBanner: () => {},
        hideAuthBanner: () => {},
        keysApiAuth: () => null,
        highlightJSON: (o) => { u8Calls.hl++; u8HlArgs.push(o); return "HL:" + JSON.stringify(o); },
        ICONS: { warn: "", check: "", clock: "" },
        fetch: u8Fetch,
        document: { getElementById: u8El, querySelectorAll: () => [], querySelector: () => null, createElement: () => u8Node() },
      };
      const u8Exports = ["$", "esc", "cfgSave", "cfgDeepCopy", "cfgEnvNames", "cfgField", "loadEnvVars", "envAdd", "envSaveEdit", "envRemove", "renderConfig", "renderCfgBackends", "refreshConfigFromDisk"].concat(u8NewBroken.length ? [] : u8New);
      let api8 = null;
      let u8EvalError = null;
      if (u8Broken.length === 0) {
        const program = [dollarDecl, rEsc].concat(u8Bodies, ["({ " + u8Exports.join(", ") + " })"]).join("\n");
        try { api8 = vm.runInContext(program, vm.createContext(u8Sandbox)); } catch (e) { u8EvalError = e.message; }
      }
      const u8Found = u8Needed.length - u8Broken.filter((n) => u8Needed.includes(n)).length;
      const u8Ok = u8Broken.length === 0 && !u8EvalError && !!api8 && typeof api8.cfgSave === "function" && typeof api8.envAdd === "function" && typeof api8.envSaveEdit === "function" && typeof api8.envRemove === "function" && typeof api8.renderConfig === "function";
      assert(
        u8Ok,
        "u8a) the post-save refresh harness extracted " + u8Found + "/" + u8Needed.length + " function(s) plus $ and esc and evaluated them in a vm context" +
          (u8Broken.length ? " [NOT FOUND OR UNPARSEABLE: " + u8Broken.join(", ") + "]" : "") +
          (u8EvalError ? " [evaluation failed: " + u8EvalError + "]" : "")
      );
      assert(
        u8NewBroken.length === 0,
        "u8b) the refresh helpers exist in index.html and were extracted [NOT FOUND OR UNPARSEABLE: " + u8NewBroken.join(", ") + "]"
      );

      if (!u8Ok) {
        assert(false, "u8a2) the post-save refresh contract could not be exercised: the harness did not come up");
      } else {
        const jw8 = (v) => JSON.stringify(v);
        const u8ViewerCfg = () => ({ port: 8787, backends: [{ id: "go", baseURL: "http://go", apiKeyEnv: "KEY_GO" }], models: { "u8-marker-model": { providers: [{ backend: "go", upstream: "up-1" }] } }, presets: {} });
        // Stage the picker the way the modal does: the REAL cfgField builds the
        // options out of cfgEnvNames() and marks the entry's current value.
        const u8StagePicker = (value) => {
          u8El("f_apiKeyEnv").innerHTML = api8.cfgField("Key env (NAME, not the key)", "f_apiKeyEnv", {
            type: "select",
            value,
            options: [{ value: "", label: "(choose an environment variable)" }].concat(api8.cfgEnvNames().map((n) => ({ value: n, label: n }))),
          });
          return u8El("f_apiKeyEnv");
        };
        const u8CanRefresh = typeof api8.renderEnvPickers === "function" && typeof api8.refreshCfgEnvNames === "function";

        // (1) A successful save must leave the config viewer showing the config that
        // is now on disk, not the file the page loaded at startup.
        const firstDraft = u8ViewerCfg();
        u8Cfg.loaded = true;
        u8Cfg.mode = "forms";
        u8Cfg.dirty = true;
        u8Cfg.draft = JSON.parse(JSON.stringify(firstDraft));
        u8Cfg.revision = "rev-1";
        u8Raw = { config: JSON.parse(JSON.stringify(firstDraft)), revision: "rev-1", envNames: ["KEY_GO"], snapshots: [] };
        u8El("configView").innerHTML = "STALE-VIEWER";
        u8Calls.hl = 0;
        u8HlArgs.length = 0;
        await api8.cfgSave();
        const viewHTML = u8El("configView").innerHTML;
        assert(
          viewHTML !== "STALE-VIEWER" && /u8-marker-model/.test(viewHTML) && /u8ServerNormalized/.test(viewHTML) && u8Calls.hl === 1 && jw8(u8HlArgs[0]) === jw8(u8Cfg.draft),
          "u8c) a successful save rewrites the config viewer from the config just written (viewer " + jw8(viewHTML.slice(0, 90)) + ", highlighter calls " + u8Calls.hl + ")" +
            (u8Calls.hl === 1 && jw8(u8HlArgs[0]) !== jw8(u8Cfg.draft) ? " [highlighted a different object than cfg.draft]" : "")
        );

        // (2) A successful env write must add the new name to the backend "Key env"
        // picker that cfgField built once, and keep the open form's selection.
        u8Served = ["KEY_GO", "KEY_CC"];
        u8Cfg.envNames = ["KEY_GO"];
        u8Cfg.draft = u8ViewerCfg();
        const u8Sel = u8StagePicker("KEY_GO");
        const u8Before = u8Sel.optionValues();
        u8El("cfgBackendsTable").innerHTML = "STALE-BACKENDS-TABLE";
        u8El("envAddName").value = "U8_ADDED_KEY";
        u8El("envAddValue").value = "u8-secret-value";
        u8Env.revision = "env-rev-1";
        await api8.envAdd();
        const u8After = u8Sel.optionValues();
        assert(
          u8After.includes("U8_ADDED_KEY") && u8After.includes("KEY_CC") && u8After.includes("KEY_GO") && u8Sel.value === "KEY_GO",
          "u8d) a successful env add puts the new name in the open backend Key env picker without a reload and keeps the selection (before " + jw8(u8Before) + ", after " + jw8(u8After) + ", selection " + jw8(u8Sel.value) + ")"
        );
        assert(
          u8El("cfgBackendsTable").innerHTML !== "STALE-BACKENDS-TABLE",
          "u8e) the env refresh re-renders the backends table too (its 'not set in this environment' badge reads the same list) (got " + jw8(u8El("cfgBackendsTable").innerHTML.slice(0, 60)) + ")"
        );

        // (3) The other env success path (inline row save) must refresh identically.
        u8Served = ["KEY_GO", "KEY_CC"];
        u8Cfg.envNames = ["KEY_GO"];
        u8StagePicker("KEY_GO");
        u8Env.draftValue = "u8-rotated-value";
        await api8.envSaveEdit("KEY_GO");
        const u8AfterEdit = u8El("f_apiKeyEnv").optionValues();
        assert(
          u8AfterEdit.includes("KEY_CC") && u8El("f_apiKeyEnv").value === "KEY_GO",
          "u8f) a successful inline env edit refreshes the same picker and keeps the selection (after " + jw8(u8AfterEdit) + ", selection " + jw8(u8El("f_apiKeyEnv").value) + ")"
        );

        // (4) A name the open form still holds but the served list has dropped must
        // survive the re-render as an extra option instead of being reset to "".
        u8Cfg.draft = u8ViewerCfg();
        u8Cfg.envNames = ["KEY_GO", "KEY_ORPHAN"];
        u8StagePicker("KEY_ORPHAN");
        assert(
          u8El("f_apiKeyEnv").value === "KEY_ORPHAN",
          "u8g0) harness self-check: the staged Key env picker holds KEY_ORPHAN before the refresh"
        );
        u8Served = ["KEY_GO"];
        if (u8CanRefresh) await api8.refreshCfgEnvNames();
        const u8OrphanOpts = u8El("f_apiKeyEnv").optionValues();
        assert(
          u8CanRefresh && u8OrphanOpts.includes("KEY_ORPHAN") && u8El("f_apiKeyEnv").value === "KEY_ORPHAN",
          "u8g) a stored env name the served list no longer carries stays as a selected extra option (options " + jw8(u8OrphanOpts) + ", selection " + jw8(u8El("f_apiKeyEnv").value) + ")" +
            (u8CanRefresh ? "" : " [renderEnvPickers/refreshCfgEnvNames NOT FOUND IN index.html]")
        );

        // (5) The refresh calls renderConfig(), so pin that it cannot reach into an
        // open modal or the draft: that would be a new way to lose typed input.
        u8Cfg.draft = u8ViewerCfg();
        u8Cfg.modal = { kind: "backends", key: null, data: { id: "u8-new", baseURL: "http://x", apiKeyEnv: "KEY_GO" }, index: null, deploys: [], models: [] };
        const draftBefore = jw8(u8Cfg.draft);
        const modalBefore = jw8(u8Cfg.modal);
        u8El("cfgModalBody").innerHTML = "<input id='f_id' value='u8-typed-by-user'>";
        u8Served = ["KEY_GO", "KEY_CC"];
        if (u8CanRefresh) await api8.refreshCfgEnvNames();
        assert(
          u8CanRefresh &&
            jw8(u8Cfg.draft) === draftBefore &&
            jw8(u8Cfg.modal) === modalBefore &&
            /u8-typed-by-user/.test(u8El("cfgModalBody").innerHTML) &&
            u8El("f_apiKeyEnv").optionValues().includes("KEY_CC"),
          "u8h) the env refresh updates the picker while leaving an open modal body and the unsaved draft untouched (draft changed " + (jw8(u8Cfg.draft) !== draftBefore) + ", modal changed " + (jw8(u8Cfg.modal) !== modalBefore) + ", options " + jw8(u8El("f_apiKeyEnv").optionValues()) + ")"
        );

        // (6) A removal changes the name set exactly as much as an add does, so it
        // is the same contract: the removed name leaves the picker, and the open
        // form's selection stays where it was.
        u8Cfg.draft = u8ViewerCfg();
        u8Cfg.envNames = ["KEY_GO", "KEY_CC", "U8_DOOMED"];
        u8Served = ["KEY_GO", "KEY_CC", "U8_DOOMED"];
        const u8RemSel = u8StagePicker("KEY_GO");
        const u8RemBefore = u8RemSel.optionValues();
        await api8.envRemove("U8_DOOMED");
        const u8RemAfter = u8RemSel.optionValues();
        assert(
          u8RemBefore.includes("U8_DOOMED") && !u8Served.includes("U8_DOOMED") && !u8RemAfter.includes("U8_DOOMED") && u8RemAfter.includes("KEY_CC") && u8RemSel.value === "KEY_GO",
          "u8i) a successful env removal drops the name from the open Key env picker and keeps the selection (before " + jw8(u8RemBefore) + ", after " + jw8(u8RemAfter) + ", selection " + jw8(u8RemSel.value) + ", served " + jw8(u8Served) + ")"
        );
      }
    }
  }

  // zPair1) A v3 entry names a PHYSICAL deployment, so two upstreams on ONE account are two
  //   candidates. Collapsing them to the account (the legacy dedup) would make pair-scoped
  //   cooling unreachable and hide the second upstream from every request that ever runs.
  {
    const cfgP = {
      backends: [{ id: "pA", baseURL: "http://x" }],
      models: {
        mPair: { deployments: [{ backend: "pA", upstream: "u1" }, { backend: "pA", upstream: "u2" }] },
        mLegacy: { providers: [{ backend: "pA", upstream: "u1" }, { backend: "pA", upstream: "u2" }] },
      },
      presets: {
        pPair: { strategy: "failover", models: ["mPair"] },
        pLegacy: { strategy: "failover", models: ["mLegacy"] },
      },
    };
    const cPair = SRV.candidates(cfgP, "pPair", "s1");
    const pairs = cPair.ordered.map((a) => a.backend + "/" + a.upstream).sort();
    assert(
      pairs.length === 2 && pairs[0] === "pA/u1" && pairs[1] === "pA/u2",
      "zPair1a) both deployments of one account are distinct v3 candidates (got " + JSON.stringify(cPair.ordered) + ")"
    );
    const cLeg = SRV.candidates(cfgP, "pLegacy", "s1");
    assert(
      cLeg.ordered.length === 1 && cLeg.ordered[0].upstream === "u1",
      "zPair1b) a legacy entry keeps backend-only dedup, first occurrence wins (got " + JSON.stringify(cLeg.ordered) + ")"
    );
  }

  // zPair2) Recovery is keyed by the physical pair, so two names for one deployment share it:
  //   the same route key is the whole basis of "an alias cannot cool a sibling name".
  {
    const cfgS = {
      backends: [{ id: "sA", baseURL: "http://x" }],
      models: { mS1: { deployments: [{ backend: "sA", upstream: "uS" }] }, mS2: { deployments: [{ backend: "sA", upstream: "uS" }] } },
      presets: { pS1: { strategy: "failover", models: ["mS1"] }, pS2: { strategy: "failover", models: ["mS2"] } },
    };
    const a1 = SRV.candidates(cfgS, "pS1", "s9").ordered[0];
    const a2 = SRV.candidates(cfgS, "pS2", "s9").ordered[0];
    assert(
      SRV.routeKey(a1) === SRV.routeKey(a2) && SRV.routeKey(a1) === JSON.stringify(["sA", "uS"]),
      "zPair2) two names for one deployment share one physical route key (" + SRV.routeKey(a1) + " vs " + SRV.routeKey(a2) + ")"
    );
  }

  // zBackup1) A backupOnly deployment is never the hash primary for a FRESH session: it is the
  //   ordered tail, behind the weighted heads, for every session key.
  {
    const cfgB = {
      backends: [{ id: "bA", baseURL: "http://x" }, { id: "bB", baseURL: "http://x" }],
      models: { mB: { deployments: [{ backend: "bA", upstream: "uP" }, { backend: "bB", upstream: "uB", backupOnly: true }] } },
      presets: { pB: { strategy: "failover", models: ["mB"] } },
    };
    let bad = null;
    for (let i = 0; i < 12; i++) {
      const c = SRV.candidates(cfgB, "pB", "fresh-" + i);
      if (c.ordered.length !== 2 || c.ordered[0].upstream !== "uP" || c.ordered[1].upstream !== "uB") { bad = c.ordered; break; }
    }
    assert(
      bad === null,
      "zBackup1) a fresh session keeps the backupOnly deployment in the tail (got " + JSON.stringify(bad) + ")"
    );
  }

  // zAcct1) A 5xx retires its ACCOUNT for the remainder of the request - the account's other
  //   deployments share a dead endpoint, so they are not probed in turn. It must NOT be a
  //   global judgement: the sibling route stays healthy for every other request (v3 spec T5).
  {
    let deadHits = 0;
    let liveHits = 0;
    const srvAcct = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const b = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      if (b.model === "uAcctDead") {
        deadHits++;
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "endpoint on fire" } }));
        return;
      }
      liveHits++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:acct" } }] }));
    });
    const portAcct = await listen(srvAcct);
    const cfgAcct = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "acctA", baseURL: `http://${HOST}:${portAcct}`, apiKeyEnv: "KEY_ACCT" }],
      models: { mAcct: { deployments: [{ backend: "acctA", upstream: "uAcctDead" }, { backend: "acctA", upstream: "uAcctLive" }] } },
      presets: { pAcct: { strategy: "failover", models: ["mAcct"] } },
      backoff: { ...BO, serverBaseMs: 5000, serverMaxMs: 9000 },
    };
    // The hash decides which deployment leads, so the leading one is CHOSEN rather than assumed.
    const pick = (upstream, tag) => {
      for (let i = 0; i < 400; i++) {
        const key = tag + "-" + i;
        const c = SRV.candidates(cfgAcct, "pAcct", key);
        if (c && c.ordered[0] && c.ordered[0].upstream === upstream) return key;
      }
      return null;
    };
    const sDead = pick("uAcctDead", "acct-dead");
    const sLive = pick("uAcctLive", "acct-live");
    const hAcct = await startRouterCfg(cfgAcct, "KEY_ACCT=k\n");
    const call = (session) => fetch(hAcct.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master", "x-session-affinity": session },
      body: JSON.stringify({ model: "pAcct", messages: [{ role: "user", content: "ping" }] }),
      signal: AbortSignal.timeout(15000),
    });
    try {
      assert(sDead !== null && sLive !== null, "zAcct1pre) both deployments are reachable as a hash primary (" + sDead + ", " + sLive + ")");
      const first = await call(sDead);
      await first.text();
      const rows = ((await (await api(hAcct.base, "/health", { method: "GET" })).json()).routes || []).filter((r) => r.backend === "acctA");
      const cooling = rows.filter((r) => r.state === "cooling").map((r) => r.upstream);
      assert(
        first.status === 503 && deadHits === 1 && liveHits === 0 && cooling.length === 1 && cooling[0] === "uAcctDead",
        "zAcct1) a 5xx retires that account for the rest of the request only (status=" + first.status + ", dead=" + deadHits + ", sibling=" + liveHits + ", cooling=" + JSON.stringify(cooling) + ")"
      );
      const second = await call(sLive);
      const secondText = await second.text();
      assert(
        second.status === 200 && liveHits === 1 && deadHits === 1 && secondText.includes("ok:acct"),
        "zAcct1b) the skipped sibling stays globally healthy for other requests (status=" + second.status + ", dead=" + deadHits + ", sibling=" + liveHits + ")"
      );
    } finally {
      hAcct.child.kill();
      srvAcct.close();
    }
  }

  // zDialect1) Two deployments of ONE account may carry different dialect settings, and each
  //   attempt must use ITS OWN: the pair is the candidate, so the second is reachable at all,
  //   and dropParams/paramMap are read per deployment rather than from the first one.
  {
    const bodies = [];
    const srvDial = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const b = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      bodies.push(b);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: b.model, object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:dial" } }] }));
    });
    const portDial = await listen(srvDial);
    const cfgDial = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "dialA", baseURL: `http://${HOST}:${portDial}`, apiKeyEnv: "KEY_DIAL" }],
      models: {
        mDial: {
          deployments: [
            { backend: "dialA", upstream: "uPlain" },
            { backend: "dialA", upstream: "uDialect", dropParams: ["temperature"], paramMap: { top_p: "top_k" } },
          ],
        },
      },
      presets: { pDial: { strategy: "failover", models: ["mDial"] } },
      backoff: { ...BO },
    };
    const pick = (upstream) => {
      for (let i = 0; i < 400; i++) {
        const key = "dial-" + i;
        const c = SRV.candidates(cfgDial, "pDial", key);
        if (c && c.ordered[0] && c.ordered[0].upstream === upstream) return key;
      }
      return null;
    };
    const sPlain = pick("uPlain");
    const sDialect = pick("uDialect");
    const hDial = await startRouterCfg(cfgDial, "KEY_DIAL=k\n");
    const call = (session) => fetch(hDial.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master", "x-session-affinity": session },
      body: JSON.stringify({ model: "pDial", messages: [{ role: "user", content: "ping" }], temperature: 0.5, top_p: 0.9 }),
      signal: AbortSignal.timeout(15000),
    });
    try {
      assert(sPlain !== null && sDialect !== null, "zDialect1pre) both deployments of the account are selectable (" + sPlain + ", " + sDialect + ")");
      await (await call(sPlain)).text();
      await (await call(sDialect)).text();
      const plain = bodies.find((b) => b.model === "uPlain");
      const dialect = bodies.find((b) => b.model === "uDialect");
      assert(
        !!plain && !!dialect && plain.temperature === 0.5 && plain.top_p === 0.9 && !("top_k" in plain)
          && !("temperature" in dialect) && dialect.top_k === 0.9 && !("top_p" in dialect),
        "zDialect1) each deployment keeps its own dialect (plain=" + JSON.stringify(plain && { t: plain.temperature, tp: plain.top_p, tk: plain.top_k }) + ", dialect=" + JSON.stringify(dialect && { t: dialect.temperature, tp: dialect.top_p, tk: dialect.top_k }) + ")"
      );
    } finally {
      hDial.child.kill();
      srvDial.close();
    }
  }

  // zAliasPhys1) Recovery is keyed by the physical pair, so two names for one deployment share
  //   it: a failure charged through one alias refuses the other alias' request too, and the
  //   health projection holds ONE row carrying both names.
  {
    let hits = 0;
    const srvAlias = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      hits++;
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "alias outage" } }));
    });
    const portAlias = await listen(srvAlias);
    const cfgAlias = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "aliasA", baseURL: `http://${HOST}:${portAlias}`, apiKeyEnv: "KEY_ALIAS" }],
      models: {
        mAlias1: { deployments: [{ backend: "aliasA", upstream: "uAlias" }] },
        mAlias2: { deployments: [{ backend: "aliasA", upstream: "uAlias" }] },
      },
      presets: {
        pAlias1: { strategy: "failover", models: ["mAlias1"] },
        pAlias2: { strategy: "failover", models: ["mAlias2"] },
      },
      backoff: { ...BO, serverBaseMs: 5000, serverMaxMs: 9000 },
    };
    const hAlias = await startRouterCfg(cfgAlias, "KEY_ALIAS=k\n");
    const call = (preset, session) => fetch(hAlias.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master", "x-session-affinity": session },
      body: JSON.stringify({ model: preset, messages: [{ role: "user", content: "ping" }] }),
      signal: AbortSignal.timeout(15000),
    });
    try {
      const first = await call("pAlias1", "alias-s1");
      await first.text();
      const rows = ((await (await api(hAlias.base, "/health", { method: "GET" })).json()).routes || []).filter((r) => r.backend === "aliasA");
      // A DIFFERENT session for the second alias: session-scoped exclusions cannot be what stops it.
      const second = await call("pAlias2", "alias-s2");
      await second.text();
      assert(
        first.status === 503 && second.status === 503 && hits === 1 && rows.length === 1 && (rows[0].models || []).length === 2,
        "zAliasPhys1) a cooled physical route refuses both names that resolve to it (first=" + first.status + ", second=" + second.status + ", hits=" + hits + ", rows=" + rows.length + ", models=" + JSON.stringify(rows[0] && rows[0].models) + ")"
      );
    } finally {
      hAlias.child.kill();
      srvAlias.close();
    }
  }

  // zPinBackup1) A session that has SUCCEEDED on a backupOnly deployment keeps preferring it:
  //   backupOnly rules out the hash primary, not a published recovery pin. The control request
  //   with a fresh session proves the primary was healthy again (so the pin, not a cooldown,
  //   is what steered the pinned session).
  {
    let primHits = 0;
    let backupHits = 0;
    const srvPin = http.createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const b = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      if (b.model === "uPinPrimary") {
        primHits++;
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "primary outage" } }));
        return;
      }
      backupHits++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: "m", object: "chat.completion", choices: [{ message: { role: "assistant", content: "ok:backup" } }] }));
    });
    const portPin = await listen(srvPin);
    const cfgPin = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [
        { id: "pinA", baseURL: `http://${HOST}:${portPin}`, apiKeyEnv: "KEY_PIN" },
        { id: "pinB", baseURL: `http://${HOST}:${portPin}`, apiKeyEnv: "KEY_PIN" },
      ],
      models: {
        mPin: { deployments: [{ backend: "pinA", upstream: "uPinPrimary" }, { backend: "pinB", upstream: "uPinBackup", backupOnly: true }] },
      },
      presets: { pPin: { strategy: "failover", models: ["mPin"] } },
      backoff: { ...BO, serverBaseMs: 60, serverMaxMs: 150 },
    };
    const hPin = await startRouterCfg(cfgPin, "KEY_PIN=k\n");
    const call = (session) => fetch(hPin.base + "/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer local-master", "x-session-affinity": session },
      body: JSON.stringify({ model: "pPin", messages: [{ role: "user", content: "ping" }] }),
      signal: AbortSignal.timeout(15000),
    });
    try {
      const first = await call("pin-s1");
      const firstText = await first.text();
      // The primary failed, the account was retired for the request, and the backup served it:
      // that success is the pin this session now holds.
      assert(
        first.status === 200 && firstText.includes("ok:backup") && primHits === 1 && backupHits === 1,
        "zPinBackup1pre) the backupOnly deployment serves after the primary fails (status=" + first.status + ", primary=" + primHits + ", backup=" + backupHits + ")"
      );
      // The primary must become eligible again WITHOUT an operator action: /admin/reset-health
      // deliberately drops every learned pin (an operator health decision outranks a session's
      // preference), so the test waits for the cooldown to lapse on its own instead - readiness
      // by observation, not by sleeping.
      let primRow = null;
      for (let i = 0; i < 200; i++) {
        const hh = await (await api(hPin.base, "/health", { method: "GET" })).json();
        primRow = (hh.routes || []).find((r) => r.backend === "pinA");
        if (primRow && primRow.retryInMs <= 0) break;
        await new Promise((r) => setTimeout(r, 25));
      }
      assert(!!primRow && primRow.retryInMs <= 0, "zPinBackup1pre2) the cooled primary becomes eligible again on its own (retryInMs=" + (primRow && primRow.retryInMs) + ")");
      const pinned = await call("pin-s1");
      const pinnedText = await pinned.text();
      assert(
        pinned.status === 200 && pinnedText.includes("ok:backup") && primHits === 1 && backupHits === 2,
        "zPinBackup1) an established recovery pin keeps preferring the backup deployment (status=" + pinned.status + ", primary=" + primHits + ", backup=" + backupHits + ")"
      );
      const fresh = await call("pin-s2");
      await fresh.text();
      // The fresh session reaches the primary (and is failed over by its 500), while the pinned
      // session never touched it: the DIFFERENCE between the two calls is the pin, not a cooldown.
      assert(
        primHits === 2 && backupHits === 3,
        "zPinBackup1b) a fresh session probes the recovered primary again, so the pin - not a cooldown - steered the pinned session (primary=" + primHits + ", backup=" + backupHits + ")"
      );
    } finally {
      hPin.child.kill();
      srvPin.close();
    }
  }



  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error("test crashed:", e);
  process.exit(1);
});
