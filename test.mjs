// test.mjs — mock-based test for switchblade. No real keys or network.
// Spins up local mock backends and a server instance using a temp config.

import http from "node:http";
import { once } from "node:events";
import { writeFile, mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
// Unit-level imports: server.mjs only starts the server when run as a script,
// so importing it here gives direct access to strategy selection for tests.
import { candidates, normalizeConfig, buildPayload, cacheHitPctOf, buildAlert, resetAlertCooldowns, computeCost } from "./server.mjs";

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
    const rec = { method: req.method, url: req.url, auth, cache, sess, body };
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
  // Wait for the listening banner.
  let port = null;
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server did not start")), 8000);
    let out = "";
    child.stdout.on("data", (d) => { out += d; });
    child.stderr.on("data", (d) => {
      const s = d.toString();
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
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server did not start")), 8000);
    child.stderr.on("data", (d) => {
      const m = /listening on http:\/\/127\.0\.0\.1:(\d+)/.exec(d.toString());
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
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
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
      fh.backends.find((b) => b.id === "f1").state === "cooling" && fh.backends.find((b) => b.id === "f2").state === "healthy",
      "s2) failover: f1 cooled after 500, f2 healthy"
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
    backoff: { rateLimitBaseMs: 50, rateLimitMaxMs: 200, serverBaseMs: 50, serverMaxMs: 200, authBaseMs: 50, authMaxMs: 200, weeklyDefaultMs: 1000 },
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
    // Request now: both cooling; fallback excludes manual u1 -> tries u2 only.
    const u1HitsBefore = u1.hits.length;
    r = await api(baseU, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    assert(
      r.status === 503 && r.headers.get("x-router-backend") === "u2",
      "u5) all-cooling request 503s via system-cooled u2 (status=" + r.status + ", header=" + r.headers.get("x-router-backend") + ")"
    );
    assert(u1.hits.length === u1HitsBefore, "u6) manual-cooled u1 never received the request (hits=" + u1.hits.length + ")");
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
    // Both cooling now: fallback must exclude manual w1 and try weekly w2.
    const w1HitsBefore = w1.hits.length;
    r = await api(baseW, "/v1/chat/completions", { body: { model: "main", messages: [] } });
    assert(r.status === 503 && r.headers.get("x-router-backend") === "w2", "w2) fallback tries weekly w2, not manual w1 (status=" + r.status + ")");
    assert(w1.hits.length === w1HitsBefore, "w3) manual-cooled w1 never received a request (hits=" + w1.hits.length + ")");
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

  // z10) same-key precedence + reserved model: preset 0.2 > model 0.5, body 0.9
  //      wins; params-layer `model` keys ignored; forwarded model = upstream.
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
    const { child: childZ10, base: baseZ10, dir: dirZ10 } = await startRouterCfg(cfgZ10, "KEY_Z10=test-key-z10\n");
    try {
      await api(baseZ10, "/v1/chat/completions", { body: { model: "p10", messages: [], temperature: 0.9 } });
      const bodyWin = JSON.parse(z10.hits[z10.hits.length - 1].body);
      await api(baseZ10, "/v1/chat/completions", { body: { model: "p10", messages: [] } });
      const presetWin = JSON.parse(z10.hits[z10.hits.length - 1].body);
      assert(
        bodyWin.temperature === 0.9 && bodyWin.model === "deepseek-v4-flash" &&
        presetWin.temperature === 0.2 && presetWin.model === "deepseek-v4-flash",
        "z10) precedence body>preset>model; params-layer model ignored (" + JSON.stringify({ bodyWin, presetWin }) + ")"
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
    const hangSrv = http.createServer(() => { /* never respond */ });
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
      backoff: BO,
    };
    const { child: childZ17, base: baseZ17, dir: dirZ17 } = await startRouterCfg(cfgZ17, "KEY_Z17HANG=k\nKEY_Z17OK=k\n");
    try {
      const rr = await fetch(baseZ17 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p17", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const backend = rr.headers.get("x-router-backend");
      const health = await (await api(baseZ17, "/health", { method: "GET" })).json();
      const hang = health.backends.find((b) => b.id === "z17hang");
      assert(
        backend === "z17ok" && hang && hang.fails >= 1,
        "z17) timeout failover: hang backend times out (80ms), request served by next backend (backend=" + backend + ", hangFails=" + (hang ? hang.fails : "?") + ")"
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
    const cfgZ20 = {
      port: 0, prefix: "/v1", masterKeyEnv: null,
      backends: [{ id: "z20", baseURL: `http://${HOST}:${z20port}`, apiKeyEnv: "KEY_Z20" }],
      models: {
        "m20": {
          providers: [{ backend: "z20", upstream: "u20" }], affinityPool: 1,
          retry: { maxRetries: 3, baseMs: 10, maxMs: 50 },
        },
      },
      presets: { "p20": { strategy: "affinity", models: ["m20"] } },
      backoff: BO,
    };
    const { child: childZ20, base: baseZ20, dir: dirZ20 } = await startRouterCfg(cfgZ20, "KEY_Z20=k\n");
    try {
      const rr = await fetch(baseZ20 + "/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer local-master" },
        body: JSON.stringify({ model: "p20", messages: [] }),
        signal: AbortSignal.timeout(8000),
      });
      // Client errors are not retried (hits must stay 1). The router exhausts
      // the pool and surfaces a 503 carrying the upstream's error body - that
      // is pre-existing behavior, not a retry concern; what matters is no retry.
      assert(
        rr.status === 503 && hits === 1,
        "z20) non-retryable client error does NOT retry (hits=" + hits + ", status=" + rr.status + ")"
      );
    } finally {
      childZ20.kill();
      clientSrv.close();
      await rm(dirZ20, { recursive: true, force: true });
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

  // z21) maxRetries exhausted -> falls through to next candidate provider.
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
        rr.status === 200 && backend === "z21good" && badHits === 3,
        "z21) retries exhausted then fallback: badSrv hit 3x (1 + 2 retries), served by good backend (backend=" + backend + ", badHits=" + badHits + ")"
      );
    } finally {
      childZ21.kill();
      badSrv.close(); goodSrv.srv.close();
      await rm(dirZ21, { recursive: true, force: true });
    }
  }

  // z22) totalMs budget stops retries mid-way: with baseMs large enough that
  //      the second retry would exceed the budget, only 2 attempts happen
  //      (1 initial + 1 retry) then failover to the good backend.
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
          providers: [{ backend: "z22bad", upstream: "u22" }, { backend: "z22good", upstream: "u22" }], affinityPool: 1,
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
        rr.status === 200 && backend === "z22good" && badHits === 2,
        "z22) totalMs budget stops retries: badSrv hit 2x (1 + 1 retry), fallback served (backend=" + backend + ", badHits=" + badHits + ")"
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
        const hist = await (await api(baseZ24, "/api/history?limit=1", { method: "GET" })).json();
        if (Array.isArray(hist.entries) && hist.entries[0]) relayErr = hist.entries[0].relayError || null;
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

  // z21) unit-level: top-level strategy governs ordering over the EXPANDED
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
      "z21a) failover: nested presets expand in declared order, top-level strategy governs (" + JSON.stringify(fo.ordered.map((o) => o.model)) + ")"
    );
    // weighted: rng 0.01 -> glm band (weight 3) -> first model m1; rest weight
    // desc with declared-order tiebreak: m2 (glm's second), then os models.
    const cw = candidates(wcfg, "superW", "sess", () => 0.01);
    assert(
      JSON.stringify(cw.ordered.map((o) => o.model)) === JSON.stringify(["m1", "m2", "m3", "m4"]),
      "z21b) weighted: nested preset refs carry top-level weight (" + JSON.stringify(cw.ordered.map((o) => o.model)) + ")"
    );
    // nested preset's own strategy is ignored for ordering (os is affinity but
    // inside a failover parent it contributes declared order).
    assert(
      JSON.stringify(candidates(wcfg, "super", "sess").ordered.map((o) => o.backend)) === JSON.stringify(["b1", "b2", "b3", "b4"]),
      "z21c) nested preset own strategy ignored: failover parent, declared order (" + JSON.stringify(candidates(wcfg, "super", "sess").ordered.map((o) => o.backend)) + ")"
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

  // y1) session affinity from request body session fields (session_id / sessionId);
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
      assert(b1a && b1a === b1b && b1b === b1c, "y1) body session_id binds one session to one backend (" + JSON.stringify([b1a, b1b, b1c]) + ")");
      const b2a = await backendOf({ body: { ...baseOpts, sessionId: "sy-case-1" } });
      const b2b = await backendOf({ body: { ...baseOpts, sessionId: "sy-case-1" } });
      assert(b2a && b2a === b2b, "y2) body sessionId (camelCase) binds one session to one backend (" + JSON.stringify([b2a, b2b]) + ")");
      const h1 = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" }, headers: { "x-session-id": "sy-head-1" } });
      const h2 = await backendOf({ body: { ...baseOpts, session_id: "sy-body-1" }, headers: { "x-session-id": "sy-head-1" } });
      const h3 = await backendOf({ body: { ...baseOpts }, headers: { "x-session-id": "sy-head-1" } });
      assert(h1 && h1 === h2 && h2 === h3, "y3) x-session-id header wins over body session_id (" + JSON.stringify([h1, h2, h3]) + ")");
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

      // Read-only endpoints stay open even without a valid token.
      const openChecks = [
        ["/health", 200], ["/v1/models", 200], ["/api/stats", 200],
        ["/api/history", 200], ["/api/config", 200],
      ];
      for (const [path, want] of openChecks) {
        const rrr = await api(h.base, path, { method: "GET" });
        assert(rrr.status === want, "zA5) read-only " + path + " open without valid token (status=" + rrr.status + ")");
      }
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

    // zM3) error counting + cache denominators: a 429-exhausted flow bumps
    //      kpis.errors/errorRate; a signal request sets cacheHitRate 0.4 and a
    //      signal-less request (no usage at all) must NOT corrupt it.
    {
      let callsM3 = 0;
      const srvM3 = http.createServer(async (req, res) => {
        for await (const c of req) { /* drain */ }
        callsM3++;
        if (callsM3 === 1) {
          res.writeHead(429, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: { message: "rate limited" } }));
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
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error("test crashed:", e);
  process.exit(1);
});
