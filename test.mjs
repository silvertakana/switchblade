// test.mjs — mock-based test for switchblade. No real keys or network.
// Spins up local mock backends and a server instance using a temp config.

import http from "node:http";
import { once } from "node:events";
import { writeFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
// Unit-level imports: server.mjs only starts the server when run as a script,
// so importing it here gives direct access to strategy selection for tests.
import { candidates, normalizeConfig, buildPayload } from "./server.mjs";

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
    } finally {
      childZ16.kill();
      hitSrv.close(); missSrv.close(); unkSrv.close();
      await rm(dirZ16, { recursive: true, force: true });
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

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error("test crashed:", e);
  process.exit(1);
});
