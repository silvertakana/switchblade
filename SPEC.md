# Local Model Router — Spec

A local, boot-launched Node server that replaces the homelab LiteLLM proxy for
DeepSeek V4 Flash routing. It load-balances two OpenCode Go accounts
(session-affinity hashing) with a DeepSeek direct fallback, plus per-account
health tracking with exponential backoff. LiteLLM stays running on the homelab
as a backstop until this is proven, then is decommissioned for the deepseek
family.

## Why

The user is tired of maintaining homelab LiteLLM (config.yaml, Coolify redeploys,
fallback chains, model routing quirks). They want the routing logic on their own
machine, launched at boot, fully under their control.

## Scope

- Serve the OpenAI-compatible chat/completions surface OpenCode and dsh already
  expect (`POST /v1/chat/completions`, `GET /v1/models`).
- Route the **deepseek-v4-flash** family across two OpenCode Go accounts with
  a DeepSeek direct fallback.
- **Out of scope (v1):** the rest of the homelab proxy's surface (GLM family,
  MiMo vision, free/groq/cerebras/openrouter, pro models). Those stay on the
  homelab LiteLLM proxy for now. The router must simply NOT be asked for them
  (clients keep pointing those at litellm) OR pass them through to litellm.
  Decision: V1 serves ONLY the deepseek-flash families it is configured for.
  Models it does not know get a 404-style `model_not_found` error.

## Backends / Targets (from investigation)

| backend id        | baseURL                       | env key           | upstream model |
|-------------------|-------------------------------|-------------------|----------------|
| go-primary        | https://opencode.ai/zen/go/v1 | OPENCODE_GO_KEY   | deepseek-v4-flash |
| go-alt            | https://opencode.ai/zen/go/v1 | OPENCODE_GO_ALT_KEY | deepseek-v4-flash |
| direct            | https://api.deepseek.com/v1   | DEEPSEEK_API_KEY  | deepseek-v4-flash |

Keys: OPENCODE_GO_KEY and OPENCODE_GO_ALT_KEY live ONLY on the homelab
container currently. They must be pulled into a local env file
(`~/.local-model-router/.env` or the project dir) for the router to use.
DEEPSEEK_API_KEY already exists locally (`~/.dsh/.env`).

## Health state machine (per backend)

States: `healthy`, `cooling` (unavailable until `nextAvailableAt`).

- **429 with `GoUsageLimitError`** (weekly limit): mark cooling until the weekly
  reset. Parse `Resets in N days` from the error message if present; else
  default to 7 days. This is the long backoff.
- **429 generic rate-limit**: exponential backoff, base 30s, doubling up to a
  cap (e.g. 10 min), reset on success.
- **5xx / network error / timeout**: exponential backoff (base 30s, cap 5 min).
- **401/403 (auth/RegionError)**: treat as cooling with a longer backoff (these
  usually need manual intervention) — backoff base 1 min, cap 30 min.
- On **success**: reset consecutive-fails to 0, backoff to base, state healthy.

## Routing algorithm

For a request for model `deepseek-v4-flash`:

1. Resolve session key: `x-session-affinity` header, else `X-Session-Id` header,
   else fall back to request `user` field, else `no-session`. (dsh sends none —
   see wire-up below.)
2. Session-affinity: `hash(sessionKey) % 2` picks the primary GO account
   (go-primary or go-alt) so one session consistently uses one account, and the
   load spreads roughly evenly across sessions.
3. Build candidate list in preference order:
   - primary GO (from step 2)
   - the other GO account
   - direct
4. For each candidate in order that is `healthy` (or not `cooling`):
   - forward the request (strip incoming auth key, inject that backend's key,
     translate model name if needed).
   - If non-2xx retryable → mark the candidate's health, try next candidate.
   - On **429** specifically: try the next candidate immediately (do not burn
     time), and apply the backoff to this backend.
   - If success → return response (or stream it) to client. Stop.
5. If all candidates failed → return 503 with the last upstream error body.

## Streaming

The proxy MUST pass through SSE streaming byte-for-byte for
`stream:true` requests so OpenCode/dsh streaming works. Implementation: use
`fetch` with the upstream, pipe the response body (already SSE) straight to the
client, copying through status code, content-type, and SSE headers. Do NOT
re-encode or buffer. Non-stream (`stream:false`) requests: pass through the JSON
body.

Important: when retrying a streamed request after a mid-stream failure, we
cannot rewind a stream that already sent data. Design decision: for streaming,
only retry on a **pre-first-byte** failure (connection error, HTTP error status,
upstream 4xx/5xx before any SSE chunk). Once the first SSE `data:` line is
forwarded, stop retrying and propagate the (possibly truncated) stream. This
matches how LiteLLM behaves.

## Request/response handling

- Accept the incoming `Authorization: Bearer <key>`. The router validates the
  key against a configured local master key (optional, default: accept any) —
  but v1: do NOT hard-require a specific incoming key; OpenCode/dsh already
  send their configured keys. The router substitutes the backend key in the
  outbound request.
- `reasoning_effort`: OpenCode sends it (high/max variants); forward it as-is.
  Do NOT override (the old LiteLLM patch defaulted it to high, but OpenCode
  sends it explicitly, so pass-through is correct).
- `interleaved` reasoning_content: pass through as-is.
- `setCacheKey: true` in OpenCode uses a `cacheKey` header for prompt caching —
  forward any `x-cache-key`/`cacheKey` style headers verbatim.

## Config

Single JSON config file (`config.json` in project dir) + env vars for keys.
Fields:
- `port` (default 8787)
- `prefix` (default `/v1`) — OpenCode/dsh use baseURL `<host>:<port>/v1`
- `masterKeyEnv` (optional; if set, incoming keys must match)
- `backends`: array of {id, baseURL, apiKeyEnv, model}
- `models`: map of public model id -> {backends: [ordered ids], affinityGroup}
- `backoff` tuning: {rateLimitBaseMs, rateLimitMaxMs, serverBaseMs, serverMaxMs,
  authBaseMs, authMaxMs}

Config on-disk change → auto-reload via fs.watch (no restart). Keys always read
from env at request time (so editing .env + reload is enough).

## Boot integration

Windows scheduled task: "At logon", runs `node <project>/server.mjs` (or a
`start.cmd`) with working dir = project. Logs to `router.log` in project dir.
Add `GET /health` returning `{ok:true, backends:{id: state}}`.

## Consumer wire-up (post-build migration, with LiteLLM kept as backstop)

- **OpenCode** (`opencode.jsonc`): point the `litellm` provider's baseURL at
  `http://localhost:8787/v1` (keep the model definitions; only the baseURL
  changes). Keep a separate provider entry / the homelab baseURL for models the
  router doesn't serve (glm, vision) — or leave litellm as the backstop URL in a
  second provider. Since OpenCode's default model is `litellm/deepseek-v4-flash`,
  that model routes through the local router.
- **dsh** (`settings.yaml`): change the `litellm` provider baseURL to
  `http://localhost:8787/v1`, OR add a new `local` provider with `api:
  openai-completions` and set `agent-default-model` to it. Also set
  `compat.sendSessionAffinityHeaders: true` on that provider so the router can
  do session affinity for dsh (currently dsh sends nothing).

## Verification plan

1. Unit-ish: start server, `/health` returns backend states.
2. Live: `POST /v1/chat/completions` with `{"model":"deepseek-v4-flash","stream":false}`
   → expect a 200 JSON completion (routes through one of the GO accounts / direct).
3. Live streaming: same but `stream:true` → expect SSE chunks, terminates.
4. Session affinity: two requests with the same `x-session-affinity` header hit
   the same backend; different headers may hit different backends.
5. Failover: temporarily mark go-primary cooling (or use a poisoned key) and
   confirm the request succeeds via go-alt; mark both GO cooling and confirm it
   hits direct; mark all three cooling and confirm 503.
6. OpenCode E2E: point a throwaway Copy of the config (or a scratch opencode run)
   at the router and get a real completion. dsh: a `dsh --profile headless` call
   through the router.
7. Non-regression: confirm the homelab litellm path still works untouched as the
   backstop.
