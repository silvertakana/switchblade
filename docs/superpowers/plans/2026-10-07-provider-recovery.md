# Provider cooldown and sticky session failover - implementation plan

## Outcome in plain language
When a provider fails, the router moves the session to a working provider and keeps it there, accepting possible cache loss. Recovering the original provider does not move that session back automatically. Cooldowns are respected rather than bypassed when every provider is unavailable.

A blank `400` is not proof that a provider is broken. Those failures temporarily exclude that provider for the affected session only; clear provider failures cool the provider/model route across sessions. Account-wide faults still cool the whole account.

**Goal:** Reliable failover, correctly scoped cooldowns, sticky successful replacements, and understandable failures.

**Architecture:** Extend the existing health, candidate-ordering, forwarding, relay, and history paths. Router logic stays in `server.mjs`, regression coverage in `test.mjs`, dashboard changes in `index.html`. No new dependencies or separate service.

**Stack:** Node.js 22+ built-ins, ESM, PowerShell, existing mock-server tests. No build or lint exists; syntax checks and the mock suite are the quality gates.

**Spec:** The policy, interfaces, and task steps below are the complete design. This file is written only after plan approval.

## Global constraints and release baseline
- Do not edit the dirty main checkout (`C:/dev/local-model-router`, branch `feat/config-v3`) or deploy its unfinished changes.
- Production and remote `main` were verified at `10ba63573db22aa72d3dfe936aa3eb39e4923233`. Its committed and deployed router source share SHA-256 `63e3f2f4ca42386b600d6967b4dd854b8b841d9545087fd60725a74dcfe91a6f`. Recheck these mutable release facts before implementation and before release.
- Preserve configured models, preset model ordering, session hashing for initial selection, backend deduplication, dialect mappings, credential sources, and manual exclusions.
- No automatic cross-model downgrade, account repairs, credit purchases, key changes, or DSH restart.
- Routing recovery state is in memory, like existing health. Preserve it across config hot reload; process restart clears it. Persist diagnostic summaries, not live routing state.
- Cache preservation is subordinate to completing the request. Never replay a partly delivered answer on another provider.
- Isolated worktree for this plan: `C:/dev/local-model-router.fix-provider-recovery`, branch `fix/provider-recovery`, cut from the production revision above.

## Approved policy to implement

| Failure | Scope and action |
|---|---|
| Blank, message-less, or unreadable upstream `400/422` | `upstream_rejection`: exclude that physical route for this authenticated client/session/model for 30 seconds; immediately try another eligible provider; never retry the same provider in this request. No shared/account cooldown, no fleet escalation. |
| Explained bad-request `400/422` | Stop without cooldown or fallback; return the actual `400/422`, not `503`. Billing classification takes precedence. |
| Upstream `5xx`, upstream relay failure, or a completed `200` with no usable output | Cool that physical provider/model route using existing server backoff: default 30 seconds, exponential to five minutes. |
| Auth/subscription, billing, account rate/weekly limit, network/connect failure | Preserve existing account-wide classification and backoff. |
| Client cancellation | Stop forwarding/reading as appropriate, release recovery leases, do not penalize the provider. |

**Physical route identity:** `[backendId, upstreamModel]`. Logical aliases of one physical route share route health. A failure on a different upstream model on the same account does not cool this route.

**Account-outage escalation:** Escalate to account-wide server cooldown when at least two distinct upstream routes of one backend return `5xx` within 60 seconds and no valid current-generation backend success occurred in that window. Exclude opaque rejections, genuine client errors, and client cancellations. A successful route clears its own route-failure evidence; successful account traffic breaks the outage window. Old in-flight success cannot undo a newer account cooldown. This is a conservative heuristic, not proof of the provider's internal cause.

**Strict eligibility:** Remove the existing path that retries automatically cooled accounts when every account is cooling. A route is eligible only if account health, shared route health, and the applicable session exclusion all allow it. Recheck immediately before each dispatch. If none is eligible, return a structured `503`; include `Retry-After` for the earliest finite recovery time. An in-progress recovery probe may use a one-second hint. Indefinite manual exclusions carry no finite hint; timed manual exclusions keep their expiry.

**Retry boundary:** If another eligible provider exists, fail over immediately instead of consuming same-provider transient retries. If none exists, keep the configured bounded transient retry policy, but only before the final failure is marked cooling. Before each retry, recheck client cancellation, manual exclusion, and recovery generations. Opaque rejections never retry. No retry is exempt from an already active cooldown.

**Session stickiness:** Store the successful provider per authenticated-client scope, session, and resolved logical model. No idle expiry for a healthy pin; keep it until failure, route removal, explicit recovery reset, process restart, or bounded-map eviction. A primary becoming healthy does not rewrite another session's successful backup preference. Keep the map at 10,000 entries with LRU eviction and expose an aggregate eviction counter. This bound is the explicit rare exception to stickiness.

**Requests without session IDs:** Do not pin them together. Their opaque exclusions use a client-scoped `no-session` bucket plus logical model and physical route, lasting 30 seconds. Shared provider/account cooldowns still apply.

## Review focus
1. A malformed request must not cool the provider fleet.
2. A late old response must not undo a newer cooldown or successful session switch.
3. An expired cooldown must admit one recovery probe, not a burst.
4. Client disconnects and partial streams must not cause duplicate answers or false provider failures.
5. Config reload, old history records, and release isolation must stay safe.

---

## Task 0: Isolate the release and establish the baseline

**Files:** Create this plan in the worktree; leave the main checkout untouched.

**Interfaces:** Consumes the verified production revision and hash; produces an isolated `fix/provider-recovery` worktree and baseline test evidence.

- [x] Resolve the live production container and compare its deployed source bytes with the release base. If production or `main` moved, review the new base before continuing; do not ship unrelated commits.
- [x] Create the worktree (done: `C:/dev/local-model-router.fix-provider-recovery`, branch `fix/provider-recovery`, from the verified revision). Sibling-directory worktrees match this host's convention for other repos.
- [x] Save this plan in the worktree and record the ledger.
- [x] Run the baseline checks and record the actual suite count and any pre-existing failures.

```pwsh
node --check server.mjs
node --check test.mjs
node test.mjs
```

**Expected:** Syntax checks exit zero; the suite exits zero and prints its actual final `N passed, 0 failed` count. No production mutation in this task.

## Task 1: Separate opaque rejection from genuine client error

**Files:** `server.mjs` classification, retry, and terminal chat paths; `test.mjs` existing `z20`/billing tests plus new rejection cases.

**Interfaces:**
- `classify(status, bodyText, errorBodyState = "present")` adds `upstream_rejection`.
- `isRetryableError(status, bodyText, errorBodyState)` rejects opaque and client errors.
- `upstreamExplanation(bodyText)` returns a diagnostic explanation or an empty string.
- Every classification caller passes body-state metadata once Task 4 supplies it.

Complete explanation helper:

```js
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
    if (text.startsWith("{") || text.startsWith("[") || text.startsWith("<")) return "";
    return text;
  }
}
```

Billing detection stays first. For `400/422`, no explanation means opaque. If the body read was incomplete, only a complete parseable structured explanation can justify treating it as a genuine client error; partial plain text cannot. Preserve existing auth/rate/server/network classifications. Opacity is decided from the body text itself (blank, whitespace, message-less JSON, terse model-only JSON), so this task does not depend on Task 4; `errorBodyState` is used only to withhold the genuine-client verdict on an incomplete read.

- [x] Add a failing integration test: empty `400` from A, usable `200` from B, exactly one hit each. Run it against unfixed source and capture the failure naming the actual `503`.
- [x] Parameterize whitespace, `{}`, terse model-only JSON, empty messages, JSON string errors, `error.message`, top-level `message`, explanatory plain text, and billing errors.
- [x] Introduce the opaque classification and continuation behaviour without account-wide cooling; Task 2 supplies the session-only exclusion.
- [x] Return the actual `400/422` for terminal genuine client errors. Update `z20` from expected `503` to `422`, keeping its one-hit assertion. Keep the final status consistent in history, stats, analytics, detail, and the response. Genuine client errors must not raise an exhaustion alert.
- [x] Run syntax checks and the full suite; commit `fix: distinguish unexplained upstream rejections from client errors`.

## Task 2: Scoped cooldowns, strict eligibility, and recovery probes

**Files:** `server.mjs` health registry, candidate eligibility, admin reset/uncool, config reload, `/health`, cooling events; `test.mjs` health tests including the existing all-cooling `u`/`w` cases.

**Interfaces:**
- `routeKey(attempt)` identifies backend/upstream, never a client-supplied route.
- Shared route state: `{generation, fails, nextAvailableAt, probeInFlight, lastErrorKind, lastStatus}`. Account rows keep existing fields and gain generation/probe metadata.
- Session exclusion state is indexed by the client/session/model key and physical route, with `blockedUntil` and `generation`.
- `claimAttempt(attempt, requestContext, now)` synchronously validates eligibility and atomically claims required account/route/session recovery leases. Returns generation snapshots and lease ownership, or a skip reason.
- `settleAttempt(token, outcome, cfg, now)` performs generation-checked failure/recovery updates; `releaseAttempt(token)` releases only leases that token owns.
- Inject `now` into state helpers for deterministic tests; production uses `Date.now()`.

Reuse existing backoff computation:

```js
function routeKey(attempt) {
  return JSON.stringify([attempt.backend, attempt.upstream]);
}

function routeBackoffMs(fails, cfg) {
  return backoffFor("server", cfg, Math.max(1, fails));
}
```

- [x] Add red fake-clock tests for route isolation, alias sharing, 30/60/120-second progression to the five-minute cap, and failure-streak preservation after timer expiry. A real valid recovery resets the streak; expiry alone does not.
- [x] Implement the three eligibility scopes. Opaque failures exclude only the affected session/client route for 30 seconds; server failures update shared route health; account faults update existing account health.
- [x] Remove all-cooling forced attempts. Update the `u`/`w` regressions to assert no cooled upstream receives a request, the backend header is `none` when no attempt ran, and the `503` carries correct retry information. Manual exclusions stay sticky, timed exclusions stay timed.
- [x] Admit one probe at a time per recovering account/route. Acquire leases before awaiting network work; release on every exit. Use one monotonically increasing generation allocator that is not reset when maps are cleared or a route is removed and re-added. Stale callbacks must never match a newly created state.
- [x] Implement the 60-second cross-route account-outage heuristic. Test escalation and the counterexample where another route succeeds (preventing escalation). Opaque failures never count toward it.
- [x] Preserve unchanged states across config hot reload; prune removed physical routes and preferences/exclusions referencing them. Account `uncool` clears that account's route recovery states; explicit `reset-health` clears account/route recovery, preferences, and session exclusions. Keep admin authentication. Never invoke either during normal failover.
- [x] Extend `/health` with route-scoped rows listing logical model references, retry time, failure count, and probe state. Tag cooling events with scope; keep account-only analytics from counting route events as account outages. Do not expose session maps or authentication fingerprints.
- [x] Run syntax checks and the full suite; commit `feat: respect scoped provider cooldowns and recovery probes`.

## Task 3: Keep sessions on successful replacements

**Files:** `server.mjs` candidate ordering, retry loop, forwarding/relay completion, session state; `test.mjs` multi-turn and concurrency fixtures.

**Interfaces:**
- `sessionPreferenceKey(authScope, sessionKey, logicalModel)` returns a fixed-size hash; no pin for `no-session`. Compute authentication scope internally without retaining or printing raw credentials.
- Preference row: `{routeKey, latestRequestSequence, lastUsedAt}`. Sequence numbers use a process-wide monotonic allocator.
- `beginSessionRequest(...)` captures a sequence token; `rememberProvider(token, attempt, completion)` publishes only the current request's valid, eligible, generation-current provider.
- `orderSessionProviders(ordered, context, now)` promotes a healthy pin only inside its existing logical-model group. It never reorders models or undoes backend deduplication.
- Relay returns `hasContent`, `hasReasoning`, `hasToolCalls`, `relayError`, and `clientCancelled` alongside existing usage metrics. Usability comes from the parsed response, not token estimates or a hand-fed flag.

```js
function sessionPreferenceKey(authScope, sessionKey, logicalModel) {
  if (!sessionKey || sessionKey === "no-session") return null;
  return crypto.createHash("sha256")
    .update(JSON.stringify([authScope, sessionKey, logicalModel]))
    .digest("hex");
}
```

- [x] Write the core red multi-turn fixture: A returns an empty `400`; B produces a valid response; later turns of the same session hit only B. After A's exclusion expires and another session proves A works, the original session still hits B. Then B fails, C succeeds, and the session stays on C. Assert provider hit counts, not just answer text.
- [x] Implement the bounded 10,000-entry LRU preference map with no idle TTL for healthy pins. Lazily remove expired negative exclusions; empty non-pinned rows may be pruned. Different authentication scopes with identical session IDs must not share state.
- [x] Apply failover-first retry selection. Same-provider retries only for configured transient errors when no alternative is eligible, before final cooling, with eligibility/cancellation/generation rechecks before each retry. Keep the existing single-provider retry tests.
- [x] Move healthy marking from HTTP `200` headers to clean usable completion. Record upstream stalls, cuts, and empty output as route failures. For already-delivered streams, never replay or change the sent status; keep `200` plus failure telemetry, exclude the route for later requests, and do not pin it as a success. Content-only, reasoning-only, and tool-call-only responses all qualify as usable.
- [x] Propagate client-disconnect cancellation distinctly through forwarding and relay; never classify a caller-aborted pipeline as an upstream outage. Release owned probe leases without penalizing the provider.
- [ ] Add held-response concurrency tests: an older slow A success cannot erase a newer A cooldown or overwrite a newer B preference. Cover account and route generations, probe cancellation, `no-session` client-scoped exclusion, model ordering, config removal/re-add, long idle, and LRU eviction.
  - DEVIATION (recorded 2026-10-08): the two-overlapping-live-requests half of this box is NOT covered. The older-slow-A-vs-newer-B-preference and newer-cooldown cases are pinned at unit level (zSess4f/zSess4h) instead of with concurrent live requests. Residual risk: a live held-response race is untested.
- [x] Run syntax checks and the full suite; commit `feat: keep sessions on working fallback providers`.

## Task 4: Useful errors and durable safe diagnostics

**Files:** `server.mjs` `forward`, error responses, history/detail; `test.mjs` body-read and telemetry regressions.

**Interfaces:**
- `readUpstreamError(response, {maxBytes = 16384, timeoutMs = 5000})` returns `{body, errorBodyState, errorBodyBytes}`. States: `present`, `empty`, `truncated`, `timeout`, `read_error`. The byte count is captured bytes, not advertised size.
- `terminalChatError(...)` returns an OpenAI-style JSON envelope with message/type/code/callId/backend/upstreamStatus and an optional recovery timestamp. Chat errors only; config/admin envelopes unchanged.
- History adds `attemptsSummary`, `attemptsOmitted`, `fallbackCount`, `selectionReason`; old records default to empty/zero.

- [x] Add red tests for never-ended error bodies, exact-limit and oversized multichunk bodies, UTF-8 boundaries, and socket-reset read failures.
- [x] Replace unbounded error `.text()` reads with a reader capped at 16 KiB and one five-second total deadline. Race pending reads against the deadline, keep the bounded prefix, and distinguish timeout from empty body. Clear the timer and cancel/release readers on every exit without waiting indefinitely for cancellation. Successful-response and stream timeouts otherwise stay as they are.
- [x] Return nonempty safe JSON for every terminal chat failure. Genuine client errors keep `400/422`; opaque/provider exhaustion returns `503`; no-eligible responses explain cooldown/probe/manual state accurately. Keep `x-router-call` and `x-router-backend`. Never turn a blank upstream rejection into "all backends cooling" unless eligibility actually explains it.
- [x] Redact configured backend/master/UI credential values from explanatory messages and correlation IDs. Cap messages at 512 characters and allowed IDs at 160; capture only `x-request-id`, `request-id`, and `cf-ray`. Do not add arbitrary headers or raw HTML to new diagnostic envelopes.
- [x] Persist body-free final provider outcomes on success and failure: backend/model/upstream/status/classification/decision/latency/body-read state/captured bytes/retry count/cooldown scope/recovery time/allowed request IDs. Keep up to 32 outcomes; when larger, keep the first 31 and the final outcome and record the omitted count.
- [x] Count cross-provider transitions separately from same-provider retries. Distinguish configured selection, session preference, cooldown-driven selection, and request failover. Provider-switch telemetry says the cache **may** be cold, never asserts an unmeasured miss. In-memory detail keeps its existing bounded payload policy.
- [x] Test persistence after detail-ring eviction and in the temporary history file. Verify key/body/prompt/header sentinels are absent from new durable summaries, that terminal status agrees across surfaces, and that partial-stream failures are not misrepresented as clean success.
- [x] Run syntax checks and the full suite; commit `feat: retain safe provider recovery diagnostics`.

## Task 5: Dashboard and behaviour documentation

**Files:** `index.html` health/history/detail rendering; `test.mjs` existing VM UI tests; focused sections of `README.md`, `DESIGN-3LAYER.md`, `DESIGN-PRESETS.md`.

**Interfaces:** Consumes optional new health/history fields; displays scope, actual attempts, recovery time, body-read state, and the session/provider-switch reason. No new framework or dashboard redesign.

- [ ] Add red rendering tests driven by records produced by real mock-router API calls. Cover old records, absent explanation, successful fallback, an account healthy with one model route cooling, and escaped malicious explanatory text.
- [ ] Show account cooldown and provider/model cooldown separately. Show session-only exclusions in the affected call's detail, not as a public list of sessions. Say plainly when no explanation was supplied.
- [ ] Render trace data through the existing `esc()` handling. Use compact persisted summaries when detailed attempts were evicted. Add a fallback count separate from retries and show aggregate preference evictions.
- [x] Document strict cooldown enforcement, scoped opaque rejection, sticky preferences and their restart/LRU exceptions, failover-first retries, and truthful `400/422` responses. Update only affected contract text; do not import the unfinished configuration migration.
- [ ] Run syntax checks and the full suite; commit `feat: explain cooldown and session failover decisions`.

## Task 6: Final verification and narrow production release

**Interfaces:** Consumes reviewed commits and CI evidence; produces a verified live revision and rollback record. No live configuration or provider-catalog migration.

- [ ] Run every gate: `node --check server.mjs`, `node --check test.mjs`, `node test.mjs`, `git diff --check`.
- [ ] Obtain independent code review. Demonstrate the fallback and sticky-session regressions fail on unfixed committed bytes and pass with the fix. Confirm fixture cleanup, no real keys or network in the suite, and actual zero-failed output.
- [ ] Review staged files for credentials and unrelated artifacts. Push only `fix/provider-recovery` and open its PR against the production release branch. Never push the dirty feature branch. STOP AND ASK before any merge, push, or production release.
- [ ] Before merge, recheck live revision and remote release history. If unrelated changes would be released, stop and coordinate. Snapshot live config, model catalog/order, credential-source presence, and manual exclusions without printing secret values. Record the previous image/revision for rollback.
- [ ] Release only through the normal router auto-deploy at a quiet point with no in-flight router requests. This briefly interrupts routing and clears in-memory health/preferences; it does not restart DSH. Restore recorded manual exclusions before the smoke request. Do not reset health wholesale or uncool exhausted accounts. Disclose the short startup reset window.
- [ ] Verify the deployed source hash matches the commit, health responds, model IDs/order and live config are unchanged, and a minimal real streamed request produces usable output with expected usage/history. Verify dashboard recovery fields. Never deliberately break live providers to manufacture a fallback demonstration.
- [ ] If an affected session hits another real rejection, confirm the actual fallback and later stickiness. If the provider no longer rejects, report fallback as mock-proven with no live recurrence.
- [ ] Roll back to the recorded prior release if live source/config/health/smoke gates fail, then restore manual exclusions again.

## Acceptance checklist
- [ ] A failed provider does not trap a session in repeated same-provider attempts when a healthy alternative exists.
- [ ] After A fails and B succeeds, the same session keeps B even when A recovers or the session idles.
- [ ] Cooldown timers cannot be bypassed; recovery probes do not stampede.
- [ ] Malformed/opaque requests cannot cool unrelated clients or the provider fleet.
- [ ] Genuine account-wide failures and conservative cross-model outage detection cool the correct account.
- [ ] Late completions, client cancellation, and partial streams corrupt no recovery state and duplicate no answers.
- [ ] Errors are nonempty, statuses truthful, and successful fallback stays inspectable after detail eviction.
- [ ] Full mock suite and CI pass, and the narrow fix is verified live with no unrelated changes and no DSH restart.
