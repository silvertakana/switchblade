# Combined recovery + config-v3 execution plan

**Goal:** Combine both branches into one tested, deployment-ready branch without changing live routing, settings, credentials, catalog order, or parked providers.

**Boundary:** Build and verify the integration branch. No push to main, deployment, live configuration writes, production health reset, or uncooling. Deployment is a subsequent finishing workflow.

**Architecture:** One canonical account-health registry, physical-route registry, exclusion registry and session-preference registry. Adapt v3 deployments to recovery; do not create parallel recovery state.

**Stack:** Node22+, ESM, built-ins only; no dependencies, build step, formatter or lint setup.

**Contracts:** Read [provider recovery](<C:/dev/local-model-router.fix-provider-recovery/docs/superpowers/plans/2026-10-07-provider-recovery.md>) and the [config-v3 spec](<C:/dev/local-model-router/docs/superpowers/specs/2026-09-19-switchblade-config-v3-spec.md>). Recovery policy wins where cooldown scopes conflict. Retain legacy support through deployment and at least24 healthy hours; removing it is outside this implementation.

## Task 1: Isolate the work and establish baselines

Inputs: recovery `e0f2774c1296a512f9fff633fffee3076cdec756` and config-v3 `e03cac48fe0678171061e6a47c5b0c0f7a58cd26`.

Never edit, stash, reset or stage the shared checkout. Its dirty files belong to other work. Its configuration mirror and the image seed are not live release input.

~~~pwsh
git -C C:/dev/local-model-router.fix-provider-recovery status --short
git -C C:/dev/local-model-router.fix-provider-recovery rev-parse HEAD
git -C C:/dev/local-model-router rev-parse HEAD
git -C C:/dev/local-model-router.fix-provider-recovery worktree add -b integration/provider-recovery-config-v3 C:/dev/local-model-router.integration-provider-recovery-config-v3 e0f2774c1296a512f9fff633fffee3076cdec756
~~~

Expect a clean recovery tree and matching hashes. If the target branch/path exists, inspect it rather than overwrite/delete it. If inputs changed, stop and report the difference.

After approval, save this plan under `docs/superpowers/plans/2026-10-08-combined-release-readiness.md` in the integration worktree. Keep logs and assertion ledger under `.superpowers/sdd/combined/`; verify it is ignored before storing captures.

In that worktree run:

~~~pwsh
node --check server.mjs
node --check test.mjs
node test.mjs
git diff --check
~~~

Expect exit0, no FAIL lines and `N passed, 0 failed`. Diagnose crashes; do not obtain green through unexplained reruns.

## Task 2: Correct recovery with separate test-first commits

Files: integration `server.mjs` and `test.mjs`. For each fix: add the named regression; record its expected failure; make the minimal implementation; run syntax checks/full suite/diff check; commit that fix and its tests. Preserve existing passing coverage.

### 2.1 Documented backoff settings

Replace `backoffFor` with:

~~~js
function backoffFor(kind, cfg, fails) {
  const bp = cfg.backoff || {};
  const token = kind === 'rate' ? 'rateLimit' : kind === 'billing' ? 'server' : kind;
  const defaults = token === 'rateLimit' ? [30000, 600000]
    : token === 'auth' ? [60000, 1800000] : [30000, 300000];
  const base = bp[token + 'BaseMs'] ?? bp[kind + 'BaseMs'] ?? defaults[0];
  const max = bp[token + 'MaxMs'] ?? bp[kind + 'MaxMs'] ?? defaults[1];
  return Math.min(base * 2 ** (Math.max(1, fails) - 1), max);
}
~~~

Test canonical rate settings, cap, canonical-over-alias precedence, legacy rate/billing aliases, configured auth, default auth. Rate base17/cap60 gives17 then60 on failure3; legacy rate base9 gives18 on failure2; billing uses server base21; default auth is60000. Configured auth already works: characterize rather than claim it was broken. Weekly behavior stays unchanged.

Commit: `fix: honor documented cooldown settings with legacy aliases`.

### 2.2 Opaque-rejection identity

Add `sessionScopeOf(authScope, sessionKey)`: SHA-256 of JSON tuple `[authScope, sessionKey || "no-session"]`. Exclusion keys: SHA-256 of `[scope, attempt.model, attempt.backend, attempt.upstream]`.

Pass that scope consistently through recording, eligibility, retry eligibility, recovery hints and preference eligibility. Keep raw sessions for affinity, forwarded headers and history. No-session requests get caller-specific exclusion buckets, never successful-provider pins.

Strengthen `zScope4`: A returns empty400; B returns explained400 so B cannot create a pin. Same caller/session/model repeats skip A; another credential, session or resolved model may try A. No-session repeats share only their caller bucket. Assert exact30s expiry, no shared cooldown, and no identity leakage through health.

Commit: `fix: scope opaque rejections by caller session and resolved model`.

### 2.3 Resolved-model preference tokens

Allocate one request sequence per incoming request and a token per distinct `attempt.model`, not requested preset/alias. Extend `beginSessionRequest` with optional sequence. Restrict preference ordering to its logical-model group; publish with the successful attempt's token.

Regression: p1, p2 and bare m resolve to m. A fails, B succeeds, then A cooldown expires. All names continue using B for that caller/session. Preserve credential isolation, no-session behavior, stale-completion rejection, no idleTTL, and true10,000-entry LRU.

Commit: `fix: share recovery pins across aliases of the resolved model`.

### 2.4 Actual HTTP5xx outage evidence

Relay cuts/stalls and unusable200 still cool routes. Only actual HTTP500-599 contributes distinct-route account-outage evidence. Never suppress route-failure recording just because status is0.

Refresh a repeated physical route's latest event. Store its attempt dispatch sequence. Successful account settlement clears the60s evidence window only when it owns the settlement and is not older than retained failures.

Regressions: two distinct HTTP5xx routes escalate; repeated same route does not; failure A then usable sibling success then failure C does not escalate; held older success cannot erase newer evidence; two relay/empty200 failures cool routes without outage evidence.

Commit: `fix: fence account outage evidence and reset it on valid success`.

### 2.5 Network versus relay failure

Add internal failure-stage marker to `forward` results and pass it to `noteAttemptFailure`. Connect/header/network failure cools account; relay/body failure cools physical route. Billing takes precedence; explained400/422 is truthful terminal response, without fallback/cooldown.

Regressions: destroy mock socket before headers and prove account cooling; destroy after delivered SSE and prove route cooling with no replay. Diagnostics report the state actually recorded.

Commit: `fix: distinguish connection failures from relay failures`.

### 2.6 Downstream cancellation

Create a request-lifetime AbortController before forwarding; observe request abortion and unfinished downstream close; pass signal to forwarding/body relay; dispose listeners in finally. Pre-header caller disconnect aborts upstream work, releases lease, and neither charges health nor publishes a pin.

In terminal relay, capture upstream/idle failure before pipeline teardown can emit downstream close. Do not latch pre-header provider failure as the final request cause: fallback may succeed. Preserve existing `client_cancelled` metrics/diagnostics.

Event-held regressions: hold headers, abort caller, confirm upstream closes/B receives nothing; deliver one SSE chunk, release upstream-reset gate, confirm route cooling/no replay. Also test clean completion and real mid-stream caller cancellation. No timing sleeps used as readiness barriers.

Commit: `fix: cancel upstream work without mischarging provider health`.

### 2.7 Probe leases and strict retry admission

Make `ineligibility(attempt, scope, now, lease)` exempt only matching account/route lease generation. Remove coarse "owns either probe" exemption and own-route-cooldown retry bypass. Foreign account probe must block an attempt owning a route probe. Release is idempotent and generation-checked.

Directly test actual `claimAttempt` account/route guards, partial-claim rollback and stale release. Event-hold concurrent probes using separate sessions so pins cannot mask admission. Mutate each actual admission guard separately in disposable committed-source copies; each mutant must fail its corresponding regression.

Commit: `fix: enforce generation-owned probe admission on every dispatch`.

## Task 3: Merge config-v3 into canonical recovery

~~~pwsh
git merge --no-commit --no-ff e03cac48fe0678171061e6a47c5b0c0f7a58cd26
~~~

Resolve the three known textual conflicts:

1. Attempt loop: retain v3 backend lookup map plus indexed recovery loop, leases and fallback accounting. Resolve with `modelDeployments(model, attempt.model, backendById)` and match both backend AND upstream.
2. Server exports: union both branches, including `validateConfig` and `convertLegacyConfig`; retain added test exports.
3. Test imports: union named imports; retain namespace import.

Review dashboard semantics even if Git reports no text conflict.

Use existing deployment adapter in candidates, pruning and health projections. Legacy requests retain backend-only dedup; v3 deduplicates physical backend/upstream pairs. After5xx/connect/relay failure, skip remaining deployments of that account within that request - not globally healthy siblings. Opaque rejection excludes its scoped physical route only.

Regressions: two upstreams on one account are distinct v3 candidates; aliases share physical recovery; deployment dialect settings remain distinct; model/preset order remains stable. Fresh v3 sessions retain backup-only tail; an established successful recovery pin may prefer a backup.

Run syntax/full-suite/diff gates, then commit: `merge: combine config v3 with canonical provider recovery`.

## Task 4: Preserve legacy runtime, save, restore and editor

Files: integration `server.mjs`, `index.html`, `test.mjs`, design/spec documentation.

- Stop automatic conversion in `bridgeToEffectiveConfig`. Boot/reload/same-shape save/snapshot restore retain supplied legacy shape. Keep pure converter exported/tested; explicit v3 configs remain supported.
- Validate a copy, not the object written to disk. Canonicalize the two older structural forms for validation only. Never discard invalid references/values before validation. Grandfather unchanged legacy empty-string params/retry already on disk; reject newly introduced invalid values. Preserve original stored values.
- Restore legacy preset-param layer: global < backend < provider < model < preset < request. V3 retains documented layering. Reserved model/dialect order remain unchanged.
- Legacy structured editor writes providers and retains model/preset affinity pools, strategies, reference weights, unknown fields and unchanged inert values. V3 writes deployments. Never convert one legacy model into a mixed-schema file or silently materialize v3 weights/backup flags.
- Replace assertions explicitly requiring automatic conversion with approved compatibility assertions. Retain pure-converter tests. Relevant families: `v37`, `v39` and `u9` editor round trips. Record every changed predicate in ledger; no coverage deletion to meet floor.

Regressions: boot/reload; no-op raw/structured save; metadata-only edit; snapshot restore; classic backends/preset-members forms; preset-wins collisions; weighted refs/affinity pools; layered/dialect payload parity; invalid save leaves bytes unchanged; existing EBUSY/in-place bound-file write fallback.

Make separate test-first commits for server and editor compatibility. Document recovery-authoritative scopes and successful-backup-pin exception.

## Task 5: Health projections, manual controls and settlement fencing

- Preserve account rows and `/health.routes`. Project v3 `backends[].deployments[]` from the same route registry, including healthy configured routes, aliases, deadlines/errors/probes. No global sessions or exclusion identities.
- Extend `/admin/backend` with optional upstream. Absent means existing account-wide behavior; present means that physical route only. Malformed400; unknown route404. Targeted uncool cannot clear account/manual block.
- Fence route deletion/uncool/reset/removal so held old work cannot recreate/clear newer state. Track active leases; retain deletion fences only while older leases can settle. Make release idempotent. Fence account removal/re-addition. Unchanged accounts/routes preserve health on reload.
- Preserve explicit manual-action preference clearing. No automatic health reset/uncooling.
- Recovery hints: maximum simultaneous blocker deadline per candidate, then minimum across candidates. No invented finite recovery for indefinitely parked providers or probes with unknown completion time.

Regressions: route/account cool and uncool; sibling isolation; aliases; sticky/timed manual controls; reload persistence; held failure after uncool/remove; held success after manual cool/reset; combined blocker deadlines; no all-cooling dispatch.

Commit health projection, manual controls and settlement fencing separately with tests.

## Task 6: Committed-byte release gates

Assertion ledger covers both original suites: source assertion, predicate/message, loop context/runtime cases, destination and approved replacement. Static-label union is insufficient.

~~~pwsh
node --check server.mjs
node --check test.mjs
node scripts/ci/check.mjs static
node scripts/ci/check.mjs suite
git diff --check
git status --short
~~~

Never lower incoming594 suite/300 static-ID floors. Raise suite floor only after measuring clean final suite and proving coverage preservation.

Commit, then rerun gates from a fresh detached worktree at that exact commit. Run Node22/24/25 CI, secret scans and container checks. Record hash, commands, exits and count. Perform one independent review of committed diff against contracts; fix findings and repeat affected gates. Browser-check edited dashboard against disposable fixtures, not unchanged production UI.

## Task 7: Read-only live compatibility and rollback readiness

Use Switchblade/homelab runbooks to re-resolve current production container at192.168.68.69:8787. Read live host-mounted config, catalog, health and consumer settings. No live writes. Never print credentials or commit raw captures.

Replay sanitized live config through disposable baseline/integrated routers with mock URLs/credentials. Compare served IDs/order, name resolution, selected upstreams, outgoing params/dialects, legacy save/restore and manual exclusions. Cover real live shapes. Unexplained differences fail readiness; do not alter live settings to make gates pass.

Record current image/commit and rollback source; rehearse rollback on mocks. Code restart loses ephemeral health: subsequent deployment uses a quiet window and restores captured manual exclusions before traffic/smoke. Preserve single-file mount and inode.

**Acceptance:** Clean isolated branch; original assertions retained or explicitly strengthened/replaced; committed-byte CI green; independent code review complete; live compatibility comparison and rollback rehearsal pass. Report exact commit and deployment action remaining. No push/deploy in this phase; no claim that changes are live. Deployment plus24 healthy hours precedes any separately authorized legacy removal.
