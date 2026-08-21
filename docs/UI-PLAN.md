# Switchblade — Web UI Plan (v2)

Comprehensive feature plan for the router web UI (Dashboard + Playground).
Covers every use case we could devise, user stories, and traced user flows.
This is the working contract for implementation.

---

## 1. Actors

| Actor | Who | Relationship to the UI |
|---|---|---|
| **Operator** (you) | Human, runs this machine | Uses the UI to watch, understand, control, and test the router. Primary user. |
| **Consumer agents** (OpenCode, dsh) | Automated clients of the API | Generate the traffic the UI shows. Never touch the UI themselves, but their requests are its main subject matter. |
| **Occasional human** (rare) | Someone at this machine, or via a future share | No auth by design (localhost only). UI must be safe to glance at: no destructive surprises, secrets never visible. |

---

## 2. Use-case catalog (exhaustive)

### 2.1 Watch & understand

| # | Use case | What the UI must do |
|---|---|---|
| W1 | Glance health | One screen answers "is everything OK?" without reading anything: status dot + per-backend badges. |
| W2 | Read per-backend state | Show state (healthy/cooling), fails, last error, next available per backend. |
| W3 | Understand WHY cooling | Surface the failure kind explicitly: weekly limit / rate limit / auth / server / manual. Never just "cooling". |
| W4 | Countdown to recovery | Live per-second countdown to nextAvailableAt, no layout shift. |
| W5 | Traffic distribution | Per-backend request counts, share %, avg latency, errors. Answers "is load balancing actually working?". |
| W6 | Live request history | Rolling feed: time, model, backend, stream, status, latency, session. Newest first. |
| W7 | Model inventory | Which models exist, which backends each routes to, affinity pool size. |
| W8 | Session affinity mapping | Understand that sessions stick to a backend. The Playground session-ID control demos this live. |
| W9 | Uptime & port | Header shows port, uptime, since-when stats started. |
| W10 | Router unreachable | Clear banner + auto-retry; dashboard must not look "empty" as if nothing exists. |

### 2.2 Act & operate

| # | Use case | What the UI must do |
|---|---|---|
| O1 | Reset all health | One-click reset of every cooling state (admin endpoint). Confirm is not needed (recoverable) but button must show it did something. |
| O2 | Cool a backend | Force traffic off a specific backend (simulate failure, drain it). Must show the manual state clearly so it is not confused with a real failure. |
| O3 | Uncool a backend | Bring a backend back before its backoff expires. |
| O4 | Distinguish manual vs auto state | Manual cool shows "manual" as the kind; auto cool shows weekly/rate/auth/server. |

### 2.3 Test & experiment (Playground)

| # | Use case | What the UI must do |
|---|---|---|
| T1 | Normal chat | Multi-turn conversation with streaming output, visible as messages. |
| T2 | Non-stream chat | Toggle to get the full JSON response instead of SSE. |
| T3 | Model switching | Choose any configured model (flash / flash-alt / flash-direct). |
| T4 | System prompt | Optional system prompt prepended to the conversation. |
| T5 | Temperature / max tokens | Advanced controls, clamped to sane ranges. |
| T6 | Session affinity demo | Type a session ID; see requests stick to one backend. This is the router's core feature, made visible. |
| T7 | Which backend served | Debug panel shows X-Router-Backend for the last request. |
| T8 | Latency per request | Debug panel shows time to first byte / total. |
| T9 | Token usage | Debug panel shows prompt/completion tokens. |
| T10 | Raw response | Expandable full raw JSON for deep debugging. |
| T11 | Reasoning/thinking | Show the reasoning_content stream separately, toggleable. |
| T12 | Multi-turn context | Conversation history is maintained and sent with each request. |
| T13 | Clear / start fresh | Clear the thread (and optionally the session). |
| T14 | Export conversation | Copy the conversation as readable text (for sharing/reference). |

### 2.4 Configure & maintain

| # | Use case | What the UI must do |
|---|---|---|
| C1 | View loaded config | Read-only config viewer with the live values (never key values). |
| C2 | See key status | Show which env var each backend uses, masked (sk-h...PrPg), never full keys. |
| C3 | Know where to edit | Footer/note: config.json auto-reloads; keys in .env; restart not needed. |
| C4 | See config change apply | After editing config.json, the models table updates without a page reload. |
| C5 | Invalid config during edit | Router keeps the last good config; UI must keep serving and not break. |
| C6 | History file location | Note where router-history.jsonl lives for post-restart inspection. |

### 2.5 Edge cases (occasional, rare — breadth)

| # | Edge case | What the UI must do |
|---|---|---|
| E1 | Router down | Unreachable banner, auto-retry, no broken-looking empties. |
| E2 | Both GO weekly-limited | Show both cooling with weekly reason + "all traffic now on direct" awareness. User must understand this is expected, not a bug. |
| E3 | All backends cooling | 503s in history, all cards cooling, banner-level alert that the router is effectively down. |
| E4 | Backend flapping | Change detection prevents card rebuild thrash; badge transitions are calm (no flash). |
| E5 | Missing env key for a backend | That backend's requests fail (401/undefined key) and it cools; UI should surface "key missing" when the error is identifiable. |
| E6 | Client errors (400/422, e.g. developer role) | Router does not cool on these; history shows the 4xx; Playground shows the error body. Operator learns the payload was wrong, not the backend. |
| E7 | Empty history / no traffic | Meaningful empty states with a hint ("send a message in Playground"). |
| E8 | Ring buffer full (500) | Oldest drops from the UI list; JSONL keeps everything. No error, no confusion. |
| E9 | JSONL unwritable | Router must not crash; UI unaffected. |
| E10 | Huge prompt / huge response | UI truncates display sensibly; pre scrolls; input has max height; no freeze. |
| E11 | Mid-stream network failure | Playground shows partial output + error note; router already stops retrying after first byte (by design). |
| E12 | Sleep/wake, clock changes | Countdowns recompute from nextAvailableAt on next tick/poll; no negative/weird values (clamp to 0). |
| E13 | Multiple browser tabs | Independent polls; actions in one tab appear in the other within 2s. |
| E14 | Key rotation / auth failure (401/403) | Backend cools with auth kind; operator sees it and updates .env. |
| E15 | Direct key out of balance | Same as E14 family; failure surfaced with the upstream message. |
| E16 | Router restart wipes stats | Stats/history reset (by design, in-memory); UI shows fresh zeros; JSONL note points to file. |
| E17 | Model removed from config | Model select and models table refresh (config change detection); Playground clears a now-invalid selection. |
| E18 | Backend id renamed in config | Health entry orphaned (minor); UI keys off config, so the new id appears fresh. Acceptable; documented. |
| E19 | Manual cool + real failure interplay | Manual cool sets lastError kind "manual"; a later real failure overwrites it. UI reflects the latest truth. |
| E20 | Session hash collision | Two sessions on the same backend is expected LB behavior; UI must not imply it's an error. |
| E21 | Stream interrupted after first byte | Per spec, truncated stream propagates; Playground shows partial + a note if the stream ended unexpectedly. |
| E22 | First run / no data yet | Skeletons on first load, then clean empty states; page should feel alive, not broken. |
| E23 | Backend healthy but slow | Latency column + avg latency surface this; Playground latency per request. |
| E24 | Very long-running request (deep reasoning) | Streaming keeps showing progress; latency counter keeps rising; UI stays responsive. |

---

## 3. User stories (key journeys, written as stories)

### Monitoring
- As the operator, I want to glance at one screen and know whether every backend is healthy, so I don't have to read logs to check the router.
- As the operator, when a backend is cooling, I want to see the exact reason (weekly limit, rate limit, auth, server, manual), so I know whether to fix a key, wait, or act.
- As the operator, I want to see a live countdown to when a cooled backend recovers, so I know when traffic will shift back.
- As the operator, I want to see how traffic splits across backends, so I can confirm session-affinity load balancing is working and notice when everything falls over to direct.
- As the operator, I want a request history with backend + latency + status, so I can trace what happened after a user reports slowness or failure.
- As the operator, when the router is unreachable, I want a clear banner with automatic retry, so I don't mistake "down" for "empty".

### Control
- As the operator, I want to manually cool a backend (and uncool it), so I can test failover or drain a problematic account.
- As the operator, I want manual cooling to be visually distinct from real failures, so I don't mistake my own test action for an incident.

### Testing
- As the operator, I want to chat with any model through the router, so I can confirm routing works end-to-end without opening a terminal.
- As the operator, I want to see which backend served my request, so I can verify session affinity and failover behavior live.
- As the operator, I want to set a session ID and watch requests stick to one backend, so the core routing feature is demonstrable.
- As the operator, I want to see reasoning and raw JSON, so I can debug model behavior deeply.
- As the operator, I want multi-turn conversation, so I can test context handling realistically.
- As the operator, I want to export a conversation, so I can share or keep a record without screenshots.

### Maintain
- As the operator, I want to view the live config and know where to edit it, so config changes don't require reading source.
- As the operator, when I edit config.json, I want the UI to reflect it without a reload, so I can verify the change took effect.

---

## 4. User flows (traced through the UI)

### Flow 1 — First open (orientation)
1. User opens http://127.0.0.1:8787/
2. Page loads: header with title + status dot (green if all healthy), auto-refresh on, Reset health button.
3. Dashboard tab is active. Backend cards render (staggered entrance, no flicker).
4. Each card: id, badge (healthy green / cooling amber), next available, fails, last error (expandable), Cool/Uncool buttons.
5. Traffic stats below: total requests, uptime, per-backend bars.
6. Request history: newest-first table; empty state if none.
7. Models table; Config viewer collapsed at the bottom.
8. User understands the whole state at a glance; scrolls or switches to Playground.

### Flow 2 — Diagnose a slowdown
1. User notices requests are slow (or a consumer complains).
2. Opens Dashboard; sees a backend card cooling with amber badge + countdown.
3. Expands the error line: sees `GoUsageLimitError` / "Weekly usage limit reached. Resets in 2 days."
4. Sees the "Cooled due to weekly usage limit" note and the countdown (~2d).
5. Checks Traffic stats: all requests now on direct; GO accounts idle.
6. Checks Request history: recent rows show direct serving, higher latency.
7. Conclusion: expected state, no action needed until the limit resets. (Or: uncool/check balance if it was auth.)

### Flow 3 — Force traffic off a backend (test failover)
1. User clicks Cool on go-primary.
2. Card flips to cooling (amber) with a manual note; next poll shows traffic stats shifting off it.
3. User opens Playground, sends a message: debug panel shows `go-alt` (or direct) served.
4. User clicks Uncool on go-primary; card returns to healthy; next requests may use it again.
5. User watches history to confirm the shift.

### Flow 4 — Demo session affinity
1. User opens Playground.
2. Types a session ID, e.g. "demo-1", in the Session ID field.
3. Sends message A: debug shows backend X.
4. Sends message B (same session): debug shows backend X again (sticky).
5. Clears session ID, sends message C: backend may differ (hash of "no-session" or its own).
6. User understands: same session = same account; different sessions spread load.

### Flow 5 — Test streaming + reasoning
1. User opens Playground, model = deepseek-v4-flash, stream checked, show reasoning checked.
2. Types a prompt requiring thinking ("explain how session affinity hashing works").
3. Sends: assistant message streams in; reasoning block appears above it (toggleable).
4. Debug panel updates live: backend, latency, tokens.
5. User expands Raw response to inspect the full SSE-parsed JSON.
6. Clear or export when done.

### Flow 6 — Recover from the "everything on direct" state
1. Both GO accounts weekly-limited: both cards amber, weekly notes, countdowns.
2. Status dot amber + pulse (attention).
3. Traffic stats show 100% direct.
4. User recognizes expected state; can set a reminder mentally from the countdowns.
5. After a limit resets, that account returns healthy automatically on next success; traffic rebalances.

### Flow 7 — Handle an auth failure (key rotation)
1. A backend cools with auth kind; card shows the 401/403 body on expand.
2. User updates .env with the new key (edit + router picks it up at next request; no restart).
3. User clicks Uncool (or waits for the auth backoff to expire).
4. Next request succeeds; card returns to healthy; history shows the recovery.

---

## 5. Design principles derived from the use cases

1. **Status is never color-only.** Every state carries icon + text (badge label, dot + title, table status text). E9/E3 demand this.
2. **Change detection everywhere.** Sections redraw only when their data changed. Kills the flicker (W6/W4/E4), keeps animations meaningful.
3. **Failure reasons are explicit.** The UI must never say just "cooling" — always the kind (weekly/rate/auth/server/manual) + expandable body (W3/O4/E14/E15/E19).
4. **Manual actions are visually distinct** from automatic states (O4/E19).
5. **Empty states teach.** "No requests yet - send one in Playground" (E7/E22).
6. **The Playground is the router's demo.** Session affinity, failover, backend visibility are its star features, not incidental (T6/T7).
7. **Graceful degradation everywhere.** Router down (E1), all cooling (E3), invalid config (C5/E10), mid-stream failure (E11) — never a broken-looking page.
8. **No secrets, ever.** Keys only as masked env-var references (C2, actor table).
9. **Accessible + reduced motion.** Keyboard, focus rings, prefers-reduced-motion (established).
10. **Localhost-only, no auth, safe to glance at.** Destructive actions are recoverable (reset, cool/uncool) and never accidental (E-guest).

---

## 6. Implementation gaps vs current state

Current state (already built): Dashboard (backends, stats, history, models, config), Playground (chat, stream, reasoning toggle, session ID, temp/max tokens, system prompt, raw JSON, debug panel), monochrome theme, change detection, X-Router-Backend header.

Gaps to close per this plan:
1. **Failure kind on the card body** — show kind explicitly in the card (not only inside the JSON). Low effort, high value (W3).
2. **Manual cool distinct** — label the manual-cool state ("Manually cooled") so it is not confused with real failures (O4/E19).
3. **Banner when ALL backends cooling** — prominent alert + status dot red (E3).
4. **Status dot red when unreachable** (currently separate banner only) (W10/E1).
5. **Export conversation** — copy-to-clipboard button in Playground (T14).
6. **Mid-stream failure note** — if stream ends without [DONE], append an error hint (E11/E21).
7. **Clamp temperature/max tokens** in the UI before send (T5/E20).
8. **Missing-key detection** — when a backend fails with a clear missing-key signature, surface it in the card note (E5).
9. **Models select refresh on config change** — the config reload path already re-populates; ensure a removed model is deselected (E17).
10. **Stats "everything on direct" awareness** — optional small note in stats when GO share is 0% (E2).

## 7. Open decisions for the operator

1. Export format: plain text transcript vs JSONL vs both? (Recommend plain text + JSONL.)
2. Should the Dashboard get a small time-series (per-minute request sparkline) or stay cumulative-only? (Recommend stay cumulative for v1; sparkline is nice-to-have.)
3. Should backend state flips surface as toasts ("go-primary cooled: weekly limit")? (Recommend a subtle log line in the history table rather than toasts; toasts are noise.)
4. Should the UI offer a "force direct" quick-switch in Playground (a button that sends with model flash-direct)? (Recommend no — the model selector already covers it.)
5. Confirm the Playground is the right home for session-affinity demoing, vs a dedicated "Routing" tab. (Recommend keeping it in Playground.)
