## 1. Baseline and Contracts

- [x] 1.1 Inspect the existing canonical Market Pulse context, scenario evaluator, notification surface, repository patterns, and deployed refresh lifecycle before editing code
- [x] 1.2 Add typed live-setup lifecycle, stable identity, revision, alert eligibility, blocker, and delivery-state contracts without changing existing SPX strategy rules
- [x] 1.3 Add focused contract tests for serialization, stable setup ids, deterministic alert ids, and monotonic state revisions

## 2. Durable Setup Lifecycle

- [x] 2.1 Implement the server-owned `WATCHING`, `ARMED`, `CONFIRMED`, `TARGET_REACHED`, `INVALIDATED`, and `EXPIRED` transition service from canonical completed five-minute evidence
- [x] 2.2 Implement deterministic primary selection by lifecycle state, scenario lane, score, proximity, family priority, and stable identity
- [x] 2.3 Implement the configurable 3:15 PM ET default cutoff and late review-only behavior without changing historical Replay eligibility
- [x] 2.4 Add focused lifecycle tests for every transition, intrabar non-confirmation, separate same-family levels, terminal-state monotonicity, cutoff boundaries, and stale-data pauses

## 3. Persistence and Idempotency

- [x] 3.1 Add repository-managed durable storage for current setup state, bounded recent setup events, acknowledgement, and per-channel alert delivery using existing application-data conventions
- [x] 3.2 Add atomic compare-and-set or unique-key behavior that prevents duplicate confirmation events and deliveries across retries, reloads, restarts, and concurrent workers
- [x] 3.3 Fail alert delivery closed when persistence is unavailable and expose the degraded reason without mutating trades, journal entries, positions, orders, or P&L
- [x] 3.4 Add focused persistence and concurrency tests for duplicate polls, manual/automatic overlap, worker races, reload restoration, restart restoration, and storage failure

## 4. Canonical API Integration

- [x] 4.1 Evaluate the live setup monitor only from a validated canonical SPX generation and add it to the canonical Market Pulse response with generation, revision, freshness, state age, evaluation timing, and blockers
- [x] 4.2 Enforce server-side alert gates for SPX, B-or-better score, fresh coherent components, safe permission, completed-candle confirmation, pre-cutoff timing, and first delivery
- [x] 4.3 Expose compact lower-ranked watch summaries and bounded recent terminal events while preserving Setup Replay as a separate read-only response
- [x] 4.4 Add API tests that reject mixed generations, locked permission, stale gamma/bars/levels, replay-derived signals, older revisions, and contradictory alert eligibility

## 5. In-App Monitor and Alerts

- [x] 5.1 Add a compact full-width Live Setup Monitor below Alternative and Dormant Watch with collapsed state, direction, level, grade, age, freshness, and next-evaluation countdown
- [x] 5.2 Add expanded location, trigger, confirmation, action, invalidation, target, evidence checklist, recent state history, acknowledgement, and collapsed best-to-least secondary watches
- [x] 5.3 Reconcile setup revisions atomically with the canonical client refresh, preserve existing chart/ladder/disclosure state, reject older revisions, and remove action language while paused or locked
- [x] 5.4 Wire exactly-once confirmed events into the existing in-app notification center and add optional permission-gated browser notification/sound with a persistent mute control
- [x] 5.5 Add focused JavaScript tests for rendering, ranking, countdown behavior, stale pause, acknowledgement, mute, out-of-order responses, reload restoration, and duplicate-delivery prevention

## 6. Reliability and Receiving-Surface Verification

- [x] 6.1 Add operational diagnostics for last evaluation, next evaluation, canonical generation, setup revision, state age, persistence health, and alert-delivery outcome
- [x] 6.2 Run focused Python tests, JavaScript tests, syntax checks, formatting checks, and `git diff --check` for the changed paths
- [x] 6.3 Rebuild the local Podman application with `./scripts/run_podman_app.sh` and verify `/healthz` plus canonical Market Pulse endpoint behavior
- [x] 6.4 Verify on the receiving Market Pulse page that a simulated qualifying transition alerts once, reload restores state without replaying it, stale data pauses actionability, late setups are review-only, and Replay cannot trigger the monitor
- [x] 6.5 Document verification evidence, remaining provider-dependent limitations, the active cutoff value, and rollback/feature-flag behavior
