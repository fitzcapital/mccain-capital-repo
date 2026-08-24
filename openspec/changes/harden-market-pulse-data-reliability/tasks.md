## 1. Baseline and generation contract

- [x] 1.1 Map current quote, candle, gamma, level, scenario, replay, cache, and client refresh paths
- [x] 1.2 Add focused regression tests that reproduce mixed generations, stale gamma, worker drift,
  overlapping polls, and suspended-tab recovery
- [x] 1.3 Define the canonical generation envelope, required component metadata, validation result,
  and retry classification in the Market Pulse service layer

## 2. Server-side reliability

- [x] 2.1 Assemble candidates under one single-flight refresh transaction with bounded provider
  timeouts
- [x] 2.2 Validate SPX symbol, market session, source timestamps, component freshness, and derived
  execution coherence before promotion
- [x] 2.3 Persist verified envelopes with validated reads, atomic replacement, monotonic generation
  ordering, and last-known-good fallback
- [x] 2.4 Make every worker adopt the newest shared verified envelope before serving context
- [x] 2.5 Return component ages, blockers, refresh duration, next interval, and retry classification in
  the existing context response without exposing sensitive provider data
- [x] 2.6 Ensure observation-only quote updates cannot mutate canonical action, trigger, invalidation,
  target, permission, or last-valid time

## 3. Client polling and reconciliation

- [x] 3.1 Refactor the Market Pulse refresh coordinator into one explicit single-flight lifecycle
- [x] 3.2 Add abort timeouts, bounded exponential backoff with jitter, and server-directed cadence
- [x] 3.3 Trigger immediate revalidation after tab visibility and network restoration
- [x] 3.4 Derive the visible countdown from the actual next-attempt timestamp and render idle,
  refreshing, retrying, delayed, stale, and locked states
- [x] 3.5 Stage, validate, and atomically commit newer generations while rejecting older, malformed,
  cross-session, or contradictory responses
- [x] 3.6 Preserve chart timeframe, ladder selection, ticker, scroll position, and disclosure state
  across successful in-place updates

## 4. Failure clarity and diagnostics

- [x] 4.1 Show the exact blocking component, component age, last verified time, and next recovery attempt
  in the compact execution diagnostics surface
- [x] 4.2 Add structured local logging for generation promotion/rejection, refresh duration, retry
  reason, component ages, and worker adoption
- [x] 4.3 Confirm logs exclude credentials, raw provider payloads, orders, and personal financial data

## 5. Verification

- [x] 5.1 Run focused Python tests for freshness, generation promotion, worker convergence, endpoint
  semantics, stale fallback, and recovery
- [x] 5.2 Run focused JavaScript tests and syntax checks for single-flight polling, countdown truth,
  atomic reconciliation, and lifecycle recovery
- [x] 5.3 Rebuild with `./scripts/run_podman_app.sh` and verify `/healthz`
- [x] 5.4 Verify repeated deployed polls advance coherently without full-page reloads and preserve user
  interaction state
- [x] 5.5 Exercise forced quote, candle, and gamma failure cases and confirm execution fails closed,
  identifies the blocker, retains the last verified generation, and recovers automatically
- [x] 5.6 Record the verified reliability baseline before beginning time-aware ladder enhancements
