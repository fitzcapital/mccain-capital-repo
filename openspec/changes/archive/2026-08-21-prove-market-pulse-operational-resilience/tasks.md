## 1. Shared Exchange Session Calendar

- [x] 1.1 Inventory duplicate market-session and holiday helpers and their callers
- [x] 1.2 Add one reusable Eastern-time US equity session calendar service
- [x] 1.3 Cover weekends, observed holidays, early closes, and exceptional-closure overrides
- [x] 1.4 Migrate the Market Pulse refresh contract and next-open calculation to the shared service
- [x] 1.5 Add boundary tests for regular sessions, holidays, early closes, year transitions, and DST

## 2. Operational Health Contract

- [x] 2.1 Define a sanitized operational-health model and status classification
- [x] 2.2 Aggregate canonical promotion, component age, retry, worker adoption, and persistence health
- [x] 2.3 Add operational-health metadata to initial Market Pulse and canonical API payloads
- [x] 2.4 Add bounded local reliability event recording with an allowlisted schema
- [x] 2.5 Test healthy, delayed, locked, persistence-failed, worker-lagged, and recovered states

## 3. Market Pulse Watchdog Interface

- [x] 3.1 Add a compact operational-health watchdog to the Market Pulse receiving page
- [x] 3.2 Keep healthy state quiet and expand warning or critical state with blocker and retry details
- [x] 3.3 Reconcile watchdog state in the same atomic client commit as canonical execution state
- [x] 3.4 Preserve watchdog truth across initial load, in-place refresh, reload, pause, and recovery
- [x] 3.5 Add focused template, JavaScript, accessibility, and responsive contract tests

## 4. Deterministic Fault Harness

- [x] 4.1 Add test-only injectable boundaries for provider fetches and canonical persistence
- [x] 4.2 Add test-only injectable boundaries for alert-ledger persistence, clocks, and worker adoption
- [x] 4.3 Simulate timeout, malformed, stale, mixed-generation, network, and I/O failures
- [x] 4.4 Simulate delayed older responses, overlapping requests, and concurrent worker evaluation
- [x] 4.5 Prove last-valid retention, immediate lock, alert suppression, one-flight retry, and recovery
- [x] 4.6 Prove persistence failures fail closed without modifying financial or personal runtime data

## 5. Receiving-Page Resilience

- [x] 5.1 Add authenticated browser coverage for atomic header, execution, chart, ladder, and monitor state
- [x] 5.2 Verify in-place promotion and full reload converge to the same canonical generation
- [x] 5.3 Verify network restoration, focus return, and tab wake trigger one authoritative recovery check
- [x] 5.4 Verify partial, malformed, and older responses cannot partially update execution surfaces
- [x] 5.5 Verify terminal setup state and alert idempotency survive refresh, reload, and restart
- [x] 5.6 Verify holiday and early-close pages remain paused until the server-advertised next session

## 6. Soak Runner and Acceptance Report

- [x] 6.1 Add a bounded deterministic soak runner with configurable cycles and injected clocks
- [x] 6.2 Record sanitized counts, maximum recovery time, retries, locks, worker adoption, and alerts
- [x] 6.3 Fail the runner on mixed commits, duplicate alerts, unsafe unlocks, stuck requests, or reload-only recovery
- [x] 6.4 Add a short default verification mode and an explicit longer local live-session mode
- [x] 6.5 Produce a human-readable pass/fail report without raw provider or financial data

## 7. Final Verification

- [x] 7.1 Run focused calendar, health, persistence, lifecycle, API, coordinator, and browser tests
- [x] 7.2 Run Python formatting or lint checks, JavaScript syntax checks, and `git diff --check`
- [x] 7.3 Validate the OpenSpec change strictly and confirm every acceptance scenario has coverage
- [x] 7.4 Rebuild the local Podman app and verify `/healthz`
- [x] 7.5 Verify the deployed Market Pulse receiving page in healthy, forced-lock, and recovered states
- [x] 7.6 Run the short soak and record the final operational-resilience result
