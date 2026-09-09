## 1. Authoritative Session Contract

- [x] 1.1 Extend the existing exchange-calendar service to return the current phase, server time, regular-session boundaries, next transition, next valid open, polling permission, and recommended cadence for normal, holiday, and early-close sessions.
- [x] 1.2 Add the session contract to the initial Market Pulse render and canonical context response while preserving current response fields.
- [x] 1.3 Add focused server tests for premarket, 09:30 open, regular session, 16:00 close, weekend, full holiday, early close, and clock-boundary behavior.

## 2. Single-Flight Browser Lifecycle

- [x] 2.1 Implement one Market Pulse refresh coordinator that owns the active request, regular poll, boundary wake, retry timer, and latest reconciled session contract.
- [x] 2.2 Route manual refresh, timer, focus, visibility, online, and cache-restore triggers through the coordinator with request coalescing, timeouts, and bounded retry backoff.
- [x] 2.3 Schedule boundary revalidation from server timestamps so a page left open enters the regular session at 09:30 ET and leaves it at the calendar-defined close without a reload.
- [x] 2.4 Suspend recurring work while hidden and perform one immediate reconciliation after sleep, a crossed boundary, meaningful staleness, restored connectivity, or browser-cache restoration.

## 3. Atomic Market Pulse Reconciliation

- [x] 3.1 Reconcile the header, phase label, countdown, freshness, spot, regime, execution permission, chart context, gamma state, and setup monitor from one canonical generation.
- [x] 3.2 Preserve the last valid coherent generation and fail execution closed with a precise blocker when a response is partial, stale, timed out, or generation-mismatched.
- [x] 3.3 Gate the five-second tape and live setup monitor on regular-session permission and page visibility, and remove superseded independent polling timers.
- [x] 3.4 Update the compact lifecycle status and expanded diagnostics to show phase, last successful check, next transition or retry, polling state, and blocker without exposing sensitive data.

## 4. Reliability Coverage

- [x] 4.1 Add client tests for simultaneous triggers, in-flight close boundaries, retry exhaustion, hidden-tab suspension, focus recovery, online recovery, and back-forward-cache restoration.
- [x] 4.2 Add receiving-page tests proving that session transitions update all Market Pulse surfaces together and never show mixed planning/live generations.
- [x] 4.3 Add a deterministic soak test covering repeated automatic cycles with no overlapping flights, duplicate timers, indefinite loading state, or after-hours provider polling.

## 5. Verification

- [x] 5.1 Run focused Python tests, JavaScript syntax checks, formatting checks, and `git diff --check` for the changed lifecycle paths.
- [x] 5.2 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and inspect the deployed Market Pulse endpoint and assets.
- [x] 5.3 Verify normal open/close, early-close, holiday, sleep/wake, hidden-tab, and failure-recovery acceptance scenarios with deterministic clocks; leave final visual inspection to the user.

## 6. Cached-First Initial Validation

- [x] 6.1 Schedule one immediate non-forced canonical reconciliation after a coherent Market Pulse page load or reload when the server session contract permits automatic polling.
- [x] 6.2 Carry the rendered generation/ETag through the shared single-flight coordinator so an unchanged generation remains a cheap no-op, a newer generation reconciles atomically, and stale state uses the existing bounded recovery path.
- [x] 6.3 Preserve closed-session boundary-only wake behavior, completed-candle safety, and the manual forced-provider refresh fallback.
- [x] 6.4 Add focused receiving-page and JavaScript contract tests for immediate open-session validation, unchanged/newer generation handling, no automatic closed-session refresh, request coalescing, and manual forced refresh.
- [x] 6.5 Run focused tests and syntax checks, rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and inspect the deployed Market Pulse behavior and assets.
