## 1. Deterministic Monitor Core

- [x] 1.1 Extract or expose a one-cycle server setup evaluation service that reuses canonical
  generation collection, the existing exact-pattern builder, live evaluator, durable ledger, and
  analytics backfill without requiring request context.
- [x] 1.2 Add completed-candle cursoring so unchanged candles are skipped while retained gap candles
  are evaluated in order.
- [x] 1.3 Add wall-clock versus monotonic discontinuity detection and require canonical session
  revalidation before alert eligibility resumes.
- [x] 1.4 Add focused service tests for exact reversals, continuation rejection, stale required data,
  unchanged candles, inside-window recovery, and late-review recovery.

## 2. Runtime Ownership and Scheduling

- [x] 2.1 Implement one bounded regular-session monitor loop with exchange-calendar sleep/wake,
  configurable cadence, error backoff, and clean shutdown behavior.
- [x] 2.2 Wire runtime startup behind an enabled configuration flag while keeping automatic startup
  disabled in tests.
- [x] 2.3 Guard duplicate initialization and verify page and server concurrency produces one durable
  setup transition and at most one alert.
- [x] 2.4 Add lifecycle tests for page-closed operation, hidden client polling, duplicate startup,
  session close, holiday, restart, and provider recovery.

## 3. Health and Reliability Evidence

- [x] 3.1 Add sanitized monitor ownership, heartbeat, last attempt, last success, last evaluated
  candle, failure count, clock state, and last error fields to Market Pulse operational health.
- [x] 3.2 Record and reconcile monitor degraded/recovered reliability events without impossible or
  backward recovery timestamps.
- [x] 3.3 Update the bounded Market Pulse soak harness to fail on stale regular-session heartbeat,
  duplicate alert, missed eligible page-closed setup, clock-recovery failure, or Replay mismatch.
- [x] 3.4 Add health endpoint and reliability-ledger tests for healthy, sleeping, degraded, recovered,
  clock-discontinuous, and persistence-failure states.

## 4. Receiving Surface and Compatibility

- [x] 4.1 Make the Market Pulse page adopt server-persisted setup state and show a concise setup
  coverage warning when the server monitor is unhealthy.
- [x] 4.2 Preserve hidden-tab client suspension, manual refresh, current alert controls, and additive
  API compatibility.
- [x] 4.3 Add authenticated receiving-page contract tests proving server-first and page-first setup
  convergence without duplicate notifications.

## 5. Verification and Deployment

- [x] 5.1 Run focused Market Pulse pattern, live setup, Replay, data-trust, operational-health,
  watchdog, and new server-monitor tests.
- [x] 5.2 Run Python formatting/lint checks for changed modules, JavaScript syntax checks when client
  assets change, OpenSpec strict validation, and `git diff --check`.
- [x] 5.3 Run the bounded page-closed soak and confirm eligible setup coverage, Replay parity, durable
  idempotency, thread stability, and database integrity.
- [x] 5.4 Rebuild the local Podman application, verify `/healthz` and Market Pulse health, and inspect
  the deployed receiving page and monitor heartbeat.
