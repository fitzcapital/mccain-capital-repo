## Why

Market Pulse currently evaluates live setups only while an active browser page requests canonical
context. Today, valid 09:55 and 10:15 ET reversals were retained by Setup Replay but were not
discovered until 11:12 ET, making them ineligible for real-time alerts. Live detection must be owned
by the server and must continue during the regular session when the page is closed or hidden.

## What Changes

- Add a bounded server-side Market Pulse monitor that refreshes completed SPX five-minute bars,
  evaluates exact Strat reversal setups, persists state, and emits eligible alerts during the
  regular session without requiring a browser request.
- Preserve the browser's existing hidden-tab polling suspension; page visibility will no longer
  determine whether server-side setup detection runs.
- Reuse the canonical setup builder, monotonic durable ledger, replay persistence, deduplication,
  market calendar, cutoff, and fail-closed data-trust rules so server and page evaluations cannot
  disagree.
- Record monitor heartbeat, last completed evaluation, last evaluated candle, delivery result,
  clock health, and sanitized failure state in Market Pulse operational health.
- Reconcile gaps after startup, sleep, or provider recovery by persisting historical setups as late
  review while alerting only setups first observed inside the configured live eligibility window.
- Reject duplicate workers and duplicate alerts through one process-local owner plus the existing
  durable idempotency ledger.
- Add deterministic tests for page-closed operation, hidden tabs, normal cadence, stale data,
  restart recovery, clock discontinuity, late review, alert deduplication, and Replay parity.
- Non-goals: changing setup definitions, treating continuations as reversals, changing gamma's role,
  adding order execution, contacting a broker, or rewriting historical/personal runtime data.

Acceptance requires a server monitor to detect a newly completed eligible setup within 30 seconds
without an open Market Pulse page, persist it once, deliver at most one alert, match Setup Replay,
and expose a healthy or precisely degraded heartbeat. Required-data failure must suppress alerts and
retain the last valid generation.

## Capabilities

### New Capabilities

- `market-pulse-server-setup-monitoring`: Server-owned regular-session setup evaluation, durable
  delivery, recovery, ownership, lifecycle, and observability independent of browser activity.

### Modified Capabilities

- `market-pulse-operational-resilience`: Operational health and soak evidence will include the
  server monitor heartbeat, clock continuity, evaluation coverage, and missed-window detection.
- `market-pulse-live-state-coherence`: Browser rendering and server monitoring will adopt the same
  canonical generation and persisted setup state without duplicate evaluation or alerts.

## Impact

- Affected services: Market Pulse canonical snapshot, live setup evaluator, alert ledger, replay
  analytics persistence, runtime lifecycle, and operational health.
- Affected UI/API: existing Market Pulse context and health payloads gain additive monitor fields;
  hidden-tab client behavior remains unchanged.
- Data sources: existing completed five-minute bars, canonical levels, gamma context, and exchange
  calendar. No new provider or financial assumption is introduced.
- Runtime: one bounded local background worker per application process, with single-flight refresh,
  explicit shutdown behavior, and no polling outside the exchange session.
- Verification: focused unit/integration tests, bounded soak coverage, database integrity checks,
  container rebuild, `/healthz`, Market Pulse health, and deployed setup parity checks.
