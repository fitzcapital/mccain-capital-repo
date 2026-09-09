## Why

Market Pulse already fails closed when required inputs are stale, but reliability evidence is
fragmented across transient process memory, repeated status panels, and source-specific messages.
Because the page guides live decisions, the trader needs durable proof of what was fresh, what
failed, how the system recovered, and whether the displayed setup was built from complete data.

## What Changes

- Create a durable, bounded data-quality event ledger for canonical promotions, source failures,
  stale locks, retries, recovery, worker adoption, and setup-evidence persistence.
- Add per-source service-level indicators for freshness, availability, latency, consecutive
  failures, last success, and current fallback/proxy use.
- Add completeness checks for spot, completed bars, SPY volume proxy, Gamma/options, levels,
  strategy evidence, setup ledger persistence, and analytics outcome capture.
- Make every execution generation expose one concise trust verdict: `Verified`, `Degraded`, or
  `Locked`, with the exact reason and next automatic recovery attempt.
- Consolidate the current Data Pipeline, Data Health, and session-state repetition into one modern
  Trust Center summary with progressive disclosure for technical detail and recent incidents.
- Add a separate read-only reliability history view for incident timing, duration, affected source,
  recovery, and daily coverage trends.
- Add local alert events for transitions into degraded/locked state and for recovery, with
  deduplication so persistent outages do not repeatedly notify the trader.
- Preserve fail-closed execution and the last verified generation; no degraded input may silently
  alter action, trigger, invalidation, target, or setup history.

Non-goals:

- No autonomous trading, order placement, or weakening of freshness thresholds.
- No public hosting, external observability platform, Redis, or second database.
- No new market-data vendor in this phase; the work measures and exposes the current sources.
- No redesign of strategy logic, Strat pattern rules, Gamma calculations, or outcome definitions.
- No storage of credentials or raw provider responses in reliability history.

Acceptance criteria:

- Reliability events survive worker restarts and container rebuilds in the existing application
  database while remaining bounded by retention policy.
- Every Market Pulse response provides one monotonic generation trust verdict and component-level
  source, timestamp, age, threshold, fallback, and failure reason.
- A stale or incomplete required component locks execution and cannot be masked by a fresh quote.
- The page distinguishes current verified data, usable observation-only data, and unavailable data
  without repeating contradictory status messages.
- Degraded/locked transitions produce one alert event, recovery produces one recovery event, and
  unchanged states do not create alert noise.
- Focused fault-injection and restart tests prove failure detection, durable incident history,
  recovery, and truthful UI rendering.

## Capabilities

### New Capabilities

- `market-pulse-data-trust-center`: Durable source-quality history, trust verdict presentation,
  reliability trends, incident drill-down, and deduplicated trader alerts.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Require completeness and provenance for every execution
  generation, including proxy/fallback disclosure and setup-evidence persistence.
- `market-pulse-operational-resilience`: Persist bounded reliability events and expose measurable
  source availability, latency, failure, and recovery indicators across workers and restarts.

## Impact

- Affected data: one additive reliability-event table and bounded aggregate quality history in the
  existing SQLite database; no raw provider payloads or secrets.
- Affected backend: canonical freshness, operational health, source adapters, setup persistence,
  handlers, and read-only reliability APIs.
- Affected UI: Market Pulse trust summary, component detail drawer, alert events, and a focused
  reliability-history page linked from Market Pulse.
- Affected tests: source fault injection, multi-worker/restart durability, deduplication, retention,
  generation coherence, UI contracts, and deployed desktop/narrow verification.
- Dependencies: existing Flask, SQLite, canonical snapshot envelope, alert ledger, and local
  monitoring patterns; no new external dependency.
