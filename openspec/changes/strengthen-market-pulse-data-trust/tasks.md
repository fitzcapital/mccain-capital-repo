## 1. Durable Reliability Evidence

- [x] 1.1 Add an additive migration for bounded reliability incidents and daily source-quality aggregates with indexed time, ticker, generation, component, and state fields.
- [x] 1.2 Implement a repository that opens, updates, closes, queries, and prunes sanitized reliability incidents idempotently across workers and restarts.
- [x] 1.3 Replace the process-only reliability deque as the authoritative history while retaining a bounded in-memory read cache and safe logging fallback.
- [x] 1.4 Add repository tests for stable ids, transition updates, recovery closure, worker restart, retention, pruning, and database-write failure.

## 2. Canonical Trust Evaluation

- [x] 2.1 Implement a pure `Verified`/`Degraded`/`Locked` evaluator over the existing canonical generation and required-component contract.
- [x] 2.2 Extend component diagnostics with source, provider/received timestamps, age, threshold, latency, completeness, fallback/proxy mode, persistence, and sanitized reason codes.
- [x] 2.3 Add setup-evidence completeness covering detection, alert-ledger persistence, Replay evaluation, analytics persistence, and the valid `no setup` state.
- [x] 2.4 Add explicit SPY-volume proxy coverage and ensure unmatched timestamps remain gaps rather than falling back to SPX index volume.
- [x] 2.5 Preserve the last verified generation and prove that fresh observation quotes cannot promote stale Gamma, bars, or persistence.

## 3. Reliability Metrics and Read APIs

- [x] 3.1 Aggregate bounded component availability, freshness compliance, latency, failures, fallback use, incident duration, and recovery time.
- [x] 3.2 Add authenticated read-only endpoints for the current trust verdict, recent incidents, and bounded reliability history.
- [x] 3.3 Add validation, pagination, range limits, empty states, and revision-aware caching for reliability queries.
- [x] 3.4 Add API tests for healthy, degraded, locked, partial proxy coverage, no-incident, invalid-range, and persistence-unavailable states.

## 4. Trust Center Design

- [x] 4.1 Replace repeated Data Pipeline, Data Health, and session health summaries with one compact Trust Center strip before the execution chart.
- [x] 4.2 Add an accessible detail drawer with plain-language component cards, provenance, freshness, completeness, blockers, and next recovery attempt.
- [x] 4.3 Add a dedicated reliability-history page with daily availability, incident duration, affected sources, and recovery timeline visualizations.
- [x] 4.4 Add Market Pulse navigation to reliability history without crowding the execution surface.
- [x] 4.5 Add responsive styling that wraps technical labels and timestamps without horizontal page overflow at desktop and narrow widths.

## 5. Reliability Alerts

- [x] 5.1 Map degraded/locked transitions and verified recovery into the existing sanitized application-alert lane.
- [x] 5.2 Deduplicate unchanged incidents with stable alert ids and preserve current mute and acknowledgement behavior.
- [x] 5.3 Add tests proving one degradation alert, no repeated outage noise, and one recovery alert with incident duration.

## 6. Fault and Deployment Verification

- [x] 6.1 Add deterministic faults for stale Gamma, missing bars, provider timeout, malformed timestamps, worker lag, failed canonical writes, failed setup persistence, and proxy gaps.
- [x] 6.2 Add a restart-boundary integration test proving incidents and current trust state remain coherent after process/container restart.
- [x] 6.3 Run focused Python/JavaScript tests, Ruff, syntax checks, query-plan inspection, strict OpenSpec validation, and `git diff --check`.
- [x] 6.4 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, exercise a controlled degraded-to-recovered transition, and inspect deployed desktop and narrow Trust Center/history layouts.
