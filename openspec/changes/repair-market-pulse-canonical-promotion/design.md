## Context

Market Pulse renders a canonical execution generation alongside faster observation surfaces such as
the live quote, chart, and Gamma Ladder. The current refresh path can receive a newer current-session
spot quote and gamma snapshot but still validate older cached component records. The visible market
data advances while the canonical generation remains locked on `spot, gamma`.

The repair must preserve the existing safety boundary: no execution generation may promote unless
spot, completed bars, and gamma are fresh, session-compatible, and coherent. Provider credentials,
thresholds, strategy rules, and runtime data are unchanged.

## Goals / Non-Goals

**Goals:**

- Reconcile each required component to the newest valid current-session observation available to the
  canonical refresh.
- Ensure spot price and its authoritative observation timestamp travel together.
- Ensure the canonical gamma input and the Gamma Ladder identify the same source generation.
- Promote a coherent generation atomically, or retain the last valid generation with a real blocker.
- Cover stale-cache/current-provider regressions with focused tests.

**Non-Goals:**

- Relaxing freshness thresholds or execution locks.
- Changing gamma calculations, scenario ranking, or trading guidance.
- Treating an observation-only quote as canonical when its source time or session is unknown.
- Adding providers or altering credentials.

## Decisions

### Reconcile before freshness validation

Canonical assembly will select the newest valid candidate for each required component before calling
the existing freshness/coherence validator. Candidate selection compares normalized observation
times and rejects future, malformed, or wrong-session records. This is preferable to raising
thresholds because the defect is source selection, not acceptable staleness.

### Keep value and provenance atomic

Spot overlay will replace price, source, session, and `as_of` as one observation. An older cache row
must not retain its timestamp after receiving a newer price. This prevents a fresh price from being
classified using stale provenance.

### Reconcile gamma at the source boundary

When the ladder worker supplies a newer valid gamma snapshot, canonical assembly will use that same
snapshot before computing the generation id and freshness. The client will not clear a gamma lock by
appearance alone; the server must publish one coherent generation shared by execution and ladder.

### Preserve fail-closed promotion

Promotion remains all-or-nothing. Missing timestamps, incompatible sessions, provider errors, or
component divergence retain the last valid generation and name the exact component. No client-side
override will authorize execution.

## Risks / Trade-offs

- **A malformed provider timestamp could appear newer** -> Normalize and validate timestamps and
  session membership before comparison.
- **Concurrent refreshes could select different observations** -> Preserve the existing single-flight
  refresh and compute one generation id after reconciliation.
- **Ladder-only data could be mistaken for canonical gamma** -> Accept only the structured gamma
  snapshot that passes the same freshness/coherence contract.
- **A regression could hide a genuine outage** -> Retain fail-closed tests for missing and stale
  inputs in addition to the new successful-promotion tests.

## Migration Plan

No data migration is required. Deploy the service and focused regression tests, rebuild the local
application, and verify the authenticated Market Pulse page advances its last-valid generation
without reloading. Rollback is the service-code revert; persisted last-valid snapshots remain
compatible.

## Open Questions

None. Existing provider, session, freshness, and promotion contracts remain authoritative.
