## Context

The page has multiple refresh paths: live quote/chart updates, Gamma Ladder updates, and a canonical
Market Pulse context refresh. During the Aug 19 live session, quote and ladder values advanced while
the authoritative timestamp stayed on Aug 18. The UI then truthfully locked execution but
incorrectly described the partial check as successful. Runtime inspection also found repeated
provider timeouts and enough outstanding threads to exhaust the container process limit.

The existing scenario evaluator and completed-candle evidence services are the correct foundation.
This change must preserve their deterministic guardrails, existing chart state, and the user's
unrelated worktree changes.

## Goals / Non-Goals

**Goals:**

- Make Market Pulse a trustworthy live execution guide based on one synchronized generation.
- Bound refresh concurrency and prevent slower work from overlapping the next refresh.
- Preserve current observations without presenting them as execution-authoritative when incomplete.
- Reconstruct potential earlier setups without look-ahead bias and explain them as review evidence.
- Keep live guidance visually primary and historical replay clearly secondary.

**Non-Goals:**

- Placing or routing orders.
- Predicting future price or guaranteeing an outcome.
- Replacing the strategy's completed-candle and confirmation rules.
- Backfilling missing provider observations with invented values.
- Persisting replay results as actual trades without an explicit user action.

## Decisions

### Use a canonical immutable generation envelope

The server will assemble quote, bars, gamma, levels, ranked scenarios, evidence, and permission into
one envelope with a generation id, session date, source timestamps, observed timestamps, and
component freshness. It will validate the whole envelope before publishing it to the canonical
cache. The client will commit all execution-facing nodes from that envelope in one transaction.

This is preferred over independent DOM patches because values can be individually current while
their combined decision is invalid. Observation-only quote and ladder streams may continue to move,
but their UI will explicitly identify them as such until canonical promotion succeeds.

### Use single-flight refresh orchestration with bounded provider work

Only one canonical refresh may run per ticker/process. Automatic checks read the cached generation
first and request provider work only when policy says it is due. A manual refresh may request a
forced provider attempt but joins an existing attempt rather than starting another. Provider calls
use finite connect/read deadlines; concurrency uses a long-lived bounded executor or equivalent
service-level limiter rather than a new pool for every request.

This is preferred over increasing process limits because higher limits hide the leak and delay the
same failure.

### Separate refresh outcome from data outcome

The API will report `promoted`, `partial`, `unchanged`, or `failed`, plus component results. “Live
execution ready” requires a promoted or still-valid canonical generation. A successful quote fetch
alone cannot produce a successful playbook message.

### Derive the live guide from permission plus the primary scenario

The guide has a fixed sequence: location, evidence now, trigger, confirmation, action, invalidation,
and target. Action language appears only when data permission is live-safe and the primary scenario
is confirmed. Otherwise, the same surface explains exactly what is missing and what event would
unlock the next step.

### Replay setups with an as-of-time evaluator

For each completed five-minute candle in the selected regular session, replay builds an input slice
containing only bars and timestamped level/gamma observations available at or before that close. It
runs the same scenario/evidence functions used live. A setup is recorded when it first transitions
to confirmed; duplicate confirmations for the same candidate are coalesced until invalidation.

Outcome analytics are computed only after the signal record is frozen. Signal grade and eligibility
never use later candles. The UI visually separates “Known at signal” from “What happened next.”

### Keep replay ephemeral by default

Replay is computed from existing session data and returned by an API; it does not write to the trade
ledger. This avoids treating hypothetical setups as executed trades and avoids a migration.

## Risks / Trade-offs

- **Provider delays can keep execution locked** → Preserve last valid context, show the failing
  component and retry timing, and never weaken freshness thresholds.
- **Sparse historical gamma observations reduce replay coverage** → Mark gamma unavailable for
  that timestamp, reduce disclosed confluence accordingly, and never substitute the day's final
  gamma snapshot.
- **Replay can encourage hindsight interpretation** → Separate signal-time evidence from outcome
  metrics and label every record “potential setup, not recorded execution.”
- **One canonical envelope may update less often than quotes** → Continue lightweight observation
  streams, but distinguish them from execution readiness.
- **Refresh changes touch a busy page** → Keep the API additive, reuse existing evaluators, and
  verify chart viewport, drawings, timeframe, and ladder selection survive refresh.

## Migration Plan

1. Add the envelope and outcome model behind the existing Market Pulse context endpoint.
2. Add single-flight/bounded orchestration while retaining the current manual endpoint contract.
3. Update the client to consume the new fields with compatibility fallback.
4. Add the live guide and replay API/panel after atomic refresh tests pass.
5. Rebuild the local Podman app, verify health and bounded thread growth across repeated refreshes,
   and exercise the authenticated page during both promoted and partial refreshes.
6. Roll back by disabling the new client controller and replay panel; the existing locked fallback
   remains valid and no data migration is required.

## Open Questions

- Exact provider freshness thresholds remain the existing configured thresholds unless focused
  tests show they are internally inconsistent.
- Historical gamma availability will determine whether some replay setups are fully scored or
  explicitly marked partial/non-actionable.
