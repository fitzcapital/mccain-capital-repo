## Context

Market Pulse currently assembles a rich canonical payload, while several client-side refresh paths
also update individual visual components. During live-market inspection those paths exposed newer
quote/chart values beside an older ladder value, and the primary decision, Trade Decision card, and
ordered checklist disagreed about whether execution was permitted. The existing architecture already
has canonical context and last-valid fallback concepts, so this change tightens their receiving
semantics instead of introducing another state store.

## Goals / Non-Goals

**Goals:**
- Give each canonical response a deterministic generation id and component timestamps.
- Compute one server-authoritative execution state and make every execution surface obey it.
- Stage and validate a response before committing it to the rendered page.
- Keep observational quote updates visible without allowing them to authorize execution.
- Preserve user-selected chart, ladder, and disclosure state across successful refreshes.

**Non-Goals:**
- Altering SPX setup definitions, scoring weights, levels, or gamma interpretation.
- Replacing market-data providers or adding a new persistence layer.
- Expanding the strategy to SPY, QQQ, or other instruments.
- Redesigning Market Pulse.

## Decisions

1. **Use the canonical context response as the execution transaction boundary.** The service will
   attach one generation id to its quote, bars, gamma, levels, scenario, permission, and timestamps.
   This is smaller and safer than coordinating independent browser stores.
2. **Separate observation from authorization.** A newer spot may be displayed as observation-only,
   but action state remains locked unless the complete canonical generation validates. This avoids
   hiding useful tape movement while preventing a partial update from changing a trade decision.
3. **Normalize action state on the server and enforce it again at the client boundary.** The server
   derives `ACTIVE`, `WAIT`, or `LOCKED` from permission, freshness, and ordered hard confirmation.
   The client rejects actionable copy when that contract is internally inconsistent. Defense in
   depth is justified because stale markup can otherwise survive an in-place update.
4. **Stage before commit.** Client reconciliation first validates generation identity, required
   component status, and authoritative action state, then updates all execution nodes in one commit.
   On failure it retains the last valid generation and exposes the blocker.
5. **Measure divergence against the canonical generation.** Quote, bars, gamma, and levels carry
   timestamps and statuses. Missing, stale, or mismatched required inputs produce a lock rather than
   a best-effort actionable state.
6. **Bound asynchronous status copy.** A single-flight `unchanged` response schedules a short retry
   and clears manual feedback after a fixed deadline. Setup Replay uses a request timeout and always
   resolves to results, empty, or retryable error UI.
7. **Name regime scope.** The canonical regime remains the execution authority. The ladder may show
   its distribution classification, but when it differs the UI labels it as ladder positioning rather
   than presenting it as a second execution regime.

## Risks / Trade-offs

- [Observation-only quote can differ from the locked playbook] → Label it explicitly and show the
  canonical generation time used for decisions.
- [Stricter validation may lock more often during provider delays] → Preserve the last valid context
  and name the blocking component so the failure is honest and diagnosable.
- [Existing DOM update code may bypass canonical reconciliation] → Route execution-facing updates
  through one adapter and add contract tests that reject independent actionable mutation.
- [Generation ids could change for semantically identical payloads] → Base identity on the server
  snapshot timestamp and normalized execution inputs, not browser render time.
- [Provider latency can monopolize the single-flight] → Return retained context immediately, queue a
  bounded retry, and never leave a persistent running message.
- [Replay analysis can be slower than the main page] → Abort the browser request at a bounded
  deadline and expose a retry control without blocking live execution guidance.

## Migration Plan

Deploy additively: extend the context payload, update the page reconciliation contract, then enable
strict locking. Existing clients tolerate the added fields. Rollback consists of reverting the
service/template/runtime changes; no stored data migration is required.

## Open Questions

None. Existing component freshness thresholds remain authoritative; this change only enforces them
consistently.
