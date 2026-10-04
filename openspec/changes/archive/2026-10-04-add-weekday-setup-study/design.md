## Context

Setup Analytics currently reads durable setup events, applies one normalized filter set, removes
duplicate actionable events, and derives metrics, time buckets, setup-family comparisons, leaders,
and ledger rows. The same canonical population can support weekday analysis without a schema change
or another data source. The current history contains enough outcomes for an initial study, but only
four to six sessions per weekday, so evidence maturity must remain prominent.

## Goals / Non-Goals

**Goals:**

- Aggregate the current filtered canonical setup population by New York weekday.
- Identify each weekday's strongest setup and setup-plus-time combination using the existing
  evidence-aware leader method.
- Reconcile weekday totals with the existing analytics metrics and duplicate count.
- Provide compact comparison and drill-down controls without adding permanent explanatory noise.
- Preserve honest missing-data and gamma-coverage behavior.

**Non-Goals:**

- Change stored setup events, trading rules, target definitions, or outcome evaluation.
- Infer option executions, realized P&L, or absent historical gamma context.
- Add a new analytics store, background job, or third-party charting library.
- Claim that a weekday leader predicts future performance.

## Decisions

### 1. Derive weekday from the canonical session date in New York context

The service will assign each canonical event to Monday through Friday using `session_date`, which is
already the authoritative New York trading-session date. This avoids UTC conversion moving an event
to an adjacent calendar day. Invalid dates will be reported as unavailable rather than guessed.

Deriving weekday from browser locale was rejected because clients may use other time zones and
would not produce an authoritative market-session grouping.

### 2. Extend the existing analytics payload additively

The server will add a `weekday_study` payload containing ordered weekday cohorts, weekday leaders,
and reconciliation metadata. Each cohort will include total occurrences, completed outcomes,
target/invalidation counts, open/unavailable counts, coverage, raw target rate, Wilson-adjusted
target rate, median MFE/MAE, evidence maturity, and drill-down filter values.

The weekday filter will be normalized server-side and applied before all aggregations so KPIs,
leaders, charts, family cards, and ledger always describe the same selected population. Existing
response fields remain compatible.

A client-only aggregation was rejected because paginated ledger rows are incomplete and browser
logic could drift from canonical deduplication.

### 3. Reuse the current ranking method within each weekday

For every weekday, the service will group completed outcomes by family and by family plus 30-minute
bucket. Cohorts will expose the same Wilson lower bound, evidence maturity, median excursions, and
stable tie-breaking used by all-history leaders. Open and unavailable outcomes affect coverage only.

The UI may show raw target rate for readability, but the winning label is selected using the
adjusted ranking. A cohort with one or two wins can appear as early evidence, but its raw 100% alone
cannot make it a dependable leader.

### 4. Use a compact weekday study with optional help

The Overview will include a Monday-through-Friday comparison with one concise card or row per day.
The default state shows the weekday, target rate, completed fraction, evidence label, best setup,
and best time. Median MFE/MAE, coverage definitions, adjusted-ranking explanation, and supporting
counts live in hover/focus helpers or expandable details.

Selecting a weekday or leader applies server-authoritative filters and provides a clear return to
the prior horizon. The layout must stack cleanly on narrow screens and remain keyboard accessible.

### 5. Fail honestly when evidence is incomplete

If a weekday has no completed outcomes, it will say `No completed outcomes` rather than `0%`. If no
eligible family or combination exists, no winner is named. Missing gamma coverage is disclosed, and
gamma-conditioned weekday conclusions are withheld until captured samples exist for the cohort.

## Risks / Trade-offs

- [Only four to six sessions currently exist per weekday] → Show exact denominators and evidence
  maturity; do not use predictive or statistical-significance language.
- [Weekday totals could drift from headline metrics] → Aggregate after the same filtering and
  canonicalization pass and return an explicit reconciliation result tested at boundaries.
- [The page could become visually crowded] → Default to a compact comparison and keep definitions
  in accessible optional helpers.
- [An additive payload increases response size] → Return aggregate cohorts only; reuse existing
  occurrence summaries and avoid duplicating full ledger records.
- [Gamma coverage is currently sparse] → Report coverage but do not rank weekday-gamma combinations
  until data is adequate.

## Migration Plan

1. Add deterministic weekday aggregation and ranking helpers with service tests.
2. Add the optional weekday filter and additive API payload.
3. Add the compact weekday study and accessible details/drill-down behavior.
4. Run focused Python and JavaScript checks, rebuild the local app, verify `/healthz`, and verify the
   deployed Today and All History receiving states.

Rollback removes the additive payload, filter, and UI study. No stored data requires migration or
rollback.

## Open Questions

None required for implementation. The existing five-completed-outcome threshold remains the
definition of established evidence so weekday leaders stay consistent with current leader cards.
