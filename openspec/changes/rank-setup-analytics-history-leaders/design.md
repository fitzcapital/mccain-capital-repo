## Context

Setup Analytics already aggregates one normalized selection into time buckets, setup families,
KPIs, and a ledger. The header currently promotes the highest raw target-rate time bucket, which can
make a 1-of-1 or 2-of-2 cohort look like the best historical opportunity. The page needs a concise
decision layer built from the same selected rows without changing the underlying outcomes.

## Goals / Non-Goals

**Goals:**

- Answer "what setup worked best?", "when?", and "which setup at which time?" immediately.
- Rank only completed outcomes and disclose the denominator behind every result.
- Penalize small samples and surface evidence strength without implying statistical certainty.
- Keep all leader cards synchronized with presets and advanced filters.
- Allow one-click drill-down from a leader to its supporting setups.

**Non-Goals:**

- Re-score, rewrite, or infer setup outcomes.
- Rank by hypothetical option profit.
- Claim predictive certainty or statistical significance.
- Introduce a database migration or separate analytics dataset.

## Decisions

### 1. Compute three leader cohorts from one filtered row set

The server will aggregate completed outcomes by setup family, 30-minute New York time bucket, and
family-plus-time bucket. All three use the same normalized selection already feeding the page. This
avoids a second interpretation of dates, presets, or outcome eligibility.

Client-side ranking was rejected because it could drift from server filters and would require
shipping more raw rows before the header could render.

### 2. Use an evidence-adjusted success measure

Each cohort will expose target successes, completed outcomes, raw target rate, Wilson lower bound,
median favorable move, median adverse move, and total occurrences. A cohort needs at least five
completed outcomes to be called "Established evidence." Two to four completed outcomes are "Early
evidence," one is "Single example," and zero is unavailable.

Ranking first compares the Wilson lower bound, then prefers cohorts meeting the established
threshold when adjusted rates tie, then compares the favorable-to-adverse movement profile and
completed sample size, with a stable alphabetical tie-breaker. This prevents an established cohort
with no successful outcomes from outranking a genuinely stronger early cohort while still penalizing
tiny samples. The page labels early samples as provisional.

Bayesian priors and a proprietary composite score were rejected because the current sample is small
and the result would be harder for the trader to audit.

### 3. Present the result as a compact decision strip

The Overview places "What worked best" directly below the horizon controls. It contains three cards:
Best Setup, Best Time, and Best Setup + Time. Each card shows the name/window, target result as a
fraction and percentage, evidence label, median favorable/adverse move, and a "Study these setups"
control. The existing frequency chart and full family cards remain supporting detail.

On All History, the strip title explicitly says "All-history leaders." Other presets use the active
horizon label so users never confuse today with the full record.

### 4. Drill-down reuses existing filters

Leader controls set the corresponding family and/or time-range query fields and preserve the active
date horizon. The resulting request remains authoritative for KPIs, charts, cards, and ledger. A
clear-filter action returns to the prior horizon.

### 5. Missing evidence fails honestly

If no completed cohort exists, the strip says that no winner can be measured yet and reports how
many setups are awaiting resolution or lack historical outcomes. It never treats missing outcomes
as losses and never promotes a frequency-only winner as performance.

### 6. Count canonical actionable events without deleting evidence

Analytics groups rows by session, signal candle, setup family, direction, and level. When more than
one stored row represents that same actionable event, the service keeps one canonical row for every
metric, chart, leader, and ledger view. It prefers completed outcome evidence, then richer excursion
data, higher score, newer source revision, and a stable identifier. Raw database rows remain intact
for auditability.

Different candles, levels, families, or directions remain distinct. Pattern labels such as 2-1-2
and 2-2 are not independently counted when they point to the same trade decision on the same candle
and level.

### 7. Image generation remains explicit

Normal Setup Analytics and Market Pulse requests do not write screenshots. Tracked documentation
images are refreshed only by explicit capture scripts; journal images require a user upload; gamma
maps and Vanquish debug captures remain bounded runtime artifacts outside tracked application assets.

## Risks / Trade-offs

- [Five outcomes is still a small sample] → Label it "Established for this sample," retain the exact
  denominator, and avoid predictive language.
- [Wilson ranking feels unfamiliar] → Show plain-language evidence strength by default and put the
  adjusted-rate explanation in a concise details disclosure.
- [One setup dominates by frequency] → Rank completed effectiveness, not occurrence count, while
  displaying both total and completed samples.
- [Drill-down hides the larger context] → Keep the active preset visible and provide a one-click
  return to the unfiltered horizon.
- [All History becomes slower] → Reuse the existing indexed query and aggregate in the current
  service pass; do not add provider calls or per-card queries.

## Migration Plan

1. Add deterministic cohort aggregation and ranking helpers with boundary tests.
2. Add leader payloads without removing current response fields.
3. Add the decision strip and drill-down controls.
4. Verify Today and All History receiving states, rebuild the local app, and check `/healthz`.

Rollback removes the additive leader payload and decision strip. Existing analytics remain intact.

## Open Questions

None required for implementation. The five-completed-outcome threshold is explicit and can be tuned
later using observed history without changing stored data.
