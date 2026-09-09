## Context

The existing Live Setup ledger stores stable event identities and frozen point-in-time facts, while
Setup Replay adds forward outcome measurements from completed five-minute candles. That is enough
for a single-session review, but repeatedly reading raw candles and JSON ledgers across many
sessions would be slow and would risk recomputing old setups with newer Gamma levels.

The new page is a study surface for one trader. It must remain read-only, preserve the existing
Live/Replay definitions, use New York market time, and avoid implying option-contract profitability
from underlying SPX movement.

## Goals / Non-Goals

**Goals:**

- Retain one canonical analytics record per stable setup event across sessions.
- Query and aggregate that history efficiently with consistent filters.
- Make setup frequency, time-of-day concentration, outcomes, and excursion behavior easy to study.
- Preserve transparent links from aggregate evidence back to the applicable Replay session.
- Disclose incomplete coverage and unavailable measurements.

**Non-Goals:**

- Changing setup generation, scoring, targets, alerts, or Live/Replay lifecycle semantics.
- Calculating options P&L, fills, slippage, contract returns, or trading recommendations.
- Loading every historical candle into the browser or adding an external analytics platform.

## Decisions

### 1. Store normalized setup facts in the existing application database

Add a repository-backed `market_pulse_setup_events` table keyed by `setup_event_id`. Store ticker,
session date, signal/resolution timestamps, family, direction, pattern, anchor and target facts,
score/grade, outcome, MFE, MAE, target progress, and data-quality flags. Structured evidence that
does not need filtering remains compact JSON.

The Live/Replay evaluator remains authoritative. Persistence performs idempotent upserts and freezes
signal facts after first insertion; only lifecycle and outcome fields may advance monotonically.
Indexes cover ticker/session date, signal time, family, pattern, direction, and outcome.

Alternative considered: scan the per-ticker JSON ledger for every request. Rejected because it does
not provide bounded filtering, pagination, indexes, or clean long-term schema evolution. A separate
analytics database is also rejected because the existing application database is sufficient.

### 2. Write through during normal event evaluation and backfill conservatively

Each stable Live/Replay event is upserted into the analytics repository when its canonical outcome
is available. An idempotent one-time backfill imports stable records already present in the durable
ledger. Missing historical facts remain unavailable; the migration does not reconstruct old Gamma
targets from current levels.

Alternative considered: recompute all history on every page load. Rejected because it is expensive
and can introduce point-in-time drift.

### 3. Use one filtered server-side analytics response

A read-only endpoint accepts bounded date range, session-time range, family, pattern, direction,
level key, grade, outcome, sort, and page parameters. One normalized filter object drives headline
metrics, chart series, filter options, data-quality counts, and the paginated ledger so panels cannot
silently describe different populations.

Default range is the latest 20 market sessions. Requests are capped at 180 calendar days, page size
defaults to 50, and page size is capped at 200. Aggregation occurs in SQL/service code; raw candles
are never returned. Small responses may be cached by normalized filter key and invalidated when an
event revision advances.

### 4. Define metrics without overstating the evidence

`total_setups` counts stable event ids. `setups_per_session` uses distinct covered sessions.
Target-reached and invalidation rates use terminal, outcome-available events as the denominator;
open and unavailable events are reported separately. MFE, MAE, and progress medians exclude nulls
and include their sample sizes. No metric is labeled win rate or profit.

All signal and resolution timestamps are normalized to `America/New_York`. The time chart uses
30-minute buckets beginning at the regular-session open and labels the bucket size.

### 5. Keep the trading workspace compact

Add a small `Analyze setups` link in Setup Replay. The new route renders a dedicated responsive page
with KPI cards, four charts, filters, a data-quality note, and a paginated ledger. Selecting a ledger
row expands frozen facts and provides a Replay link scoped to its `session_date`; it does not place
execution controls on the analytics page.

Reuse the application's existing charting and design patterns instead of adding a dependency.

### 6. Fail closed and visibly

Invalid filter values return a structured 400 response. Database or aggregation failures show a
retryable unavailable state rather than zero-filled charts. Empty valid results show `No setups in
this range`. Authentication and local-only access follow existing Market Pulse routes.

### 7. Present the evidence as a research cockpit

Lead with an evidence-qualified best-time-window insight and an outcome-coverage indicator. Make a
30-minute timing heatmap the visual centerpiece, followed by compact setup-family comparison cards,
session occurrence context, excursion distribution, and the auditable ledger. Keep the filter form
inside a native collapsible drawer and expose Setup Analytics from the desktop Tools menu and mobile
navigation.

Frequency views include every retained stable event. Outcome rates, favorable/adverse excursion,
and ranked insights use only records whose outcomes were captured. Legacy rows remain visible in
the ledger with an explicit `Legacy · outcome not captured` label. Insights with fewer than three
evaluated observations are labeled early evidence rather than presented as conclusive.

### 8. Use a tabbed terminal workspace instead of one long report

Combine the page title, best-window evidence, coverage indicator, and Replay navigation into one
compact command header. Render the six headline metrics as a single horizontal ribbon. Place deeper
content behind four client-side tabs: `Overview`, `Timing`, `Families`, and `Ledger`; tab changes do
not refetch or alter the active filters.

Overview shows a compact session-timing preview and the four most useful family comparisons. Timing
shows the complete horizontal regular-session timeline and session-frequency chart. Families shows
all comparison cards and excursion evidence. Ledger replaces the wide table with concise expandable
event rows. Tabs use button semantics, selected-state attributes, keyboard focus, and responsive
horizontal scrolling. Motion is subtle and disabled when reduced motion is requested.

### 9. Prefer readable chart geometry over dense cards

Never ellipsize the page title, panel titles, setup-family names, or chart labels. Allow headings to
wrap and allocate more command-header width to identity text. Replace the time-bucket card strip
with a frequency profile: bar height represents total occurrences, bar color represents captured
target rate, and every bucket shows its complete time label and evaluated sample size. Render the
full Timing profile across the panel width rather than beside another chart.

The session chart uses short, complete month/day axis labels with full dates in accessible titles,
horizontal grid lines, and sufficient bottom padding. Family preview entries use a single-column
ranked layout so names are never cramped; the full Families view may use wider multi-column cards.

### 10. Spell out analytical meaning at the point of use

Family comparisons do not present bare `Coverage`, `MFE`, or `MAE` abbreviations. They show
`Outcomes captured: X of Y`, `Median favorable move: Z SPX pts`, and `Median adverse move: Z SPX
pts`, including the applicable sample size when it differs from captured outcomes. Target rate is
paired with `X of Y evaluated setups reached target` so a percentage cannot be mistaken for all
historical occurrences.

### 11. Separate estimated opportunity from realized P&L

The Families view includes a compact legend defining MFE as the most favorable SPX movement after
the signal and MAE as the most adverse SPX movement after the signal. A potential-profit estimate
may convert median MFE with the same planning model used by Replay: `$750` estimated contract cost,
`|delta| 0.40`, and a `100` multiplier. The display states both the estimated dollars per contract
and percentage of the assumed premium, labels the result as an estimate, and notes that IV, theta,
gamma, spread, slippage, and fills are excluded. The planned 15–20% take-profit benchmark is shown
as `$112.50–$150` per `$750` contract for context.

The Families view uses the full content width. Family cards render two per row on desktop with one
vertical metric list rather than three compressed mini-cards. The excursion chart sits below the
cards and its legend is visible without relying on hover.

## Risks / Trade-offs

- [Historical ledger records may lack complete excursion data] → Preserve nulls, show coverage
  counts, and never coerce unavailable values to zero.
- [Analytics rows could drift from Live/Replay] → Share stable event ids and upsert mapping, freeze
  signal facts, enforce terminal monotonicity, and add parity fixtures.
- [Large ranges could slow the page] → Use indexed columns, bounded ranges, server-side aggregates,
  pagination, and revision-aware caching.
- [Rates may be misread as trade profitability] → Use outcome-specific labels and explicitly state
  that estimated opportunity is not realized options P&L.
- [Legacy records could dominate or flatten the page] → Retain them for occurrence counts while
  excluding unavailable outcomes from performance calculations and styling them as incomplete.
- [Tabbed content could hide important caveats] → Keep coverage and interpretation boundaries in
  the command header/ribbon and preserve filter state across every tab.
- [Chart labels could be clipped by fixed SVG/card geometry] → Size charts from complete labels,
  keep overflow inside the chart region, and verify title/axis visibility at desktop and narrow widths.
- [Schema migration could fail] → Create the table additively, leave Live/Replay operational if the
  analytics repository is unavailable, and expose analytics as unavailable.

## Migration Plan

1. Add the table and indexes through the existing database initialization/migration path.
2. Add the repository and canonical event-to-analytics mapping with focused parity tests.
3. Backfill stable durable-ledger events idempotently without altering source records.
4. Add the bounded endpoint, page, charts, ledger, and Replay navigation link.
5. Rebuild locally, verify health, compare sampled analytics rows with Live/Replay, and inspect the
   deployed page at desktop and narrow widths.

Rollback removes the route, page link, and write-through call. The additive analytics table may
remain dormant without affecting Live Setup or Replay.

## Open Questions

None required for the first version. Export, custom bucket sizes, and options-return tracking can be
evaluated later from actual usage and data coverage.
