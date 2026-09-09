## Why

Live Setup and Setup Replay now agree on stable setup events, but the trader still has to inspect
individual replay cards to study frequency, timing, and follow-through. A dedicated analytics
surface is needed to turn that event history into measurable evidence without crowding the live
execution page.

## What Changes

- Add a separate Setup Analytics page linked directly from the Setup Replay section.
- Build a durable, queryable historical setup dataset from the same stable Live/Replay event
  identities and frozen signal-time facts.
- Add date-range, session-time, setup-family, Strat-pattern, direction, level, grade, and outcome
  filters.
- Show concise headline metrics: total setups, setups per session, target-reached rate,
  invalidation rate, open/unresolved count, median MFE, median MAE, and median target progress.
- Add charts for occurrences over time, occurrences and outcomes by 30-minute session bucket,
  outcome mix by setup family, and MFE-versus-MAE distribution.
- Add a sortable, paginated setup ledger with signal, resolution, target, and excursion timestamps,
  plus a link back to the applicable Replay session.
- Refine the page into a research cockpit with a prominent evidence-qualified insight, a
  time-of-day heatmap, compact KPI and coverage strip, family comparison cards, collapsible
  filters, and a cleaner expandable ledger.
- Keep incomplete legacy records available for occurrence-frequency study while excluding them
  from outcome-performance rates and visually separating them from evaluated setups.
- Add Setup Analytics to the Market Pulse navigation paths so the page is discoverable outside
  Setup Replay.
- Modernize the cockpit into a compact terminal-style workspace with a combined command header,
  metric ribbon, Overview/Timing/Families/Ledger tabs, a horizontal session timeline, ranked family
  previews, and expandable setup rows instead of a continuously stacked report.
- Guarantee that headings, setup-family names, timestamps, and chart-axis labels remain fully
  readable at supported widths, and use the available page width for chart-first visualizations
  instead of clipped cards or truncated labels.
- Replace unexplained analytics shorthand with plain-language denominators and units, including
  captured-outcome counts and favorable/adverse movement measured in SPX points.
- Add a clearly labeled potential-profit estimate that converts median favorable SPX movement
  using the Replay planning assumptions, while keeping actual fills, Greeks, and realized P&L out
  of scope.
- Show explicit data coverage and quality states so missing candles, unavailable targets, and
  incomplete sessions are not treated as valid zeroes.
- Keep analytics read-only and separate from live alerts, ranking, or execution guidance.

Non-goals:

- Do not present estimated option opportunity as realized P&L, actual contract returns, fills,
  slippage, or win rate.
- Do not alter setup detection, scoring, target selection, lifecycle rules, or alert eligibility.
- Do not introduce an external analytics service, a second database, or public hosting.
- Do not add predictive recommendations until enough complete historical observations exist.

Acceptance criteria:

- Every analytics row has the same stable event id, family, target, signal time, and terminal
  outcome as Live/Replay for the same session.
- Changing any filter updates all metrics, charts, and the ledger from one consistent result set.
- Time-of-day analysis uses America/New_York session time and clearly labels its bucket size.
- Open, unavailable, and incomplete outcomes remain separate from target-reached and invalidated.
- The analytics endpoint remains bounded through server-side aggregation, pagination, and a
  reasonable date-range limit; the page does not load raw candles for every session.
- The Market Pulse page links to analytics without adding dense charts to the execution workspace.
- The analytics page leads with time-window and family evidence, clearly labels low-sample
  insights, and keeps filter controls collapsed until needed.
- The default Overview fits the primary decision evidence within a practical desktop viewport,
  while deeper timing, family, and ledger content remains one click away and keyboard accessible.
- Frequency charts show complete time/date labels, all regular-session buckets fit or scroll within
  their own chart region, and no title is ellipsized when wrapping space is available.
- Family comparisons state the target result as a count, outcome coverage as `captured of total`,
  and MFE/MAE as median favorable/adverse SPX movement with their own sample sizes.
- The Families view defines MFE and MAE, discloses the contract-cost and delta assumptions behind
  potential profit, and uses a spacious layout without compressed stat boxes.

## Capabilities

### New Capabilities

- `market-pulse-setup-analytics`: Historical Live/Replay event retention, filtered aggregation,
  time-of-day and outcome analysis, data-quality disclosure, and drill-down presentation.

### Modified Capabilities

- `market-pulse-scenario-ranking`: Require the existing Setup Replay surface to expose a clear
  navigation path to historical setup analytics while preserving its execution-review role.

## Impact

- Affected data: durable SPX setup-event records and their frozen signal facts, forward outcomes,
  timestamps, MFE, MAE, and target progress; estimated profit uses disclosed planning assumptions
  and never claims brokerage fills.
- Affected backend: Market Pulse services, a bounded analytics query/aggregation service, handlers,
  routes, and read-only JSON endpoints.
- Affected UI: a new Jinja page with focused JavaScript/CSS and a compact Setup Replay link.
- Affected tests: event parity, historical retention, filter consistency, session-time bucketing,
  unavailable-data handling, pagination, endpoint contracts, and deployed receiving-page checks.
- Dependencies: reuse the existing Flask, charting, persistence, and Market Pulse patterns; no new
  external dependency is required.
