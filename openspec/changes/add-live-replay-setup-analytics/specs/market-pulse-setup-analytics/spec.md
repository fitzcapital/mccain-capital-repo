## ADDED Requirements

### Requirement: Canonical setup analytics history
The system SHALL retain one analytics record per stable Live/Replay setup event in the existing
application database. Signal-time family, direction, pattern, level, target, entry, score, and grade
facts MUST remain frozen, while lifecycle and outcome fields SHALL advance monotonically from the
same completed-candle evaluation used by Live and Replay.

#### Scenario: Stable event is recorded once
- **WHEN** Live and Replay encounter the same stable setup event more than once
- **THEN** one analytics row exists for its event id without duplicate occurrence counts

#### Scenario: Terminal outcome advances
- **WHEN** a previously open event reaches its target or invalidates on a later completed candle
- **THEN** its analytics lifecycle, resolution time, and excursion fields advance to that terminal outcome without changing frozen signal facts

#### Scenario: Existing history is imported
- **WHEN** the additive analytics store is first enabled with stable events in the durable Live ledger
- **THEN** those events are imported idempotently and missing historical measurements remain unavailable rather than being reconstructed from current levels

### Requirement: Consistent bounded filtering
The analytics API SHALL apply one normalized filter population to all metrics, charts, data-quality
counts, filter options, and ledger results. It SHALL support ticker, session-date range,
New-York-time range, family, pattern, direction, level, grade, and outcome filters.

#### Scenario: Filter changes every panel
- **WHEN** the trader selects a bearish `2-2 REV D` family and a morning time range
- **THEN** every returned metric, chart series, quality count, and ledger row describes only that population

#### Scenario: Range and pagination are bounded
- **WHEN** a request exceeds the maximum date range or page-size limit
- **THEN** the endpoint rejects or clamps it according to the published contract without loading unbounded event or candle history

#### Scenario: Filter is invalid
- **WHEN** a date, time, outcome, sort, or pagination value is malformed
- **THEN** the endpoint returns a structured validation error and no misleading aggregate payload

### Requirement: Honest setup-study metrics
The analytics page SHALL report total setups, setups per covered session, target-reached rate,
invalidation rate, open/unresolved count, median MFE, median MAE, and median target progress. Rates
MUST use terminal outcome-available events as their denominator, null excursion values MUST be
excluded, and each partial-sample metric SHALL disclose its sample size.

#### Scenario: Open events do not dilute terminal rates
- **WHEN** the filtered population contains target-reached, invalidated, and open events
- **THEN** target and invalidation rates use only terminal outcome-available events while open events appear as a separate count

#### Scenario: Excursion is unavailable
- **WHEN** some matching events have no MFE, MAE, or target-progress measurement
- **THEN** those events are excluded from the applicable median and the page shows the smaller sample size instead of substituting zero

#### Scenario: No matching setups
- **WHEN** valid filters match no setup events
- **THEN** the page shows an explicit empty state rather than zero-filled performance claims

### Requirement: Time and outcome visualizations
The analytics page SHALL visualize occurrence trend by session, occurrences and outcome mix by
30-minute regular-session bucket, outcome mix by setup family, and MFE-versus-MAE distribution.
Time-of-day calculations MUST use `America/New_York` and the page MUST label the bucket size.

#### Scenario: Setup is assigned to a time bucket
- **WHEN** a setup signal occurs at 10:44 AM New York time
- **THEN** it is counted once in the labeled 10:30–10:59 AM bucket across the chart and filtered ledger

#### Scenario: Daylight-saving boundary is crossed
- **WHEN** the selected date range spans a daylight-saving transition
- **THEN** session-date and time buckets remain based on New York market time rather than server or UTC wall time

#### Scenario: Chart data is unavailable
- **WHEN** the analytics query fails
- **THEN** charts show a retryable unavailable state and do not render empty series as valid evidence

### Requirement: Auditable setup ledger and Replay drill-down
The page SHALL provide a sortable, paginated ledger containing stable event id, signal and
resolution timestamps, family, pattern, direction, anchor, entry, target, grade, outcome, MFE, MAE,
and target progress. A row SHALL expand to show frozen evidence and SHALL link to Setup Replay for
the same session date.

#### Scenario: Trader inspects an aggregate observation
- **WHEN** the trader expands a ledger row and chooses its Replay link
- **THEN** Setup Replay opens for that row's session date and identifies the same stable event facts

#### Scenario: Outcome remains unresolved
- **WHEN** an event is still open at the evaluated-through boundary
- **THEN** the ledger labels it open with no fabricated resolution timestamp

### Requirement: Visible coverage and interpretation limits
The page SHALL disclose covered session count, earliest and latest retained signal times, evaluated
through time, unavailable outcome count, and missing excursion count. It MUST state that setup
outcomes measure underlying SPX movement and do not represent option fills, contract returns, or
realized profitability.

#### Scenario: History is incomplete
- **WHEN** retained events begin after the selected range or contain missing measurements
- **THEN** the page identifies the incomplete coverage and does not present the dataset as exhaustive

#### Scenario: Trader reviews performance language
- **WHEN** analytics results are displayed
- **THEN** no metric is labeled win rate or profit unless actual trade and option-fill data is introduced under a separate approved capability

### Requirement: Research cockpit hierarchy
The page SHALL lead with an evidence-qualified best time window, prominently show outcome coverage,
make the 30-minute timing heatmap its primary visualization, and provide compact setup-family
comparisons. Filters SHALL be collapsible, and Setup Analytics SHALL be reachable from Market Pulse
navigation in addition to the Replay link.

#### Scenario: Trader opens the analytics page
- **WHEN** filtered setup history is available
- **THEN** the page presents the strongest supported time window, its evaluated sample size, the outcome-coverage ratio, the timing heatmap, and family comparisons before the detailed ledger

#### Scenario: Evidence sample is small
- **WHEN** the strongest time bucket or setup family has fewer than three evaluated outcomes
- **THEN** the insight is labeled early evidence and is not presented as a conclusive trading recommendation

### Requirement: Separate occurrence evidence from performance evidence
Occurrence counts SHALL include retained stable events with unavailable historical outcomes.
Performance rates, ranked insights, and excursion comparisons MUST exclude unavailable outcomes by
default. The ledger SHALL retain those rows and label them as legacy records whose outcomes were not
captured.

#### Scenario: Legacy records exist in a time bucket
- **WHEN** a bucket contains setups without captured outcomes
- **THEN** its total occurrence count includes them while its target rate and evaluated sample use only outcome-available terminal records

#### Scenario: Legacy row is inspected
- **WHEN** the trader views a setup whose historical outcome was unavailable
- **THEN** the ledger labels it `Legacy · outcome not captured` and does not display unavailable measurements as zero

### Requirement: Compact terminal-style navigation
The page SHALL avoid presenting every analytic surface as one continuously stacked report. It SHALL
provide accessible Overview, Timing, Families, and Ledger views, preserve active filters while
switching views, and keep coverage limitations visible from the default Overview.

#### Scenario: Trader opens the default view
- **WHEN** Setup Analytics finishes loading
- **THEN** Overview is selected and shows the command insight, metric ribbon, compact timing evidence, and no more than four family previews before deeper views

#### Scenario: Trader changes analytic view
- **WHEN** the trader selects Timing, Families, or Ledger
- **THEN** the matching view becomes visible without a network reload, active filters remain unchanged, and the selected tab is exposed to assistive technology

#### Scenario: Trader studies setup history
- **WHEN** the Ledger view is selected
- **THEN** setup events appear as compact expandable rows that retain the required frozen facts, outcome evidence, and Replay link without requiring a wide table

### Requirement: Complete labels and chart-first presentation
The page SHALL render the complete page title, panel titles, setup-family names, time buckets, and
session-axis labels without ellipsis or container clipping at supported desktop and narrow widths.
Timing and session visualizations SHALL use chart geometry that communicates frequency and outcome
context while preserving complete labels.

#### Scenario: Trader studies timing on desktop
- **WHEN** the Timing view is rendered with every regular-session bucket
- **THEN** the frequency profile uses the full panel width, every time label remains readable, and no bucket is clipped by an adjacent panel

#### Scenario: Trader compares sessions
- **WHEN** the session-frequency chart is rendered
- **THEN** every bar has a complete month/day label, its full date remains available as detail, and labels stay inside the chart bounds

#### Scenario: Long title or family name is rendered
- **WHEN** text exceeds one line in its allocated region
- **THEN** it wraps into available space instead of being ellipsized or cut off

### Requirement: Plain-language analytical labels
Family comparisons SHALL explain denominators, sample sizes, and SPX-point units without requiring
the trader to infer the meaning of `Coverage`, `MFE`, or `MAE` abbreviations.

#### Scenario: Family has partially captured outcomes
- **WHEN** three outcomes are captured among seven retained setups and their median MFE and MAE are 13.5 and 2.4
- **THEN** the page states `Outcomes captured: 3 of 7`, `Median favorable move: 13.5 SPX pts`, and `Median adverse move: 2.4 SPX pts`

#### Scenario: Family target rate is displayed
- **WHEN** two of three evaluated setups reached target
- **THEN** the page pairs `66.7% reached target` with `2 of 3 evaluated setups`

### Requirement: Explained excursion and potential-profit estimates
The Families view SHALL define MFE and MAE in plain language and MAY show a potential-profit
estimate derived from median favorable SPX movement only when the contract-cost, absolute-delta,
and multiplier assumptions are visible. Estimated opportunity MUST remain distinct from actual
fills, option Greeks after entry, and realized profit.

#### Scenario: Trader reviews MFE and MAE
- **WHEN** the Families view is displayed
- **THEN** a visible legend explains MFE as the most favorable SPX move after signal and MAE as the most adverse SPX move after signal

#### Scenario: Potential profit is estimated
- **WHEN** a family has a median favorable move of 13.5 SPX points
- **THEN** the page may show the estimate calculated with a `$750` contract, `0.40` absolute delta, and `100` multiplier and labels it as an estimate excluding IV, theta, gamma, spread, slippage, and fills

#### Scenario: Planned scalp benchmark is displayed
- **WHEN** profit-estimate assumptions are shown
- **THEN** the page states that a 15–20% take-profit plan equals approximately `$112.50–$150` on the assumed `$750` contract

### Requirement: Spacious family comparison layout
The Families view SHALL use the available page width and SHALL NOT compress multi-line analytical
labels into narrow side-by-side statistic boxes.

#### Scenario: Families render on desktop
- **WHEN** the Families view is displayed at a supported desktop width
- **THEN** family cards use no more than two columns, their metrics read vertically, and the excursion chart appears at full width below them
