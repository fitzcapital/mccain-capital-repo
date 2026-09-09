## ADDED Requirements

### Requirement: Family occurrence context
The system SHALL identify actual recorded occurrences within family summaries rather than assign
one price to a multi-setup aggregate.

#### Scenario: Trader inspects a family
- **WHEN** a family card is displayed
- **THEN** it SHALL show the latest signal time and recorded level price
- **WHEN** its occurrence details are expanded
- **THEN** up to five latest occurrences SHALL show signal time, pattern, direction, level, entry,
  target, outcome and available resolution time; missing prices SHALL be dashes, not invented
- **AND** a limited preview SHALL be labeled as latest occurrences rather than all occurrences

### Requirement: Session-aware quick study horizons
The system SHALL provide Today, Last 3 Sessions, This Week, Last 20 Sessions, and All History quick
filters and SHALL apply the selected horizon consistently to every Setup Analytics result.

#### Scenario: Trader opens today
- **WHEN** the trader opens Setup Analytics without an explicit historical filter
- **THEN** Today SHALL be selected and every KPI, chart, card, and ledger row SHALL use the current
  New York exchange session date

#### Scenario: Trader selects last three sessions after a weekend
- **WHEN** the trader selects Last 3 Sessions on a Monday
- **THEN** the system SHALL include Monday and the two preceding exchange sessions without counting
  weekend or exchange-holiday dates as sessions

#### Scenario: Trader uses an exact custom range
- **WHEN** the trader supplies valid start and end dates
- **THEN** the system SHALL select Custom and use those exact dates until another preset is selected

#### Scenario: Trader selects all retained history
- **WHEN** the trader selects All History
- **THEN** the system SHALL include every retained setup for the ticker while preserving indexed
  pagination and the same filter contract

### Requirement: Explicit outcome evidence states
The system SHALL distinguish completed outcomes, open setups, and historical records whose outcomes
were not captured, and SHALL state the applicable denominator anywhere it displays an outcome rate.

#### Scenario: Bucket has setups but no terminal evaluations
- **WHEN** a chart bucket contains setups and its evaluated count is zero
- **THEN** the chart SHALL say “No completed outcomes” and SHALL still show the setup count

#### Scenario: Setup remains open
- **WHEN** a setup has not reached a terminal target, invalidation, or ambiguity state
- **THEN** the ledger SHALL label it “Awaiting resolution” rather than “No outcome”

#### Scenario: Historical record lacks evaluation evidence
- **WHEN** a retained setup does not contain post-signal evaluation evidence
- **THEN** the ledger SHALL label it “Historical outcome not captured” and exclude it from outcome
  rate denominators

### Requirement: Source-labeled near-the-money option premium anchor
The system SHALL use a current eligible Tradier SPX/SPXW near-the-money contract quote as the
premium and delta anchor for hypothetical option projections and SHALL disclose its source and
timing.

#### Scenario: Current eligible Tradier contract exists
- **WHEN** the options cache contains a current non-crossed directional contract with positive mid
  and usable delta at the nearest eligible strike to current SPX spot
- **THEN** the projection SHALL identify the illustrative contract, mid, premium, delta, spread,
  quote time, DTE, strike, spot distance, and Tradier source

#### Scenario: Premium preference conflicts with moneyness
- **WHEN** a farther strike is closer to the $750 planning premium than the nearest eligible strike
- **THEN** the system SHALL select the near-the-money contract and SHALL use premium and delta
  proximity only as secondary tie-breakers

#### Scenario: Tradier quote is stale or incomplete
- **WHEN** the options snapshot is stale, missing, crossed, lacks a positive mid, or lacks delta
- **THEN** analytics SHALL remain available and the projection SHALL use a clearly labeled $750 and
  0.40 absolute-delta fallback with a sanitized reason

#### Scenario: Selected evidence mixes directions
- **WHEN** an aggregate contains both bullish and bearish setups
- **THEN** the system SHALL not imply that one live call or put represents every setup and SHALL show
  direction-specific anchors or the fallback planning assumptions

### Requirement: Projection limitations remain visible
The system SHALL describe option projections as hypothetical planning estimates rather than fills,
realized profit, or historical contract returns.

#### Scenario: Trader reviews a historical family
- **WHEN** a projection appears beside historical setup statistics
- **THEN** the page SHALL state that MFE measures underlying SPX movement and that historical IV,
  theta, gamma path, spread, slippage, and fills are excluded

#### Scenario: Trader scans a family card
- **WHEN** a family has a calculable hypothetical option projection
- **THEN** the card SHALL prominently show estimated total trade dollars for the selected quantity, estimated percentage
  gain, and the median SPX move used, followed by “Illustrative, not realized P&L”

#### Scenario: Trader edits contract count
- **WHEN** the page opens
- **THEN** an editable contract count SHALL default to 3
- **WHEN** the trader enters a whole number from 1 through 1000
- **THEN** both family views SHALL update total gain and reference cost without a data request,
  preserve percentage return, and retain per-contract figures in expanded details
- **WHEN** input is invalid
- **THEN** the last valid estimate SHALL remain and a validation message SHALL be visible

#### Scenario: Trader requests calculation detail
- **WHEN** the trader expands “How this was estimated”
- **THEN** the card SHALL reveal the current or fallback premium anchor, formula inputs, contract,
  quote timing, and projection limitations without crowding the default card view

### Requirement: Tradier snapshot is consistent across web workers
The system SHALL make the sanitized options snapshot available to every web worker without storing
credentials or raw provider responses.

#### Scenario: Analytics is served by a different worker
- **WHEN** one worker refreshes Tradier options and another worker serves Setup Analytics
- **THEN** both workers SHALL read the same sanitized quote timestamp, SPX spot, and NTM candidates

#### Scenario: Shared and local caches are empty
- **WHEN** Setup Analytics loads before any worker has produced a snapshot
- **THEN** the system SHALL start the options refresh loop and permit one bounded recovery refresh
  while other workers reuse its result

#### Scenario: Tradier recovery fails
- **WHEN** the bounded refresh cannot obtain valid data
- **THEN** the last good shared snapshot SHALL remain intact and Analytics SHALL show the explicit
  fallback reason without failing the setup study
