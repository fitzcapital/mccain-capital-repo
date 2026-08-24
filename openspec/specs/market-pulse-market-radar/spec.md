# Market Pulse Market Radar

## Purpose

Define the decision-first instrument radar used by Market Pulse for index context, ranked stock leadership, and refresh-safe session positioning.

## Requirements

### Requirement: Deduplicated instrument universe
The Market Radar MUST render each normalized symbol at most once per refresh.

#### Scenario: Duplicate symbols enter the view-model
- **WHEN** the selected instrument also exists in the default tape universe
- **THEN** the rendered radar contains one card for that normalized symbol

### Requirement: Index Pulse separation
The Market Radar SHALL separate SPX, SPY, QQQ, IWM, and VIX from individual stock opportunities.

#### Scenario: Index instruments are available
- **WHEN** one or more index-universe instruments have tape data
- **THEN** they render in an Index Pulse group before the ranked watchlist
- **AND** they are not repeated in the individual-stock group

### Requirement: Ranked leaders and laggards
The Market Radar SHALL rank non-index symbols by directional conviction and selected-timeframe percentage magnitude using a deterministic tie-breaker.

#### Scenario: Strong and weak symbols exist
- **WHEN** the watchlist includes positive and negative directional candidates
- **THEN** leader and laggard summaries identify the highest-ranked candidates
- **AND** each ranked watchlist card exposes its leadership rank

#### Scenario: Only mixed symbols exist
- **WHEN** no confirmed strong or weak stock candidate exists
- **THEN** the summaries report the best available mixed candidates without inventing confirmation

### Requirement: Actionable card hierarchy
Each Market Radar card SHALL prioritize symbol/state, price, percent move, selected-timeframe chart, range position, rank, and freshness.

#### Scenario: Complete price range is available
- **WHEN** price, session low, and session high are valid and the range is nonzero
- **THEN** the card renders a clamped relative range-position indicator

#### Scenario: Range inputs are unavailable
- **WHEN** range position cannot be calculated
- **THEN** the card reports unavailable position without a fabricated value

### Requirement: Consistent semantic color
The Market Radar SHALL use teal for confirmed strength, coral for confirmed weakness, neutral blue-gray for mixed state, amber for caution, and gray for stale or unavailable state.

#### Scenario: Card state changes after refresh
- **WHEN** refreshed data changes conviction or freshness
- **THEN** the card's semantic tone updates with the data without requiring a full-page reload

### Requirement: Responsive and compatible refresh
The Market Radar SHALL reflow based on available width and SHALL preserve existing tape refresh, timeframe, chart, and symbol-control behavior.

#### Scenario: Grid contains an incomplete final row
- **WHEN** the number of cards does not evenly fill the current column count
- **THEN** remaining cards use the responsive grid without reserved empty slots

#### Scenario: Tape refresh completes
- **WHEN** fresh tape data is applied
- **THEN** cards, summaries, ranks, tones, and freshness update without a full-page reload
