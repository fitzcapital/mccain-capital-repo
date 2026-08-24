## ADDED Requirements

### Requirement: Completed-candle evidence only
The system SHALL derive failed-liquidity-sweep evidence only from valid, completed candles in the
required timeframe. An active or unfinished candle MUST NOT confirm a sweep close, Strat reversal,
trigger break, acceptance, retest, or runner state.

#### Scenario: Required five-minute candle has closed
- **WHEN** a valid five-minute candle completes and its OHLC relationship proves an evidence step
- **THEN** that step is emitted with confirmed status, candle timestamp, timeframe, and provenance

#### Scenario: Current candle is unfinished
- **WHEN** the current five-minute or fifteen-minute candle has not completed
- **THEN** its price action remains Pending and does not advance the strategy state

### Requirement: Ordered failed-sweep derivation
The extractor SHALL evaluate location, trade beyond the active level, completed close back inside,
five-minute Strat 2-2 reversal, and trigger break in chronological order. Later evidence MUST NOT
stand in for a missing earlier step.

#### Scenario: Bearish failed sweep completes in order
- **WHEN** price sweeps above resistance, closes back below, completes a bearish 2-2, and breaks the
  trigger in chronological order
- **THEN** the derived evidence advances the failed-sweep evaluator to bearish reversal ready

#### Scenario: Trigger break predates the reversal candle
- **WHEN** a price break exists before the qualifying 2-2 reversal completes
- **THEN** it does not count as the ordered trigger break

#### Scenario: Bullish failed sweep completes in order
- **WHEN** price sweeps below support, closes back above, completes a bullish 2-2, and breaks the
  trigger in chronological order
- **THEN** the derived evidence advances the failed-sweep evaluator to bullish reversal ready

### Requirement: Acceptance path remains exclusive
The extractor SHALL derive acceptance/continuation evidence separately from failed-sweep rejection.
Confirmed acceptance and reversal-ready rejection MUST remain mutually exclusive for one active
interaction.

#### Scenario: Price accepts beyond the active level
- **WHEN** completed candles close and hold beyond the level and a valid retest confirms control
- **THEN** the continuation path becomes active and reversal evidence cannot produce a fade signal

#### Scenario: Neither path is proven
- **WHEN** price interacts with the level but completed candles prove neither rejection nor acceptance
- **THEN** both terminal paths remain unconfirmed and the state names the earliest missing evidence

### Requirement: Evidence compatibility and reset
Derived evidence SHALL be scoped to ticker, session, active level identity and value, approach
direction, and timeframe. Evidence MUST reset when those identifiers change incompatibly and MUST
not cross a session boundary.

#### Scenario: Active level changes
- **WHEN** the selected active level or its material value changes after evidence was collected
- **THEN** incompatible evidence is discarded and the new interaction begins from its valid state

#### Scenario: New market session begins
- **WHEN** bars belong to a session after the evidence session
- **THEN** prior-session confirmation does not advance the new session setup

#### Scenario: Bars are malformed or stale
- **WHEN** OHLC values, timestamps, ordering, or required timeframe data are invalid or stale
- **THEN** the affected evidence is Unavailable and no ready signal is fabricated

### Requirement: Optional fifteen-minute runner evidence
The system SHALL derive fifteen-minute 2-2 evidence only as optional runner support after a valid
five-minute path has gained control. It MUST NOT substitute for the required five-minute sequence.

#### Scenario: Fifteen-minute continuation confirms
- **WHEN** a completed fifteen-minute 2-2 aligns after a valid five-minute trigger
- **THEN** runner support is confirmed with timestamp and provenance
