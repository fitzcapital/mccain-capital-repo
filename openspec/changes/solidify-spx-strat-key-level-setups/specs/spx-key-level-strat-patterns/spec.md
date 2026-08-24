## ADDED Requirements

### Requirement: Exact completed-bar classification
The system SHALL classify each completed SPX five-minute candle relative to its immediate completed
predecessor as inside `1`, directional `2U`, directional `2D`, or outside `3`. An outside candle MUST
NOT also qualify as a directional 2.

#### Scenario: Inside candle
- **WHEN** a completed candle's high does not exceed the prior high and its low does not break the
  prior low
- **THEN** it is classified as `1`

#### Scenario: Outside candle lookalike
- **WHEN** a completed candle breaks both the prior high and prior low
- **THEN** it is classified as `3` and cannot complete a 2-1-2 or 2-2 setup

### Requirement: Supported five-minute setup families
The system SHALL recognize only 2-1-2 Up, 2-1-2 Down, bullish 2-2 Reversal, and bearish 2-2
Reversal as execution-eligible Strat patterns.

#### Scenario: Bullish 2-1-2 completes
- **WHEN** completed classifications form directional `2`, inside `1`, then `2U`
- **THEN** the pattern completes as `2-1-2U` on the final completed candle

#### Scenario: Bearish 2-2 Reversal completes
- **WHEN** completed classifications form `2U` then `2D`
- **THEN** the pattern completes as bearish `2-2 REV` on the second completed directional candle

#### Scenario: Unsupported sequence
- **WHEN** completed candles form 3-1-2 or a generic same-direction break without the required
  sequence
- **THEN** no execution-eligible Strat pattern is emitted

### Requirement: Canonical key-level anchoring
An eligible setup SHALL identify one canonical Market Pulse level, and at least one constituent
pattern candle MUST span that level. Distance to a level without pattern interaction MUST NOT count
as anchoring.

#### Scenario: 2-1-2 forms at Current-Day High
- **WHEN** a valid bearish 2-1-2 pattern includes a candle whose range spans canonical CDH
- **THEN** the setup is anchored to CDH with the level value and constituent bar timestamps

#### Scenario: Mid-range pattern
- **WHEN** a valid candle sequence forms but none of its constituent candles spans a canonical level
- **THEN** it remains non-actionable diagnostic evidence

### Requirement: Safe evidence provenance
Every eligible pattern SHALL expose its code, family, direction, completion timestamp, constituent
bar timestamps, anchor level, timeframe, and completed-candle provenance. Missing or stale required
inputs MUST produce no eligible pattern.

#### Scenario: Incomplete live candle resembles a trigger
- **WHEN** the active five-minute candle would complete a supported sequence but has not closed
- **THEN** the system emits no eligible setup until that candle is completed and canonical
### Requirement: Trigger-candle execution confirmation

The system SHALL treat an exact completed five-minute 2-1-2 or opposing 2-2 reversal as an armed
trigger, not an entry by itself. Entry confirmation SHALL require a later completed candle to break
the trigger candle in the pattern direction. Replay SHALL preserve armed-but-untriggered patterns as
diagnostics and SHALL NOT count them as completed setup opportunities.

#### Scenario: Exact pattern arms before entry
- **WHEN** a supported pattern completes at an eligible key-level event
- **THEN** the setup is armed without being counted as an entry
- **AND** a later completed candle must break the trigger candle in the pattern direction

### Requirement: Intraday replay entry cutoff
Setup Replay SHALL restrict historical entry confirmations to the user's execution window ending at
3:30 PM America/New_York. Later completed bars MAY remain available to measure the outcome of an
earlier qualified setup, but MUST NOT create a new replay setup.

#### Scenario: Trigger confirms after the cutoff
- **WHEN** an otherwise qualified setup confirms after 3:30 PM ET
- **THEN** Setup Replay excludes it from setup count, markers, ranking, and latest-signal time

#### Scenario: Trigger confirms at the cutoff
- **WHEN** an otherwise qualified setup confirms exactly at 3:30 PM ET
- **THEN** it remains eligible under the normal setup rules
