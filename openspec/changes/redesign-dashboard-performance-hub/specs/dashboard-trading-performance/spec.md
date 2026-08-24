## ADDED Requirements

### Requirement: Monthly performance is the primary Dashboard read
The Dashboard SHALL lead with month-to-date realized net P&L, monthly goal, goal completion,
remaining amount, remaining trading days, required daily pace, and projected month-end result.

#### Scenario: Complete monthly inputs
- **WHEN** realized P&L, goal, calendar period, and trading-day inputs are available
- **THEN** the performance hero presents all monthly measures in one scan-first region
- **AND** identifies the period and authoritative source basis

#### Scenario: Required input is unavailable
- **WHEN** a required goal or projection input is missing
- **THEN** the affected measure reports unavailable without inventing a value
- **AND** unaffected authoritative measures remain visible

### Requirement: Trading quality statistics are supporting evidence
The Dashboard SHALL present trade count, win rate, profit factor, expectancy, average winner,
average loser, drawdown, current streak, and best/worst day beneath monthly performance.

#### Scenario: Statistics are available
- **WHEN** qualifying trades exist in the selected monthly period
- **THEN** each statistic uses that period and the existing authoritative trade source

#### Scenario: No qualifying trades exist
- **WHEN** the selected monthly period has no qualifying trades
- **THEN** the statistics region reports insufficient activity rather than zero-quality claims

### Requirement: Performance trend prioritizes equity and daily results
The Dashboard SHALL present an equity curve and daily P&L calendar ahead of extended market and
operational detail.

#### Scenario: Trend data is available
- **WHEN** daily or trade-level results exist
- **THEN** the equity trend and daily results use consistent selected-period boundaries

#### Scenario: Partial trend data is available
- **WHEN** equity or calendar history is incomplete
- **THEN** available observations render with a visible partial-data state

### Requirement: Recent performance supports diagnosis
The Dashboard SHALL provide recent sessions and breakdowns by setup, ticker, and time of day through
compact summaries or accessible disclosures.

#### Scenario: User investigates performance
- **WHEN** the user opens a breakdown
- **THEN** the selected-period results become available without a full-page reload

### Requirement: Risk status remains visible without dominating earnings
The Dashboard SHALL expose account equity, remaining drawdown, daily loss usage, and goal-pace state
as compact risk evidence adjacent to performance.

#### Scenario: Risk input is unavailable
- **WHEN** a broker risk input is missing or stale
- **THEN** the affected risk field reports unavailable and does not imply remaining capacity
