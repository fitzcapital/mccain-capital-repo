## ADDED Requirements

### Requirement: Required feeds determine execution trust
The system SHALL classify top-level execution trust from required feeds and SHALL present optional stale or event-driven feeds as advisories without degrading an otherwise healthy required-data state.

#### Scenario: Optional feed is stale
- **WHEN** Spot, completed Bars, and Gamma are current but an optional feed is stale
- **THEN** the top-level trust verdict remains healthy and the optional feed shows its advisory state

#### Scenario: Required feed is stale
- **WHEN** any required execution feed is stale or unavailable
- **THEN** the top-level trust verdict communicates degraded or locked execution trust

### Requirement: Live timestamps identify their meaning
The system SHALL distinguish the last completed five-minute candle from the live forming bar and SHALL display Fast Tape event times in New York time.

#### Scenario: Completed and forming timestamps differ
- **WHEN** a live bar is newer than the last completed candle
- **THEN** both timestamps are labeled by their distinct meanings without implying inconsistency

#### Scenario: Tape event contains a UTC timestamp
- **WHEN** a Fast Tape event is rendered
- **THEN** its visible time is converted to New York time

### Requirement: Setup guidance is concise and unambiguous
The system SHALL use readable monitor countdowns, correctly formatted 2-2 labels, distinguish checklist evidence from the setup result, and provide explicit Gamma confirmation and failure language.

#### Scenario: Monitor is actively checking
- **WHEN** the next evaluation is due now
- **THEN** the monitor displays `Checking...` rather than `Next check 0s`

#### Scenario: Checklist result is displayed
- **WHEN** five evidence inputs are evaluated
- **THEN** the sixth card is labeled as the result and is not counted as a sixth evidence input

#### Scenario: Bearish Gamma confirmation is shown
- **WHEN** the bearish confirmation level is available
- **THEN** guidance says to close below and then hold below the level, with failure expressed as a reclaim above it
