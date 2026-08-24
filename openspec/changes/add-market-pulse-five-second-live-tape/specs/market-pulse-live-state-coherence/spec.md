## ADDED Requirements

### Requirement: Incomplete live movement is non-authoritative
The system SHALL treat the five-second live tape as observational context only and SHALL retain completed five-minute candles as the authoritative input for setup confirmation, replay outcomes, invalidation, and execution state.

#### Scenario: Five-second price crosses a strategy level
- **WHEN** the live tape crosses a configured level before a required five-minute candle completes
- **THEN** the crossing does not confirm, activate, invalidate, or score a setup

#### Scenario: Completed five-minute evidence arrives
- **WHEN** the canonical completed five-minute candle satisfies a strategy condition
- **THEN** the existing authoritative state pipeline evaluates the condition independently of the live tape
