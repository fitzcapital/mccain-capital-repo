## ADDED Requirements

### Requirement: Shared live and replay setup events
Live Setup and Setup Replay SHALL derive eligible setup events from the same point-in-time completed-candle evaluator and stable event identity. Replay MAY add forward outcome statistics, but it MUST NOT use a different trigger boundary, location qualification, or candidate identity from Live.

#### Scenario: Identical point-in-time inputs are processed
- **WHEN** Live and Replay process the same ordered completed bars and the same point-in-time levels, strategy rules, and available gamma evidence
- **THEN** they SHALL produce the same eligible setup event ids, signal times, pattern times, directions, families, levels, and trigger boundaries

#### Scenario: Setup is not current Primary
- **WHEN** an eligible event is outranked by another candidate after its trigger
- **THEN** Live SHALL retain its event record and Replay SHALL retain its historical result with the same identity

#### Scenario: Evidence was unavailable at signal time
- **WHEN** a historical input was not available at the setup signal timestamp
- **THEN** both paths SHALL mark that evidence unavailable and MUST NOT substitute a later observation

#### Scenario: Replay measures the outcome
- **WHEN** Replay evaluates candles after a shared eligible event
- **THEN** it MAY calculate MFE, MAE, target, and invalidation outcomes without changing the frozen live/replay event identity or signal facts
