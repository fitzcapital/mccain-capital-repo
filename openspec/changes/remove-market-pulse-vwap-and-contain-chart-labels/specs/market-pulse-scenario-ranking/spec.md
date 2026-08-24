## MODIFIED Requirements

### Requirement: Deterministic confluence scoring
The system SHALL score eligible candidates on a fixed 0–100 scale using explicit components for
location, completed boundary close, retest, Strat confirmation, gamma alignment, level clustering,
and target space. The payload SHALL expose earned points and unavailable evidence for every declared
component and MUST NOT contain a VWAP component.

#### Scenario: Missing optional evidence does not inflate score
- **WHEN** Strat or another declared evidence component is unavailable
- **THEN** that component earns zero points, the denominator remains 100, and the missing evidence is
  disclosed

#### Scenario: Equal candidates sort deterministically
- **WHEN** two candidates have the same lane and score
- **THEN** the nearer level sorts first, followed by stable level and scenario-family priority

#### Scenario: VWAP is absent from scoring
- **WHEN** Market Pulse ranks any scenario
- **THEN** its score components contain no VWAP key and the remaining possible points total 100
