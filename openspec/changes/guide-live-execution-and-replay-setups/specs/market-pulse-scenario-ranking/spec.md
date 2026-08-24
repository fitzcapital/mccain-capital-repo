## MODIFIED Requirements

### Requirement: Canonical in-place scenario refresh
Scenario rankings, confluence components, decision language, checklist state, targets, live
permission, quote, completed bars, gamma context, levels, and timestamps SHALL update from one
validated canonical generation without reloading the page. Observation-only streams MAY update
independently but MUST NOT overwrite execution-authoritative fields or be described as a successful
canonical refresh.

#### Scenario: Successful scenario transition
- **WHEN** a validated generation changes a Local Flip breakdown from alternative to active
- **THEN** all scenario and primary-decision nodes update together while chart viewport, drawings,
  timeframe, and gamma selection remain unchanged

#### Scenario: Refresh failure
- **WHEN** a scenario generation fails validation
- **THEN** the last valid generation remains visible with failure and timestamp feedback and no
  partial new scenario fields are committed

#### Scenario: Observation streams advance without canonical promotion
- **WHEN** live quote or Gamma Ladder data advances while required canonical components remain stale
- **THEN** those values are labeled observation-only, execution remains locked, and the page reports
  a partial refresh rather than a successful scenario refresh
