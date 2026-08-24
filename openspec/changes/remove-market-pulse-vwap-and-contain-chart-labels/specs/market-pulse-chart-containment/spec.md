## ADDED Requirements

### Requirement: Chart annotations remain inside the visible plot
The Market Pulse execution chart SHALL keep the current-session badge and execution-level labels
readable within the visible chart boundary at supported desktop widths.

#### Scenario: Current-session badge is visible
- **WHEN** the execution chart renders the active session
- **THEN** the complete session badge is positioned below the top plot boundary without clipped text

#### Scenario: Right-edge execution labels are visible
- **WHEN** active, invalidation, target, or structural price labels render near the right edge
- **THEN** their complete concise text remains inside the chart boundary and does not overlap the
  price-scale edge

#### Scenario: Chart refresh preserves containment
- **WHEN** canonical data refreshes in place or the user changes timeframe
- **THEN** annotation containment remains applied without resetting viewport, drawings, Strat markers,
  or selected Gamma level
