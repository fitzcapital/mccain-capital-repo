## ADDED Requirements

### Requirement: Current-session replay follows the completed-bar feed
Setup Replay SHALL evaluate the latest valid completed five-minute bars available from the same
intraday source used by the Market Pulse decision chart.

#### Scenario: Cached replay is behind the chart
- **WHEN** the current-session completed-bar feed extends beyond every compatible cached snapshot
- **THEN** Setup Replay evaluates through the latest completed provider candle
- **AND** its coverage label reports that candle time

#### Scenario: Live completed-bar refresh fails
- **WHEN** the current-session bar provider is unavailable or returns invalid bars
- **THEN** Setup Replay retains the newest compatible cached evidence
- **AND** reports the cached evaluated-through time without inventing candles

#### Scenario: Explicit historical replay
- **WHEN** a prior session date is requested
- **THEN** Setup Replay uses stored point-in-time evidence for that date
- **AND** does not overlay current-session bars
