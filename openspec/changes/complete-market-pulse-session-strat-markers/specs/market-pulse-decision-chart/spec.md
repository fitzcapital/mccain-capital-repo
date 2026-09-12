## ADDED Requirements

### Requirement: Complete active-session Strat annotation coverage
The Market Pulse decision chart SHALL retain a Strat number for every classifiable candle in the
active session, including when a directional candle also renders an arrow.

#### Scenario: Full regular five-minute session
- **WHEN** the chart contains all 78 regular-session five-minute candles and markers are enabled
- **THEN** every candle after the comparison boundary displays its correct 1, 2, or 3 number
- **AND** 2U and 2D candles retain their directional arrows

#### Scenario: Incomplete or invalid candle
- **WHEN** a candle lacks valid high or low data
- **THEN** the chart omits a classification for that candle without removing other valid markers

### Requirement: Full-session default five-minute frame
The Market Pulse decision chart SHALL frame the complete active regular session by default on the
five-minute timeframe while preserving manual pan and zoom.

#### Scenario: Intraday five-minute chart
- **WHEN** the current session contains up to 78 completed five-minute candles
- **THEN** the initial chart range includes the session from 9:30 AM through the latest candle

