## ADDED Requirements

### Requirement: Source-backed current-day levels
The system SHALL derive CDH and CDL independently for each Market Tape symbol from validated
current-session intraday OHLC rows and SHALL expose numeric values, formatted values, source state,
and freshness with the initial Dashboard data and refresh payload.

#### Scenario: Valid current-session OHLC data
- **WHEN** a symbol has validated current-session intraday rows with numeric highs and lows
- **THEN** CDH equals the maximum session high and CDL equals the minimum session low for that symbol

#### Scenario: Prior-session fallback supplies the chart
- **WHEN** the current-session source is unavailable and the chart uses prior-session, cached, or
  close-only fallback data
- **THEN** the system marks current-day levels unavailable and does not relabel fallback extremes as
  CDH or CDL

#### Scenario: Symbols have different session ranges
- **WHEN** the two Market Tape lanes display different symbols
- **THEN** each lane receives only the CDH and CDL calculated from its own symbol data

### Requirement: CDH and CDL chart price tags
The Dashboard SHALL render valid CDH and CDL as distinct horizontal references inside the Market
Tape chart with complete price tags that remain associated with their true price coordinates.

#### Scenario: Initial chart render
- **WHEN** a Dashboard chart initially receives valid CDH and CDL values
- **THEN** it displays a teal CDH rule and price tag and a rose CDL rule and price tag while retaining
  the candle chart and current-price marker

#### Scenario: Levels are outside the selected window range
- **WHEN** valid current-session levels fall outside the candles in the selected timeframe
- **THEN** the chart includes the levels in a padded y-domain so both labels and the candle pattern
  remain visible

#### Scenario: Level labels collide
- **WHEN** CDH, CDL, or the current-price marker would visually overlap
- **THEN** labels are clamped and separated with connectors preserving their true price positions

#### Scenario: Levels are unavailable
- **WHEN** current-session levels are unavailable or invalid
- **THEN** no CDH/CDL rule or price tag is rendered and the existing chart remains usable

### Requirement: Live chart parity
The Dashboard SHALL apply the same current-day level rendering rules after ticker changes,
timeframe changes, and successful tape refreshes as it applies during initial server rendering.

#### Scenario: Successful live refresh
- **WHEN** a refresh returns newer valid current-session levels
- **THEN** the chart updates the candles, CDH/CDL rules, tags, accessible description, and freshness
  together without requiring a page reload

#### Scenario: Failed live refresh
- **WHEN** a tape refresh fails or returns an invalid replacement payload
- **THEN** the last confirmed chart and CDH/CDL values remain visible with existing failure feedback

#### Scenario: Timeframe changes
- **WHEN** the user selects a different supported tape window
- **THEN** the candles and point-change calculation use that window while CDH/CDL continue to describe
  the full validated current session

### Requirement: Semantically appropriate progress visuals
The Dashboard SHALL use a consistent ring component for bounded completion KPIs and a consistent
linear component for pace, threshold, and comparison metrics without changing their calculations.

#### Scenario: Bounded completion KPI
- **WHEN** milestone, alignment, or balance/goal completion is rendered
- **THEN** the metric uses a 0–100 ring with its numeric value and status available as visible text
  and progress semantics

#### Scenario: Threshold or comparison metric
- **WHEN** pace, consistency, or discipline progress communicates a threshold or comparison
- **THEN** it remains linear and displays the applicable fill, endpoint, target or threshold marker,
  labels, and state tone

#### Scenario: Value is outside the visual range
- **WHEN** a calculated visual percentage is below zero or above one hundred
- **THEN** the rendered arc or fill is clamped to 0–100 while the authoritative textual value and
  financial calculation remain unchanged

#### Scenario: Existing calculation regression
- **WHEN** the enhanced Dashboard is compared with the same milestone, alignment, balance,
  consistency, discipline, and pace inputs
- **THEN** every displayed numeric result and status matches the pre-enhancement calculation

### Requirement: Restrained accessible motion
The Dashboard SHALL animate visual state changes briefly without delaying content or interaction and
SHALL provide an equivalent non-animated presentation for reduced-motion users.

#### Scenario: Visual enters the viewport
- **WHEN** a ring, progress track, or chart level becomes visible for the first time
- **THEN** it transitions once from its quiet start state to the already-rendered confirmed endpoint

#### Scenario: Live value changes
- **WHEN** a successful refresh changes a chart level or progress endpoint
- **THEN** only the affected visual transitions to the new endpoint and its text updates immediately

#### Scenario: Reduced motion is requested
- **WHEN** `prefers-reduced-motion: reduce` is active
- **THEN** arcs, fills, level tags, and points display their final state without interpolation or
  continuous pulsing

### Requirement: Responsive and accessible visual context
The enhanced charts and progress components SHALL remain readable and operable across supported
Dashboard widths and SHALL not rely on color or animation alone.

#### Scenario: Narrow viewport
- **WHEN** the Dashboard is rendered at a supported mobile or tablet width
- **THEN** price tags, ring labels, progress endpoints, and metric text remain within their cards
  without horizontal page overflow

#### Scenario: Assistive technology reads a visual
- **WHEN** a user encounters a Market Tape chart or progress component with assistive technology
- **THEN** the symbol, timeframe, CDH/CDL availability, numeric value, and relevant state are exposed
  through concise text or ARIA semantics

#### Scenario: Colors are not distinguishable
- **WHEN** CDH/CDL or progress states cannot be distinguished by color
- **THEN** visible labels, line patterns, shapes, and text still identify the level or state
