## MODIFIED Requirements

### Requirement: Primary chart hierarchy
The dashboard SHALL render an SPX Session Snapshot as its primary market-orientation surface and
SHALL reserve detailed execution charts for Market Pulse.

#### Scenario: Dashboard tape is available
- **WHEN** the dashboard renders current SPX tape data
- **THEN** SPX spot, session change, open, high, low, range, and range position receive primary
  visual weight
- **AND** the Dashboard does not render a multi-symbol execution canvas

### Requirement: Compact comparison context
The dashboard SHALL expose SPY, QQQ, IWM, and VIX as compact descriptive context without treating
their movement as SPX strategy confirmation.

#### Scenario: Comparison quotes are present
- **WHEN** comparison-symbol data is available
- **THEN** each symbol shows its current price and percentage change in a subordinate context row
- **AND** nearby language states that the context does not validate an SPX setup

### Requirement: Explicit market-read language
The dashboard MUST describe SPX session location and change without generating a trade signal or
duplicating Market Pulse execution guidance.

#### Scenario: SPX session data is available
- **WHEN** the SPX session snapshot has current range and quote data
- **THEN** the market read names the session character and summarizes what changed
- **AND** the read remains descriptive rather than instructing an entry

#### Scenario: Required data is unavailable
- **WHEN** SPX range or quote data is stale or missing
- **THEN** the snapshot identifies the unavailable input without inventing a range position,
  session character, or setup

### Requirement: Confirmation strip
The dashboard SHALL replace strategy-style confirmation language with SPX session statistics and
a clearly subordinate cross-market context strip.

#### Scenario: Partial refresh completes
- **WHEN** updated tape data is applied
- **THEN** SPX spot, change, range position, session statistics, session character, freshness, and
  context quotes update without a full-page reload

### Requirement: Responsive layout
The SPX Session Snapshot SHALL preserve the SPX-first hierarchy at narrower widths.

#### Scenario: Available width is narrow
- **WHEN** the horizontal snapshot no longer fits clearly
- **THEN** session metrics and comparison context reflow after the primary SPX price and location
  visualization
- **AND** all price labels remain readable without horizontal page overflow

#### Scenario: Available width is extra wide
- **WHEN** the Dashboard is displayed on a wide desktop viewport
- **THEN** the primary Dashboard workflow is centered inside a bounded reading canvas
- **AND** performance, snapshot, and calendar surfaces do not stretch into long low-density rows

## ADDED Requirements

### Requirement: Actual-price session location
The dashboard SHALL visualize SPX position within the available session range using actual low,
spot, and high prices, with open and midpoint as secondary references when available.

#### Scenario: Complete range is available
- **WHEN** current SPX low, high, and spot values are valid
- **THEN** the marker is positioned proportionally between low and high
- **AND** the low, spot, high, and percentage-through-range values are visible

#### Scenario: Zero or incomplete range is returned
- **WHEN** high does not exceed low or any required value is unavailable
- **THEN** no synthetic position is shown
- **AND** the range surface reports that session location is unavailable

### Requirement: Market Pulse boundary
The dashboard SHALL provide one clear navigation action to Market Pulse for SPX execution analysis.

#### Scenario: User needs execution detail
- **WHEN** the user activates the Market Pulse action
- **THEN** the application opens the SPX Market Pulse workspace
- **AND** the Dashboard itself does not duplicate levels, scenarios, or setup confirmation

### Requirement: Compact SPX session chart
The Dashboard SHALL show recent SPX session shape with a compact candle chart sourced from the
existing one-hour Dashboard tape payload.

#### Scenario: One-hour candle data is available
- **WHEN** the Dashboard renders or partially refreshes valid SPX one-hour candles
- **THEN** the snapshot shows a compact candlestick chart without a full-page reload
- **AND** the chart remains descriptive and contains no execution levels, setup markers, Gamma
  overlays, or trade instructions

#### Scenario: Candle data is unavailable
- **WHEN** fewer than two usable SPX OHLC candles are available from the current or last valid session
- **THEN** the chart shows a compact unavailable state without inventing price movement

#### Scenario: Current-session candle history is incomplete
- **WHEN** the current session has fewer than two usable SPX OHLC candles
- **THEN** the chart uses the latest completed-session OHLC history when available
- **AND** it never converts prior close and current spot into synthetic candles

#### Scenario: Quote fallback produces identical OHLC points
- **WHEN** two or more OHLC-shaped points contain no actual price range or movement
- **THEN** the Dashboard rejects those points as manufactured candles
- **AND** initial load and partial refresh use the latest completed-session OHLC history instead
