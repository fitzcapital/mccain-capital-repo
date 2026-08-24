## ADDED Requirements

### Requirement: Primary chart hierarchy
The dashboard SHALL render SPX as the dominant market canvas and VIX as a smaller volatility-confirmation chart.

#### Scenario: Dashboard tape is available
- **WHEN** the dashboard renders its market tape
- **THEN** SPX receives primary visual weight and VIX receives secondary visual weight

### Requirement: Compact comparison context
The dashboard SHALL expose SPY, QQQ, and IWM as compact comparison context without duplicating full-size charts.

#### Scenario: Comparison quotes are present
- **WHEN** comparison-symbol data is available
- **THEN** each symbol shows price direction and percentage context in a compact tile

### Requirement: Explicit market-read language
The dashboard MUST replace ambiguous waiting language with a location, missing confirmation, or unavailable-data explanation.

#### Scenario: No directional edge is confirmed
- **WHEN** the primary chart state is mixed
- **THEN** the market read explains that no entry is available and identifies the confirmation being watched

#### Scenario: Required data is unavailable
- **WHEN** primary or confirmation data is stale or missing
- **THEN** the market read states that confirmation is unavailable without inventing a signal

### Requirement: Confirmation strip
The dashboard SHALL summarize structure, momentum, VIX confirmation, and freshness beneath the canvas.

#### Scenario: Partial refresh completes
- **WHEN** updated tape data is applied
- **THEN** the charts, market read, and confirmation strip update without a full-page reload

### Requirement: Responsive layout
The market canvas SHALL preserve the primary-before-secondary hierarchy at narrower widths.

#### Scenario: Available width is narrow
- **WHEN** the side-by-side canvas no longer fits clearly
- **THEN** the confirmation chart stacks after the primary chart and comparison tiles reflow
