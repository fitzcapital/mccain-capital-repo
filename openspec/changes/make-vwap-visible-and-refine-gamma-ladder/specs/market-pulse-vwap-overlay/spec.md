## MODIFIED Requirements

### Requirement: VWAP source integrity
VWAP SHALL prefer compatible bars for the selected symbol. When SPX native volume is unavailable,
the system SHALL use same-session SPY OHLCV only as an explicitly labeled `SPY VWAP Proxy` and MUST
include source symbol, source timestamp, proxy status, coverage status, latest compatible chart time,
and source age in the canonical payload and visible UI. It MUST NOT silently substitute futures,
another ticker, a previous session, or a source series that cannot overlap the active chart window.

#### Scenario: SPX uses a labeled SPY proxy
- **WHEN** selected SPX bars do not contain trustworthy compatible volume and current-session SPY
  OHLCV overlaps the active chart bars
- **THEN** the system calculates same-session SPY VWAP and labels the chart and decision surface
  `SPY VWAP Proxy` with source time and current coverage

#### Scenario: Native and proxy volume unavailable
- **WHEN** neither selected-symbol nor same-session SPY OHLCV contains trustworthy compatible volume
  with sufficient chart-window coverage
- **THEN** the payload reports VWAP unavailable or partial with a reason and the chart draws no stale
  VWAP line

#### Scenario: Malformed volume
- **WHEN** volume is missing, negative, non-finite, or otherwise malformed
- **THEN** affected bars are rejected and VWAP remains unavailable if no valid session series remains

### Requirement: VWAP chart overlay
The chart SHALL render current, sufficiently covered VWAP as one clearly labeled, theme-compatible
price line distinct from candles, gamma levels, targets, and invalidation lines. The overlay SHALL
remain visible through the latest compatible bar in the active chart window without client-side
fabrication or forward extension.

#### Scenario: VWAP appears on chart
- **WHEN** a valid VWAP series overlaps the selected ticker's current visible session bars
- **THEN** the chart renders the labeled overlay aligned to bar timestamps with a readable current-
  value label through the latest compatible VWAP point

#### Scenario: VWAP source ends before visible window
- **WHEN** the latest VWAP point precedes the active visible chart window beyond the accepted coverage
  threshold
- **THEN** the chart removes the line and the page labels the source partial or unavailable with its
  timestamp instead of claiming a visible VWAP

#### Scenario: VWAP unavailable
- **WHEN** the canonical payload marks VWAP unavailable
- **THEN** any previous VWAP series is removed and the page provides a concise unavailable reason
  without affecting other overlays

### Requirement: VWAP refresh preserves chart state
VWAP SHALL reconcile from the canonical in-place refresh without recreating the chart or changing
the user's viewport, timeframe, drawings, selected Gamma level, or Strat markers. Coverage metadata
and points SHALL advance in the same accepted canonical generation.

#### Scenario: New VWAP points arrive
- **WHEN** a successful refresh appends valid same-session VWAP points that overlap current chart bars
- **THEN** the existing VWAP series receives the new canonical values and all unrelated chart state
  remains unchanged

#### Scenario: Mixed-generation refresh is rejected
- **WHEN** VWAP timestamps, coverage, or source metadata do not match the staged canonical market
  generation
- **THEN** the page keeps the last valid VWAP and decision generation and reports refresh failure
