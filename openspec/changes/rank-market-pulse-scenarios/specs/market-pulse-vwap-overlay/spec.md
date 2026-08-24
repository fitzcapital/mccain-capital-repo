## ADDED Requirements

### Requirement: Regular-session VWAP calculation
The system SHALL calculate session VWAP from compatible same-symbol intraday OHLCV bars using
cumulative typical-price-volume divided by cumulative volume and SHALL reset the calculation at the
regular-session boundary.

#### Scenario: Valid VWAP series
- **WHEN** regular-session bars contain finite high, low, close, and positive volume
- **THEN** the system emits timestamped cumulative VWAP values beginning with the first regular-session
  bar and matching the deterministic fixture calculation

#### Scenario: New trading session
- **WHEN** bars span more than one regular session
- **THEN** cumulative price-volume and volume reset at 09:30 America/New_York for each session and no
  prior-session value carries forward

### Requirement: VWAP source integrity
VWAP SHALL prefer compatible bars for the selected symbol. When SPX native volume is unavailable,
the system SHALL use same-session SPY OHLCV only as an explicitly labeled `SPY VWAP Proxy` and MUST
include source symbol, source timestamp, and proxy status in the canonical payload and visible UI.
It MUST NOT silently substitute futures, another ticker, or a previous session.

#### Scenario: SPX uses a labeled SPY proxy
- **WHEN** selected SPX bars do not contain trustworthy compatible volume
- **THEN** the system calculates same-session SPY VWAP when valid SPY OHLCV is available and labels
  the chart and decision surface `SPY VWAP Proxy` with source time

#### Scenario: Native and proxy volume unavailable
- **WHEN** neither selected-symbol nor same-session SPY OHLCV contains trustworthy compatible volume
- **THEN** the payload reports VWAP unavailable with a reason and the chart draws no VWAP line

#### Scenario: Malformed volume
- **WHEN** volume is missing, negative, non-finite, or otherwise malformed
- **THEN** affected bars are rejected and VWAP remains unavailable if no valid session series remains

### Requirement: VWAP chart overlay
The chart SHALL render available VWAP as one clearly labeled, theme-compatible price line distinct
from candles, gamma levels, targets, and invalidation lines.

#### Scenario: VWAP appears on chart
- **WHEN** a valid VWAP series is present for the selected ticker and timeframe
- **THEN** the chart renders a `VWAP` overlay aligned to bar timestamps with a readable current-value
  label

#### Scenario: VWAP unavailable
- **WHEN** the canonical payload marks VWAP unavailable
- **THEN** any previous VWAP series is removed and the page provides a concise unavailable reason
  without affecting other overlays

### Requirement: VWAP refresh preserves chart state
VWAP SHALL reconcile from the canonical in-place refresh without recreating the chart or changing
the user's viewport, timeframe, drawings, or selected gamma level.

#### Scenario: New VWAP points arrive
- **WHEN** a successful refresh appends valid session bars
- **THEN** the existing VWAP series receives the new canonical values and all unrelated chart state
  remains unchanged

#### Scenario: Mixed-generation refresh is rejected
- **WHEN** VWAP timestamps or source metadata do not match the staged canonical market generation
- **THEN** the page keeps the last valid VWAP and decision generation and reports refresh failure

### Requirement: VWAP participates in confluence transparently
An available VWAP relationship SHALL contribute only its declared score component and MUST NOT
confirm a setup by itself.

#### Scenario: VWAP supports bearish breakdown
- **WHEN** a bearish candidate has confirmed below its structural level and price is also below
  available VWAP
- **THEN** the candidate receives the documented VWAP-alignment points while retaining all other
  confirmation requirements

#### Scenario: VWAP conflicts with candidate
- **WHEN** VWAP is available but does not support the candidate direction
- **THEN** the VWAP component earns zero and the conflict is visible in the confluence breakdown
