## MODIFIED Requirements

### Requirement: Regular-session VWAP calculation
For SPX, the system SHALL calculate `McCain VWAP` from aligned one-minute SPX typical prices weighted
by same-minute positive SPY traded volume. The system SHALL calculate typical price as
`(high + low + close) / 3`, use cumulative price-volume divided by cumulative volume, and reset at
09:30 America/New_York for every regular session. Other supported symbols with trustworthy native
volume SHALL retain same-symbol VWAP calculation.

#### Scenario: Valid aligned SPX and SPY series
- **WHEN** same-session regular-session SPX bars contain finite high, low, and close and matching SPY
  minute bars contain positive volume
- **THEN** the system emits SPX-denominated cumulative `McCain VWAP` points matching the deterministic
  fixture calculation

#### Scenario: New trading session
- **WHEN** source bars span more than one regular session
- **THEN** cumulative price-volume and SPY volume reset at 09:30 America/New_York and no prior-session
  value carries forward

#### Scenario: Unmatched or invalid minute
- **WHEN** either leg is missing for a minute, SPX OHLC is invalid, or SPY volume is non-positive
- **THEN** that minute is excluded and reported in coverage diagnostics without forward-filling price
  or volume

### Requirement: VWAP source integrity
The SPX `McCain VWAP` payload SHALL identify SPX as the price leg, SPY as the volume leg, the provider
for each leg, the session date, alignment method, latest aligned timestamp, aligned and expected
minute counts, coverage ratio/status, and largest missing gap. It MUST identify the result as a
volume-weighted proxy and MUST NOT represent SPY volume as native SPX volume.

#### Scenario: Healthy live-session proxy
- **WHEN** the aligned series covers at least 95% of the overlapping regular-session window and its
  terminal minute is within the live-session grace period
- **THEN** the payload marks coverage `current`, exposes `SPX price · SPY volume`, and permits the
  chart and confluence consumers to use the value

#### Scenario: Completed session after close
- **WHEN** the aligned series reaches the final expected completed regular-session minute
- **THEN** the payload marks coverage `complete`, freezes the final value, and keeps it usable after
  close without treating ordinary wall-clock age as stale

#### Scenario: Incomplete or cross-session inputs
- **WHEN** coverage is below the threshold, the terminal gap exceeds the grace period, or the two legs
  belong to different sessions
- **THEN** the payload marks coverage `partial` or `unavailable`, explains the failure, and prevents
  confluence consumers from using the value; aligned partial points MAY remain chart-visible as an
  explicitly non-actionable diagnostic

#### Scenario: Provider fallback improves one leg
- **WHEN** the preferred provider leg is unavailable or less complete and an existing fallback offers
  better same-session coverage
- **THEN** the fallback leg is accepted with its provider disclosed while the other leg and canonical
  generation remain unchanged

### Requirement: VWAP chart overlay
The chart SHALL render accepted `McCain VWAP` as one `#F2D94E` two-pixel line titled `MC VWAP`, with
a readable current-value badge, distance from spot, coverage state, and a visibility toggle. The line
SHALL remain visually distinct from candles, Gamma levels, targets, and invalidation lines.

#### Scenario: Current McCain VWAP appears
- **WHEN** the canonical payload has `current` or `complete` coverage
- **THEN** the chart displays the aligned yellow line and current value while the decision surface
  identifies the method as `SPX price · SPY volume`

#### Scenario: User toggles VWAP
- **WHEN** the user hides or shows `MC VWAP`
- **THEN** only the VWAP series visibility changes and the chart retains its timeframe, viewport,
  drawings, Strat markers, Gamma selection, and other overlays

#### Scenario: Partial coverage with aligned points
- **WHEN** the canonical payload marks coverage `partial` and contains one or more aligned points
- **THEN** the chart displays a dashed yellow line titled `MC VWAP · PARTIAL`, the page provides a
  concise source and coverage reason, and the value remains excluded from execution confluence

#### Scenario: Unavailable coverage without aligned points
- **WHEN** the canonical payload marks coverage `unavailable` or contains no aligned points
- **THEN** the chart removes the VWAP line and the page provides a concise source and coverage reason
  without affecting other overlays

### Requirement: VWAP refresh preserves chart state
`McCain VWAP` SHALL reconcile through the canonical in-place refresh without recreating the chart or
changing viewport, timeframe, drawings, Strat markers, or selected Gamma level. Points, coverage,
provenance, value, and generation id SHALL advance atomically.

#### Scenario: New aligned minute arrives
- **WHEN** a successful canonical refresh adds a complete aligned SPX-price/SPY-volume minute
- **THEN** the existing series receives the new cumulative point and unrelated chart state remains
  unchanged

#### Scenario: Mixed-generation or regressed coverage is rejected
- **WHEN** VWAP legs, timestamps, provenance, or coverage do not match the staged canonical generation
  or regress behind the last accepted generation
- **THEN** the page keeps the last valid VWAP and decision generation and reports refresh failure
