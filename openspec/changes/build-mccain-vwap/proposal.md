## Why

SPX has no trustworthy native traded volume, so the current scaled-SPY proxy can stop early or drift
from the SPX chart and disappear. Market Pulse needs an SPX-denominated, session-complete VWAP that
behaves like the familiar TradingView overlay while disclosing exactly how it is constructed.

## What Changes

- Calculate `McCain VWAP` from aligned one-minute SPX typical prices weighted by same-minute SPY
  traded volume, resetting at the 09:30 America/New_York regular-session boundary.
- Use the existing provider stack to obtain a complete current-session SPX price path and SPY volume
  path, with deterministic provider priority, timestamp alignment, coverage validation, and
  last-valid preservation.
- During the regular session, append only complete aligned minutes. After 16:00 ET, freeze the final
  accepted session value instead of marking the completed VWAP stale solely because the market is
  closed.
- Render one TradingView-style yellow `MC VWAP` line, current-value badge, distance-from-spot state,
  source time, and a chart visibility toggle without resetting chart state.
- Keep standard-deviation bands out of the first release; the payload will remain extensible for a
  later optional band overlay.
- Non-goals: claiming native SPX volume, copying TradingView proprietary calculations, adding
  automated execution, modifying Strat numbers, or changing Gamma calculations.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-vwap-overlay`: Replace single-ratio scaled SPY VWAP with SPX typical prices weighted
  by aligned SPY volume, and add session lifecycle, provenance, toggle, and display requirements.

## Impact

- Affects Market Pulse market-data assembly, VWAP calculation services, canonical context payload,
  chart controller, template bindings, styling, and focused tests.
- Uses existing SPX price and SPY one-minute data sources; no database or broker integration changes.
- Financial assumption: SPY traded volume is an explicit liquidity-weight proxy for the SPX price
  path and is never represented as native SPX volume.
- Acceptance requires deterministic fixture math, at least 95% aligned-minute coverage during a
  healthy session, a frozen completed-session value after close, and no chart-state reset when the
  overlay is refreshed or toggled.
