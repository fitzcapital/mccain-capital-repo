## Why

Market Pulse currently reports an available `SPY VWAP Proxy`, but its latest point can stop hours
before the visible candles, leaving no VWAP line in the active chart window. The Gamma Ladder also
contains useful data but still reads as a long list of similarly weighted cards instead of an
execution-first map of spot, nearby boundaries, and positioning strength.

## What Changes

- Require a displayed VWAP series to overlap the visible session bars and expose source age and
  coverage; an incomplete proxy must be refreshed, marked partial, or shown unavailable rather than
  appearing valid while remaining off-chart.
- Give VWAP a persistent, clearly labeled current-value treatment without changing chart timeframe,
  zoom, drawings, Strat numbers, or Gamma selections during in-place refresh.
- Replace the ladder's flat row treatment with a centered price-spine presentation: resistance above
  spot, spot as the visual anchor, and support/acceleration structure below.
- Encode distance, signed net GEX, strength, role, and interaction state with consistent position,
  bar length, color, typography, and concise labels.
- Keep a compact execution map and the nearest actionable rows visible by default while preserving
  full-ladder disclosure, row inspection, symbol/window/DTE controls, and chart coordination.
- Add responsive and accessibility behavior so the price spine remains scannable on desktop and
  mobile without relying on color alone.
- Non-goals: changing provider formulas, inventing VWAP values, automated execution, profitability
  claims, changing Strat numbers, or removing full-depth research.

## Capabilities

### New Capabilities

- `gamma-ladder-price-spine`: Execution-first Gamma Ladder presentation centered on spot with
  deterministic visual encoding, progressive disclosure, and responsive inspection behavior.

### Modified Capabilities

- `market-pulse-vwap-overlay`: Require same-session visible coverage, explicit partial/stale source
  handling, and a readable overlay that remains present across canonical in-place refreshes.

## Impact

- Affects Market Pulse VWAP assembly in `mccain_capital/services/`, the canonical context payload,
  `static/js/spx_hero_chart.js`, the Gamma Ladder template/controller, and Market Pulse CSS.
- Uses existing SPX/SPY intraday inputs and Gamma Ladder rows; no new provider, dependency, database,
  persistence, or broker integration is introduced.
- Acceptance requires the live chart to show VWAP through the current visible bar when source data is
  valid, truthful unavailable/partial handling otherwise, and a ladder that identifies spot plus the
  nearest upside/downside decision rows without opening secondary detail.
