## Why

The Market Pulse execution strip presents four equally important decisions with uneven visual
weight, while the chart's blue bearish candles compete with blue structural levels. The page
needs a clearer decision hierarchy and direction palette without removing the user's Strat
numbers or changing trading logic.

## What Changes

- Present Execution Read, Active Level, Next Evidence, and Invalidation as one equal-weight,
  visually connected decision rail with distinct semantic accents and a shared explanation.
- Render bullish candles with a soft-white body (`#F4F6FA`) and bearish candles with a deep-blue
  body (`#1F4ACB`), using silver/electric-blue borders and shared cool-gray wicks.
- Render directional Strat arrows in coordinated white/blue colors while preserving all Strat
  numerals and classification logic.
- Preserve all existing Strat numbers, arrows, marker meanings, toggles, and calculations.
- Emphasize spot, active, and invalidation levels while visually reducing secondary levels.
- Add concise chart distance labels and subtle regular-session/after-hours context where the
  underlying session metadata supports it.
- Keep invalidation emphasis proximity-aware instead of permanently alarming.
- Keep automatic refresh status icon-only and force updated Market Pulse assets through explicit
  revisioned URLs so an already-open page cannot remain on stale presentation files.
- Make Gamma Ladder expiry fallback explicit, calculate displayed strength against net GEX,
  remove repeated row language, and make the selected-level panel execution-relevant.
- Do not change market-data sources, refresh behavior, strategy rules, level values, or trade
  permissions.

## Capabilities

### New Capabilities

- `market-pulse-decision-chart`: Covers the execution decision rail, semantic candle palette,
  preserved Strat annotations, key-level hierarchy, and session-aware chart context.

### Modified Capabilities

None.

## Impact

- Affected UI: Market Pulse execution strip and SPX hero chart.
- Expected implementation areas: `mccain_capital/templates/core/market_pulse.html`,
  `static/css/market_pulse.css`, `static/js/spx_hero_chart.js`, and focused tests.
- APIs and data sources remain unchanged; the work consumes existing quote, completed-bar,
  level, and session metadata.
- Acceptance: all four decision cards have clear equal prominence; candle colors match the
  approved hex values; Strat numbers remain visible and unchanged; key levels read more strongly
  than secondary levels; session and distance context are concise; targeted tests, JavaScript
  syntax checks, OpenSpec strict validation, application rebuild, and `/healthz` pass.
