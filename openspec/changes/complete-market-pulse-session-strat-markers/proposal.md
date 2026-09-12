## Why

Market Pulse drops early-session Strat annotations as the marker-object cap is exhausted, and Setup
Replay can lag the chart by hours because it reads cached bars instead of the latest completed-bar
feed. This makes the chart and replay appear incomplete even while live market data is available.

## What Changes

- Preserve a Strat number for every classifiable candle in the active regular session.
- Make the default 5-minute chart frame show the full active session while retaining pan and zoom.
- Refresh current-session Setup Replay from the same completed 5-minute source used by the chart.
- Report replay coverage from the actual last evaluated candle and keep historical-date replay frozen.
- Add full-session regression coverage for marker counts, bar boundaries, and replay freshness.
- Non-goals: changing Strat definitions, setup qualification rules, levels, targets, or Gamma logic.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-decision-chart`: Require complete active-session Strat-marker and viewport coverage.
- `market-pulse-scenario-ranking`: Require current-session replay to consume the latest completed
  five-minute bars without changing historical replay evidence.

## Impact

- Affected code: `static/js/spx_hero_chart.js`, Market Pulse replay sourcing in
  `mccain_capital/services/core.py`, and focused JavaScript/Python tests.
- Data source: existing Tradier completed intraday bars; no new provider or financial assumption.
- Acceptance: all 78 regular-session 5-minute candles can remain visible/classified, current replay
  reaches the latest provider-completed candle, historical replay remains unchanged, and focused
  tests plus deployed K8s verification pass.
