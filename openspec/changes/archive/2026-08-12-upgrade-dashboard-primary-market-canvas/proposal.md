## Why

The dashboard Market Tape gives SPX and VIX equal visual weight and leaves the operator to infer why the state says “Wait.” A primary market canvas should make SPX location, volatility confirmation, and the next executable condition readable in seconds.

## What Changes

- Promote SPX to a dominant primary chart and render VIX as a compact confirmation chart.
- Add compact SPY, QQQ, and IWM comparison controls without duplicating full charts.
- Replace ambiguous state copy with explicit location, trigger, and no-entry language.
- Add a four-signal confirmation strip for structure, momentum, VIX, and freshness.
- Preserve timeframe selection, symbol controls, and partial refresh behavior.
- Non-goals: changing Market Pulse, adding indicators, or modifying strategy rules.

## Capabilities

### New Capabilities
- `dashboard-primary-market-canvas`: Dashboard chart hierarchy, decision language, confirmations, and refresh behavior.

### Modified Capabilities

None.

## Impact

Touches the dashboard template, dashboard-only CSS, client refresh rendering, and focused contract tests. It uses the existing quote/timeframe payload and introduces no data-model or financial-calculation changes.
