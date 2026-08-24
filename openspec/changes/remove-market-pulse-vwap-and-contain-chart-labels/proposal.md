## Why

VWAP is not providing a dependable execution aid on Market Pulse and should no longer occupy chart,
decision, or data-refresh surface area. The chart also clips the current-session badge and right-edge
level labels, obscuring execution-critical context.

## What Changes

- **BREAKING** Remove VWAP calculation, fallback retrieval, canonical payload fields, scenario scoring,
  chart series, toggle, status card, styling, and Market Pulse tests.
- Remove VWAP from the declared scenario-confluence score and normalize the remaining weights to 100.
- Keep the current-session badge inside the visible plot bounds.
- Keep active, invalidation, target, and level labels readable inside the chart's right boundary.
- Preserve chart timeframe, drawings, Strat markers, Gamma selection, and in-place refresh behavior.
- Non-goals: changing candle evidence, Gamma levels, scenario triggers, or adding another volume-derived
  indicator.

## Capabilities

### New Capabilities

- `market-pulse-chart-containment`: Visible-boundary behavior for session badges and execution-level labels.

### Modified Capabilities

- `market-pulse-vwap-overlay`: Remove VWAP from Market Pulse data, chart, controls, and decision scoring.
- `market-pulse-scenario-ranking`: Remove VWAP confluence and retain a normalized 100-point score.

## Impact

- Affects Market Pulse scenario/domain services, canonical payload assembly, market-data leg selection,
  Jinja bindings, chart JavaScript, CSS, OpenSpec contracts, and focused tests.
- Uses existing price-line and session-label rendering; no new dependency, data source, persistence, or
  financial assumption is introduced.
- Acceptance requires no VWAP field or visible VWAP surface on Market Pulse, scenario weights totaling
  100 without VWAP, contained chart annotations at supported desktop widths, focused tests, strict
  validation, and a healthy rebuilt local app.
