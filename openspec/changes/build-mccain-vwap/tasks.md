## 1. Aligned McCain VWAP Domain

- [x] 1.1 Normalize SPX price and SPY volume rows into regular-session minute buckets
- [x] 1.2 Inner-join same-session minute legs and reject malformed, duplicate, unmatched, and non-positive-volume rows
- [x] 1.3 Calculate cumulative SPX typical-price weighted by aligned SPY volume with a 09:30 ET reset
- [x] 1.4 Calculate coverage ratio, terminal gap, largest missing gap, and current/complete/partial status
- [x] 1.5 Add deterministic tests for math, session reset, missing minutes, malformed inputs, and completed sessions

## 2. Provider and Canonical Integration

- [x] 2.1 Add deterministic current-session SPX-price and SPY-volume leg selection using existing provider priority
- [x] 2.2 Prefer the most complete same-session fallback leg and serialize independent provider provenance
- [x] 2.3 Replace SPX single-ratio scaled VWAP assembly with the aligned McCain VWAP contract
- [x] 2.4 Keep native same-symbol VWAP behavior for supported symbols with trustworthy volume
- [x] 2.5 Allow scenario confluence to consume McCain VWAP only for current or complete coverage
- [x] 2.6 Add service and canonical-payload tests for provider fallback, provenance, coverage, and generation coherence

## 3. TradingView-Style Chart Experience

- [x] 3.1 Render a `#F2D94E` two-pixel `MC VWAP` line and current-value badge
- [x] 3.2 Show distance from spot, method, providers, session status, and concise unavailable reasons
- [x] 3.3 Add an accessible chart toggle that changes only VWAP visibility
- [x] 3.4 Preserve timeframe, viewport, drawings, Strat markers, Gamma selection, and other overlays during toggle and refresh
- [x] 3.5 Add JavaScript, template, and styling contract tests for the overlay and non-destructive behavior
- [x] 3.6 Render aligned partial coverage as a dashed `MC VWAP · PARTIAL` diagnostic while keeping it non-actionable

## 4. Verification

- [x] 4.1 Run focused VWAP, scenario-ranking, Market Pulse route, chart, and refresh tests
- [x] 4.2 Run Ruff, JavaScript syntax checks, strict OpenSpec validation, and `git diff --check`
- [x] 4.3 Rebuild the local Podman app and verify `/healthz`
- [x] 4.4 Verify the receiving page shows a current or completed McCain VWAP when coverage qualifies and a truthful reason otherwise
- [x] 4.5 Rebuild and verify partial live coverage appears on the chart without entering scenario confluence
