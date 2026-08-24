## 1. Dashboard Snapshot Structure

- [x] 1.1 Replace the Market Tape matrix and dual chart lanes with SPX Session Snapshot markup
- [x] 1.2 Add actual-price session-location, statistics, descriptive read, and Market Pulse action
- [x] 1.3 Add compact non-SPX context with explicit non-validation language

## 2. Refresh and Presentation

- [x] 2.1 Update Dashboard tape hydration to derive and refresh SPX snapshot values without reload
- [x] 2.2 Implement stale, missing, and zero-range fallback behavior
- [x] 2.3 Add compact desktop and responsive styling while preserving the SPX-first hierarchy

## 3. Verification

- [x] 3.1 Update focused Dashboard template and JavaScript contract coverage
- [x] 3.2 Run focused tests, JavaScript syntax validation, and OpenSpec validation
- [x] 3.3 Rebuild the local app, verify health, and inspect the deployed Dashboard receiving surface

## 4. Compact SPX Mini-Chart Refinement

- [x] 4.1 Replace the oversized range rail with a compact one-hour SPX candle chart and three supporting statistics
- [x] 4.2 Reuse the existing Dashboard candle renderer and refresh payload for initial and partial-refresh hydration
- [x] 4.3 Preserve compact responsive layout, unavailable fallback, and the Market Pulse execution boundary
- [x] 4.4 Add focused chart contracts, validate OpenSpec, rebuild, and verify local health

## 5. Mini-Chart Data Integrity

- [x] 5.1 Require real OHLC history for the Dashboard mini-chart
- [x] 5.2 Use the last completed session when the current session lacks enough candles
- [x] 5.3 Remove synthetic prior-close/spot and close-only chart fallbacks

## 6. Closed-Session Resilience

- [x] 6.1 Reject identical quote-derived OHLC points as a valid candle series
- [x] 6.2 Fall back to the latest completed session on both initial load and partial refresh
- [x] 6.3 Add regression coverage and rebuild the deployed app

## 7. Wide-Desktop Density

- [x] 7.1 Center and cap the complete Dashboard workflow on wide desktop viewports
- [x] 7.2 Tighten major-section rhythm while preserving card hierarchy and responsive behavior
- [x] 7.3 Add a focused density contract, validate, rebuild, and inspect the deployed Dashboard
