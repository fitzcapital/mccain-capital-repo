## 1. VWAP Coverage Contract

- [x] 1.1 Add deterministic helpers for comparing VWAP source coverage with current-session chart bars
- [x] 1.2 Extend native and SPY proxy VWAP payloads with coverage status, chart time, source age, and reason
- [x] 1.3 Reject previous-session, non-overlapping, and materially incomplete proxy series from chart-visible status
- [x] 1.4 Add focused service tests for current, partial, unavailable, malformed, and session-boundary coverage

## 2. VWAP Chart Reconciliation

- [x] 2.1 Update canonical view bindings to distinguish current, partial, and unavailable VWAP states
- [x] 2.2 Reconcile only accepted current-session points into the existing chart series
- [x] 2.3 Add a readable VWAP label/value treatment without changing zoom, timeframe, drawings, Strat markers, or Gamma selection
- [x] 2.4 Add JavaScript/template contract tests for coverage handling and in-place refresh preservation

## 3. Gamma Price-Spine Model

- [x] 3.1 Normalize visible rows into above-spot, spot-anchor, and below-spot zones while preserving price order
- [x] 3.2 Add robust signed-GEX display normalization without changing exact payload values
- [x] 3.3 Mark decision, dominant, nearest-upside, and nearest-downside rows deterministically
- [x] 3.4 Add focused presentation-model tests for between-strike spot, outliers, and missing decision rows

## 4. Gamma Ladder Presentation

- [x] 4.1 Replace flat row styling with a centered zero-axis depth track and visually dominant spot rail
- [x] 4.2 Render concise strike, distance, role, state, strength, and exact signed-GEX labels with selective emphasis
- [x] 4.3 Preserve symbol/window/DTE controls, row selection, chart coordination, and local full-depth disclosure
- [x] 4.4 Add responsive mobile behavior and non-color accessibility cues
- [x] 4.5 Add DOM/style contract tests for semantic classes, ordering, disclosure, and inspection states

## 5. Verification

- [x] 5.1 Run focused VWAP, Gamma service, ladder presentation, canonical refresh, and Market Pulse route tests
- [x] 5.2 Run Ruff, JavaScript syntax checks, strict OpenSpec validation, and `git diff --check`
- [x] 5.3 Rebuild the local Podman app and verify `/healthz`
- [x] 5.4 Verify on `/market-pulse?ticker=SPX` that current VWAP is visible or truthfully unavailable and the price spine remains coherent across refresh, filters, selection, desktop, and mobile

## 6. Scenario Language and Ladder Follow-up

- [x] 6.1 Replace generic scenario-family labels with explicit price-action language
- [x] 6.2 Add regression coverage for reclaim, acceptance, sweep, and failed-reclaim labels
- [x] 6.3 Inspect the deployed ladder hierarchy, state labels, and controls for remaining defects
- [x] 6.4 Rebuild and verify the corrected receiving page
