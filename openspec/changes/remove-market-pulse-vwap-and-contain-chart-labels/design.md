## Context

Market Pulse currently assembles VWAP across market-data services, canonical context, scenario
ranking, template cards, and a Lightweight Charts series. Removing only the visible line would leave
unused provider calls and a hidden score input. Separately, chart-native labels are drawn at plot
edges where the session badge and long execution labels can be clipped.

## Goals / Non-Goals

**Goals:**

- Remove the VWAP data and UI path end to end.
- Keep scenario scores deterministic and normalized to 100 without VWAP.
- Reserve visible chart space for the session badge and right-side execution labels.
- Preserve existing chart interactions and non-VWAP overlays.

**Non-Goals:**

- Replace VWAP with another indicator.
- Change scenario confirmation gates, candles, Strat markers, or Gamma calculations.
- Redesign the entire chart or ladder.

## Decisions

### 1. Remove the complete VWAP dependency chain

Delete VWAP construction and SPX/SPY fallback retrieval from canonical assembly, omit VWAP from the
payload, remove its score component, and remove all corresponding chart/template/CSS bindings.
Leaving an empty or permanently unavailable VWAP object was rejected because it retains dead UI and
ambiguous financial semantics.

### 2. Renormalize confluence weights explicitly

Distribute the removed VWAP weight across existing structural evidence while retaining a total of
100. No candidate gains a new confirmation path; score continues to rank within evidence lanes.

### 3. Contain annotations through chart margins and label width control

Reserve top and right plot margins through chart scale options, constrain label text to concise
execution language, and position the session marker below the upper plot boundary. This is preferred
to overlaying HTML labels because native price anchoring and refresh behavior remain intact.

### 4. Verify receiving behavior without subjective visual approval

Use DOM/chart contract checks for removed VWAP surfaces, plot option assertions, console inspection,
and live page state. Final visual judgment remains with the user.

## Risks / Trade-offs

- [Historical payload consumers expect `vwap`] → Remove only Market Pulse consumers and cover the
  canonical route contract with focused tests.
- [Weight changes alter numeric grades] → Keep the score at 100 and document the exact replacement
  weights in tests.
- [Extra chart margins reduce candle area] → Use the minimum top/right spacing needed for readable
  annotations and keep responsive checks.
- [Rollback requires restoring several layers] → Revert this focused change; no data migration or
  persisted state is involved.
