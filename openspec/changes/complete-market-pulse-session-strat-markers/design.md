## Context

The hero chart receives all active-session bars but renders only a 60-bar default window and trims
the generated marker-object list to 96. Because directional Strat candles produce both an arrow and
a number, the marker trim removes valid early-session annotations. Setup Replay separately chooses
the newest cached snapshot with bars, which can lag the chart's direct completed-bar endpoint.

## Goals / Non-Goals

**Goals:**

- Keep every classifiable active-session candle annotated without duplicate or continuation labels.
- Frame the full regular session on the default five-minute chart.
- Hydrate current-session replay with the latest completed five-minute bars before evaluation.
- Preserve historical point-in-time replay and existing Strat definitions.

**Non-Goals:**

- Changing pattern classification, setup eligibility, targets, Gamma, or market-data providers.
- Making incomplete candles eligible for setup evaluation.

## Decisions

1. Generate marker coverage by active-session candle count, not a small global marker-object cap.
   Retain a bounded safety cap large enough for a full 390-minute one-minute session. This avoids
   unbounded rendering while preserving all normal-session annotations.
2. Use 78 as the five-minute regular-session viewport target. The chart still pans and zooms, while
   a normal 9:30 AM–4:00 PM session is visible by default.
3. For current-session replay only, fetch the existing hero completed-bar payload and overlay its
   current-session bars onto the best cached context before candidate selection. Explicit historical
   dates continue to use frozen cached/archive evidence.
4. Accept only valid, completed, same-session OHLC bars and fall back to the existing cached source
   on provider failure. Replay never fails solely because the live overlay is unavailable.

## Risks / Trade-offs

- More marker objects increase chart work slightly → cap at 800, enough for a full one-minute
  session with directional arrows and numbers.
- A provider request can fail or lag → preserve the existing cached snapshot and expose its honest
  evaluated-through time.
- Default full-session framing makes individual candles narrower → retain zoom and timeframe controls.

## Migration Plan

Deploy the shared K8s image, verify the current-session bar boundary and marker coverage, then check
Setup Replay's evaluated-through label. Rollback is the prior image/manifests; no data migration is
required.

## Open Questions

None.
