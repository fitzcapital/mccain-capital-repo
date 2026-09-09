## Why

Setup Replay can describe one completed Strat reversal as multiple setups when that candle interacts
with nearby structural levels. The duplicate cards, missing grades, overlapping targets, mismatched
session cutoffs, and generic labels make the replay harder to study and can disagree with what the
Live Setup Monitor was actually allowed to alert.

## What Changes

- Define one canonical setup event per completed Strat pattern, timestamp, and direction rather than
  one event per qualifying anchor level.
- Combine compatible nearby levels into one confluence cluster with one primary anchor and visible
  supporting levels.
- Select targets beyond the setup's anchor cluster and reject targets that do not provide meaningful
  directional space.
- Require every eligible replay card to expose a numeric score and matching letter grade.
- Use level-aware reversal language so the card states what price swept, reclaimed, or rejected.
- Apply the same 3:15 PM ET new-setup cutoff to Live Setup Monitor and Setup Replay while preserving
  already-confirmed setup lifecycle tracking.
- Preserve distinct completed patterns, even when they occur near the same level or in the same
  direction; only duplicate representations of the same completion are merged.
- Add diagnostics explaining merged supporting levels and rejected target choices.
- Align replay outcomes with the written trade plan: targets resolve on intrabar touch, while
  invalidation requires a completed five-minute close through the anchor.
- Timestamp target touches, invalidating closes, ambiguous candles, and the last evaluated candle
  for unresolved setups; retain MFE/MAE as measurements rather than implied lifecycle events.
- Add compact estimated +15%, +20%, and +30% option-return SPX price targets using disclosed
  contract-cost and absolute-delta assumptions, while retaining the structural level as Runner.
- State clearly that dealer Gamma affects confluence only and is not used in TP estimation.
- Treat an exact completed reversal as located at an SPX key level when either pattern candle comes
  within 0.25 SPX points of that level, while retaining exact Strat classification requirements.
- Recover a dynamic level's point-in-time value from an existing durable Live setup event when the
  newest Gamma snapshot was computed after the historical replay candle.
- Non-goals: changing Strat candle classification, counting continuation candles as reversals,
  changing gamma/VWAP inputs, deleting durable setup history, or retroactively issuing alerts.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-scenario-ranking`: Canonicalize replay events across nearby anchors, require coherent
  scoring and contextual labels, and choose a meaningful target outside the anchor cluster.
- `market-pulse-live-state-coherence`: Use the canonical event identity and common 3:15 PM ET
  admission cutoff consistently across live monitoring and replay.

## Impact

- Affected services: replay analysis, scenario generation/ranking, Strat setup event construction,
  live setup ledger reconciliation, scoring, and target selection.
- Affected UI: Setup Replay and Live Setup Monitor summaries on Market Pulse.
- Affected data: canonical SPX five-minute bars and the existing point-in-time structural/gamma
  context; no runtime data migration or destructive ledger rewrite is required.
- Acceptance criteria: the audited 10:05 AM event renders once with nearby levels shown as
  confluence; every card has a score/grade; post-3:15 PM completions are not admitted as new setups;
  continuation candles do not create reversal cards; targets skip the anchor cluster and satisfy the
  existing minimum target-space rule; target touches and close-based invalidations show their actual
  candle times; unresolved setups show their evaluation-through time; repeated evaluation remains
  idempotent in live and replay; the audited 11:30 AM reversal 0.19 points below Call Wall is
  retained without admitting continuation candles.
