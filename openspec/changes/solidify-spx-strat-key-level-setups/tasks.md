## 1. Exact Pattern Engine

- [x] 1.1 Add pure completed-bar `1`, `2U`, `2D`, and `3` classification
- [x] 1.2 Detect exact 2-1-2 Up/Down and opposing 2-2 Reversal sequences
- [x] 1.3 Require canonical key-level interaction and expose complete pattern provenance

## 2. Canonical Scenario Integration

- [x] 2.1 Replace loose candle evidence reversal logic with the exact shared detector
- [x] 2.2 Hard-gate active scenario ranking on a direction-aligned anchored pattern
- [x] 2.3 Freeze exact pattern evidence in Setup Replay and rejected diagnostics
- [x] 2.4 Update live execution labels to name only supported pattern families

## 3. Verification

- [x] 3.1 Add unit fixtures for valid bullish and bearish 2-1-2 and 2-2 patterns
- [x] 3.2 Add rejection fixtures for outside bars, generic breaks, mid-range patterns, and unfinished bars
- [x] 3.3 Verify scenario ranking, live monitor, replay ordering, and existing failed-sweep safety tests
- [x] 3.4 Run focused formatting, syntax, OpenSpec validation, and deployed receiving-surface checks

## 4. Point-in-Time Session Extremes

- [x] 4.1 Reconstruct CDH and CDL from each completed replay bar prefix
- [x] 4.2 Use point-in-time session extremes for pattern anchoring and target selection
- [x] 4.3 Add a regression fixture where a later high must not erase an earlier CDH 2-1-2 Down
- [x] 4.4 Rebuild and verify the missed CDH setup appears on the deployed Setup Replay surface

## 5. Canonical Replay Bar Source

- [x] 5.1 Make Setup Replay prefer the newest canonical context generation with non-empty completed 5-minute bars
- [x] 5.2 Preserve snapshot-derived structure, levels, gamma provenance, and session-date isolation when the replay bar source falls back
- [x] 5.3 Add an API regression proving replay remains populated when the playbook snapshot has no bars but canonical context does
- [x] 5.4 Rebuild and verify today’s deployed replay receives bars and evaluates the approximately 11:50 AM 2-1-2 Down candidate

## 6. Location-event Replay Correction

- [x] 6.1 Require CDH/CDL creation or an ordered liquidity sweep inside the pattern formation
- [x] 6.2 Expose frozen location-event provenance on every eligible replay row
- [x] 6.3 Reject random mid-range and ordinary wall 2-1-2/2-2 patterns in focused regression tests
- [x] 6.4 Collapse duplicate scenario-family rows for the same qualified setup
- [x] 6.5 Rebuild and verify the deployed noon CDH setup without promoting unrelated patterns

## 7. Ordered Eligibility And Quality Grading

- [x] 7.1 Separate setup maturity from quality grading and expose the ordered gate state
- [x] 7.2 Preserve completed-candle trigger evidence before entry eligibility
- [x] 7.3 Keep gamma and higher-timeframe context as quality modifiers, never eligibility substitutes
- [x] 7.4 Report incomplete patterns as watches/diagnostics rather than historical trades
- [x] 7.5 Add focused regressions, rebuild, and verify the deployed live and replay payloads

## 8. Point-in-time Level And Family Integrity

- [x] 8.1 Attach canonical observation timestamps to gamma-derived replay levels
- [x] 8.2 Exclude dynamic levels before their observation time while preserving prior-day and reconstructed session levels
- [x] 8.3 Map sweep/failure events only to failed-high or failed-low and reject continuation duplicates
- [x] 8.4 Add focused regressions and verify today's replay count against the corrected rules

## 9. Replay Entry Window

- [x] 9.1 Exclude replay entry confirmations after 3:30 PM America/New_York while retaining later bars for prior-setup outcome review
- [x] 9.2 State the replay cutoff in the Setup Replay controls and response contract
- [x] 9.3 Add focused cutoff regressions and rebuild the local app

## 10. Replay Outcome Clarity

- [x] 10.1 Expose separate setup-status, target-status, excursion, and target-progress fields
- [x] 10.2 Render clear full-target, favorable-excursion, limited-follow-through, invalidated, and ambiguous labels
- [x] 10.3 Add focused outcome regressions and verify the rebuilt deployed surface

## 11. Correct 2-2 Trigger Attribution

- [x] 11.1 Make an opposing second directional candle entry-eligible at the first candle's broken
  boundary without requiring a third-candle continuation break
- [x] 11.2 Freeze the true 2-2 signal timestamp and trigger-boundary entry price in replay payloads
- [x] 11.3 Calculate MFE, MAE, target progress, target status, and invalidation from the displayed
  trigger-boundary entry using only subsequent candles
- [x] 11.4 Update Setup Replay labels and chart markers to distinguish pattern trigger from later
  continuation/follow-through
- [x] 11.5 Add focused bullish and bearish regressions proving a third candle cannot replace the
  original 2-2 signal, then rebuild and verify the deployed Market Pulse surface

## 12. Apply Trigger Attribution To Every Supported Pattern

- [x] 12.1 Make bullish and bearish 2-1-2 patterns entry-eligible on their third directional candle
  at the inside candle's broken boundary without requiring a fourth candle
- [x] 12.2 Generalize replay signal metadata and entry-basis labels across 2-1-2 and 2-2 families
- [x] 12.3 Add focused bullish and bearish 2-1-2 regressions proving later candles affect only
  follow-through and outcome measurements
- [x] 12.4 Rebuild and verify the deployed Setup Replay uses one consistent trigger-attribution rule
  for every supported pattern
