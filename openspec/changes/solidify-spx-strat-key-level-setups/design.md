## Context

Scenario ranking currently derives continuation confirmation from two closes plus a retest, while
failed sweeps consume a broad `REVERSAL_READY` strategy state. Candle evidence calls any opposing
directional break a 2-2 and waits for an additional trigger bar. Replay invokes the same ranker, but
does not freeze an exact pattern identity. These paths can disagree with the user's actual SPX
five-minute playbook.

## Goals / Non-Goals

**Goals:**

- Classify completed bars deterministically as `1`, `2U`, `2D`, or `3` relative to the prior bar.
- Detect exact 2-1-2 Up/Down and opposing 2-2 Reversal patterns.
- Require a supported canonical level to intersect the pattern's formation window.
- Reuse one pattern result in live ranking, monitoring, and point-in-time replay.
- Fail closed when bars, level identity, or freshness are unavailable.

**Non-Goals:**

- Predicting an unfinished candle, supporting 3-1-2, expanding beyond SPX, or executing trades.
- Treating gamma, score, proximity, or a level cross as a replacement for the exact pattern.

## Decisions

1. **Use a pure shared classifier.** A small service will classify each bar against its immediate
   predecessor. Inclusive boundaries are treated as inside unless the opposite boundary breaks;
   a bar breaking both sides is `3`, never `2U` or `2D`. This avoids duplicated live/replay logic.
2. **End the signal on the confirming completed candle.** For 2-1-2, the third pattern candle is the
   signal. For 2-2 Reversal, the opposing second directional candle is the signal; no unrelated
   fourth candle is required.
3. **Anchor by actual pattern interaction.** At least one candle in the named pattern must span the
   canonical level. Proximity alone does not qualify. CDH uses the same canonical level mechanism as
   Call Wall, Put Wall, Local Flip, prior-day levels, and other supported Market Pulse levels.
4. **Pattern gates action; context ranks quality.** Boundary close/retest, gamma, clustering, and
   target space remain score components, but a candidate cannot be `confirmed` without a supported
   pattern aligned to its direction and level.
5. **Freeze evidence in replay payloads.** Pattern code, family, direction, level, constituent bar
   times, completion time, and provenance are copied into each replay result for honest review.
6. **Reconstruct evolving session extremes and respect level provenance in replay.** CDH and CDL are
   calculated from the bar prefix available at each historical signal. Prior-day levels are known at
   the open. Gamma-derived walls and flips are eligible only at or after their canonical observation
   timestamp; an end-of-session snapshot is never projected backward. This prevents both later
   extremes and later level observations from rewriting earlier setup eligibility.
7. **Require the location event before historical pattern observation.** Replay may record an exact
   supported pattern before a later live path gate only when the pattern formation itself makes the
   directionally relevant CDH/CDL or performs an ordered liquidity sweep and rejection/recovery at
   the canonical level. Merely touching or crossing a wall is not a setup. The row is explicitly
   marked `pattern_observed`, never as a confirmed live entry, and duplicate scenario families
   collapse to the strongest row.
8. **Keep reversal and continuation paths distinct.** A sweep followed by failure/recovery may arm
   only `failed_high` or `failed_low`. Breakout and breakdown require acceptance plus a pullback hold
   and cannot borrow a reversal location event, preventing one pattern from appearing as two trades.

## Risks / Trade-offs

- **[Fewer live callouts]** Exact confirmation intentionally rejects prior loose matches → expose
  rejected diagnostics with a precise missing-pattern reason.
- **[Equal highs/lows vary by feed precision]** Inclusive comparisons can affect classification →
  normalize finite OHLC values and cover equality boundaries with fixtures.
- **[Historical results change]** Replay counts will drop and timestamps may move to the actual
  confirming candle → label results with their exact pattern and keep them read-only.
- **[Final-session CDH/CDL can rewrite history]** A later extreme can hide a valid earlier setup →
  derive current-day extremes point-in-time from each replay prefix.
- **[Level interaction can occur just before the pattern]** Strict pattern-window anchoring may miss
  discretionary context → keep the first version conservative; do not silently widen the window.

## Migration Plan

Deploy the pure classifier and tests first, then integrate it into candle evidence, scenario ranking,
live payloads, and replay. No stored data migration is required. Rollback consists of reverting these
service and presentation changes; canonical market data remains unchanged.

## Open Questions

None. The first release intentionally uses strict completed-candle and pattern-window rules.
# Ordered eligibility and grading

Setup maturity and setup quality are separate concepts. A reversal progresses through location,
sweep, failure, completed five-minute Strat pattern, armed trigger, and a later break of the trigger
candle. A continuation progresses through break, acceptance, pullback hold, armed trigger, and a
later break. Only the final trigger break makes a setup entry-eligible.

Quality is scored only after entry eligibility. Location, ordered structural evidence, the exact
five-minute pattern, trigger quality, target space, gamma context, and optional higher-timeframe
context may improve or reduce the grade. Gamma and higher-timeframe context must never manufacture
eligibility. Replay may retain incomplete observations as diagnostics but must not count them as
historical trade setups.
## Replay execution-window follow-up

- New replay entries are eligible through 3:30 PM America/New_York and excluded afterward.
- The cutoff applies to confirmation/entry time, not merely pattern discovery time.
- Bars after the cutoff remain available for MFE, MAE, target, and invalidation outcomes of setups that qualified earlier.
- The response and replay controls expose the cutoff so a missing late-day row is explainable rather than appearing to be a detector failure.
