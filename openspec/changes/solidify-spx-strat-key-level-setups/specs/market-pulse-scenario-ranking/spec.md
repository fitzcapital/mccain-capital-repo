## MODIFIED Requirements

### Requirement: Evidence-gated scenario paths
The system SHALL require path-specific completed-candle evidence for reversal, breakout, breakdown,
and Local Flip candidates. Every active candidate MUST include an exact direction-aligned 2-1-2
Up/Down or 2-2 Reversal anchored to its canonical level. A confluence score, gamma state, level
proximity, boundary close, or retest MUST NOT bypass a missing pattern prerequisite.

#### Scenario: Bearish breakdown becomes active
- **WHEN** price completes the required boundary evidence below support and an exact bearish 2-1-2
  or bearish 2-2 Reversal completes from that support level
- **THEN** the breakdown candidate becomes `ACTIVE_NOW` with the exact pattern evidence, a lower
  target, and cancellation on a completed reclaim

#### Scenario: Bullish breakout becomes active
- **WHEN** price completes the required boundary evidence above resistance and an exact bullish
  2-1-2 or bullish 2-2 Reversal completes from that resistance level
- **THEN** the breakout candidate becomes `ACTIVE_NOW` with the exact pattern evidence, a higher
  target, and cancellation on a completed loss of the level

#### Scenario: Initial boundary break is insufficient
- **WHEN** price crosses or retests a level without a supported completed five-minute pattern
- **THEN** the candidate remains pending or alternative and no entry instruction is emitted

### Requirement: Failed-high and failed-low compatibility
The system SHALL preserve ordered failed-sweep reversal requirements for high and low candidates,
require an exact opposing 2-2 Reversal or direction-aligned 2-1-2 at the swept level, and associate
the evidence with a stable candidate id and level.

#### Scenario: Failed high confirms
- **WHEN** an upper level is swept, price closes back below it, and an exact bearish supported
  five-minute pattern completes at that level
- **THEN** the failed-high candidate becomes active on the pattern completion candle with bearish
  direction and the nearest valid lower target

#### Scenario: Failed low confirms
- **WHEN** a lower level is swept, price closes back above it, and an exact bullish supported
  five-minute pattern completes at that level
- **THEN** the failed-low candidate becomes active on the pattern completion candle with bullish
  direction and the nearest valid higher target

### Requirement: Setup Replay terminates and ranks honestly
Setup Replay SHALL use the same exact completed-candle pattern detector and canonical level anchors
as live ranking. For Current-Day High and Current-Day Low, Replay MUST reconstruct the level from
only the completed candles available at the candidate timestamp; a later session extreme MUST NOT
change earlier eligibility. It SHALL terminate with best-to-least ranked results, an explicit empty
state, or a retryable error without blocking the canonical live decision.

#### Scenario: Replay results return
- **WHEN** replay analysis finds eligible historical setups
- **THEN** each result freezes the exact pattern code, direction, anchor level, constituent candles,
  completion time, score, and point-in-time data availability before sorting by descending score and
  deterministic time

#### Scenario: Dynamic level was not yet known
- **WHEN** Replay evaluates a historical candle before a wall, flip, or other gamma-derived level's
  canonical observation timestamp
- **THEN** that dynamic level is unavailable for the candle and MUST NOT create or grade a setup

#### Scenario: Sweep failure matches one reversal family
- **WHEN** a bearish high sweep/failure or bullish low sweep/failure qualifies at a key level
- **THEN** Replay arms only the directionally matching `failed_high` or `failed_low` family
- **AND** it MUST NOT duplicate that event as breakout or breakdown continuation

#### Scenario: Qualified historical location pattern precedes a later live path gate
- **WHEN** an exact supported pattern completes while its formation makes the directionally relevant
  point-in-time CDH/CDL or performs an ordered liquidity sweep and rejection/recovery at a canonical
  level but an additional live execution path gate is not complete on that candle
- **THEN** Replay includes one deduplicated `pattern_observed` row for review
- **AND** it does not label the row as a confirmed live entry

#### Scenario: Pattern merely intersects a level
- **WHEN** an exact 2-1-2 or 2-2 pattern touches or crosses a canonical level without making CDH/CDL
  and without an ordered liquidity sweep and rejection/recovery inside the pattern formation
- **THEN** Replay excludes it from eligible setups as an unqualified location pattern

#### Scenario: Historical generic break
- **WHEN** replay encounters a prior result that satisfied loose close/retest logic but not an exact
  supported pattern at the level
- **THEN** it is excluded from eligible setups and may appear only as rejected diagnostics

#### Scenario: Later high follows a valid bearish CDH setup
- **WHEN** a completed bearish 2-1-2 forms at the then-current CDH and the session prints a higher
  high after the setup
- **THEN** Replay retains the setup anchored to the CDH value known on its completion candle
- **AND** the later high does not retroactively remove or re-anchor it

#### Scenario: Replay request stalls or fails
- **WHEN** replay analysis exceeds its timeout or returns an error
- **THEN** the loading message is replaced by a concise unavailable state and a retry control

### Requirement: Replay separates setup, target, and excursion outcomes

Setup Replay SHALL report the setup lifecycle, structural-target status, and observed price
excursion independently. Reaching a full target MUST remain distinct from favorable movement that
stops short of the target, and neither result SHALL create a recorded trade or financial result.

#### Scenario: Favorable move stops before the target
- **WHEN** price moves favorably after entry but does not reach the structural target
- **THEN** Replay identifies the favorable excursion and states that the full target was not reached
- **AND** it continues to report whether the setup remained open or was subsequently invalidated

#### Scenario: Full structural target is touched
- **WHEN** a later five-minute candle touches the structural target before invalidation
- **THEN** Replay labels the target as fully reached and reports the supporting MFE and MAE

#### Scenario: Target and invalidation occur in one candle
- **WHEN** both boundaries occur inside the same five-minute candle
- **THEN** Replay labels the ordering ambiguous and does not infer a favorable sequence
### Requirement: Setup eligibility precedes quality grading

The system SHALL expose ordered setup maturity independently from quality score. A supported
pattern's final directional candle SHALL make the setup entry-eligible when it breaks the pattern's
STRAT boundary and all location gates are satisfied: the first candle's opposite boundary for 2-2,
or the inside candle's directional boundary for 2-1-2. A later candle MUST NOT be treated as a second
trigger gate. The system SHALL assign a letter grade only to entry-eligible setups. Gamma and
higher-timeframe context SHALL modify quality only and SHALL NOT substitute for a missing
eligibility gate.

#### Scenario: Opposing second candle completes the 2-2 trigger
- **WHEN** the opposing second directional candle completes a supported 2-2 Reversal at a qualified
  location after breaking the first candle's trigger boundary
- **THEN** the setup is `triggered` at that boundary and candle timestamp
- **AND** the quality score and grade are published

#### Scenario: Continuation follows a qualified 2-2
- **WHEN** a later candle continues through the completed 2-2 signal candle's range
- **THEN** it changes only follow-through and outcome facts
- **AND** the original signal timestamp, entry boundary, score eligibility, and pattern identity are
  preserved

#### Scenario: Third candle completes a qualified 2-1-2
- **WHEN** the third directional candle completes a supported 2-1-2 at a qualified location after
  breaking the inside candle's directional boundary
- **THEN** the setup is `triggered` at that boundary and third-candle timestamp
- **AND** no fourth-candle break is required for scoring, replay inclusion, or chart marking
