## ADDED Requirements

### Requirement: Canonical replay event per completed pattern
Setup Replay SHALL emit at most one setup event for one symbol, session, completed Strat pattern,
completion timestamp, and direction. Qualifying structural levels MUST describe that event's
location and MUST NOT create duplicate cards.

#### Scenario: One completion qualifies at two nearby highs
- **WHEN** one bearish reversal completion qualifies at Prior-Day High and Current-Day High
- **THEN** Replay displays one setup card with one primary anchor and the other compatible level as
  supporting confluence

#### Scenario: Separate completions revisit the same cluster
- **WHEN** two valid reversal patterns complete on different candles near the same level cluster
- **THEN** Replay preserves two independently timed setup events

#### Scenario: Continuation follows a reversal
- **WHEN** same-direction continuation candles follow a completed reversal pattern
- **THEN** those continuation candles do not create additional reversal setup events

### Requirement: Deterministic setup level confluence
Each canonical setup event SHALL select one deterministic primary anchor from point-in-time
sweep/reclaim or sweep/reject evidence and SHALL expose every compatible nearby qualifying anchor as
supporting confluence with its name, value, and role.

#### Scenario: Several anchors qualify equally
- **WHEN** several compatible anchors qualify for the same completion and evidence strength ties
- **THEN** stable structural-level priority and proximity select the same primary anchor on every run

#### Scenario: Incompatible anchor is nearby
- **WHEN** a nearby level does not satisfy the completed candle's direction and location evidence
- **THEN** that level is excluded from the setup's confluence cluster

### Requirement: Narrow SPX key-level proximity
An exact completed supported reversal SHALL qualify for SPX key-level location when either pattern
candle spans the canonical level or comes within 0.25 SPX points. Proximity MUST NOT change Strat
classification, admit continuation candles, or create a setup without a supported reversal.

#### Scenario: Reversal misses Call Wall by nineteen cents
- **WHEN** a `2U` candle highs at 7,769.81, Call Wall is 7,770.00, and the next completed candle is
  an exact `2D`
- **THEN** Replay and Live recognize one `2-2 REV D` at Call Wall

#### Scenario: Newer Gamma replaces the current wall snapshot
- **WHEN** Live durably recorded Call Wall 7,770.00 at the reversal timestamp and Replay later sees
  a newer Gamma observation
- **THEN** Replay uses the durable 7,770.00 point-in-time observation for that candle without
  projecting the newer wall backward

#### Scenario: Continuation is near a level
- **WHEN** a same-direction continuation candle comes within 0.25 points of a canonical level
- **THEN** proximity does not relabel it as a reversal setup

#### Scenario: Reversal is outside tolerance
- **WHEN** no pattern candle spans or comes within 0.25 points of the canonical level
- **THEN** the level does not qualify as the reversal's location

### Requirement: Complete replay score and grade
Every eligible Setup Replay card SHALL contain a numeric 0–100 score, its component evidence, and a
letter grade derived from that score. Immediate signal candidates MUST use the same scoring contract
as all other eligible candidates.

#### Scenario: Immediate candidate lacks a cached quality score
- **WHEN** a pattern qualifies immediately but its source candidate does not contain a cached score
- **THEN** the replay builder computes the canonical score and matching grade before rendering it

#### Scenario: Optional evidence is unavailable
- **WHEN** gamma or another optional component is unavailable at signal time
- **THEN** that component earns zero, remains disclosed as unavailable, and the card still contains a
  coherent score and grade

### Requirement: Meaningful target outside the anchor cluster
Setup target selection SHALL exclude the primary anchor and all supporting anchors in its confluence
cluster. The selected target SHALL be the nearest eligible level in the setup direction that meets
the existing minimum target-space requirement.

#### Scenario: Adjacent structural level belongs to the cluster
- **WHEN** the nearest directional level is part of the setup's anchor cluster
- **THEN** target selection skips it and evaluates the next eligible directional level

#### Scenario: No meaningful target is available
- **WHEN** no directional level beyond the cluster satisfies minimum target space
- **THEN** Replay marks the target unavailable and does not present the event as actionable

### Requirement: Contextual replay language
Setup Replay SHALL name the price action and primary structural location in the visible title and
trigger description while retaining the stable scenario family for filtering.

#### Scenario: Bearish reversal rejects a prior-day high
- **WHEN** a bearish setup sweeps and closes back below Prior-Day High
- **THEN** the visible setup language identifies a sweep and rejection of Prior-Day High

#### Scenario: Bullish reversal reclaims a current-day low
- **WHEN** a bullish setup sweeps and closes back above Current-Day Low
- **THEN** the visible setup language identifies a sweep and reclaim of Current-Day Low

### Requirement: Timestamped plan-consistent replay outcomes
Setup Replay SHALL resolve a target when a later completed candle's directional extreme touches or
passes the target. It SHALL resolve invalidation only when a later completed five-minute candle
closes through the anchor in the adverse direction. Every terminal outcome SHALL name its event
candle time, and every unresolved outcome SHALL name the latest completed candle through which it
was evaluated.

#### Scenario: Later candle touches bearish target
- **WHEN** a bearish setup's later five-minute low reaches or falls below its target
- **THEN** Replay reports `Target touched at <time>` and the planned point distance from entry

#### Scenario: Wick crosses anchor but close remains valid
- **WHEN** a later bearish candle trades above the anchor but completes at or below it
- **THEN** Replay keeps the setup valid because the completed-close invalidation rule did not occur

#### Scenario: Completed close invalidates setup
- **WHEN** a later bearish candle completes above the anchor
- **THEN** Replay reports `Invalidated on the <time> close`

#### Scenario: Setup remains unresolved
- **WHEN** neither the target-touch nor completed-close invalidation condition occurs
- **THEN** Replay reports `Still open as of the <time> candle`

#### Scenario: Target and invalidation occur on one candle
- **WHEN** the target extreme and adverse invalidating close first occur on the same completed candle
- **THEN** Replay reports ambiguous ordering and names that candle time

### Requirement: Compact estimated option-return price targets
Each Replay setup SHALL expose estimated SPX prices for +15%, +20%, and +30% option returns using a
disclosed contract-cost and absolute-delta assumption. The structural target SHALL remain separately
identified as Runner. Dealer Gamma availability MUST NOT change the TP calculation and SHALL be
described separately from its confluence role.

#### Scenario: Bearish setup receives estimated targets
- **WHEN** a bearish setup enters at 7,685.13 using a $750 contract and 0.40 absolute delta estimate
- **THEN** Replay exposes compact SPX targets near 7,682.32, 7,681.38, and 7,679.50 for +15%, +20%,
  and +30%, respectively

#### Scenario: Dealer Gamma is unavailable
- **WHEN** dealer Gamma was unavailable at signal time
- **THEN** Replay states that dealer Gamma is unavailable and not used for TP estimates
