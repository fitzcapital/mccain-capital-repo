## ADDED Requirements

### Requirement: Multi-level scenario generation
The system SHALL generate candidate scenarios for failed high, failed low, bearish breakdown,
bullish breakout, and Local Flip loss/reclaim from every valid supported structural level. Waiting
on one candidate MUST NOT prevent another level or family from becoming active.

#### Scenario: Distant high remains dormant while Local Flip breaks
- **WHEN** Current-Day High has not been retested and a completed five-minute candle loses Local
  Flip with compatible bearish evidence
- **THEN** the high-rejection candidate remains dormant and the Local Flip breakdown candidate is
  evaluated independently for active or alternative status

#### Scenario: Invalid level is excluded
- **WHEN** a supported level is missing, non-finite, stale, or incompatible with the candidate
  evidence
- **THEN** the system does not generate an actionable candidate from that level and identifies the
  unavailable input in diagnostics

### Requirement: Evidence-gated scenario paths
The system SHALL require path-specific completed-candle evidence for reversal, breakout, breakdown,
and Local Flip candidates. A confluence score MUST NOT bypass a missing prerequisite.

#### Scenario: Bearish breakdown becomes active
- **WHEN** price completes a five-minute close below support, retests from underneath, fails to
  reclaim it, and provides the required bearish confirmation
- **THEN** the breakdown candidate becomes `ACTIVE_NOW` with an action below support, a lower target,
  and cancellation on a completed reclaim

#### Scenario: Bullish breakout becomes active
- **WHEN** price completes a five-minute close above resistance, retests it successfully from above,
  and provides the required bullish confirmation
- **THEN** the breakout candidate becomes `ACTIVE_NOW` with an action above resistance, a higher
  target, and cancellation on a completed loss of the level

#### Scenario: Initial boundary break is insufficient
- **WHEN** price crosses a level intrabar without the required completed close or retest evidence
- **THEN** the candidate remains pending or alternative and no entry instruction is emitted

### Requirement: Failed-high and failed-low compatibility
The system SHALL preserve the ordered failed-sweep reversal requirements for high and low candidates
and SHALL associate their evidence with a stable candidate id and level.

#### Scenario: Failed high confirms
- **WHEN** an upper level is swept, price closes back below it, the bearish five-minute Strat sequence
  confirms, and its trigger breaks
- **THEN** the failed-high candidate becomes active with bearish direction and the nearest valid lower
  target

#### Scenario: Failed low confirms
- **WHEN** a lower level is swept, price closes back above it, the bullish five-minute Strat sequence
  confirms, and its trigger breaks
- **THEN** the failed-low candidate becomes active with bullish direction and the nearest valid higher
  target

### Requirement: Deterministic confluence scoring
The system SHALL score eligible candidates on a fixed 0–100 scale using explicit components for
location, completed boundary close, retest, Strat confirmation, gamma alignment, level clustering,
VWAP alignment, and target space. The payload SHALL expose earned points and unavailable evidence
for every component.

#### Scenario: Missing optional evidence does not inflate score
- **WHEN** VWAP or Strat evidence is unavailable
- **THEN** that component earns zero points, the denominator remains 100, and the missing evidence is
  disclosed

#### Scenario: Equal candidates sort deterministically
- **WHEN** two candidates have the same lane and score
- **THEN** the nearer level sorts first, followed by stable level and scenario-family priority

### Requirement: Ranked scenario lanes
The system SHALL assign candidates to `ACTIVE_NOW`, `ALTERNATIVE`, or `DORMANT` from evidence state
before using score to order candidates within the lane.

#### Scenario: Confirmed candidate outranks high-scoring dormant candidate
- **WHEN** a confirmed Local Flip breakdown has a lower numeric score than a distant high-rejection
  candidate
- **THEN** the confirmed breakdown remains the primary scenario because lane state takes precedence
  over score

#### Scenario: No scenario is confirmed
- **WHEN** no candidate satisfies its confirmation gates
- **THEN** the page shows the best alternative as a specific watch plan and does not label it active
  or actionable

### Requirement: Structured execution plan per scenario
Every ranked candidate SHALL provide exact structured location, trigger, action, target, and
cancellation fields. Waiting SHALL apply to that scenario rather than the entire market.

#### Scenario: Alternative breakdown plan
- **WHEN** price is approaching Local Flip but has not completed a breakdown
- **THEN** the candidate names Local Flip and its value, requires a completed close and failed reclaim,
  withholds entry and target authorization, and defines the reclaim that cancels the thesis

#### Scenario: Active plan refreshes to a new level
- **WHEN** price confirms a different candidate and the previous primary becomes invalid or dormant
- **THEN** the primary card moves to the newly ranked scenario in one canonical refresh without
  retaining action, target, or cancellation copy from the old candidate

### Requirement: Data safety and mutual exclusion
Critical stale data SHALL lock all scenarios, and contradictory bullish/bearish or
acceptance/rejection evidence for one candidate MUST NOT produce simultaneous active paths.

#### Scenario: Stale data with a previously active candidate
- **WHEN** required data becomes critically stale after a candidate was active
- **THEN** the page preserves last-valid context but locks execution and displays no current active
  instruction

#### Scenario: Conflicting evidence affects one candidate
- **WHEN** one level reports incompatible acceptance and rejection evidence
- **THEN** that candidate is invalidated or unavailable while independently valid candidates at
  other levels remain rankable

### Requirement: Compact scenario presentation
The Market Pulse page SHALL present one primary scenario, at least one compact alternative when
available, and a collapsed dormant watch list without obscuring the chart or gamma ladder.

#### Scenario: Multiple candidates render
- **WHEN** active, alternative, and dormant candidates are available
- **THEN** the primary lane exposes its complete five-stage plan, secondary lanes expose concise
  state and trigger summaries, and each lane is visually distinguishable without implying that a
  pending lane is actionable

### Requirement: Canonical in-place scenario refresh
Scenario rankings, confluence components, decision language, checklist state, and targets SHALL
update from one validated canonical payload without reloading the page.

#### Scenario: Successful scenario transition
- **WHEN** a refresh changes a Local Flip breakdown from alternative to active
- **THEN** all scenario and primary-decision nodes update together while chart viewport, drawings,
  timeframe, and gamma selection remain unchanged

#### Scenario: Refresh failure
- **WHEN** a scenario refresh fails validation
- **THEN** the last valid generation remains visible with failure and timestamp feedback and no
  partial new scenario fields are committed

### Requirement: Coherent grade and confluence score
The Trade Decision letter grade and numeric confluence score SHALL derive from the same primary
scenario score and SHALL display together with visible grade bands. A legacy page-wide grade MUST
NOT be paired with a different scenario score.

#### Scenario: Thirty-point scenario
- **WHEN** the primary scenario earns 30 of 100 confluence points
- **THEN** the Trade Decision displays an F-range grade beside `30/100 confluence`, not an A grade

#### Scenario: High confluence remains unconfirmed
- **WHEN** a scenario earns an A-range score but lacks a required hard trigger
- **THEN** the card may display the A-range confluence grade but remains `WAIT` and explains that
  confirmation—not score—is blocking execution
