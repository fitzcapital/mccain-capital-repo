## ADDED Requirements

### Requirement: Ordered failed-sweep state evaluation
The system SHALL evaluate a reversal as an ordered state machine with states equivalent to
`WAITING_FOR_LOCATION`, `LEVEL_BEING_TESTED`, `SWEEP_DETECTED`,
`WAITING_FOR_CLOSE_BACK_INSIDE`, `WAITING_FOR_5M_2_2`, `WAITING_FOR_TRIGGER_BREAK`,
`REVERSAL_READY`, `SETUP_INVALIDATED`, plus the acceptance states defined below. A later event
MUST NOT satisfy an earlier missing event.

#### Scenario: Bearish sequence becomes ready
- **WHEN** price reaches and sweeps above a valid upper level, a completed five-minute candle
  closes back below it, a bearish five-minute 2-2 forms, and price then breaks the trigger low
- **THEN** the state is `REVERSAL_READY` with bearish direction and sell-the-rip interpretation

#### Scenario: Bullish sequence becomes ready
- **WHEN** price reaches and sweeps below a valid lower level, a completed five-minute candle
  closes back above it, a bullish five-minute 2-2 forms, and price then breaks the trigger high
- **THEN** the state is `REVERSAL_READY` with bullish direction and buy-the-dip interpretation

#### Scenario: Trigger cannot skip prerequisites
- **WHEN** a trigger boundary breaks without valid sweep, close-back-inside, and five-minute 2-2
  evidence in order
- **THEN** the setup remains at the earliest unmet state and is not reversal-ready

#### Scenario: Active context changes
- **WHEN** the active level, interaction direction, timeframe evidence, or gamma regime changes
- **THEN** the system recomputes the result from compatible evidence and does not carry
  incompatible completion forward

### Requirement: Supported levels and interaction direction
The system SHALL evaluate available Call Wall, Put Wall, Gamma Flip, New Call Wall, New Put Wall,
prior-day high/low, and current-day high/low levels and SHALL identify the active level and whether
it is being tested from above or below. It MUST NOT infer that merely being above or below a level
confirms a setup.

#### Scenario: Put Wall tested from below
- **WHEN** price approaches Put Wall from below without confirmed acceptance or failed breakout
- **THEN** the output identifies a test from below and says to wait for acceptance above or a
  failed breakout that closes back below and completes the bearish reversal sequence

#### Scenario: No usable active level
- **WHEN** no supported level value and interaction evidence are available
- **THEN** the state and checklist location are unavailable and no directional setup is confirmed

### Requirement: Rejection and acceptance are mutually exclusive
The system SHALL distinguish rejection-driven reversal from acceptance-driven continuation.
Confirmed acceptance beyond an active level SHALL prevent `REVERSAL_READY`, and reversal-ready
output SHALL prevent acceptance or continuation output for the same evaluation.

#### Scenario: Break and hold above Call Wall
- **WHEN** price breaks Call Wall and a completed candle holds beyond it or a retest succeeds
- **THEN** the state is `ACCEPTANCE_CONFIRMED` or `CONTINUATION_ACTIVE`, no bearish fade is
  recommended, and the next higher relevant level is identified

#### Scenario: Neither path confirmed
- **WHEN** a level is being tested but neither rejection nor acceptance has sufficient evidence
- **THEN** the system reports a pending state and names the next missing evidence

#### Scenario: Conflicting evidence
- **WHEN** input simultaneously claims acceptance and reversal confirmation for one interaction
- **THEN** the system does not emit both paths and returns an invalidated or unavailable result
  with the conflict identified

### Requirement: Gamma-aware interpretation and targets
The system SHALL use gamma regime to alter movement and management expectations without using it
as an automatic setup veto or directional prediction. Targets SHALL be selected deterministically
from available relevant liquidity and structural levels in the reversal or continuation direction.

#### Scenario: Positive-gamma failed high sweep
- **WHEN** a bearish failed sweep above Call Wall completes in positive gamma
- **THEN** the state is `REVERSAL_READY`, the nearest lower valid level is the primary target, and
  management describes a quick mean-reversion scalp toward the nearest meaningful target

#### Scenario: Negative-gamma failed high sweep
- **WHEN** a bearish failed sweep above prior-day high or Call Wall completes in negative gamma
- **THEN** the nearest lower valid level is Target 1, additional lower valid levels are expansion
  targets, and a supportive fifteen-minute 2-2 is runner confirmation rather than an entry gate

#### Scenario: Negative-gamma continuation
- **WHEN** acceptance confirms beyond a level in negative gamma
- **THEN** the output describes expansion in the accepted direction rather than assuming every
  negative-gamma interaction is continuation

#### Scenario: Gamma Flip transition
- **WHEN** price is crossing or unresolved around Gamma Flip
- **THEN** confidence is reduced and the output requires clear rejection or acceptance

### Requirement: Evidence-safe missing and malformed data behavior
The system SHALL derive all confirmations from explicit valid evidence. Missing, stale, or malformed
required market data MUST produce Pending or Unavailable results, MUST NOT fabricate completion,
and MUST NOT cause malformed JSON rendering or a runtime error.

#### Scenario: Incomplete sweep
- **WHEN** price touches or briefly trades around a level without valid sweep or completed
  close-back-inside evidence
- **THEN** the state remains pending, setup-ready is false, and the next required evidence is
  described

#### Scenario: Required candle data missing
- **WHEN** five-minute candle or Strat evidence is absent
- **THEN** the corresponding state and checklist item are unavailable or pending and trigger
  evidence cannot make the setup ready

#### Scenario: Malformed payload member
- **WHEN** a strategy input contains an invalid type, non-finite value, or malformed structure
- **THEN** it is normalized as unavailable, the payload remains valid/renderable, and the page
  continues to load

### Requirement: Compact checklist reflects ordered evidence
The SPX Playbook SHALL show exactly the strategy checklist items Location reached, Liquidity swept,
Five-minute close back inside, Five-minute 2-2 confirmed, Trigger broken, and Setup ready. Each item
SHALL display Pending, Confirmed, Failed, or Unavailable from the state-engine result.

#### Scenario: Partial ordered progress
- **WHEN** location, sweep, and close-back-inside are confirmed but no five-minute 2-2 exists
- **THEN** the first three items are Confirmed, the 2-2 and subsequent items are Pending or
  Unavailable as evidence allows, and Setup ready is not Confirmed

#### Scenario: Acceptance blocks checklist readiness
- **WHEN** acceptance is confirmed beyond the active level
- **THEN** reversal-only remaining items and Setup ready are Failed or otherwise explicitly
  non-confirmed, never Confirmed

### Requirement: Deterministic compact decision card
The decision card SHALL display the active level, test direction, current strategy state, reversal
direction when established, rejection or acceptance status, gamma interpretation, nearest logical
target, applicable negative-gamma expansion targets, and concise missing evidence. The card SHALL
use structured deterministic output instead of generated commentary.

#### Scenario: Decision card for pending interaction
- **WHEN** a supported level is being tested without a completed path
- **THEN** the card identifies the interaction accurately, withholds a trade-ready recommendation,
  and names the earliest missing evidence

#### Scenario: Decision card for reversal ready
- **WHEN** the ordered reversal sequence completes
- **THEN** the card shows reversal direction, rejection confirmed, regime-appropriate management,
  primary target, and expansion targets only when applicable

### Requirement: Existing Playbook experience remains intact
The change SHALL preserve the current McCain Capital dark styling, compact page length, chart,
gamma ladder, navigation, market-data providers, refresh behavior, and unrelated working features.
It SHALL NOT add risk-management or account-control UI.

#### Scenario: Desktop and narrow viewport rendering
- **WHEN** the SPX Playbook is rendered at desktop and narrow viewport widths
- **THEN** status text and checklist states remain readable without material overflow or page-length
  growth, and the chart and gamma ladder remain usable

#### Scenario: Scope exclusion
- **WHEN** the updated page is inspected
- **THEN** no risk-control, risk-calculator, daily-loss, account-locking, or contract-sizing panel
  has been added

### Requirement: Canonical manual refresh
The Playbook SHALL expose one clearly labeled `Refresh data` control near the updated timestamp.
The control SHALL refresh the canonical quote, gamma, strategy, guardrail, chart-context, and
checklist state together and SHALL NOT leave competing full-page refresh controls visible.

#### Scenario: Successful refresh
- **WHEN** the user activates `Refresh data` and the providers return valid current data
- **THEN** the control displays a refreshing state, prevents duplicate activation, and completes
  with all shared spot values, timestamps, guardrail, decision, and setup surfaces derived from the
  same refreshed contract

#### Scenario: Failed refresh
- **WHEN** the manual refresh fails or returns invalid required data
- **THEN** the last valid values remain primary, the control reports that refresh failed and last
  valid data is shown, and no confirmation or actionable state is fabricated

### Requirement: Decision hierarchy is internally coherent
The Playbook SHALL apply data safety first, then ordered strategy state, then gamma interpretation
and targets. A locked, unavailable, pending, or non-ready setup MUST NOT render as `Actionable` or
`Trigger confirmed` anywhere on the page.

#### Scenario: Critical stale guardrail is active
- **WHEN** the data guardrail is locked by critical-stale required quotes
- **THEN** the execution read is `Data locked`, the detail explains the stale-data condition, and
  actionable or trigger-confirmed language is absent

#### Scenario: Data is healthy but reversal remains pending
- **WHEN** required data is current but the strategy state is not `REVERSAL_READY` and continuation
  is not active
- **THEN** the execution read is `Not actionable yet` and the earliest missing evidence is shown

#### Scenario: Strategy is ready with healthy data
- **WHEN** the data guardrail is clear and the state engine returns `REVERSAL_READY`
- **THEN** the execution read may be `Actionable` and the trigger detail reflects the confirmed
  ordered strategy evidence
