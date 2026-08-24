## ADDED Requirements

### Requirement: Equal-weight execution decision rail
The Market Pulse page SHALL present Execution Read, Active Level, Next Evidence, and Invalidation
as a visually connected, equal-weight decision sequence while preserving Execution Read as the
authoritative permission state.

#### Scenario: Decision rail renders
- **WHEN** a user opens Market Pulse with canonical strategy context
- **THEN** all four decisions are prominent, individually identifiable, and available without
  expanding another section

#### Scenario: Locked data
- **WHEN** Execution Read is data locked
- **THEN** the rail clearly communicates the lock without visually suppressing Active Level, Next
  Evidence, or Invalidation

### Requirement: Shared decision guidance
The execution decision rail SHALL provide one accessible help control that explains the purpose
of all four decisions and the meaning of execution states.

#### Scenario: User opens decision guidance
- **WHEN** the user activates the shared help control
- **THEN** the page explains whether the user can act, where the decision occurs, what evidence is
  required, and what invalidates the thesis

### Requirement: Semantic candle direction palette
The hero chart SHALL render bullish current-session candles with soft-white bodies and bearish
current-session candles with deep-blue bodies, with coordinated silver/electric-blue borders,
cool-gray wicks, volume, and prior-session treatments.

#### Scenario: Mixed-direction chart
- **WHEN** completed bar data contains bullish and bearish candles
- **THEN** bullish and bearish directions use the approved soft-white and deep-blue colors

#### Scenario: Prior-session context
- **WHEN** prior-session bars are displayed
- **THEN** they retain the same direction semantics at reduced prominence

### Requirement: Preserve Strat annotations
The chart MUST preserve every existing Strat number, arrow, classification, calculation, and
visibility toggle while applying visual chart upgrades. Directional arrows SHALL use coordinated
white bullish and blue bearish colors while numeral semantics remain unchanged.

#### Scenario: Strat markers are enabled
- **WHEN** the user enables Strat markers
- **THEN** the same numbers and arrows produced from the same bar data remain visible and retain
  their existing meanings

### Requirement: Quiet refresh and current assets
Automatic Market Pulse refresh SHALL communicate visually through its compact status indicator
without showing transient refresh prose, and revised Market Pulse presentation assets MUST use a
new cache-busting revision.

#### Scenario: Automatic refresh succeeds
- **WHEN** an automatic canonical refresh finds current or newly updated data
- **THEN** the status indicator updates accessibly without rendering visible auto-refresh text

#### Scenario: User opens the revised page
- **WHEN** the browser loads Market Pulse after deployment
- **THEN** the page requests the revised stylesheet and chart script URLs rather than reusing the
  prior presentation revision

### Requirement: Key-level visual hierarchy
The chart SHALL emphasize spot, active level, and invalidation over secondary reference levels
without changing any underlying level value.

#### Scenario: Key and secondary levels coexist
- **WHEN** the chart receives a populated level stack
- **THEN** spot, active, and invalidation are easier to identify than secondary levels and include
  concise distance context where spot is available

#### Scenario: Invalidation proximity changes
- **WHEN** spot approaches or crosses the invalidation threshold
- **THEN** invalidation emphasis increases without changing execution permission or strategy state

### Requirement: Session context remains direction-neutral
The chart SHALL distinguish regular-session and after-hours context without changing candle
direction colors or requiring a page reload.

#### Scenario: After-hours bars are displayed
- **WHEN** chart metadata identifies after-hours context
- **THEN** the chart provides a subtle session distinction behind or adjacent to the plotted data
  while candle colors continue to represent direction only

#### Scenario: Session metadata is unavailable
- **WHEN** no reliable session boundary is available
- **THEN** the chart omits session shading and continues rendering bars and levels normally

### Requirement: Gamma Ladder expiry truthfulness
The Gamma Ladder SHALL distinguish requested DTE from effective expiry whenever session rules
substitute the next available expiration.

#### Scenario: After-hours zero-DTE fallback
- **WHEN** 0DTE was requested after the session and the service loads the next expiry
- **THEN** the ladder identifies both values and visually selects the effective preset

### Requirement: Net-relative ladder strength
The Gamma Ladder SHALL classify displayed net strength using maximum absolute net GEX across the
rendered ladder independently from call/put depth scaling.

#### Scenario: Dominant node is rendered
- **WHEN** a row has the largest absolute net GEX in the rendered ladder
- **THEN** it is labeled Dominant and is not simultaneously labeled Weak

### Requirement: Execution-focused ladder rows
Each Gamma Ladder row SHALL present one strike, one signed distance, one role, dealer-depth
visualization, net GEX, and one strength label without repeating equivalent language.

#### Scenario: Focused row renders
- **WHEN** a ladder row is visible
- **THEN** direction, role rank, and polarity headings are each communicated once

### Requirement: Actionable selected-level detail
The selected-level panel SHALL remain compact before selection and, after selection, show role,
distance, net GEX, state, expected behavior, failure context, and chart linkage.

#### Scenario: Selected strike survives refresh
- **WHEN** a selected strike is still present after a ladder refresh
- **THEN** the selection and detail panel remain active and synchronized with the chart
