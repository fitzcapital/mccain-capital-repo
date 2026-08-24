# market-pulse-live-state-coherence Specification

## Purpose

Canonical Market Pulse generation, authoritative execution state, atomic in-place reconciliation,
honest freshness, bounded refresh behavior, and unambiguous regime scope.

## Requirements

### Requirement: Canonical execution generation
The system SHALL publish quote, completed bars, gamma, levels, ranked scenario, ordered evidence,
permission, execution state, and component timing as one validated Market Pulse generation. Before
validation, canonical refresh MUST reconcile each required component to the newest valid
current-session observation available to that refresh, and an older cached component MUST NOT
override a newer provider observation. Every execution-facing surface MUST identify and render the
same promoted generation.

#### Scenario: Required components are coherent
- **WHEN** all required SPX components pass freshness and coherence validation
- **THEN** the generation is atomically promoted and every execution surface uses its values and id

#### Scenario: Required component diverges
- **WHEN** a required component is missing, stale, or belongs to a different generation
- **THEN** the last valid generation remains visible, execution is locked, and the blocker is named

#### Scenario: Current provider observation supersedes stale cache
- **WHEN** canonical refresh receives a valid current-session provider observation newer than the
  cached value for spot or gamma
- **THEN** the newer value and its provenance are validated together and used in the candidate
  generation

#### Scenario: Fresh chart and ladder inputs form a coherent generation
- **WHEN** the chart quote, completed bars, and Gamma Ladder source inputs are current and
  session-compatible while persisted component caches are stale
- **THEN** canonical refresh promotes those current inputs once, advances last-valid time, and does not
  retain a false `spot, gamma` lock

#### Scenario: Newer observation is not safe for promotion
- **WHEN** a newer observation lacks an authoritative timestamp, belongs to another session, or cannot
  be reconciled with the other required components
- **THEN** the system retains the last valid generation and names the real mismatched component

### Requirement: Single authoritative execution state
The system SHALL derive one authoritative `ACTIVE`, `WAIT`, or `LOCKED` state from canonical data
permission and ordered strategy evidence. Confluence score or grade MUST NOT independently authorize
execution.

#### Scenario: High score lacks hard confirmation
- **WHEN** a scenario has an A-range score but its required ordered confirmation is incomplete
- **THEN** every execution surface displays `WAIT`, withholds entry language, and names the next event

#### Scenario: Required confirmation completes
- **WHEN** fresh canonical evidence completes every required action gate
- **THEN** every execution surface may display `ACTIVE` with the same trigger, action, invalidation,
  and target

#### Scenario: Permission is unsafe
- **WHEN** canonical freshness or coherence makes execution permission unsafe
- **THEN** every execution surface displays `LOCKED` regardless of scenario score or prior state

### Requirement: Atomic in-place reconciliation
The client SHALL stage and validate a canonical response before updating execution-facing nodes and
SHALL preserve user-selected chart, ladder, ticker, and disclosure state during a successful update.

#### Scenario: Valid generation refreshes
- **WHEN** the in-place refresh receives a coherent newer canonical generation
- **THEN** all execution-facing nodes update together without a page reload or interaction reset

#### Scenario: Mixed payload is received
- **WHEN** the response contains mismatched generation or contradictory action fields
- **THEN** no partial execution fields are committed and the last valid generation remains primary

### Requirement: Honest component freshness
The page SHALL expose canonical last-valid time and component-level freshness sufficient to explain
why execution is active, waiting, or locked. Observation-only data MUST NOT be presented as a
successful canonical refresh.

#### Scenario: Quote advances without gamma
- **WHEN** a live quote advances but required gamma does not validate
- **THEN** the quote may update as observation-only while execution stays locked and gamma is named
  as the blocker

#### Scenario: Full generation advances
- **WHEN** every required component validates in a newer generation
- **THEN** last-valid time advances once and all execution state is reevaluated from that generation

### Requirement: Bounded refresh lifecycle
Manual and automatic refresh feedback SHALL resolve within a bounded interval even when another
single-flight refresh is active or a provider times out.

#### Scenario: Manual refresh overlaps automatic refresh
- **WHEN** a manual refresh receives an unchanged response because canonical work is already active
- **THEN** the page retains the current playbook, clears transient feedback, and retries after the
  server-provided interval without reloading the page

#### Scenario: Provider refresh times out
- **WHEN** a provider timeout prevents a newer generation from being promoted
- **THEN** the last valid generation remains visible, the refresh control becomes available again,
  and no indefinite running state remains

### Requirement: Regime scope is unambiguous
The canonical execution regime SHALL remain authoritative. Any ladder-specific distribution regime
that differs from it MUST be explicitly scoped so the page does not present two competing execution
regimes.

#### Scenario: Ladder distribution differs from canonical regime
- **WHEN** the ladder classifies positioning as mixed while the canonical execution regime is negative
- **THEN** the ladder labels the value as a positioning mix and continues to display the canonical
  execution regime as the live guidance authority

### Requirement: Reload-equivalent receiving state
The Market Pulse receiving page SHALL converge to the same canonical execution, freshness, setup,
ladder, and watchdog state whether it receives a newer generation through in-place polling or through
a full page load.

#### Scenario: Reload follows in-place promotion
- **WHEN** a coherent generation is promoted in place and the user subsequently reloads the page
- **THEN** the initial server render matches the promoted generation and does not regress or require a
  second refresh to show the correct regime or setup state

#### Scenario: Reload occurs during a partial failure
- **WHEN** the user reloads while a required component is unavailable
- **THEN** the server render retains the last valid generation, displays the same execution lock and
  blocker as the API contract, and does not publish a partial candidate

### Requirement: Automatic receiving-page recovery
The receiving page SHALL recover without a full reload after bounded network, provider, tab-suspension,
page-cache restoration, or overlapping-request failures, while retaining one authoritative polling
flight. It SHALL suspend scheduled canonical polling while hidden and SHALL resume the visible cadence
from one immediate reconciliation when the prior validation is stale.

#### Scenario: Network returns
- **WHEN** the browser returns online after one or more failed canonical checks
- **THEN** one immediate canonical validation runs, the watchdog reports recovery, and a coherent newer
  generation commits atomically when available

#### Scenario: Multiple tabs request recovery
- **WHEN** multiple visible tabs request the same canonical recovery concurrently
- **THEN** server single-flight behavior prevents duplicated provider work and every tab adopts the
  same newest verified generation

#### Scenario: Page remains hidden
- **WHEN** the Market Pulse tab is hidden or backgrounded
- **THEN** scheduled client canonical polling is suspended and last-valid state remains visible

#### Scenario: User returns after one visible cadence
- **WHEN** the page becomes visible or focused more than 15 seconds after its last canonical check
- **THEN** exactly one immediate single-flight validation runs and visible polling resumes without a
  full reload

#### Scenario: Page is restored from browser cache
- **WHEN** a persisted page is restored after `pagehide` cleared its timers
- **THEN** the countdown and canonical coordinator are re-armed and one immediate validation runs

### Requirement: End-to-end execution invariants
Authenticated receiving-page tests SHALL verify that header, execution strip, chart authority,
Gamma Ladder, setup monitor, countdown, and diagnostics preserve their canonical relationships during
success, lock, retry, terminal setup, and recovery states.

#### Scenario: Required component becomes stale
- **WHEN** an end-to-end test injects a stale required component into the active page
- **THEN** every execution surface locks in one commit, the observation quote remains explicitly
  non-authoritative, and no actionable notification appears

#### Scenario: Terminal setup remains terminal
- **WHEN** a setup reaches target, invalidation, or expiration and subsequent older or malformed data
  arrives
- **THEN** the terminal state remains monotonic, its checks remain paused, and no duplicate alert fires
