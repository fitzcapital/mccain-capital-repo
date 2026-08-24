## ADDED Requirements

### Requirement: Canonical execution generation
The system SHALL publish quote, completed bars, gamma, levels, ranked scenario, ordered evidence,
permission, execution state, and component timing as one validated Market Pulse generation. Every
execution-facing surface MUST identify and render the same promoted generation.

#### Scenario: Required components are coherent
- **WHEN** all required SPX components pass freshness and coherence validation
- **THEN** the generation is atomically promoted and every execution surface uses its values and id

#### Scenario: Required component diverges
- **WHEN** a required component is missing, stale, or belongs to a different generation
- **THEN** the last valid generation remains visible, execution is locked, and the blocker is named

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
