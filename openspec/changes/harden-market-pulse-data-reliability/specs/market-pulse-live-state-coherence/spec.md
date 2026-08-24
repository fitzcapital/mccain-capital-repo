## MODIFIED Requirements

### Requirement: Canonical execution generation
The system SHALL publish quote, completed bars, gamma, levels, ranked scenario, ordered evidence,
permission, execution state, and component timing as one immutable, validated Market Pulse
generation. Every execution-facing surface MUST identify and render the same promoted generation,
and every worker MUST converge on the newest shared verified generation before responding.

#### Scenario: Required components are coherent
- **WHEN** all required SPX components pass symbol, session, freshness, timestamp, and coherence
  validation
- **THEN** the generation is atomically promoted and every execution surface uses its values and id

#### Scenario: Required component diverges
- **WHEN** a required component is missing, stale, future-dated, or belongs to a different generation
  or session
- **THEN** the last valid generation remains visible, execution is locked, and the blocker is named

#### Scenario: Older generation completes late
- **WHEN** an older candidate or worker response arrives after a newer generation is verified
- **THEN** the older generation is rejected and no execution-facing value regresses

### Requirement: Atomic in-place reconciliation
The client SHALL validate and stage a canonical response before updating execution-facing nodes,
SHALL commit all such nodes in one render transaction, and SHALL preserve user-selected chart,
ladder, ticker, and disclosure state during a successful update.

#### Scenario: Valid generation refreshes
- **WHEN** the in-place refresh receives a coherent newer canonical generation
- **THEN** all execution-facing nodes update together without a page reload or interaction reset

#### Scenario: Mixed payload is received
- **WHEN** the response contains mismatched generation, session, or contradictory action fields
- **THEN** no partial execution fields are committed and the last valid generation remains primary

#### Scenario: Observation-only quote advances
- **WHEN** a quote advances without a complete verified execution generation
- **THEN** it may update only in an explicitly observation-only surface and cannot change canonical
  action, trigger, invalidation, target, permission, or last-valid time

### Requirement: Bounded refresh lifecycle
Manual and automatic refresh feedback SHALL resolve within a bounded interval even when another
single-flight refresh is active or a provider times out. The visible countdown MUST represent the
coordinator's actual next-attempt time and the lifecycle MUST recover after page visibility or
network restoration without a full reload.

#### Scenario: Manual refresh overlaps automatic refresh
- **WHEN** a manual refresh is requested while canonical work is already active
- **THEN** the page retains the current playbook, joins or follows the active flight, clears transient
  feedback, and schedules one next attempt without reloading the page

#### Scenario: Provider refresh times out
- **WHEN** a provider timeout prevents a newer generation from being promoted
- **THEN** the last valid generation remains visible, the refresh control becomes available again,
  and bounded retry timing replaces the running state

#### Scenario: Polling resumes after suspension
- **WHEN** the page becomes visible or the network returns after a missed polling interval
- **THEN** the coordinator revalidates immediately and then resumes the server-directed cadence
