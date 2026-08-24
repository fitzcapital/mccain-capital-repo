## MODIFIED Requirements

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
