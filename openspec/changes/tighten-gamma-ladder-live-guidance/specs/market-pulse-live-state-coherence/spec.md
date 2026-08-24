## MODIFIED Requirements

### Requirement: Canonical execution generation
The system SHALL publish quote, completed bars, gamma, levels, ranked scenario, ordered evidence,
permission, execution state, and component timing as one validated Market Pulse generation. Every
execution-facing surface MUST identify and render the same promoted generation. The Gamma Ladder
MUST expose whether its accepted snapshot is aligned with that canonical generation and MUST NOT
present live command guidance while alignment is missing, stale, or contradictory.

#### Scenario: Required components are coherent
- **WHEN** all required SPX components pass freshness and coherence validation
- **THEN** the generation is atomically promoted, every execution surface uses its values and id,
  and the Gamma Ladder displays aligned source timing

#### Scenario: Required component diverges
- **WHEN** a required component is missing, stale, or belongs to a different generation
- **THEN** the last valid generation remains visible, execution is locked, the blocker is named, and
  Gamma command guidance displays a synchronization state

#### Scenario: Initial page generation includes Gamma lineage
- **WHEN** the Market Pulse page renders with an available Gamma Ladder snapshot
- **THEN** the ladder, canonical freshness, verdict, scenario state, and execution guide share the same combined generation before client polling begins

#### Scenario: Ladder controller starts from the canonical bootstrap
- **WHEN** the embedded Gamma Ladder matches the requested symbol, window, and expiration controls
- **THEN** the controller renders that aligned payload and does not replace it with an unbound standalone fetch during startup

#### Scenario: Polling changes canonical permission
- **WHEN** a coordinated poll promotes a generation with a different permission, action state, lock state, or Gamma lineage
- **THEN** the Gamma Ladder updates both its guidance and its exposed root lineage attributes from that same response
