## ADDED Requirements

### Requirement: Complete generation provenance
Every execution-facing canonical generation SHALL expose component source, provider timestamp,
received timestamp, freshness age, threshold, completeness, fallback/proxy mode, and persistence
status. Missing provenance for a required component MUST prevent a verified trust verdict.

#### Scenario: Fresh quote accompanies stale Gamma
- **WHEN** the observation quote is current but the required Gamma snapshot is stale
- **THEN** execution remains locked and the generation identifies Gamma as the blocker

#### Scenario: Required persistence fails
- **WHEN** canonical or setup-evidence persistence fails for the current generation
- **THEN** the generation cannot be verified and execution remains locked without discarding the last verified generation

### Requirement: Setup evidence completeness
The canonical generation SHALL disclose whether setup detection, alert-ledger persistence, Replay
evaluation, and analytics recording completed for the same session and generation.

#### Scenario: Setup analytics write fails
- **WHEN** Live/Replay evaluation succeeds but the analytics event upsert fails
- **THEN** execution guidance retains its existing safe state, the generation is marked degraded for evidence persistence, and a reliability incident is recorded

#### Scenario: No setup exists
- **WHEN** all setup evaluation and persistence steps complete and no setup qualifies
- **THEN** evidence completeness is current and `no setup` is not treated as missing data

### Requirement: Proxy and fallback honesty
Proxy, cached, and fallback values SHALL retain their source identity and SHALL NOT silently inherit
the authority classification of the primary execution source.

#### Scenario: SPY volume is missing for one SPX candle
- **WHEN** no timestamp-aligned SPY volume exists
- **THEN** the histogram reports a proxy coverage gap and does not reuse native SPX index volume

#### Scenario: Cached Gamma is displayed
- **WHEN** the last verified Gamma remains visible during refresh failure
- **THEN** it is labeled cached with its original timestamp and cannot promote a newer verified execution generation
