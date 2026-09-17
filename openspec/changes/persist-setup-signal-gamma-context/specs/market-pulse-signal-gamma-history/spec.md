## ADDED Requirements

### Requirement: Setup events freeze point-in-time Gamma context
The system SHALL attach Gamma regime and provenance to a setup only from an explicit observation
whose timestamp is at or before the setup signal time.

#### Scenario: Eligible observation exists
- **WHEN** several timestamped Gamma observations exist before and after a setup signal
- **THEN** the setup stores the latest observation at or before the signal, including regime,
  observation timestamp, source, and availability status

#### Scenario: Only future Gamma exists
- **WHEN** every Gamma observation is later than the setup signal
- **THEN** signal-time Gamma is unavailable and no later regime is attached to the setup

### Requirement: Analytics persists immutable signal Gamma
The system SHALL persist normalized signal-time Gamma context with the canonical setup event and
SHALL NOT replace it during later outcome or excursion updates with newer Gamma data.

#### Scenario: Outcome resolves after the signal
- **WHEN** an open setup later receives its terminal outcome, MFE, or MAE
- **THEN** its stored Gamma regime and provenance remain the original signal-time values

#### Scenario: Existing database migrates
- **WHEN** the migration runs against an existing analytics database
- **THEN** additive Gamma fields and indexes are created without deleting or rewriting setup,
  outcome, target, evidence, or personal data

### Requirement: Missing historical Gamma remains unavailable
The system MUST NOT infer historical Gamma from current snapshots, price action, or a timestamp
that lacks an explicit regime.

#### Scenario: Legacy record has no proven regime
- **WHEN** an existing setup lacks an explicit eligible signal-time Gamma regime
- **THEN** Analytics labels its Gamma as unavailable and excludes it from positive, negative, or
  transition Gamma cohorts

#### Scenario: Persisted Gamma timestamp exceeds signal time
- **WHEN** a record claims Gamma provenance later than its signal
- **THEN** canonicalization rejects that context as unavailable rather than persisting future data

### Requirement: Analytics supports Gamma-aware study
Setup Analytics SHALL expose signal-time Gamma as a filter, comparison dimension, coverage metric,
and ledger detail without changing canonical setup counts or outcome calculations.

#### Scenario: User filters by negative Gamma
- **WHEN** the user selects the negative Gamma filter
- **THEN** charts, leaders, family comparisons, metrics, and ledger rows use only setups with a
  stored negative Gamma regime at signal time

#### Scenario: User studies all history
- **WHEN** the selected range contains captured and unavailable Gamma contexts
- **THEN** the page reports Gamma coverage, retains unavailable records in overall setup counts,
  and keeps unavailable records out of named-regime performance claims

### Requirement: Receiving surfaces explain Gamma provenance
The analytics API and page SHALL identify Gamma as signal-time market context and SHALL present the
observation timestamp or unavailable reason without implying that Gamma authorized a trade.

#### Scenario: Ledger row has captured Gamma
- **WHEN** a setup has valid persisted Gamma context
- **THEN** its ledger details display the normalized regime and observation time used at signal

#### Scenario: Ledger row lacks captured Gamma
- **WHEN** a setup has no valid persisted Gamma context
- **THEN** its ledger details display signal-time Gamma unavailable rather than today's regime
