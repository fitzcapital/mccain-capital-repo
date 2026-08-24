## ADDED Requirements

### Requirement: Point-in-time-safe setup detection
The system SHALL evaluate potential historical setups using only bars, levels, gamma observations,
and other evidence whose timestamps are at or before the evaluated completed candle. Later data MUST
NOT affect signal eligibility, direction, grade, entry, stop, or target.

#### Scenario: Setup confirms at noon
- **WHEN** the noon completed candle provides the final required confirmation
- **THEN** the recorded potential setup contains only evidence available by the noon close even if
  later candles produce a favorable outcome

#### Scenario: Historical gamma was unavailable
- **WHEN** no gamma observation was available by the evaluated candle close
- **THEN** gamma is marked unavailable, its confluence points are not earned, and the day's later
  gamma snapshot is not substituted

### Requirement: Reuse live strategy rules
Setup Replay SHALL use the same supported scenario families, completed-candle prerequisites,
confluence components, deterministic ordering, and invalidation rules as live evaluation.

#### Scenario: Intrabar cross without confirmation
- **WHEN** historical price crossed a level intrabar but did not provide the required completed close
  and retest
- **THEN** replay records no actionable setup for that cross

#### Scenario: Candidate confirms once
- **WHEN** the same scenario remains confirmed across several candles before invalidation
- **THEN** replay emits one potential setup at its first confirmed timestamp rather than duplicate
  setups on every candle

### Requirement: Explainable setup record
Each potential setup SHALL expose ticker, session date, signal time, direction, scenario family,
level and price, entry zone, confirmation, invalidation, target path, signal-time confluence, data
availability, and stable candidate id.

#### Scenario: User reviews an earlier breakdown
- **WHEN** the user opens a replayed Local Flip breakdown
- **THEN** the panel explains what was known at confirmation and why the setup qualified without
  requiring interpretation of raw internal state names

### Requirement: Separate signal from outcome
Replay SHALL freeze signal-time evidence before calculating subsequent maximum favorable excursion,
maximum adverse excursion, target/invalidation sequence, and end-of-window result. Outcome fields
MUST be visually and structurally separate from eligibility fields.

#### Scenario: Target occurs after signal
- **WHEN** later candles reach the proposed target before invalidation
- **THEN** replay labels that result as an observed outcome and does not add outcome knowledge to the
  original grade

#### Scenario: Both target and stop occur within one bar
- **WHEN** bar resolution cannot establish whether target or invalidation occurred first
- **THEN** replay marks the sequence ambiguous instead of selecting the favorable result

### Requirement: Replay session controls and presentation
The Market Pulse page SHALL provide a clearly labeled intraday Setup Replay for the selected ticker
and session, ordered from strongest to weakest by signal-time confluence, with deterministic
signal-time tie breaking and filters for direction, scenario family, and qualified or rejected
state. It SHALL render as a full-width collapsed disclosure below the alternative and dormant-watch
lanes, and its expanded content SHALL remain height-bounded and scrollable. It MUST remain secondary
to live execution status.

#### Scenario: Live session has earlier setups
- **WHEN** one or more point-in-time-safe setups occurred earlier today
- **THEN** the collapsed replay summary shows the setup count and most recent signal time, and the
  expanded view shows their explainable records from strongest to weakest without using outcome
  knowledge to determine rank

#### Scenario: No valid earlier setup
- **WHEN** no candidate completed every required gate in the selected session
- **THEN** replay says no qualified setup was found and may show rejected candidates only when the
  user requests diagnostic detail

### Requirement: Replay is non-executing and non-ledger
Historical setup records SHALL be labeled as potential setups and SHALL NOT create trades, journal
entries, or performance results without a separate explicit user action.

#### Scenario: Replay calculation completes
- **WHEN** replay returns historical setup records
- **THEN** the trade ledger and recorded performance remain unchanged
