## ADDED Requirements

### Requirement: Durable canonical Gamma observation history
The Market Pulse canonical refresh path SHALL append each accepted Gamma generation to a bounded,
durable per-ticker observation history containing session, source timestamp, regime, generation, and
numeric dynamic levels. Failed or unverified generations MUST NOT enter this history.

#### Scenario: Canonical Gamma refresh succeeds
- **WHEN** a verified canonical generation contains Gamma regime and dynamic levels
- **THEN** one deduplicated observation is retained for point-in-time Live and Replay evaluation

#### Scenario: App restarts after several observations
- **WHEN** Market Pulse restarts during or after a session
- **THEN** prior retained observations remain available without depending on process memory

#### Scenario: Gamma refresh fails
- **WHEN** the provider refresh fails or the candidate generation is not accepted
- **THEN** no fabricated observation is persisted and the last verified history remains unchanged
