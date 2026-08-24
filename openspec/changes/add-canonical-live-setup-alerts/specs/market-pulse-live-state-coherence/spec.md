## ADDED Requirements

### Requirement: Canonical setup and execution coherence
The canonical Market Pulse generation SHALL include the primary live setup, setup revision, lifecycle
evidence, alert eligibility, and blocking reasons alongside execution state and ranked scenarios. The
client MUST commit these fields atomically.

#### Scenario: Setup confirms in a promoted generation
- **WHEN** a coherent canonical generation changes the primary setup from armed to confirmed
- **THEN** execution guidance, setup state, alert eligibility, freshness, and generation id update together in one client commit

#### Scenario: Setup payload conflicts with execution permission
- **WHEN** a payload marks a setup alert-eligible while canonical execution permission is locked
- **THEN** the client rejects the inconsistent setup revision, retains the last valid generation, and emits no alert

### Requirement: Monotonic setup reconciliation
The client SHALL reject an older or regressive live-setup revision even when it arrives after a newer
canonical response. Reload restoration MUST use the server's persisted revision rather than browser
memory as authority.

#### Scenario: Slow response arrives out of order
- **WHEN** a response containing an older setup revision arrives after a newer revision was committed
- **THEN** the client ignores the older setup state and preserves the newer canonical view

#### Scenario: Page reloads
- **WHEN** Market Pulse loads after a setup was already confirmed or acknowledged
- **THEN** the server returns the persisted setup revision and delivery state in the initial canonical payload
