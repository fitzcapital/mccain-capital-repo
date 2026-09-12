## ADDED Requirements

### Requirement: Explicit runtime roles
Application startup SHALL support `standalone`, `web`, and `worker` roles, with `standalone` retaining
current Podman behavior for rollback compatibility.

#### Scenario: Web role starts
- **WHEN** the application starts with the web role
- **THEN** it serves HTTP traffic without starting trade sync, backup, dispatch, or Market Pulse
  monitor background loops

#### Scenario: Worker role starts
- **WHEN** the application starts with the worker role
- **THEN** it starts the required background loops without binding the public HTTP port

#### Scenario: Standalone role starts
- **WHEN** no Kubernetes role is configured
- **THEN** current single-container web and background behavior remains available

### Requirement: Single background owner
The Kubernetes deployment SHALL maintain exactly one worker replica and SHALL expose worker health
that identifies active loop ownership and recent heartbeat state.

#### Scenario: Worker is healthy
- **WHEN** all required worker loops are running and heartbeats are current
- **THEN** Kubernetes reports the worker pod ready

#### Scenario: Duplicate worker requested
- **WHEN** configuration or scaling would create more than one worker replica
- **THEN** validation fails or the additional process cannot acquire the single-owner lease

### Requirement: Safe worker failure behavior
Worker failure SHALL not corrupt persistent state and SHALL be visible through pod readiness, logs,
and Freelens.

#### Scenario: Worker loop becomes stale
- **WHEN** a required heartbeat exceeds its allowed age
- **THEN** worker readiness fails and the pod is restarted without starting a second healthy owner

### Requirement: Behavioral compatibility
Separating runtime roles SHALL preserve existing web routes, session/authentication behavior,
Market Pulse setup evaluation, alerts, trade synchronization, and backup schedules.

#### Scenario: Post-cutover verification
- **WHEN** the web and worker roles are deployed
- **THEN** focused application contracts and live smoke checks produce the same user-visible behavior
  as the standalone runtime
