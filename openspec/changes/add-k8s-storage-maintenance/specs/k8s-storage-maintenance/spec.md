## ADDED Requirements

### Requirement: Scheduled storage observation
The system SHALL run storage maintenance every 30 minutes, report PVC utilization before and after maintenance, prevent overlapping runs, and retain bounded job history.

#### Scenario: Scheduled check
- **WHEN** the CronJob schedule fires
- **THEN** one resource-bounded job reports total, used, available, and utilization values for `/data`

### Requirement: Allowlisted scratch cleanup
The maintenance command MUST delete only regular files older than the configured retention period beneath `/data/tmp` or `/data/cache` and MUST reject every other cleanup root.

#### Scenario: Aged scratch file
- **WHEN** an allowlisted scratch file is older than seven days
- **THEN** maintenance removes it and may remove empty descendant directories

#### Scenario: Protected application data
- **WHEN** maintenance runs against a PVC containing the database, uploads, books, backups, and Market Pulse evidence
- **THEN** those protected paths remain untouched

#### Scenario: Unsafe configured root
- **WHEN** a cleanup root resolves outside the immutable allowlist
- **THEN** maintenance exits nonzero before deleting any file

### Requirement: Low-overhead operation
The maintenance workload SHALL be non-resident and SHALL define explicit CPU and memory requests and limits.

#### Scenario: Idle cluster
- **WHEN** no maintenance run is active
- **THEN** no maintenance pod consumes CPU or memory

### Requirement: Operator visibility
Local status tooling SHALL show the CronJob schedule, last schedule time, active jobs, and recent maintenance logs, and SHALL support a manual one-shot run.

#### Scenario: Operator checks maintenance
- **WHEN** the operator runs the local Kubernetes status command
- **THEN** it reports the maintenance schedule and recent job state without changing data
