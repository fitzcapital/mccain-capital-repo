## ADDED Requirements

### Requirement: Consolidated session readiness
The dashboard SHALL present preparation state in one Session Readiness surface containing the
current score, blockers, priority, and next required action.

#### Scenario: Session has blockers
- **WHEN** one or more readiness requirements are incomplete
- **THEN** the Session Readiness summary identifies the blockers and the next action
- **AND** detailed checklist and sync controls remain available through an accessible disclosure

#### Scenario: Session is ready
- **WHEN** all readiness requirements are complete
- **THEN** the summary reports the ready state without routine warning copy

#### Scenario: Readiness state changes without navigation
- **WHEN** planning hydration, import sync, or checklist interaction changes readiness
- **THEN** the score, blockers, and summary update without a full-page reload

### Requirement: Attention-aware Ops Band
The application SHALL render a healthy Ops Band as a compact status summary and SHALL keep warning
or error guidance prominent and discoverable.

#### Scenario: Operations are healthy
- **WHEN** the Ops Band has no warning or error condition
- **THEN** routine explanatory guidance is collapsed by default
- **AND** the healthy status remains visible

#### Scenario: Operations require attention
- **WHEN** the Ops Band reports warning, stale, failed, or unavailable state
- **THEN** the exception summary and supporting guidance are visible without requiring discovery

### Requirement: Canonical dashboard workflow language
The dashboard SHALL use the stage labels Command, Prepare, Execute, and Review for its primary
workflow navigation and section markers.

#### Scenario: Dashboard renders
- **WHEN** a user opens the dashboard
- **THEN** primary workflow navigation presents `Command → Prepare → Execute → Review`
- **AND** competing stage names such as Decision Tools, Plan, or Monitor are not used as primary
  workflow markers
