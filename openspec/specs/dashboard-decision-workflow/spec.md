# Dashboard Decision Workflow

## Purpose

Define the dashboard's scan-first decision hierarchy, canonical import readiness, and accessible
progressive disclosure without changing its trading or financial business rules.

## Requirements

### Requirement: Consolidated command hierarchy
The dashboard SHALL present one primary Command Center containing the current operating state,
trading permission, market context, account-risk state, and next required action.

#### Scenario: Dashboard initially renders
- **WHEN** a user opens the dashboard
- **THEN** the five command values are available in one scan-first region
- **AND** the page does not repeat a separate command summary for the same values

#### Scenario: A command value is blocked
- **WHEN** alignment, permission, market data, or account risk is blocked or unavailable
- **THEN** that value includes a concise cause or required next action

#### Scenario: A command value is healthy
- **WHEN** a command value requires no attention
- **THEN** it renders as a compact status without routine explanatory copy

### Requirement: Unified execution plan
The dashboard SHALL present one Execution Plan containing bias, location, trigger, invalidation,
risk, and next action, with a single execution-navigation action row.

#### Scenario: Execution plan is available
- **WHEN** dashboard decision context is rendered
- **THEN** the plan fields and current trade permission are grouped in one region
- **AND** duplicate Daily Brief decision prose and duplicate execution CTA groups are absent

#### Scenario: Plan is planning-only
- **WHEN** current permission does not allow a trade
- **THEN** the Execution Plan clearly labels the state as planning-only or no-trade
- **AND** no removed supporting copy changes the underlying permission

### Requirement: Canonical import readiness
The dashboard MUST derive all import-readiness labels, details, tones, and actions from one
authoritative resolved state.

#### Scenario: Import is complete
- **WHEN** the authoritative import state is complete
- **THEN** every visible import reference reports completion
- **AND** no reference reports pending or no trades loaded

#### Scenario: Import is pending
- **WHEN** the authoritative import state is pending
- **THEN** every visible import reference reports the pending condition and its next action
- **AND** no reference reports completion

#### Scenario: Import state is unavailable
- **WHEN** import readiness cannot be determined
- **THEN** the dashboard reports an unavailable state without claiming completion

### Requirement: Progressive supporting detail
The dashboard SHALL keep execution-critical decision information visible while placing Foundation,
Review, Health, Calendar, and extended explanatory material behind accessible disclosure controls.

#### Scenario: Supporting content initially renders
- **WHEN** a user opens the dashboard
- **THEN** supporting sections expose a concise label and useful current status
- **AND** their extended content is collapsed by default

#### Scenario: Supporting content is expanded
- **WHEN** a user activates a supporting disclosure using pointer or keyboard input
- **THEN** its existing detailed content becomes available without a full-page reload

### Requirement: Existing behavior remains authoritative
The consolidation MUST preserve existing strategy calculations, permission decisions, account and
market inputs, import execution, and user-entered values.

#### Scenario: Dashboard presentation is consolidated
- **WHEN** the new hierarchy is rendered
- **THEN** source values and business-rule outcomes match the pre-consolidation context

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
