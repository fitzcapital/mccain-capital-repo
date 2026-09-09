## MODIFIED Requirements

### Requirement: Exchange-session-aware polling
The system SHALL publish one server-authoritative US equity session contract and SHALL use it to control Market Pulse polling across regular sessions, weekends, holidays, and early closes in Eastern Time. The contract SHALL identify the current phase, session date, regular-session open and close, next transition, next valid session open, whether automatic polling is allowed, a server timestamp, and the recommended polling cadence. The browser SHALL revalidate the contract at a scheduled boundary instead of changing session phase from a locally assumed clock time.

#### Scenario: Page remains open across the regular-session open
- **WHEN** Market Pulse is open before the exchange session begins
- **THEN** it SHALL avoid live provider polling, schedule a wake for the server-provided opening boundary, perform one immediate server revalidation at that boundary, and begin the regular-session cadence only when the returned contract permits it

#### Scenario: Page loads while automatic polling is allowed
- **WHEN** the initial server contract permits regular-session automatic polling
- **THEN** the browser SHALL schedule one immediate non-forced canonical validation through the shared coordinator before beginning the recommended recurring cadence

#### Scenario: Page loads while automatic polling is paused
- **WHEN** the initial server contract identifies premarket, after-hours, a weekend, or a holiday
- **THEN** the browser SHALL NOT start an immediate automatic provider refresh and SHALL retain only the server-provided boundary wake and manual forced-refresh fallback

#### Scenario: Regular session closes
- **WHEN** the server-provided regular-session close is reached
- **THEN** Market Pulse SHALL perform one boundary revalidation, enter after-hours planning state, stop live-only polling, and display the next valid session opening

#### Scenario: Full market holiday
- **WHEN** the exchange calendar marks the selected date as a full holiday
- **THEN** automatic live polling SHALL remain disabled and the contract SHALL identify the next valid session opening

#### Scenario: Early-close session
- **WHEN** the exchange calendar marks the selected date with an early close
- **THEN** the early-close time SHALL be the closing boundary and live-only polling SHALL stop after that boundary

#### Scenario: Device wakes after a missed boundary
- **WHEN** the browser resumes after sleep, suspension, or a hidden interval that crossed a session boundary
- **THEN** it SHALL perform exactly one immediate server revalidation and follow the returned contract without replaying missed polls

#### Scenario: Client clock differs from the server
- **WHEN** the client clock and server timestamp differ
- **THEN** countdowns and boundary scheduling SHALL be derived from the server timestamp and transition values rather than fixed client-side market hours

## ADDED Requirements

### Requirement: Session lifecycle diagnostics
The system SHALL expose a compact lifecycle status containing the current session phase, last successful reconciliation, next scheduled transition or retry, polling activity, and any blocking component without exposing provider credentials or sensitive payloads.

#### Scenario: Lifecycle is healthy
- **WHEN** the session contract is current and polling matches the allowed phase
- **THEN** the status SHALL remain compact and identify the next transition without presenting a critical warning

#### Scenario: Lifecycle cannot reconcile
- **WHEN** a boundary or retry revalidation fails
- **THEN** the status SHALL retain the last valid generation, identify the failed component and next retry, and mark execution unavailable until a coherent generation is restored
