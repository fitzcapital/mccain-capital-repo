## ADDED Requirements

### Requirement: Session-aware automatic refresh
The system SHALL run recurring Market Pulse canonical and provider refreshes only during the regular
weekday session from 9:30 AM through 4:00 PM Eastern time. Outside that session, it MUST retain the
last validated generation, stop recurring market-data requests, expose the closed phase in the
refresh contract, and identify when automatic validation can resume.

#### Scenario: Regular session is open
- **WHEN** Market Pulse is visible during the regular session
- **THEN** the client polls at the server-provided live cadence and reconciles validated generations

#### Scenario: Regular session closes
- **WHEN** a canonical response reports that the regular session is closed
- **THEN** the client cancels recurring refresh, displays that automatic updates are paused, and
  retains the last validated generation without presenting it as live

#### Scenario: Page remains open overnight
- **WHEN** a closed-session page reaches the next regular-session opening time or regains focus
- **THEN** it performs one server validation and resumes live polling only if the returned contract
  enables automatic refresh

#### Scenario: User requests an after-hours diagnostic refresh
- **WHEN** the user manually activates Refresh data outside the regular session
- **THEN** one bounded refresh is allowed, the page remains in closed-session mode, and recurring
  polling remains paused

#### Scenario: Closed-session retry or component event occurs
- **WHEN** a network retry, visibility change, or component event attempts to schedule a refresh
  while the refresh contract is closed
- **THEN** the phase-aware scheduling guard prevents a recurring market-data request
