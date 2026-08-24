## ADDED Requirements

### Requirement: Reload-equivalent receiving state
The Market Pulse receiving page SHALL converge to the same canonical execution, freshness, setup,
ladder, and watchdog state whether it receives a newer generation through in-place polling or through
a full page load.

#### Scenario: Reload follows in-place promotion
- **WHEN** a coherent generation is promoted in place and the user subsequently reloads the page
- **THEN** the initial server render matches the promoted generation and does not regress or require a
  second refresh to show the correct regime or setup state

#### Scenario: Reload occurs during a partial failure
- **WHEN** the user reloads while a required component is unavailable
- **THEN** the server render retains the last valid generation, displays the same execution lock and
  blocker as the API contract, and does not publish a partial candidate

### Requirement: Automatic receiving-page recovery
The receiving page SHALL recover without a full reload after bounded network, provider, tab-suspension,
or overlapping-request failures, while retaining one authoritative polling flight.

#### Scenario: Network returns
- **WHEN** the browser returns online after one or more failed canonical checks
- **THEN** one immediate canonical validation runs, the watchdog reports recovery, and a coherent newer
  generation commits atomically when available

#### Scenario: Multiple tabs request recovery
- **WHEN** multiple visible tabs request the same canonical recovery concurrently
- **THEN** server single-flight behavior prevents duplicated provider work and every tab adopts the
  same newest verified generation

### Requirement: End-to-end execution invariants
Authenticated receiving-page tests SHALL verify that header, execution strip, chart authority,
Gamma Ladder, setup monitor, countdown, and diagnostics preserve their canonical relationships during
success, lock, retry, terminal setup, and recovery states.

#### Scenario: Required component becomes stale
- **WHEN** an end-to-end test injects a stale required component into the active page
- **THEN** every execution surface locks in one commit, the observation quote remains explicitly
  non-authoritative, and no actionable notification appears

#### Scenario: Terminal setup remains terminal
- **WHEN** a setup reaches target, invalidation, or expiration and subsequent older or malformed data
  arrives
- **THEN** the terminal state remains monotonic, its checks remain paused, and no duplicate alert fires
