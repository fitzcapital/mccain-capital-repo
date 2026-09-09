## MODIFIED Requirements

### Requirement: Compact operational health watchdog
The Market Pulse page and sanitized health endpoint SHALL expose a compact operational-health state
derived from server-authoritative generation, component diagnostics, and the server setup monitor.
Healthy state SHALL remain quiet; degraded state MUST name the blocking component, last successful
generation time, server-monitor ownership and heartbeat age, last evaluated candle, recovery state,
clock status, and actual retry deadline.

#### Scenario: All required systems are healthy
- **WHEN** canonical generation, required components, worker adoption, alert persistence, and the
  regular-session server monitor validate
- **THEN** the watchdog shows a compact healthy state with the generation, last-check age, and current
  monitor coverage

#### Scenario: Recovery is delayed
- **WHEN** one or more consecutive checks fail, a worker has not adopted the newest generation, or the
  server monitor heartbeat exceeds its allowed regular-session age
- **THEN** the watchdog expands to name the failure count, blocker, last valid time, and retry timing
  without claiming that execution data or setup coverage refreshed successfully

#### Scenario: Session is closed
- **WHEN** the shared exchange calendar reports a closed session
- **THEN** the watchdog identifies the setup monitor as sleeping rather than stale or failed

#### Scenario: Clock continuity fails
- **WHEN** the runtime detects a material wall-clock discontinuity
- **THEN** the watchdog reports timing recovery until session and canonical data are revalidated

### Requirement: Repeatable operational soak evidence
The repository SHALL provide a bounded local soak runner that executes repeated canonical checks,
server-monitor heartbeats, page-closed setup transitions, clock discontinuities, and restart recovery;
records sanitized reliability metrics; and exits nonzero when an acceptance threshold fails.

#### Scenario: Soak run passes
- **WHEN** the configured run completes with zero mixed commits, zero duplicate alerts, zero missed
  eligible page-closed setups, bounded recovery, and consistent worker generations
- **THEN** the report identifies the run as passing and includes counts, maximum recovery time,
  monitor coverage, and component-failure outcomes

#### Scenario: Soak run detects a regression
- **WHEN** any duplicate alert, mixed generation, indefinite refresh, missed lock, stale regular-session
  heartbeat, missed eligible setup, or unrecovered fault occurs
- **THEN** the report identifies the exact failed invariant and the runner exits unsuccessfully
