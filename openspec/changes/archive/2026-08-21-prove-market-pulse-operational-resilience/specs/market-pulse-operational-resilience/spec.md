## ADDED Requirements

### Requirement: Deterministic failure simulation
The system SHALL provide a test-only resilience harness that can independently simulate quote,
completed-bar, gamma, level, canonical-response, network, persistence, worker, and restart faults
without modifying personal runtime data or contacting a live broker.

#### Scenario: Required component times out
- **WHEN** the harness makes one required execution component exceed its bounded deadline
- **THEN** the last valid generation remains visible, execution becomes locked, the blocker is named,
  no setup alert is delivered, and recovery begins through one scheduled retry

#### Scenario: Older response arrives after recovery
- **WHEN** a delayed older generation arrives after a newer coherent generation has been promoted
- **THEN** the older response is rejected and no execution-facing field moves backward

#### Scenario: Persistence becomes unavailable
- **WHEN** the verified-generation or alert ledger cannot be read or atomically written
- **THEN** the system fails closed, preserves the last readable generation, suppresses unsafe alert
  delivery, and exposes persistence as the operational blocker

### Requirement: Compact operational health watchdog
The Market Pulse page SHALL expose a compact operational-health state derived from server-authoritative
generation and component diagnostics. Healthy state SHALL remain quiet; degraded state MUST name the
blocking component, last successful generation time, recovery state, and actual retry deadline.

#### Scenario: All required systems are healthy
- **WHEN** canonical generation, required components, worker adoption, and alert persistence validate
- **THEN** the watchdog shows a compact healthy state with the generation and last-check age

#### Scenario: Recovery is delayed
- **WHEN** one or more consecutive checks fail or a worker has not adopted the newest generation
- **THEN** the watchdog expands to name the failure count, blocker, last valid time, and retry timing
  without claiming that execution data refreshed successfully

### Requirement: Exchange-session-aware polling
The server SHALL determine Market Pulse automatic polling availability and next session boundaries
from one reusable US equity session-calendar service that covers weekends, observed holidays, and
scheduled early closes in Eastern Time.

#### Scenario: Full market holiday
- **WHEN** Market Pulse is open on a recognized full-session holiday
- **THEN** automatic provider polling remains disabled and the next-session time points to the next
  valid exchange session

#### Scenario: Scheduled early close
- **WHEN** the exchange session reaches its scheduled early-close time
- **THEN** automatic polling pauses, execution changes to closed-session planning, and the wake time
  points to the next valid session

#### Scenario: Laptop wakes after a session boundary
- **WHEN** a sleeping or hidden page becomes active after the advertised boundary
- **THEN** it performs one server-authoritative revalidation and follows the returned session contract

### Requirement: Repeatable operational soak evidence
The repository SHALL provide a bounded local soak runner that executes repeated canonical checks and
setup transitions, records sanitized reliability metrics, and exits nonzero when an acceptance
threshold fails.

#### Scenario: Soak run passes
- **WHEN** the configured run completes with zero mixed commits, zero duplicate alerts, bounded
  recovery, and consistent worker generations
- **THEN** the report identifies the run as passing and includes counts, maximum recovery time, and
  component-failure outcomes

#### Scenario: Soak run detects a regression
- **WHEN** any duplicate alert, mixed generation, indefinite refresh, missed lock, or unrecovered fault
  occurs
- **THEN** the report identifies the exact failed invariant and the runner exits unsuccessfully

### Requirement: Safe observability boundaries
Operational diagnostics SHALL exclude credentials, raw provider payloads, orders, financial ledger
records, and personally entered account data.

#### Scenario: Reliability event is recorded
- **WHEN** a refresh, recovery, worker adoption, or alert persistence event is logged
- **THEN** the record contains only timestamps, generation identifiers, component names and ages,
  duration, result classification, and sanitized failure reason
