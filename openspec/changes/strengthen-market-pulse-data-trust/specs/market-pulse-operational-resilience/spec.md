## ADDED Requirements

### Requirement: Durable bounded reliability events
Reliability transitions SHALL be stored in the existing application database with stable event ids,
sanitized fields, indexed time/component access, and bounded retention. Events MUST remain available
across workers, process restarts, and container rebuilds.

#### Scenario: Worker restarts during an incident
- **WHEN** a provider incident is active and the serving worker restarts
- **THEN** the same incident remains visible and subsequent checks update it rather than creating an unrelated duplicate

#### Scenario: Retention boundary is reached
- **WHEN** detailed events exceed the configured retention window
- **THEN** old detail is pruned while retained daily aggregates continue to describe historical availability

### Requirement: Transition-based incident lifecycle
The reliability service SHALL open incidents on state transitions, update last-seen and duration for
unchanged states, and close incidents on verified recovery.

#### Scenario: Repeated timeout persists
- **WHEN** the same source timeout occurs for several consecutive checks
- **THEN** one incident accumulates failure count and duration without one row per poll

#### Scenario: Different blocker appears
- **WHEN** Gamma recovers but completed bars become stale
- **THEN** the Gamma incident closes and a separate bars incident opens with its own reason and timing

### Requirement: Source quality indicators
The operational response SHALL expose availability percentage, freshness compliance, request
latency, consecutive failures, last success, fallback/proxy use, and recovery time by component for
the selected bounded period.

#### Scenario: Source has incomplete coverage
- **WHEN** SPY volume aligns with only part of the displayed SPX candle set
- **THEN** the response reports matched and expected bars and calculates an explicit proxy coverage percentage

#### Scenario: Source is healthy
- **WHEN** a component remains current and successful throughout the selected period
- **THEN** its availability and freshness indicators show complete coverage with zero incidents

### Requirement: Persistence failure fails closed
Failure to persist the canonical envelope or required reliability evidence SHALL be visible and SHALL
prevent the system from claiming a fully verified state.

#### Scenario: Reliability database write fails
- **WHEN** an incident transition cannot be stored
- **THEN** execution remains locked or degraded as applicable, local sanitized logging continues, and the Trust Center states that durable evidence is unavailable
