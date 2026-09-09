## ADDED Requirements

### Requirement: Server and receiving-page setup convergence
The server setup monitor and every Market Pulse receiving page SHALL converge on the same canonical
generation, durable setup state, alert status, and Replay outcome while allowing client polling to
remain suspended when the page is hidden.

#### Scenario: Server detects a setup before the page opens
- **WHEN** the server persists an eligible or late-review setup while no page is open
- **THEN** the next page load or in-place refresh renders that exact durable state without a duplicate
  evaluation or alert

#### Scenario: Page detects a setup before the next server cycle
- **WHEN** a visible page persists a setup through the shared evaluator before the server's next check
- **THEN** the server adopts the durable state and does not emit another delivery

#### Scenario: Server monitor is unhealthy
- **WHEN** the page receives current canonical market data but the regular-session server monitor
  heartbeat is stale or failed
- **THEN** the page identifies setup coverage as degraded rather than claiming full live coverage
