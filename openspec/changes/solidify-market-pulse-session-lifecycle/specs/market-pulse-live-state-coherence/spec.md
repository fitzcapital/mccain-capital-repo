## MODIFIED Requirements

### Requirement: Atomic in-place reconciliation
The Market Pulse page SHALL reconcile canonical context, session phase, countdown state, execution permission, regime, spot, levels, chart data, gamma ladder, and setup-monitor state as one generation without a full-page reload. A session-boundary response SHALL update all affected surfaces from the same authoritative response before the new state is presented.

#### Scenario: Valid generation is received
- **WHEN** an automatic or manual refresh returns a complete valid generation
- **THEN** all Market Pulse surfaces SHALL update atomically and display one shared generation and freshness state

#### Scenario: Session phase changes during reconciliation
- **WHEN** a response changes the session from premarket to regular or regular to after-hours
- **THEN** the header, countdown, polling controls, chart mode, setup monitor, gamma state, and execution permission SHALL reconcile together without mixed old and new phase labels

#### Scenario: Partial generation is received
- **WHEN** any required component is absent, stale, or belongs to a different generation
- **THEN** the page SHALL retain the last valid coherent generation, show the blocking component, and keep execution locked

### Requirement: Bounded refresh lifecycle
All manual, automatic, lifecycle-boundary, focus, visibility, connectivity, and cache-restore refresh triggers SHALL pass through one single-flight coordinator. The coordinator SHALL apply bounded timeouts, use the server-recommended cadence during regular hours, use capped retry backoff after failures, and avoid provider polling when the session contract disallows it.

#### Scenario: Several refresh triggers arrive together
- **WHEN** a timer, focus event, and manual refresh occur while a request is active
- **THEN** they SHALL coalesce into the active flight with at most one follow-up reconciliation when required

#### Scenario: Refresh request times out
- **WHEN** a refresh exceeds its timeout
- **THEN** the active-flight state SHALL clear, the last valid generation SHALL remain visible, execution SHALL fail closed, and one bounded retry SHALL be scheduled

#### Scenario: Session closes while a live poll is active
- **WHEN** the closing boundary is reached during an active request
- **THEN** the response SHALL be reconciled once and no additional live-only request SHALL start unless the returned session contract permits it

### Requirement: Automatic receiving-page recovery
The Market Pulse receiving page SHALL recover without a full-page reload by using the shared refresh coordinator and server session contract. During the regular session, an initial page load or reload SHALL render the latest coherent cached generation first and immediately perform one non-forced canonical reconciliation without waiting for the normal polling interval. It SHALL suspend recurring work while hidden, perform one immediate reconciliation when restored after meaningful staleness or a crossed boundary, and keep live-only chart and setup polling disabled outside the regular session.

#### Scenario: Page loads during the regular session
- **WHEN** Market Pulse initially renders or reloads from a coherent cached generation while automatic polling is allowed
- **THEN** it SHALL immediately request one non-forced canonical reconciliation through the single-flight coordinator and SHALL NOT wait for the normal polling interval

#### Scenario: Initial validation finds the same generation
- **WHEN** the immediate canonical validation confirms the rendered generation is current
- **THEN** the page SHALL remain coherent without forcing provider collection and SHALL continue at the server-recommended cadence

#### Scenario: Initial validation finds a newer generation
- **WHEN** the immediate canonical validation returns a newer complete generation
- **THEN** all Market Pulse surfaces SHALL apply that generation atomically

#### Scenario: Page is opened before the market and left visible
- **WHEN** the page remains open through the server-provided regular-session opening
- **THEN** one boundary reconciliation SHALL activate live state and regular polling without user interaction

#### Scenario: Page remains open after the market closes
- **WHEN** the regular-session closing boundary passes
- **THEN** one boundary reconciliation SHALL place the page in after-hours planning state and stop canonical live and live-only subpolling

#### Scenario: Tab becomes visible after more than fifteen seconds
- **WHEN** the page becomes visible or focused after more than fifteen seconds hidden
- **THEN** it SHALL perform one immediate reconciliation and resume only the work allowed by the returned session contract

#### Scenario: Network connectivity returns
- **WHEN** the browser receives an online event after refresh failures
- **THEN** it SHALL perform one coalesced reconciliation and clear degraded status only after a complete coherent generation succeeds

#### Scenario: Page is restored from browser cache
- **WHEN** the page is restored from the back-forward cache
- **THEN** it SHALL discard stale timers, perform one contract reconciliation, and schedule exactly one new lifecycle timer

#### Scenario: Multiple Market Pulse tabs are open
- **WHEN** more than one tab is open
- **THEN** each visible tab SHALL remain internally single-flight and hidden tabs SHALL not continue recurring live polling
