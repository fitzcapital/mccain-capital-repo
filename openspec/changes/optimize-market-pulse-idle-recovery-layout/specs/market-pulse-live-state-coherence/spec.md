## MODIFIED Requirements

### Requirement: Automatic receiving-page recovery
The receiving page SHALL recover without a full reload after bounded network, provider,
tab-suspension, page-cache restoration, or overlapping-request failures, while retaining one
authoritative polling flight. It SHALL suspend scheduled canonical polling while hidden and SHALL
resume the visible cadence from one immediate reconciliation when the prior validation is stale.

#### Scenario: Network returns
- **WHEN** the browser returns online after one or more failed canonical checks
- **THEN** one immediate canonical validation runs, the watchdog reports recovery, and a coherent newer
  generation commits atomically when available

#### Scenario: Multiple tabs request recovery
- **WHEN** multiple visible tabs request the same canonical recovery concurrently
- **THEN** server single-flight behavior prevents duplicated provider work and every tab adopts the
  same newest verified generation

#### Scenario: Page remains hidden
- **WHEN** the Market Pulse tab is hidden or backgrounded
- **THEN** scheduled client canonical polling is suspended and last-valid state remains visible

#### Scenario: User returns after one visible cadence
- **WHEN** the page becomes visible or focused more than 15 seconds after its last canonical check
- **THEN** exactly one immediate single-flight validation runs and visible polling resumes without a
  full reload

#### Scenario: Page is restored from browser cache
- **WHEN** a persisted page is restored after `pagehide` cleared its timers
- **THEN** the countdown and canonical coordinator are re-armed and one immediate validation runs
