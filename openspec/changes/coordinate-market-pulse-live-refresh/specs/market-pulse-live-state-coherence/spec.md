## ADDED Requirements

### Requirement: Coordinated refresh scheduler
The Market Pulse page SHALL coordinate quote, bars, levels, gamma metadata, and canonical execution
checks through one lifecycle scheduler using server-provided market-phase cadence. Superseded
component timers MUST NOT independently trigger competing canonical updates.

#### Scenario: Visible live session
- **WHEN** Market Pulse is visible during an open session
- **THEN** the scheduler polls each component at its advertised cadence and prevents overlapping
  requests for the same component

#### Scenario: Page returns to the foreground
- **WHEN** a hidden or unfocused Market Pulse page becomes visible after a check is overdue
- **THEN** the scheduler immediately checks required components once and resumes normal cadence

#### Scenario: Market is closed
- **WHEN** the server identifies a closed market phase
- **THEN** the scheduler uses the slower advertised cadence without changing the last-valid execution
  generation merely because no new session data exists
- **AND** quote rendering MUST NOT relabel the retained snapshot as `Live` or replace its canonical
  last-valid timestamp with a browser/server clock

#### Scenario: Initial render follows the exchange session
- **WHEN** the page is first rendered outside the regular SPX session while a retained chart snapshot
  still reports `live_session`
- **THEN** the header and session card use the current server market-hours contract, show `Last valid`
  and `Planning`, and MUST NOT infer a live state from the retained chart mode

### Requirement: Conditional canonical generation checks
Automatic Market Pulse refresh SHALL check the cached canonical generation without forcing provider
work and SHALL avoid transferring or reconciling the complete canonical payload when the client's
generation is current. Manual refresh MUST remain available for an explicit bounded provider refresh.

#### Scenario: Generation is unchanged
- **WHEN** an automatic check presents the current generation validator and no newer coherent
  generation exists
- **THEN** the server returns an unchanged response and the client performs no full execution DOM
  reconciliation

#### Scenario: New coherent generation exists
- **WHEN** the cached server snapshot contains a newer coherent generation
- **THEN** the server returns the complete canonical payload and the client atomically applies it

#### Scenario: User forces refresh
- **WHEN** the user activates Refresh data
- **THEN** the server requests one bounded single-flight provider refresh and returns promoted,
  unchanged, partial, or retry guidance without reloading the page

### Requirement: Dependency-driven execution reevaluation
Market Pulse SHALL request canonical execution reevaluation after a new completed candle or a changed
required gamma, level, freshness, or permission version. Quote-only observation changes MUST NOT
alter execution-authoritative fields.

#### Scenario: Quote changes within an incomplete candle
- **WHEN** a newer quote arrives but no required execution dependency changes
- **THEN** observation price may update while regime, permission, scenario, evidence, action,
  invalidation, and target retain the current canonical generation

#### Scenario: Five-minute candle completes
- **WHEN** a new completed five-minute candle becomes available after the boundary grace period
- **THEN** the scheduler requests canonical reevaluation and applies a resulting coherent generation
  within ten seconds

#### Scenario: Required component changes
- **WHEN** gamma, levels, freshness, or permission publishes a newer required-component version
- **THEN** the scheduler requests one canonical reevaluation and names any component blocking
  promotion

### Requirement: Observable refresh health
The system SHALL expose generation validators, component timestamps or versions, market phase,
server next-check guidance, promotion outcome, and blocking components sufficient to diagnose live
refresh behavior without presenting routine automatic polling as prominent status text. The page
SHALL show a compact countdown derived from the coordinator's actual next canonical-check or retry
deadline, and the countdown MUST NOT independently trigger refresh work.

#### Scenario: System is current
- **WHEN** required components and canonical generation are current
- **THEN** the page shows a quiet live/age indicator with `Refresh in Ns` and keeps detailed polling
  diagnostics collapsed

#### Scenario: Session closes while the page remains open
- **WHEN** the canonical refresh contract changes from open to closed
- **THEN** the header shows `Last valid` with the canonical market-data timestamp, the session card
  shows `Planning`, and later quote renders cannot restore a `Live` label

#### Scenario: Retry is scheduled
- **WHEN** a partial, in-flight, or failed response schedules a bounded retry
- **THEN** the same indicator changes to `Retry in Ns` using the scheduler's deadline and returns to
  normal refresh timing after recovery

#### Scenario: Countdown reaches zero
- **WHEN** the displayed countdown reaches zero
- **THEN** only the coordinator initiates the scheduled check and publishes the next deadline without
  creating a duplicate request

#### Scenario: Component is delayed
- **WHEN** a required component misses its expected freshness window
- **THEN** the page preserves the last-valid generation, locks execution when required, and exposes
  the delayed component, its age, and next retry

#### Scenario: Repeated request failure
- **WHEN** automatic refresh requests fail repeatedly
- **THEN** the scheduler uses bounded backoff with jitter, keeps manual recovery available, and never
  leaves a permanent loading state

### Requirement: Compact refresh controls and candle-time truth
Market Pulse SHALL present sticky-summary, manual-refresh, and Candle Opens actions as compact,
accessible icon controls. The manual-refresh icon SHALL animate only while its bounded request is in
flight. The header timestamp SHALL identify the newest completed five-minute candle and MUST NOT be
replaced by quote, snapshot-generation, browser-clock, or server-clock timestamps.

#### Scenario: Header is reconciled by any renderer
- **WHEN** canonical, quote, gamma-context, or chart rendering updates the command header
- **THEN** the header continues to show `Last candle` with the canonical completed-bar timestamp

#### Scenario: Automatic check is scheduled
- **WHEN** the coordinator owns a future refresh or retry deadline
- **THEN** a compact pill beside the header utility icons counts down to that same deadline without scheduling another request or overlapping market-state metrics

#### Scenario: User manually refreshes
- **WHEN** the user activates the refresh icon
- **THEN** the icon spins until the existing bounded request settles and remains accessible by label and tooltip
