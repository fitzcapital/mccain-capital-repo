## ADDED Requirements

### Requirement: Server-owned regular-session evaluation
The system SHALL evaluate canonical completed SPX five-minute bars during the regular exchange
session without requiring an open, visible, or connected Market Pulse browser page.

#### Scenario: Page is closed when a setup confirms
- **WHEN** an eligible setup confirms on a completed candle while no Market Pulse page is open
- **THEN** the server detects it within 30 seconds, persists it, and makes one eligible alert available

#### Scenario: Browser tab is hidden
- **WHEN** the Market Pulse tab suspends its client polling during the regular session
- **THEN** the server monitor continues setup evaluation independently

#### Scenario: Exchange session is closed
- **WHEN** the shared exchange calendar reports premarket, after-hours, weekend, or holiday state
- **THEN** the server monitor performs no recurring provider refresh or live setup delivery

### Requirement: Canonical setup parity
The server monitor SHALL use the same coherent generation, completed bars, exact Strat pattern rules,
level observations, setup identity, and evaluator used by Setup Replay.

#### Scenario: Exact reversal completes
- **WHEN** a supported exact reversal completes at an eligible key level
- **THEN** Live, Replay, analytics, and the alert ledger identify the same setup and signal candle

#### Scenario: Continuation follows the prior candle
- **WHEN** the completed candles form a continuation rather than an exact reversal
- **THEN** neither the server monitor nor Setup Replay records it as a reversal setup

#### Scenario: Required canonical data is stale
- **WHEN** completed bars or another required execution component is stale or incoherent
- **THEN** evaluation fails closed, no alert is delivered, and the last valid state is retained

### Requirement: Durable idempotent delivery
The system MUST persist each setup and eligible alert transition atomically so concurrent page and
server evaluation cannot create duplicate setup records or notifications.

#### Scenario: Page and server evaluate the same candle
- **WHEN** the browser context request and server monitor evaluate the same confirmed setup
- **THEN** one durable setup identity exists and at most one alert delivery is emitted

#### Scenario: Application restarts after delivery
- **WHEN** the application restarts and reevaluates a previously delivered setup
- **THEN** its terminal and delivery state remain monotonic and no duplicate alert is emitted

### Requirement: Gap recovery without stale alerts
The server monitor SHALL evaluate retained completed candles after startup, sleep, or provider
recovery and SHALL distinguish live-eligible detection from historical late review.

#### Scenario: Monitor recovers inside eligibility window
- **WHEN** a setup is first observed within the configured live eligibility window
- **THEN** it remains eligible for one alert if all other delivery gates pass

#### Scenario: Monitor recovers after eligibility window
- **WHEN** a valid setup is first observed after its live eligibility window
- **THEN** it is persisted for Replay and analytics as late review and no stale alert is delivered

### Requirement: Bounded monitor ownership and lifecycle
The system SHALL run a bounded monitor lifecycle with one active owner per process, injectable timing,
clean shutdown behavior, and no unbounded task or thread growth.

#### Scenario: Runtime starts normally
- **WHEN** the application runtime becomes ready outside test mode
- **THEN** one monitor owner starts and publishes its heartbeat without blocking request handling

#### Scenario: Duplicate startup is attempted
- **WHEN** runtime initialization requests another monitor in the same process
- **THEN** the existing owner is retained and no additional loop is created

#### Scenario: Test configuration loads the application
- **WHEN** the application is created under test configuration
- **THEN** no automatic background monitor starts unless the test explicitly invokes it

### Requirement: Clock-safe alert eligibility
The server monitor MUST detect material wall-clock discontinuity and revalidate the canonical session
before using timestamps for alert eligibility.

#### Scenario: Device wakes after clock discontinuity
- **WHEN** monotonic and wall-clock elapsed time materially diverge
- **THEN** the monitor records recovery, revalidates session and data, and suppresses delivery until
  timing is coherent
