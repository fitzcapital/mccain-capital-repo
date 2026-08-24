## ADDED Requirements

### Requirement: Canonical live setup lifecycle
The system SHALL derive each SPX live setup from canonical completed-candle evidence and SHALL expose
one of `WATCHING`, `ARMED`, `CONFIRMED`, `TARGET_REACHED`, `INVALIDATED`, or `EXPIRED`. The client MUST
NOT synthesize or advance lifecycle state.

#### Scenario: Candidate approaches its level
- **WHEN** a valid ranked SPX scenario is near its structural level but lacks its completed-candle prerequisite
- **THEN** the monitor records it as `WATCHING` and names the evidence required to become armed

#### Scenario: Candidate completes prerequisite evidence
- **WHEN** a canonical completed five-minute candle satisfies the scenario's location and boundary prerequisites but not its final trigger
- **THEN** the monitor advances the setup to `ARMED` and names the exact confirmation still required

#### Scenario: Candidate confirms
- **WHEN** a newer canonical completed five-minute candle satisfies every ordered hard gate with execution-safe permission
- **THEN** the monitor advances the setup once to `CONFIRMED` with action, invalidation, target, evidence time, and generation id

#### Scenario: Quote crosses intrabar
- **WHEN** price crosses a watched level but no completed candle satisfies the required gate
- **THEN** the setup does not advance to `CONFIRMED` and no actionable alert is produced

### Requirement: Stable setup identity and monotonic transitions
The system SHALL assign a deterministic setup identity from session, ticker, scenario family,
direction, normalized level, and first qualifying evidence. Repeated evaluation MUST preserve identity,
and a terminal setup MUST NOT return to a non-terminal state.

#### Scenario: Same setup appears in repeated polls
- **WHEN** several canonical generations contain the same qualifying evidence and level
- **THEN** every evaluation returns the same setup id and does not create duplicate state events

#### Scenario: A separate level confirms later
- **WHEN** another scenario of the same direction and family confirms at a different supported level
- **THEN** it receives a distinct setup id and is evaluated independently

#### Scenario: Invalidated setup is polled again
- **WHEN** a setup has entered `INVALIDATED` and later responses still contain its earlier confirmation evidence
- **THEN** its persisted state remains terminal and does not return to `CONFIRMED`

### Requirement: Highest-priority live setup presentation
The page SHALL show one persistent primary Live Setup Monitor and SHALL rank candidates by lifecycle
actionability before lane, confluence score, proximity, family priority, and stable identity. Lower
ranked candidates MUST remain available in a collapsed best-to-least watch list.

#### Scenario: Armed setup competes with a high-scoring watch
- **WHEN** an armed setup has a lower score than a watching candidate
- **THEN** the armed setup remains primary because lifecycle actionability precedes score

#### Scenario: Several watching candidates exist
- **WHEN** no setup is armed or confirmed
- **THEN** the highest-ranked watch is primary and the remaining watches appear in deterministic best-to-least order

### Requirement: Conservative confirmed-setup alerts
The system SHALL emit an actionable alert only for a new transition to `CONFIRMED` on SPX when the
score is at least 76, canonical permission is execution-safe, all required components are coherent and
fresh, completed-candle confirmation exists, and confirmation occurs no later than the configured
late-day cutoff.

#### Scenario: Qualified setup confirms before cutoff
- **WHEN** a B-or-better SPX setup first becomes `CONFIRMED` before the cutoff with fresh canonical data
- **THEN** the in-app notification channel receives one actionable setup alert with direction, level, trigger, invalidation, and target

#### Scenario: High score lacks confirmation
- **WHEN** an A-range setup remains `WATCHING` or `ARMED`
- **THEN** no actionable alert is emitted and the monitor states the next required event

#### Scenario: Setup confirms after cutoff
- **WHEN** an otherwise qualifying setup first confirms after the configured late-day cutoff
- **THEN** it is labeled `LATE / REVIEW ONLY`, creates no actionable alert, and remains eligible for historical Replay

#### Scenario: Required data is stale
- **WHEN** gamma, completed bars, levels, or canonical coherence fails at confirmation time
- **THEN** no actionable alert is emitted and the blocking component is named

### Requirement: Idempotent durable alert delivery
The system SHALL persist setup state and per-channel alert delivery using an idempotency key before
presenting an alert. Repeated polling, manual refresh, reload, restart, or concurrent workers MUST NOT
redeliver the same setup transition.

#### Scenario: Browser reloads after delivery
- **WHEN** the user reloads Market Pulse after a confirmed alert was delivered
- **THEN** the setup is restored with its delivered and acknowledgement state and the alert is not replayed

#### Scenario: Concurrent workers evaluate confirmation
- **WHEN** two workers observe the same confirmed setup transition
- **THEN** exactly one worker records delivery eligibility and at most one in-app alert is produced

#### Scenario: Persistence fails
- **WHEN** the app cannot durably record the alert idempotency key
- **THEN** it fails closed, reports degraded alerting, and does not deliver an alert that could be duplicated

### Requirement: Paused-state recovery and observability
The monitor SHALL expose last evaluation time, next evaluation time, state age, canonical generation,
freshness, and alert status. When required data becomes unsafe, it SHALL preserve the last setup as
paused context while removing current action authorization.

#### Scenario: Data becomes stale after confirmation
- **WHEN** a confirmed setup loses required canonical freshness
- **THEN** the monitor preserves its last-known details, labels it paused or stale, removes action authorization, and names each blocker

#### Scenario: Fresh canonical data recovers
- **WHEN** a coherent newer generation restores required freshness
- **THEN** the server reevaluates the persisted setup from canonical evidence and the client atomically renders the newer revision

### Requirement: Optional notification enhancements
The in-app monitor and notification center SHALL work without browser notification permission. Sound
or browser notification MUST require explicit permission and SHALL provide a persistent mute setting.

#### Scenario: Browser permission is denied
- **WHEN** a qualified setup confirms and browser notification permission is unavailable
- **THEN** the in-app alert remains visible and no repeated permission prompt blocks monitoring

#### Scenario: Alerts are muted
- **WHEN** the user has muted optional alert effects
- **THEN** the setup state and in-app notification record still update without sound or browser pop-up

### Requirement: Replay and financial isolation
Live setup monitoring SHALL NOT create trades, journal entries, positions, orders, or P&L records.
Setup Replay MUST NOT create or advance a live setup or alert.

#### Scenario: Replay finds an earlier target-reaching setup
- **WHEN** Setup Replay reconstructs a historical setup with a favorable outcome
- **THEN** no live alert is emitted and no financial or journal record is created

#### Scenario: Live setup reaches a target
- **WHEN** a monitored setup advances to `TARGET_REACHED`
- **THEN** the operational lifecycle updates without recording an executed trade or realized result
