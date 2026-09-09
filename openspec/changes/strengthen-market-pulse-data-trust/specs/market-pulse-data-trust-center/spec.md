## ADDED Requirements

### Requirement: Unified trust verdict
Market Pulse SHALL present one trust verdict for the current canonical generation as `Verified`,
`Degraded`, or `Locked`. The verdict MUST identify blockers, last verified time, and next automatic
check without requiring the trader to reconcile multiple status panels.

#### Scenario: Required inputs are current
- **WHEN** all required components are current, coherent, persisted, and session-aligned
- **THEN** the Trust Center displays `Verified` with no blocker

#### Scenario: Required input is stale
- **WHEN** Gamma or completed bars exceed the applicable freshness threshold
- **THEN** the Trust Center displays `Locked`, names the stale component, and shows the next recovery attempt

### Requirement: Progressive component evidence
The Trust Center SHALL keep the default summary compact and SHALL expose component status, source,
provider time, received time, age, threshold, latency, completeness, and fallback/proxy mode through
an accessible detail control.

#### Scenario: Trader opens trust details
- **WHEN** the trader expands the Trust Center
- **THEN** each component explains whether it is required, current, stale, missing, proxied, cached, or observation-only in plain language

#### Scenario: SPY volume proxy is active
- **WHEN** the SPX chart histogram uses timestamp-aligned SPY volume
- **THEN** the component evidence labels SPY volume as a liquidity proxy and never as native SPX volume

### Requirement: Durable reliability history
The system SHALL provide a read-only reliability-history view containing daily availability,
incident count, incident duration, affected components, source failures, and recovery times.

#### Scenario: Trader reviews a Gamma outage
- **WHEN** Gamma was stale for multiple refresh attempts and later recovered
- **THEN** history shows one incident with its start, recovery, duration, reason, and affected generations

#### Scenario: No incidents occurred
- **WHEN** the selected period contains no degraded or locked transitions
- **THEN** history shows verified coverage rather than an empty error state

### Requirement: Reliability transition alerts
The system SHALL create one sanitized application alert when trust first becomes degraded or locked
and one alert when the incident recovers. Unchanged repeated polls MUST NOT create duplicate alerts.

#### Scenario: Outage persists
- **WHEN** the same Gamma stale condition remains across repeated checks
- **THEN** one active incident and one alert exist until the state changes

#### Scenario: Outage recovers
- **WHEN** the blocked component becomes current and a verified generation is promoted
- **THEN** the incident closes and one recovery alert identifies the component and duration

### Requirement: Responsive trust-centered design
The Trust Center SHALL replace repeated health summaries on the primary Market Pulse page and SHALL
remain readable without clipped labels or horizontal page overflow at supported widths.

#### Scenario: Desktop execution view
- **WHEN** Market Pulse is viewed on desktop
- **THEN** one trust strip presents verdict, blocker, last verified time, next check, and details control before the execution chart

#### Scenario: Narrow execution view
- **WHEN** Market Pulse is viewed at a narrow supported width
- **THEN** trust fields stack, details remain accessible, and technical timestamps wrap without overlapping execution guidance
