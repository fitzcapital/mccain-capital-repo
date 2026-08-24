## ADDED Requirements

### Requirement: Atomic live-execution generation
The system SHALL publish execution-facing quote, completed bars, gamma context, levels, ranked
scenarios, evidence, permission, and timestamps as one validated generation. The client MUST NOT
combine execution fields from different generations.

#### Scenario: All required components validate
- **WHEN** quote, completed bars, gamma, and scenario evaluation pass freshness and coherence checks
- **THEN** the system atomically promotes the generation and every execution surface displays its
  generation id and consistent values

#### Scenario: One component is stale
- **WHEN** a current quote is available but completed bars or gamma fail validation
- **THEN** the current quote may render as observation-only while the last valid playbook remains
  visible, execution remains locked, and no partial execution fields are committed

### Requirement: Bounded non-overlapping refresh
Automatic and manual refresh SHALL use single-flight orchestration and bounded provider concurrency.
Every provider call MUST have a finite deadline, and a new refresh MUST NOT create overlapping work
for the same ticker while an attempt is active.

#### Scenario: Automatic check overlaps manual request
- **WHEN** the user requests a manual refresh while an automatic provider refresh is running
- **THEN** the manual request joins or observes the active attempt rather than creating another set
  of provider workers

#### Scenario: Provider does not respond
- **WHEN** a provider exceeds its deadline
- **THEN** the attempt terminates with component-level failure, preserves the last valid generation,
  and releases its worker capacity

### Requirement: Honest refresh status
The page SHALL distinguish canonical promotion from partial observation updates and SHALL report the
last attempt time, last valid time, next retry, and blocking components.

#### Scenario: Only quote and ladder advance
- **WHEN** quote and ladder update but the canonical generation cannot be promoted
- **THEN** the page reports a partial refresh and MUST NOT display “check succeeded” or imply that
  live execution is ready

#### Scenario: Canonical generation advances
- **WHEN** every required component validates and the generation is promoted
- **THEN** the page reports the refresh as successful, advances the last-valid timestamp, and
  reevaluates execution permission

### Requirement: Actionable live execution guide
The page SHALL present location, current evidence, trigger, required confirmation, action,
invalidation, and target from the same canonical scenario generation. Entry-oriented action language
MUST appear only when permission is live-safe and hard confirmation is complete.

#### Scenario: Confirmed live scenario
- **WHEN** fresh synchronized data produces a confirmed primary scenario
- **THEN** the guide names the exact level and price, confirmation event, directional action,
  invalidation, target, and generation time

#### Scenario: Scenario is waiting
- **WHEN** data is fresh but a hard confirmation has not occurred
- **THEN** the guide identifies the next observable event and withholds entry language

#### Scenario: Data becomes stale after confirmation
- **WHEN** a previously confirmed scenario loses required freshness
- **THEN** the guide changes to observation-only, removes current action authorization, and explains
  which component locked execution

### Requirement: In-place state-preserving refresh
Canonical live refresh SHALL update without a full-page reload and SHALL preserve chart timeframe,
viewport, user drawings, display toggles, selected ticker, and selected Gamma Ladder strike.

#### Scenario: New generation is promoted
- **WHEN** a canonical refresh completes while the user is inspecting the chart
- **THEN** execution content updates atomically without resetting the user's chart or ladder state

### Requirement: Live operational health
The runtime SHALL expose refresh activity and bounded worker health sufficient to detect stuck work
before it prevents request handling.

#### Scenario: Repeated market-hours refresh
- **WHEN** automatic refresh runs repeatedly across a representative test interval with provider
  timeouts included
- **THEN** active worker/thread counts return to a bounded steady range and API requests remain
  serviceable
