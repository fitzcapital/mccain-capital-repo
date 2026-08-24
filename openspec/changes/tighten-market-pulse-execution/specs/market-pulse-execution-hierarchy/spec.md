## ADDED Requirements

### Requirement: One authoritative execution verdict
Market Pulse SHALL present one prominent execution verdict derived in this order: data safety and
session permission, active strategy path and state, next required evidence, invalidation, gamma
management, and targets. Supporting sections MUST NOT publish competing actionability conclusions.

#### Scenario: Required data is stale
- **WHEN** any required execution component is stale or unavailable
- **THEN** the authoritative verdict is locked and no supporting surface says Actionable or Trigger confirmed

#### Scenario: Data is healthy and strategy is pending
- **WHEN** required inputs are current but the ordered setup is incomplete
- **THEN** the verdict says Not actionable yet and identifies the earliest missing evidence

#### Scenario: Strategy becomes ready
- **WHEN** required inputs are current and a terminal strategy path is confirmed
- **THEN** the verdict presents the direction, path, invalidation, and first deterministic target

### Requirement: Persistent execution strip
The primary workflow SHALL provide a compact execution strip containing the authoritative verdict
and lock reason, active level and distance, next required evidence, and invalidation from the same
canonical generation. It SHALL avoid repeating spot, generation hashes, availability, path labels,
or generic instructional filler already visible elsewhere.

#### Scenario: User works lower on the page
- **WHEN** the user enables Sticky summary while reviewing the chart or checklist
- **THEN** the authoritative execution strip remains available without returning to a competing summary card

#### Scenario: Sticky summary is disabled
- **WHEN** the sticky-summary toggle is Off
- **THEN** the execution strip does not remain fixed while the page scrolls

#### Scenario: First viewport loads
- **WHEN** Market Pulse initially renders
- **THEN** the current gamma regime is named directly and the sticky-summary control visibly reports On or Off

### Requirement: Primary and supporting disclosure order
The visible execution flow SHALL prioritize the execution strip, chart/level map, one decision card,
ordered checklist, and gamma ladder. Tape, news, flow, and extended structure SHALL remain available
as secondary research without interrupting the primary decision sequence.

#### Scenario: Execution mode opens
- **WHEN** Market Pulse loads in execution mode
- **THEN** the primary execution sequence appears before secondary research content

#### Scenario: Research information is needed
- **WHEN** the user expands a secondary research section
- **THEN** existing research data and working controls remain available

#### Scenario: Lock explanation is needed
- **WHEN** the user expands Data Lock Diagnostics below the execution strip
- **THEN** canonical component evidence is available without adding a competing verdict or refresh control

### Requirement: Evidence-specific decision language
The verdict SHALL answer where price is, what level is active, what has been confirmed, what must
happen next, what invalidates the setup, and the first target when one is valid. It MUST avoid
generic trigger or invalidation text when structured evidence provides a precise statement.

#### Scenario: Setup is waiting for a close back inside
- **WHEN** location and sweep are confirmed but the completed reclaim candle is not
- **THEN** the verdict names the required completed candle and active level instead of a generic wait

#### Scenario: No target is valid yet
- **WHEN** neither reversal nor continuation is ready
- **THEN** the verdict withholds target emphasis and does not promote contract selection

### Requirement: Compatibility and scope control
The hierarchy change SHALL preserve ticker controls, chart interactions, gamma ladder, manual
refresh, navigation, and research data. It SHALL NOT add order entry, risk calculators, contract
sizing, account locks, or trade authorization.

#### Scenario: Updated page is inspected
- **WHEN** the receiving page is checked at desktop and narrow viewport widths
- **THEN** existing working controls remain usable and excluded risk or order controls are absent

### Requirement: Canonical strategy chart overlays
The existing execution chart SHALL show the structured Active level, structured Invalidation level,
and first valid Target from the canonical verdict generation. It MUST omit unavailable or invalid
prices, MUST NOT derive prices from browser-visible prose, and MUST reconcile only its own managed
strategy lines without recreating or refitting the chart.

#### Scenario: Canonical strategy levels are valid
- **WHEN** a verdict generation contains finite positive Active, Invalidation, and first Target values
- **THEN** the chart labels those prices by role from that same generation

#### Scenario: A strategy level is unavailable
- **WHEN** invalidation or target lacks a validated structured price
- **THEN** its overlay is absent and no price is inferred from its display text

#### Scenario: After-hours or pending planning state
- **WHEN** the strategy has not reached `REVERSAL_READY` or `CONTINUATION_ACTIVE`
- **THEN** canonical primary and expansion targets are absent and the chart shows no target overlay

#### Scenario: Confirmed strategy path has a target
- **WHEN** reversal-ready or continuation-active evidence produces a valid first target
- **THEN** the chart labels it `REVERSAL TARGET` or `CONTINUATION TARGET` for the confirmed path

#### Scenario: Canonical generation advances
- **WHEN** in-place refresh applies a newer valid verdict generation
- **THEN** strategy overlays reconcile without changing the chart instance, visible range, selected
  timeframe, drawings, gamma selection, or other chart levels

#### Scenario: Strategy roles share one price
- **WHEN** two or more eligible canonical roles resolve to the same numeric price
- **THEN** one readable overlay identifies all roles instead of stacking duplicate price labels
