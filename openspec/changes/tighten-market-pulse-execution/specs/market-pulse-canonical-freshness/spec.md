## ADDED Requirements

### Requirement: Canonical generation identity
Every canonical Market Pulse payload SHALL expose a stable generation identifier and independent
as-of timestamps for quote, completed bars, gamma, options, strategy evidence, and the oldest
required execution input. The generation identifier MUST change only when canonical payload content
changes and MUST be shared by every execution-critical consumer rendered from that payload.

#### Scenario: Canonical payload is returned
- **WHEN** the Market Pulse page or context API returns a valid canonical payload
- **THEN** it includes the generation identifier and every available component timestamp

#### Scenario: A component has no valid timestamp
- **WHEN** one component timestamp is missing or malformed
- **THEN** that component is reported Unavailable and cannot be disguised by a newer page timestamp

### Requirement: Visibility-aware automatic synchronization
While Market Pulse is visible, the browser SHALL check the cached canonical server generation at
least every 15 seconds without forcing upstream providers. A hidden page MUST pause those checks and
MUST check immediately when visible again if the previous check is older than the visible interval.

#### Scenario: New generation becomes available
- **WHEN** a visible page detects a generation newer than the one currently rendered
- **THEN** all execution-critical surfaces advance atomically to that generation without document
  navigation or a full-page reload

#### Scenario: Generation is unchanged
- **WHEN** an automatic check returns the currently rendered generation
- **THEN** the page remains stable without reloading, resetting chart state, or showing a busy overlay

#### Scenario: Hidden tab returns
- **WHEN** a hidden Market Pulse tab becomes visible after missing one or more checks
- **THEN** it immediately checks the canonical generation and catches up if necessary

### Requirement: Refresh concurrency and fallback safety
Automatic synchronization, focus catch-up, and forced manual refresh SHALL share one concurrency
gate. Manual `Refresh data` SHALL remain a forced provider refresh, take precedence over a pending
automatic check, and preserve the last valid generation if it fails.

#### Scenario: Refresh is already active
- **WHEN** another automatic or manual refresh is requested while one is in progress
- **THEN** no overlapping request or provider refresh is started

#### Scenario: Automatic check fails
- **WHEN** an automatic check fails or returns an invalid payload
- **THEN** the last valid generation remains rendered and the page reports degraded synchronization

#### Scenario: Manual forced refresh succeeds
- **WHEN** the user activates `Refresh data` and providers return valid data
- **THEN** the current document advances atomically to the resulting generation and reports success
  without navigation or a full-page reload

### Requirement: In-place canonical application
The browser SHALL apply one validated canonical payload through a single in-place application
boundary. The update MUST preserve the existing chart instance, scroll position, open disclosures,
execution/research mode, selected ticker and timeframe, and user-created chart state. Refresh code
MUST NOT call `location.assign`, `location.replace`, `location.reload`, or equivalent navigation.

#### Scenario: Execution generation changes during chart review
- **WHEN** a new canonical generation arrives while the user is reviewing or interacting with the chart
- **THEN** execution data updates in place and the chart interaction state remains intact

#### Scenario: Payload application fails validation
- **WHEN** the new payload is incomplete, malformed, or cannot be applied as one coherent generation
- **THEN** none of its execution-critical fields replace the last valid generation and an inline
  synchronization error is shown

### Requirement: Component freshness governs execution permission
The page SHALL show compact ages for spot, completed bars, gamma, options, and strategy evidence.
If a required component exceeds its session-aware stale threshold, execution permission MUST lock
and identify that component even when other components are current.

#### Scenario: Spot is current but gamma is stale
- **WHEN** the gamma age exceeds its active-session threshold while spot remains live
- **THEN** gamma is labeled stale and actionable strategy output is locked

#### Scenario: All required components are current
- **WHEN** every required component is within its threshold
- **THEN** freshness does not prevent the ordered strategy state from governing the verdict

### Requirement: Data-lock diagnostics
Market Pulse SHALL provide one collapsed diagnostics disclosure sourced from the canonical
generation. It SHALL list spot, completed bars, gamma, options, and strategy status, age, threshold,
last successful timestamp, required/optional role, and concise source/cache context. It SHALL also
show blocking required components, last canonical success time, and automatic retry cadence.

#### Scenario: Required bars are stale
- **WHEN** bars exceed their session-aware threshold
- **THEN** the collapsed summary names Bars as blocking and the expanded Bars row shows stale status,
  current age, threshold, last successful timestamp, and source/cache context

#### Scenario: Optional component is stale
- **WHEN** options or strategy metadata is stale but no required component is blocked by it
- **THEN** diagnostics label the component optional without falsely adding it to the execution lock

#### Scenario: Canonical generation updates in place
- **WHEN** a newer valid generation is applied
- **THEN** the diagnostics summary, component rows, last success time, and retry cadence update from
  that same generation without navigation or a separate provider request
