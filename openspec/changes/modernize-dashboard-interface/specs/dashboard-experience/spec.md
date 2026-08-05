## ADDED Requirements

### Requirement: Operational hierarchy
The Dashboard SHALL present its existing capabilities in a clear operating sequence that makes
the current state, next required action, and primary decision controls more prominent than
supporting detail.

#### Scenario: User scans the Dashboard at session start
- **WHEN** the Dashboard loads successfully
- **THEN** discipline state, session command, readiness, and the primary next action are visually identifiable before secondary analytics and support content

#### Scenario: Supporting detail remains available
- **WHEN** secondary content is collapsed or visually de-emphasized
- **THEN** the user can reach that content from the Dashboard without navigating to a replacement page or losing its state

### Requirement: Consistent control system
The Dashboard SHALL use consistent visual and behavioral treatments for primary actions,
secondary actions, quiet actions, destructive actions, toggles, segmented controls, and
icon-only controls.

#### Scenario: Primary and secondary actions appear together
- **WHEN** a control group contains both the recommended next action and supporting actions
- **THEN** the recommended action has the strongest emphasis and supporting actions remain clearly interactive without competing for equal emphasis

#### Scenario: Stateful control changes
- **WHEN** a toggle, discipline state, mode, gate, range, ticker, or disclosure control changes state
- **THEN** its visual state and relevant ARIA state update together while retaining the existing JavaScript behavior

#### Scenario: Action is unavailable or running
- **WHEN** an action is disabled or awaiting an asynchronous result
- **THEN** the control displays a distinguishable disabled or loading state and prevents duplicate activation

### Requirement: Functional parity
The redesigned Dashboard MUST retain every existing user-facing control, destination, form
action, endpoint integration, persisted preference, live update, and lazy-loaded interaction.

#### Scenario: Existing control inventory is compared
- **WHEN** the redesigned Dashboard is verified against the pre-change control inventory
- **THEN** every inventoried control is present and has an equivalent destination, submission, event binding, or state transition

#### Scenario: Existing automated behavior is exercised
- **WHEN** focused Dashboard route, API, persistence, ticker, market-tape, account, readiness, reflection, pace, and calendar tests run
- **THEN** those tests pass without weakening their existing assertions

#### Scenario: Live data is stale or missing
- **WHEN** market, gamma, broker, or synchronization data is stale, delayed, missing, or failed
- **THEN** the redesigned surface preserves the existing diagnostic state and does not present unavailable data as live or successful

### Requirement: Manual broker data protection
The redesigned Dashboard MUST preserve manually entered broker metrics and existing
diagnostic-first refresh behavior.

#### Scenario: Broker refresh fails
- **WHEN** a broker refresh or session-seeding flow does not produce a valid successful result
- **THEN** existing manual metrics remain visible and unchanged while diagnostics remain accessible

#### Scenario: Broker refresh succeeds
- **WHEN** an existing refresh or seed flow returns a valid successful result
- **THEN** the same existing update behavior occurs through the redesigned controls

### Requirement: Responsive layout
The Dashboard SHALL remain usable at representative mobile, tablet, laptop, and wide-desktop
viewport sizes without horizontal page overflow.

#### Scenario: Narrow viewport
- **WHEN** the Dashboard is displayed at a mobile viewport
- **THEN** controls maintain usable touch targets, content follows the operating sequence, and dense modules use intentional internal scrolling or stacking instead of clipping the page

#### Scenario: Wide viewport
- **WHEN** the Dashboard is displayed at a wide-desktop viewport
- **THEN** primary and supporting regions use available space without excessive line lengths, detached controls, or oversized empty areas

### Requirement: Accessible interaction
The Dashboard SHALL provide keyboard-operable controls, visible focus, meaningful accessible
names, semantic state, and reduced-motion behavior.

#### Scenario: Keyboard navigation
- **WHEN** a keyboard user traverses Dashboard controls
- **THEN** focus order follows the visual operating sequence and every interactive control has a visible focus indicator

#### Scenario: Reduced motion preference
- **WHEN** the user has requested reduced motion
- **THEN** decorative transitions and live-value effects are minimized without hiding state changes or blocking interactions

#### Scenario: Icon-only control
- **WHEN** an action is represented only by an icon
- **THEN** it has a meaningful accessible name, tooltip or title where appropriate, and an adequate activation target

### Requirement: Calendar interaction compatibility
The redesigned Dashboard SHALL retain the current lazy-loaded calendar preview and selected-day
trade navigation behavior.

#### Scenario: Calendar fragment loads after disclosure opens
- **WHEN** the Dashboard calendar is loaded or reloaded lazily
- **THEN** day preview controls bind once, remain interactive, and display the selected day's preview data

#### Scenario: User opens a selected day
- **WHEN** the user selects a calendar day and activates Open Full Day
- **THEN** the Trades page receives that selected date and excludes trades from adjacent dates

### Requirement: Visual verification
The Dashboard redesign SHALL be evaluated against reproducible before-and-after visual captures
and a control inventory.

#### Scenario: Implementation phase completes
- **WHEN** a modernization phase is ready for review
- **THEN** authenticated captures at representative desktop and mobile widths show the complete page, and any missing control, overlap, clipping, or unreadable state is resolved before the phase is accepted
