## ADDED Requirements

### Requirement: Shared accessible interaction surfaces
The Dashboard SHALL provide one reusable interaction system for modals, drawers, and responsive
sheets, with labelled semantics, focus containment, focus restoration, Escape handling, scroll
locking, and no more than one open interaction surface at a time.

#### Scenario: Keyboard opens and closes a modal
- **WHEN** a keyboard user activates a Dashboard modal trigger
- **THEN** focus moves into the labelled modal and remains within it until the user closes it
- **AND** closing with Escape restores focus to the invoking control

#### Scenario: Drawer adapts on a narrow viewport
- **WHEN** a drawer workflow opens on a narrow viewport
- **THEN** the system presents it as a bottom or full-height sheet with an accessible close control
- **AND** the underlying Dashboard does not scroll while the sheet is open

#### Scenario: Lazy fragment is replaced
- **WHEN** a Dashboard fragment containing interaction triggers is replaced after page load
- **THEN** the new triggers are bound exactly once and open the correct surface

### Requirement: Guided Pressure Check workflow
The Dashboard SHALL provide a guided Pressure Check that captures an urgency trigger, runs a short
reset interval, confirms setup, confirmation, rules, stop, and risk alignment, and requires an
explicit operating outcome.

#### Scenario: User returns to the plan
- **WHEN** the user completes the reset, confirms all required checks, and selects Return to Plan
- **THEN** the Dashboard sets discipline to Locked In and A+ Only
- **AND** it returns the user to the planning and trade-gate context

#### Scenario: User stands down
- **WHEN** the user selects Done for the Day from Pressure Check
- **THEN** the Dashboard disables new-risk actions, clears the current trade gate, and reflects the
  new mode in the discipline rail

#### Scenario: User tries to proceed without alignment
- **WHEN** the user attempts to proceed while a required gate item is incomplete or the operating
  mode blocks new risk
- **THEN** the Dashboard refuses to enable new risk and identifies the unresolved requirement

#### Scenario: Behavior logging fails
- **WHEN** the local reset outcome is applied but the behavior update request fails
- **THEN** the conservative local discipline state remains applied
- **AND** the Dashboard shows retryable logging feedback without discarding the user's reset result

### Requirement: Calendar session inspector
The Dashboard SHALL open a session inspector for a selected calendar day that displays that day's
summary, trade activity, journal or debrief state, and relevant follow-up actions.

#### Scenario: Inspect a populated trading day
- **WHEN** the user selects a calendar day with recorded activity
- **THEN** the inspector shows that date, net result, trade count, record, balance, and available
  trade or review details
- **AND** it offers actions to open the day and begin any available debrief or reconciliation flow

#### Scenario: Inspect a day with missing detail
- **WHEN** summary data exists but optional trade, journal, or debrief detail is unavailable
- **THEN** the inspector preserves the available summary, labels the missing detail, and keeps valid
  navigation actions usable

#### Scenario: Open the selected day in Trades
- **WHEN** the user opens a session dated `YYYY-MM-DD` from the inspector
- **THEN** the Trades page uses that date as a one-day scope
- **AND** trades from the previous and following dates are absent unless the user explicitly changes
  scope

### Requirement: Dashboard command palette
The Dashboard SHALL provide a visible and keyboard-accessible command palette that searches and
invokes existing Dashboard actions without duplicating their business logic.

#### Scenario: Open and navigate the palette
- **WHEN** the user presses Command+K or Control+K outside an editable field
- **THEN** the palette opens with focus in search and supports arrow-key selection, Enter, and Escape

#### Scenario: Invoke an existing action
- **WHEN** the user selects a command such as Pressure Check, Market Pulse, journal, import, or
  planning refresh
- **THEN** the system invokes the same destination or handler used by the corresponding visible
  Dashboard control

#### Scenario: Action is unavailable
- **WHEN** a command is incompatible with the current session or discipline state
- **THEN** the palette prevents execution and explains why the action is unavailable

### Requirement: Broker metrics management drawer
The Dashboard SHALL provide a focused broker metrics drawer that distinguishes manual,
statement-derived, and successfully refreshed values and exposes refresh, diagnostics, and session
seeding controls.

#### Scenario: Save manual broker values
- **WHEN** the user submits valid manual broker metrics
- **THEN** the existing server validation persists the values and the Dashboard updates their source
  and timestamp

#### Scenario: Refresh succeeds
- **WHEN** an authenticated broker refresh returns a validated successful result
- **THEN** the Dashboard displays the refreshed values, source, and success timestamp

#### Scenario: Refresh fails
- **WHEN** a broker refresh, parse, or authentication attempt fails
- **THEN** the Dashboard keeps the prior manual or confirmed values unchanged
- **AND** the drawer shows diagnostics and appropriate retry or headed session-seeding controls

### Requirement: Asynchronous operation feedback
Dashboard asynchronous operations SHALL expose a consistent state of idle, loading, success,
stale, or error while preserving the last confirmed content until replacement data succeeds.

#### Scenario: Operation is running
- **WHEN** the user starts a refresh, synchronization, import, or seed action
- **THEN** the initiating control displays a busy spinner and is protected from duplicate submission
- **AND** a status region identifies the active operation

#### Scenario: Operation succeeds
- **WHEN** an asynchronous operation returns a validated successful result
- **THEN** the relevant Dashboard content updates and the status region shows a completion summary
  and timestamp

#### Scenario: Operation fails with existing content
- **WHEN** an asynchronous operation fails while confirmed content is already displayed
- **THEN** the confirmed content remains visible and the status region offers retry and available
  diagnostic information

### Requirement: Progressive Dashboard information flow
The Dashboard SHALL organize existing content into Command, Today, Decision Tools, and Review &
Reference layers while preserving every current Dashboard action and destination.

#### Scenario: Dashboard initially loads
- **WHEN** the Dashboard completes its initial render
- **THEN** command state, current-session summary, permission, planning, and immediate actions are
  available before long-form review and reference content

#### Scenario: User needs historical or reference content
- **WHEN** the user activates a Review & Reference summary
- **THEN** the relevant existing calendar, performance, pace, behavior, or health content becomes
  available without navigating away unless the user chooses a destination action

#### Scenario: Existing action regression check
- **WHEN** the modernized Dashboard is compared with the current Dashboard action inventory
- **THEN** every existing user action remains reachable and retains its original business behavior

### Requirement: Responsive and reduced-motion behavior
Dashboard interaction workflows SHALL remain usable across supported desktop and mobile widths and
SHALL respect the user's reduced-motion preference.

#### Scenario: Narrow viewport interaction
- **WHEN** the Dashboard is used at a supported narrow viewport
- **THEN** surface content and sticky actions remain within the viewport without horizontal overflow

#### Scenario: Reduced motion is enabled
- **WHEN** the operating system requests reduced motion
- **THEN** modal, drawer, spinner, and status transitions avoid non-essential movement while
  preserving visible state changes
