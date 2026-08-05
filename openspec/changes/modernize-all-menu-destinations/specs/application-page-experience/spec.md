## ADDED Requirements

### Requirement: Complete internal destination coverage
The application SHALL modernize every unique internal destination reachable from the desktop
primary navigation, desktop overflow Menu, tablet drawer, or mobile menu while retaining the
Trading Dashboard as the reference experience.

#### Scenario: Navigation inventory is compared
- **WHEN** desktop, tablet, and mobile navigation destinations are inventoried
- **THEN** every unique internal route is assigned to a modernization phase and no destination is
  omitted because it appears only in an overflow or mobile menu

#### Scenario: External destination is encountered
- **WHEN** a navigation item points to an external website
- **THEN** its application-side navigation treatment and safety attributes remain intact while the
  external website is excluded from redesign scope

### Requirement: Cohesive page hierarchy
Each internal destination SHALL use a coherent visual hierarchy derived from the Trading Dashboard
while preserving the specialized workflow and information architecture of that page.

#### Scenario: User opens a modernized page
- **WHEN** an internal menu destination renders successfully
- **THEN** the page identity, current state, primary action, supporting controls, and secondary
  information are distinguishable without treating every surface as equally important

#### Scenario: Page contains specialized dense content
- **WHEN** a page presents a table, chart, calendar, editor, upload workspace, or operational log
- **THEN** the specialized component remains usable and is integrated into the shared hierarchy
  without being replaced by a generic card layout

### Requirement: Shared control system
Internal destinations SHALL use consistent treatments for primary, secondary, quiet, danger,
segmented, toggle, icon-only, form, disclosure, and dialog controls.

#### Scenario: Primary and supporting actions coexist
- **WHEN** a control group contains a recommended next action and supporting actions
- **THEN** the recommended action has the strongest emphasis and supporting actions remain clearly
  interactive without competing for equal emphasis

#### Scenario: Stateful or asynchronous action changes
- **WHEN** a control becomes active, expanded, disabled, loading, successful, or failed
- **THEN** its visual and semantic state update together without changing its existing event or
  submission behavior

#### Scenario: Destructive or security-sensitive action is shown
- **WHEN** a page presents delete, restore, backup, authentication, sign-out, or other sensitive
  controls
- **THEN** danger hierarchy, authorization, CSRF, confirmation, and disabled safeguards remain intact

### Requirement: Cross-page functional parity
The modernization MUST retain every existing route, link, form action, field name, query parameter,
filter, pagination control, endpoint integration, persisted setting, upload, export, lazy fragment,
and user-visible failure state.

#### Scenario: Control inventory is compared
- **WHEN** a modernized page is compared with its baseline inventory
- **THEN** every control has an equivalent destination, submission, event binding, query-state
  transition, or persistence result

#### Scenario: Navigation changes receiving-page state
- **WHEN** a control opens a filtered, scoped, paginated, or date-specific destination
- **THEN** the receiving page reflects the requested state and excludes data outside that scope

#### Scenario: Form or upload completes
- **WHEN** an existing form, upload, export, or settings action succeeds or fails
- **THEN** the same persisted result, redirect, validation, diagnostic, and error behavior occurs
  through the modernized surface

### Requirement: Data-trust and financial-state preservation
Modernized pages MUST preserve the meaning of live, stale, delayed, missing, manual, failed,
disabled, locked, unauthenticated, and diagnostic states.

#### Scenario: Data is unavailable or stale
- **WHEN** market, broker, financial, synchronization, or analytics data is stale, delayed, missing,
  or failed
- **THEN** the page does not present the data as live or successful and keeps diagnostics reachable

#### Scenario: Financial projection is displayed
- **WHEN** a projection, treasury, payout, income, pace, or account value is shown
- **THEN** its current source inputs, timing assumptions, account allocation, and fallback behavior
  remain available and unchanged

#### Scenario: Broker refresh fails
- **WHEN** an existing broker refresh or authenticated seed flow fails
- **THEN** manual broker metrics remain unchanged and the failure diagnostics remain visible

### Requirement: Responsive page-family behavior
Every modernized page family SHALL remain usable at representative mobile, tablet, laptop, and
wide-desktop widths without document-level horizontal overflow.

#### Scenario: Narrow viewport
- **WHEN** an internal page is displayed at 390 or 768 pixels wide
- **THEN** content follows a logical reading order, controls retain usable targets, and dense modules
  stack or use intentional internal scrolling instead of clipping the document

#### Scenario: Wide viewport
- **WHEN** an internal page is displayed at 1440 or 1920 pixels wide
- **THEN** the page uses available space without detached controls, unreadable line lengths, or
  oversized empty regions that obscure the operating sequence

### Requirement: Accessible interaction
Every modernized internal page SHALL provide keyboard-operable controls, visible focus, meaningful
accessible names, semantic state, adequate touch targets, and reduced-motion behavior.

#### Scenario: Keyboard traversal
- **WHEN** a keyboard user traverses a modernized page
- **THEN** focus follows the page's visual workflow and every interactive control has a visible focus
  indicator

#### Scenario: Icon-only action
- **WHEN** an action is represented only by an icon
- **THEN** it has a meaningful accessible name and an adequate activation target

#### Scenario: Reduced motion is requested
- **WHEN** the operating system requests reduced motion
- **THEN** decorative transitions are minimized without hiding state changes or blocking actions

### Requirement: Phased verification
Each page-family phase MUST be verified against reproducible behavioral and visual evidence before
implementation continues to the next higher-risk family.

#### Scenario: Page-family phase is ready for review
- **WHEN** a modernization phase is complete
- **THEN** focused route and interaction tests, JavaScript syntax checks, responsive captures, control
  inventory, accessible-name checks, and overflow checks are reviewed before the phase is accepted

#### Scenario: Baseline contract failure is discovered
- **WHEN** a focused test already fails in untouched code
- **THEN** the failure is documented separately and the modernization does not weaken the assertion
  or alter unrelated business logic to conceal it
