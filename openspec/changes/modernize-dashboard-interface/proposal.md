## Why

The Dashboard has grown into a capable operating surface, but its dense card treatment,
mixed button styles, and competing visual emphasis make it harder to scan and operate than
necessary. Modernizing the interface will improve clarity, hierarchy, responsiveness, and
interaction confidence without removing or changing existing functionality.

## What Changes

- Introduce a cohesive Dashboard visual system for buttons, icon controls, cards, status
  indicators, spacing, typography, focus states, and responsive layouts.
- Reorganize visual hierarchy around the existing operating sequence: discipline and session
  command, planning and trade gate, market monitoring, support and health, performance, pace,
  and calendar.
- Standardize primary, secondary, quiet, destructive, toggle, and icon-only controls while
  retaining their current destinations, form actions, event bindings, and state semantics.
- Reduce visual crowding through clearer section boundaries and progressive disclosure while
  keeping every existing feature reachable from the Dashboard.
- Preserve live tape updates, ticker switching, gamma context, readiness and discipline state,
  broker metric editing, account selection, uploads, reconciliation, milestone controls,
  performance ranges, projection controls, calendar previews, and day-scoped trade links.
- Improve keyboard navigation, visible focus, touch targets, accessible labels, active states,
  loading states, reduced-motion behavior, and narrow-screen usability.
- Capture visual baselines before implementation and verify desktop and mobile layouts after
  each implementation phase.

### Non-goals

- No change to trading, treasury, projection, P&L, readiness, or broker business logic.
- No removal, renaming, or replacement of existing endpoints or persisted setting keys.
- No redesign of the standalone Calendar, Trades, Analytics, Market Pulse, or Executive pages.
- No new JavaScript framework, CSS framework, or third-party component library.
- No modification of runtime or personal financial data.

### Acceptance criteria

- Every current Dashboard control remains present, reachable, and functionally equivalent.
- Existing Dashboard route, endpoint, ticker, calendar-scope, persistence, and interaction tests
  continue to pass.
- Primary actions are visually distinct from secondary and destructive actions, and every
  interactive control has visible hover, focus, active, disabled, and loading behavior where
  applicable.
- The Dashboard has no horizontal page overflow at representative mobile, tablet, laptop, and
  wide-desktop widths; dense data modules may retain intentional internal scrolling.
- Lazy-loaded calendar content remains rebinding-safe, and opening a calendar day continues to
  show only the selected day's trades.
- Manual broker metrics remain primary and are not overwritten unless an existing refresh or
  seed flow succeeds.
- Before-and-after visual captures confirm the redesign without missing sections or controls.

## Capabilities

### New Capabilities

- `dashboard-experience`: Defines the Dashboard's modern visual hierarchy, control system,
  responsive behavior, accessibility, progressive disclosure, and functional-parity contract.

### Modified Capabilities

None. No existing OpenSpec capabilities are present yet.

## Impact

- Primary surfaces: `mccain_capital/templates/dashboard.html`, Dashboard partials, and
  Dashboard-scoped rules in `static/css/app.css`.
- Interaction review: `static/js/dashboard_command_center.js`; JavaScript changes are limited to
  markup compatibility or accessibility state synchronization required by the redesign.
- Verification: focused Dashboard tests in `tests/test_app_core.py` and related Dashboard test
  modules, JavaScript syntax checks, authenticated route rendering, and visual regression
  captures at representative viewport sizes.
- APIs and data: no intentional changes to endpoints, response contracts, stored settings,
  market-data sources, broker data, financial calculations, or runtime data.
