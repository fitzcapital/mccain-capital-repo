## Why

The Dashboard contains the right trading, planning, behavior, calendar, and account data, but
important actions are fragmented across custom overlays and deeply nested disclosure panels.
Standardizing these interactions will make the page faster to operate, easier to understand, and
safer during time-sensitive trading decisions without removing existing functionality.

## What Changes

- Introduce a reusable, accessible Dashboard modal and drawer system with consistent desktop and
  mobile behavior, keyboard controls, focus management, loading states, and action hierarchy.
- Replace the static Pressure Check popup with a guided reset workflow that records the trigger,
  runs a short reset, confirms trading rules, and applies the selected operating mode.
- Expand the calendar day preview into a session inspector with daily results, trades, journal and
  debrief state, reconciliation actions, and selected-day navigation.
- Add a keyboard-accessible Dashboard command palette for common actions such as recording a
  trade, opening Market Pulse, importing activity, journaling, refreshing planning, and running a
  Pressure Check.
- Move broker metrics and synchronization controls into a focused drawer that exposes manual
  values, refresh status, diagnostics, and session-seeding controls without allowing failed
  refreshes to replace manual values.
- Add consistent progress, success, failure, retry, and diagnostic feedback for asynchronous
  Dashboard operations.
- Reorganize the Dashboard interaction flow around command, today, decision, and review layers
  while preserving existing cards, data sources, routes, trading rules, and reference content.
- Acceptance criteria include keyboard-complete modal operation, mobile sheet behavior, preserved
  selected-day trade scope, non-destructive broker refreshes, visible operation status, and no loss
  of existing Dashboard actions.
- Non-goals: redesigning Market Pulse or other destination pages, changing trade accounting or
  financial calculations, replacing existing market-data providers, or modifying personal/runtime
  data.

## Capabilities

### New Capabilities

- `dashboard-interaction-workflows`: Defines the shared modal and drawer behavior, guided reset,
  session inspection, command palette, broker metrics controls, asynchronous operation feedback,
  and progressive Dashboard information flow.

### Modified Capabilities

None. The current spec catalog does not contain an existing Dashboard interaction capability.

## Impact

- Dashboard Jinja templates and lazy-loaded Dashboard fragments.
- Dashboard JavaScript event binding, state management, and asynchronous operation handling.
- Dashboard CSS for modal, drawer, sheet, command palette, operation feedback, and responsive
  layouts.
- Existing Dashboard endpoints for planning, trade-day navigation, journal/debrief state, broker
  metrics, imports, and diagnostics may be consumed or extended, but their financial semantics and
  source-of-truth rules remain unchanged.
- Calendar navigation must continue to resolve one explicitly selected day, excluding adjacent-day
  trades.
- Broker metrics continue to use manual inputs as the primary fallback and may only replace them
  after a successful authenticated refresh or seed flow.
- Targeted template, JavaScript, route, and browser-level regression coverage will be required.
