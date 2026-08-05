## Why

The Candle Opens page places several repetitive summary strips ahead of its primary calendar,
which slows the daily timing workflow and gives low-priority information the same visual weight
as today, the selected session, and high-impact macro events. The page should operate as a modern
timing workstation while preserving every existing date, reset, macro, and day-selection behavior.

## What Changes

- Replace the oversized month hero with a compact Candle Opens command header containing the
  selected month, previous/next navigation, and a clear Today action.
- Consolidate the Today Snapshot and repeated cycle metrics into a single decision-oriented
  session summary with importance, reset count, next macro event, and timing tags.
- Move the calendar and selected Day Profile ahead of supporting insight cards so the timing map is
  visible much earlier in the page flow.
- Keep the Day Profile beside the calendar on desktop and make it a stable inspection rail while
  preserving keyboard and pointer day selection.
- Consolidate upcoming reset, macro-collision, and peak-signal information into a quieter catalyst
  strip below the primary calendar workspace.
- Preserve the expandable macro agenda, cycle definitions, legends, holiday states, event links,
  mobile weekly folds, and all existing server-calculated values.
- Improve hierarchy, spacing, buttons, calendar-cell density, selected-day states, hover/focus
  feedback, and responsive behavior without changing financial or market calculations.

### Non-goals

- No changes to reset-cycle, macro-event, importance, or trading-session calculations.
- No new market-data provider, dependency, persistence model, or execution signal.
- No removal of existing month navigation, day details, macro events, or mobile workflows.
- No redesign of Market Pulse or other menu destinations.

### Acceptance criteria

- The calendar workspace appears before supporting catalyst and macro-detail sections.
- Previous month, next month, This Month, day selection, day profile, and macro links keep their
  current destinations and semantics.
- Repeated summary content is consolidated without hiding any currently available values.
- The page has no horizontal overflow at 390px, 768px, 1440px, or 1920px widths.
- All visible controls retain accessible names and keyboard focus behavior.
- Existing Candle Opens page and macro-calendar tests continue to pass, with new contract coverage
  for the revised hierarchy.

## Capabilities

### New Capabilities

- `candle-opens-workflow`: Defines the calendar-first timing workflow, compact session summary,
  catalyst hierarchy, responsive behavior, and preservation requirements for Candle Opens.

### Modified Capabilities

- None.

## Impact

- Primary UI: `mccain_capital/templates/core/candle_opens.html` and Candle Opens rules in
  `static/css/app.css` or the existing modern-page stylesheet.
- Verification: focused Jinja/Flask contract tests and authenticated visual captures.
- Data sources and calculations remain the existing Candle Opens route context, cycle calculations,
  holiday calendar, and macro-event feed/fallback behavior.
- No API, database, financial assumption, or external dependency changes.
