## Why

The Candle Opens page contains useful timing data, but its strongest workflow—the calendar and
selected-day profile—is diluted by repeated summaries, catalyst cards, and cycle references. It is
also hidden in the Tools menu even though it is a primary preparation workspace.

## What Changes

- Add **Candle Opens** to the desktop primary navigation immediately after **Market Pulse** and
  remove its duplicate desktop Tools-menu entry while preserving responsive/mobile access.
- Make the month calendar and selected-day profile the dominant page workspace.
- Replace the large Today Snapshot with one compact, actionable today strip that does not repeat
  the selected-day profile.
- Consolidate duplicate catalyst summaries into a date-deduplicated **Next Catalysts** strip with
  no more than three upcoming dates visible at once.
- Consolidate monthly totals and day/week/month cycle legends into one collapsible **Timing
  Reference** section; retain the collapsible macro-event list.
- Preserve every timing marker, macro event, month control, selected-day detail, and explanatory
  note. This change removes repeated presentation, not source data.
- Add focused navigation, template-contract, responsive-layout, and rendered-page checks.

Non-goals:

- No changes to reset-cycle calculations, macro-event sources, financial or execution rules, or
  Market Pulse behavior.
- No new background polling, database changes, or third-party dependencies.

## Capabilities

### New Capabilities

- `candle-opens-workspace`: Defines primary-navigation access and a concise, calendar-first timing
  workspace with non-duplicative supporting context.

### Modified Capabilities

None.

## Impact

- Shared navigation in `mccain_capital/templates/base.html`.
- Candle Opens presentation in `mccain_capital/templates/core/candle_opens.html` and its scoped
  styles/scripts.
- Focused Candle Opens and navigation contract tests plus desktop/responsive visual verification.
- No API, persistence, financial-calculation, or dependency impact.

## Acceptance Criteria

- Desktop primary navigation shows Candle Opens directly after Market Pulse exactly once.
- The initial Candle Opens viewport prioritizes the month controls, compact today context, calendar,
  and selected-day profile without repeating the same day facts in multiple expanded sections.
- Upcoming catalyst cards are unique by date and show at most three dates before expansion.
- Cycle definitions and monthly reference totals remain reachable in one disclosure.
- Existing calendar selection, month navigation, macro details, and responsive behavior continue to
  work without horizontal overflow at supported widths.
