## Why

Forward Pace currently models only an integer number of weeks, which makes it difficult to answer practical questions tied to a specific deadline. The planner should make dates authoritative and explain the pace, time remaining, and likely outcome without hiding its weekday-only assumptions.

## What Changes

- Add a target date and optional target balance to the standalone Forward Pace planner.
- When a valid target date is supplied, derive the projection from the inclusive Monday-Friday sessions in the selected date window, including partial weeks.
- Keep Projection Weeks as a quick horizon control and derive the target date from it when date mode is not selected.
- Show calendar days, weekday sessions, equivalent trading weeks, elapsed/remaining sessions, projected completion, and the net daily/weekly pace required to reach an optional target balance.
- Add conservative, base, and stretch scenario outcomes using clearly disclosed pace multipliers.
- Expand the schedule and PDF export with date-window and target-progress details.
- Return clear validation errors for reversed or excessive date ranges.
- Redesign the planner into compact Account, Projection Window, and Tax & Reserve groups with an explicit Update Projection action.
- Mark edited inputs as unapplied, update results only after submission, and show a clear last-updated state.
- Lead results with a goal verdict that states the projected surplus or shortfall and the most direct planning adjustment.
- Add server-backed balance trajectory, current-versus-required pace, and scenario comparison charts.
- Estimate the target completion date for each scenario when a target balance is configured.
- Collapse calculation assumptions and the complete schedule by default while keeping them accessible for reconciliation.

### Non-goals

- Using exchange-holiday calendars, broker forecasts, or live trade results.
- Changing the existing 2026 federal and state planning-tax model.
- Persisting personal projection inputs or modifying the Dashboard Forward Pace card.
- Treating the output as tax, accounting, or investment advice.

### Acceptance Criteria

- A user can project from any start date through any valid target date within the supported two-year window.
- Partial weeks are calculated from inclusive weekdays instead of being rounded to a full week.
- The page identifies that weekday sessions exclude weekends but do not exclude market holidays.
- An optional target balance produces required net daily and weekly pace plus ahead/on-track/behind status.
- Conservative, base, and stretch scenarios reconcile to the same session count and disclosed multipliers.
- The API, page, schedule, and PDF use the same server-authoritative results.
- Existing weeks-only requests remain compatible and focused tests pass.
- Input edits do not silently replace the active projection; the user explicitly applies them with Update Projection.

## Capabilities

### New Capabilities

- `forward-pace-date-planning`: Date-window projection, target pacing, scenario comparison, transparent assumptions, and aligned exports for the standalone Forward Pace planner.

### Modified Capabilities

None.

## Impact

- `mccain_capital/services/forward_pace.py` projection and PDF output.
- `mccain_capital/templates/forward_pace.html` planning controls and output sections.
- `static/js/forward_pace.js` date-mode synchronization and rendering.
- Forward Pace styles and focused Flask/service/frontend contract tests.
- No database migration, new dependency, broker mutation, or runtime-data edit.
