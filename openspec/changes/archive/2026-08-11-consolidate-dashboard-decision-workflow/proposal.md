## Why

The dashboard repeats the same permission, alignment, planning, and import information across
multiple panels, making the actual trading decision harder to scan. Consolidating those signals
will make blockers and the next required action visible without removing supporting context.

## What Changes

- Replace overlapping command and context summaries with one concise Command Center.
- Merge Today's Decision and Daily Brief into one Execution Plan with a single action row.
- Present normal states as compact values and reserve explanatory text for blockers or exceptions.
- Make supporting Foundation, Review, Health, and Calendar content progressively disclosed.
- Derive every import presentation from one authoritative import-readiness state so the page cannot
  simultaneously report complete and pending.
- Preserve existing trading rules, account data, market-data sources, and user-entered values.
- Non-goals: changing strategy calculations, trade permissions, import execution, persistence,
  market-data providers, or financial assumptions.

## Capabilities

### New Capabilities

- `dashboard-decision-workflow`: Defines the dashboard's consolidated decision hierarchy,
  progressive disclosure, canonical import status, and exception-first copy behavior.

### Modified Capabilities

None.

## Impact

- Dashboard Jinja templates and dashboard-specific CSS/JavaScript.
- Existing dashboard view/context assembly only where needed to expose one canonical import state.
- Focused dashboard rendering and state-contract tests.
- No API, database, dependency, or financial-calculation changes.
- Acceptance: the rendered dashboard exposes one Command Center, one Execution Plan, one import
  status, one execution CTA group, and hides supporting detail behind accessible disclosure controls.
