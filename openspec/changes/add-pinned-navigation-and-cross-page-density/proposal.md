## Why

The primary desktop pages still expand into long horizontal rows on wide displays, increasing eye
travel and making important reads harder to scan. The Dashboard workflow navigation also moves out
of view during scrolling, even when the user wants it available as a stable section index.

## What Changes

- Add a compact pin toggle to the Dashboard workflow navigation.
- Persist the user's pinned or unpinned preference locally and expose the state accessibly.
- When pinned, keep the workflow navigation visible below the application header while scrolling.
- Apply centered, bounded wide-desktop reading canvases to Dashboard, Market Pulse, and Executive,
  with a modestly wider canvas for the denser Market Pulse execution surfaces.
- Keep the SPX Session Snapshot on the full Dashboard reading canvas instead of the retired side rail.
- Restore Executive as a first-class item in the shared primary navigation.
- Preserve current responsive behavior and full available width on smaller screens.
- Non-goal: change page data, trading logic, refresh cadence, or execution rules.

## Capabilities

### New Capabilities
- `cross-page-desktop-density`: Wide-desktop reading-width and pinned-navigation behavior across the
  three primary application pages.

### Modified Capabilities

None.

## Impact

- Shared navigation markup plus Dashboard interaction JavaScript.
- Shared page-scoped CSS for Dashboard, Market Pulse, and Executive.
- Focused template, styling, persistence, and responsive regression contracts.
- No API, database, market-data, or financial-calculation changes.
