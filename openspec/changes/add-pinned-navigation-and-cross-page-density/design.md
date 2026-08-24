## Context

Dashboard now uses a bounded desktop canvas, but Market Pulse and Executive still use broader
page-specific widths. Dashboard's workflow navigation is useful as a section index but currently
scrolls away. The solution must remain page-scoped so charts and dense execution surfaces are not
artificially compressed at normal desktop and mobile widths.

## Goals / Non-Goals

**Goals:**

- Give Dashboard workflow navigation an accessible pin toggle with a saved preference.
- Keep the pinned navigation below the shared application header without covering content.
- Use a consistent centered wide-desktop canvas on Dashboard, Market Pulse, and Executive.
- Keep Executive directly accessible from the shared primary navigation.
- Preserve responsive layouts and prevent horizontal overflow.

**Non-Goals:**

- Change global navigation behavior.
- Change market data, refresh, execution, account, or financial logic.
- Make every secondary page use the same maximum width.

## Decisions

1. Use a button inside the Dashboard workflow navigation rather than making the entire navigation
   permanently sticky. This preserves user control and provides a clear pinned/unpinned state.
2. Store the preference in `localStorage`; failure to read or write storage falls back safely to
   unpinned behavior.
3. Apply sticky positioning only above the desktop breakpoint. Smaller screens retain normal flow
   to avoid consuming limited vertical space.
4. Use a 1180px page-scoped wide-desktop width cap for Dashboard and Executive, and a 1360px cap
   for Market Pulse so its execution chart and Gamma Ladder gain room without returning to a
   full-width stretched layout.
5. Bump the relevant static asset cache revision so the deployed browser receives the change.
6. Express each page's width through the same page-scoped canvas token, assigning Market Pulse its
   wider value while retaining the shared responsive width formula.
7. Place Executive before Trading Dashboard in the primary navigation so the application hierarchy
   reads from overview to performance to execution.
8. Retire the Dashboard stage side rail in the performance view. Its remaining SPX Session Snapshot
   spans the bounded canvas while retaining a compact internal price/chart grid.

## Risks / Trade-offs

- [Risk] Sticky navigation could overlap the shared header. → Use a scoped top offset and elevated
  z-index with an opaque page-themed background.
- [Risk] Market Pulse charts could become too narrow. → Limit the 1180px cap to wide screens and
  preserve existing responsive chart reflow below the desktop breakpoint.
- [Risk] Cached assets could hide the change. → Update the asset revision and inspect deployed
  computed widths after rebuilding.
