## Context

The canonical context poll is scheduled in the page and coordinated across tabs. The page currently
keeps rescheduling while hidden, even though browsers throttle background timers, and `pagehide`
clears timers without a corresponding `pageshow` recovery. The scenario region is also a child of a
two-column cockpit grid but has no grid area, so it occupies only the first column.

## Goals / Non-Goals

**Goals:**

- Use the full cockpit width for scenario monitoring and replay.
- Stop client polling while hidden and reconcile immediately when the user returns.
- Recover from BFCache restoration without reloading the page.
- Keep one request in flight and retain the 15-second visible cadence.

**Non-Goals:**

- Changing provider, worker, canonical promotion, or scenario-ranking logic.
- Polling aggressively in the background.
- Replacing in-place refresh with full-page reloads.

## Decisions

1. Assign the scenario region to a named full-width cockpit grid area. Alternative and dormant cards
   remain two columns; the live monitor and replay each span both columns. This uses the existing DOM
   and keeps collapsed summaries compact.
2. Treat hidden pages as suspended receivers. Unregister the canonical lane and clear its timeout
   instead of scheduling a throttled 60-second request. The gamma stream already follows the same
   visibility principle.
3. Centralize wake-up handling in one resume function invoked by visibility, focus, online, and
   persisted `pageshow` events. It re-registers the lane, re-arms the countdown if needed, and requests
   one immediate canonical check when the previous check is older than the visible cadence or the
   page was restored from cache.
4. Retain the existing `contextRefreshPromise` as the single-flight guard and add a short wake-up
   debounce so overlapping browser lifecycle events do not create redundant requests.

## Risks / Trade-offs

- [A hidden tab no longer checks for updates] → One immediate canonical validation runs on return,
  while the server-side data lifecycle remains independent of the browser.
- [Visibility and focus can fire together] → A resume debounce and existing single-flight promise
  collapse them into one request.
- [BFCache can preserve stale timer identifiers] → `pagehide` nulls cleared identifiers and
  `pageshow` explicitly re-arms them.
- [Wide cards could become overly sparse] → Only the monitoring container expands; internal details
  remain compact and responsive breakpoints retain a single-column mobile layout.
