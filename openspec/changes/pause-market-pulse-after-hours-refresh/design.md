## Context

The context API already computes the regular-session phase, but emits the same 15-second
`next_check_seconds` and response header when closed. The browser trusts that value and schedules
another canonical request; focus, visibility, component events, and retry paths can also restart it.
Workers may remain available for other pages, so this change gates the Market Pulse request path
without globally disabling shared services.

## Goals / Non-Goals

**Goals:**

- Make the API refresh contract authoritative for whether recurring polling is enabled.
- Stop automatic canonical/provider refresh outside 9:30 AM–4:00 PM ET on weekdays.
- Preserve manual diagnostics and automatically wake a page left open for the next session.
- Render closed-session timing honestly and retain the last validated generation.

**Non-Goals:**

- Holiday-calendar integration, premarket/after-hours execution, vendor replacement, or strategy
  changes.
- Shutting down shared quote/options/gamma workers used elsewhere in the application.

## Decisions

- Add `automatic_refresh_enabled` and `next_session_open_at` to the existing refresh contract.
  This is preferable to inferring behavior from text or a large sentinel interval.
- During closed phases, automatic API calls use a read-only path and do not request market/provider
  refresh. Manual refresh remains explicit and bounded.
- The client cancels its canonical lane while closed and uses one local wake timer targeted at the
  next weekday 9:30 AM ET. After waking, the server remains authoritative; if still closed, it
  returns another closed contract.
- Every scheduling entry point passes through one phase-aware guard, preventing focus, visibility,
  component events, and retries from silently re-enabling polling.
- The page displays `Market closed · auto-refresh paused` instead of a countdown while paused.

## Risks / Trade-offs

- [Weekday calculation does not know exchange holidays] → the next server check will still return a
  closed/retained contract when no current market generation exists; holiday support remains a
  future calendar enhancement.
- [Manual after-hours refresh could be mistaken for live execution] → keep session status closed,
  preserve execution gating, and label returned data retained/diagnostic.
- [An open page could miss the opening boundary due to laptop sleep] → visibility and focus handlers
  revalidate once when the page becomes active.
- [Shared workers continue running] → provider-call suppression is enforced on the Market Pulse
  automatic route; global worker lifecycle is intentionally unchanged.
