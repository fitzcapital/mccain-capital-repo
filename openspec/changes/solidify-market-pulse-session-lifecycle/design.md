## Context

Market Pulse currently has several independent concepts of time: the header clock, canonical context refresh, chart polling, setup monitoring, and session labels. When a browser stays open across 09:30 ET, 16:00 ET, an early close, device sleep, or a long hidden interval, those timers can disagree. The result is stale candles, a planning label during the live session, after-hours work continuing unnecessarily, or a manual reload being required.

The existing exchange calendar and canonical Market Pulse context remain authoritative. This change coordinates them; it does not change the trading strategy, gamma calculations, setup qualification, or data providers.

## Goals / Non-Goals

**Goals:**

- Make Market Pulse enter and leave the regular session automatically.
- Use one server-authored session contract for every page surface and timer.
- Recover cleanly from sleep, hidden tabs, network loss, and browser cache restore.
- Prevent duplicate requests and eliminate after-hours live-only polling.
- Preserve the last coherent generation and fail execution closed when data is incomplete.
- Make lifecycle state observable without adding noisy text to the primary UI.

**Non-Goals:**

- Changing setup, The Strat, gamma, or execution business rules.
- Adding a new market-data source or websocket transport.
- Supporting non-US-equity session calendars.
- Automatically placing trades.
- Replacing manual refresh; it remains a fallback through the same coordinator.

## Decisions

### 1. The server publishes one session contract

The initial Market Pulse response and canonical context endpoint will include the current exchange phase, authoritative server time, regular-session boundaries, next transition, next valid open, automatic-poll permission, and recommended cadence. Calendar holidays and early closes are resolved on the server.

This avoids fixed 09:30/16:00 logic in JavaScript and prevents browser clock drift from becoming trading state.

### 2. One browser coordinator owns all refresh triggers

A single coordinator will own the in-flight request, next regular poll, boundary wake, retry deadline, and lifecycle event handlers. Manual refresh, timers, focus, visibility, online, and `pageshow` events all enter the same coordinator.

Independent recurring timers for canonical context, five-second tape, and setup monitoring will not decide whether the market is live. They derive enablement from the reconciled session contract.

### 3. Boundaries wake the page; the server confirms the transition

Before the open or after the close, the browser schedules one lightweight timer to the server-provided `next_transition_at`. When it fires, it requests the canonical context. It never switches directly to live or closed state based solely on the client clock.

If the device sleeps through a boundary, the next focus, visibility, online, or cache-restore event performs one immediate reconciliation instead of replaying missed polling intervals.

### 4. Cadence is phase-aware and server-recommended

- Initial regular-session load: render the coherent server snapshot immediately, then schedule one immediate non-forced canonical validation through the coordinator. The request carries the current generation/ETag so an unchanged generation is a cheap no-op and does not force provider collection.
- Regular session: use the server-provided canonical cadence, initially 15 seconds.
- Pre-market, after-hours, weekends, and holidays: no recurring provider polling; retain only the boundary wake and user-triggered fallback refresh.
- Failure: bounded exponential retry, initially 5, 10, 20, then 30 seconds maximum, while the page is visible.
- Five-second visual tape and live setup monitoring: allowed only during the regular session and only while the page is visible.

The server may tune cadence later without shipping new client timing logic.

Manual refresh remains the explicit forced-provider recovery path. Initial validation, regular cadence, boundary, and lifecycle reconciliation remain non-forced and may only enter bounded recovery when the canonical state reports staleness or a blocker.

### 5. Reconciliation is atomic and fail-closed

One canonical response updates the session label, countdown, freshness, spot, regime, execution permission, chart context, gamma ladder, and setup monitor together. If a required component is missing or belongs to another generation, the previous coherent generation remains visible and execution stays locked with a precise blocker.

### 6. Lifecycle status is compact but inspectable

The compact status exposes phase, last successful check, next transition or retry, and active blocker. Healthy state stays quiet. Expanded diagnostics show generation and timestamps for troubleshooting without exposing credentials.

## Risks / Trade-offs

- **Background browser throttling:** exact timer firing cannot be guaranteed while a tab or device is suspended. Immediate reconciliation on restore provides correctness even when punctuality is impossible.
- **Provider latency at the opening print:** the first 09:30 response may not yet contain a completed candle. The page may enter live session while execution remains locked until required data becomes coherent.
- **More server contract fields:** this modestly expands the context response, but removes duplicated client rules and lowers long-term drift risk.
- **Multiple open tabs:** no cross-tab leader election is introduced. Hidden tabs stop recurring work and each visible tab remains single-flight, which captures most resource savings with lower complexity.

## Migration Plan

1. Add the session contract to the server response without removing existing fields.
2. Introduce the coordinator behind the current refresh controls.
3. Route canonical and live-only polling through the coordinator and remove superseded timers.
4. Add boundary, sleep/wake, holiday, early-close, and overlap tests.
5. Rebuild the application and verify transitions with deterministic clock fixtures before relying on live-session observation.

No database migration or stored-data rewrite is required. Rollback restores the previous client timers while leaving the additive server fields harmless.

## Open Questions

None. The default cadence and retry caps remain server-configurable so operational tuning does not require a redesign.
