## Why

Market Pulse still requires manual attention around the 09:30 ET open, the 16:00 ET close, laptop sleep, and stale-provider recovery. The page must derive one trustworthy exchange-session state and automatically converge its polling, labels, execution permission, and data freshness to that state without wasting requests or presenting stale data as live.

## What Changes

- Establish one server-authoritative US equity session contract for premarket, regular session, after-hours planning, weekends, holidays, and early closes.
- Make Market Pulse automatically revalidate at session boundaries, on page visibility/focus recovery, and after device sleep without requiring a reload or refresh-button click.
- After a cached-first render during the regular session, immediately perform a non-forced canonical validation instead of waiting for the first regular polling interval.
- Use a bounded, single-flight polling controller that accelerates around the open, follows a stable live cadence during regular hours, backs off safely on failure, and stops provider polling after the close.
- Keep the latest coherent canonical generation visible during failures while locking execution and explaining the stale or unavailable component.
- Make the page header, countdown, chart tape, setup monitor, gamma state, and execution mode consume the same session contract and update together.
- Preserve manual refresh as an explicit forced-recovery fallback rather than the primary freshness mechanism.
- Add deterministic lifecycle, boundary, sleep/resume, overlap, stale-data, holiday, and early-close tests plus deployed verification.
- Non-goals: changing the SPX strategy, setup grading, gamma calculations, source-provider selection, market-hours definitions outside the US equity calendar, or enabling trading actions automatically.

### Acceptance Criteria

- At the regular-session open, a visible Market Pulse page enters market-open mode and begins canonical polling without a reload or click.
- A Market Pulse page loaded or reloaded during the regular session renders the latest coherent cached generation immediately, then performs one non-forced canonical validation without waiting for the normal polling interval.
- At the regular-session close or scheduled early close, live-only polling stops, execution becomes closed-session planning, and the next valid wake boundary is shown.
- A page returning after at least 30 minutes hidden or after laptop sleep performs exactly one immediate revalidation and resumes the correct cadence.
- No overlapping automatic, boundary, focus, or manual request may produce mixed generations or leave the UI indefinitely refreshing.
- All execution-authoritative surfaces reconcile from the same response and never label stale or previous-session data as live.
- Weekends and recognized holidays remain paused until the next valid exchange session.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-operational-resilience`: Strengthen exchange-session boundary scheduling, automatic wake/recovery, live-only polling suspension, and observable lifecycle health.
- `market-pulse-live-state-coherence`: Require every execution-authoritative surface to reconcile atomically from the server session contract and canonical generation.

## Impact

- Affected server areas: the reusable exchange-session calendar, Market Pulse runtime/coordinator, canonical context API, provider-refresh gating, and operational health fields.
- Affected client areas: Market Pulse polling controller, boundary countdown, visibility/focus recovery, chart live tape, setup monitor, and session/freshness labels.
- Affected data sources: existing SPX spot, completed bars, gamma, levels, and setup-state sources; no new financial source or assumption is introduced.
- Affected tests: focused Python service/route tests, JavaScript contract tests, session-boundary tests, and deployed health/behavior verification.
