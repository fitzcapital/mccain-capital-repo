## Context

Market Pulse currently has a 15-second canonical context loop plus independent chart loops for
quotes, bars, and levels. The browser sends `refresh_market=1` during each automatic canonical
check, while the chart independently requests quote, bars, and levels data. Single-flight and
generation validation prevent many unsafe commits, but the duplicated scheduling increases work
and can briefly show a newer observation beside an older execution decision.

The page is SPX-only for execution. Quotes may update as observation data, but decisions depend on
completed candles, gamma, levels, permission, and ordered evidence. The design must keep the last
valid decision authoritative until all required components form a coherent generation.

## Goals / Non-Goals

**Goals:**

- Coordinate browser refresh activity through one scheduler with server-provided session cadence.
- Separate lightweight observation updates from canonical execution promotion.
- Avoid complete payload transfer and DOM reconciliation when the generation is unchanged.
- Trigger prompt canonical evaluation after completed bars and changed gamma/levels/permission.
- Preserve last-valid execution, interaction state, bounded retries, and manual forced recovery.
- Make historical replay markers unmistakably non-live and non-sweep.
- Expose enough timing metadata to diagnose which component is late.

**Non-Goals:**

- Changing SPX strategy, scoring, confirmation order, risk, targets, or providers.
- Introducing WebSockets or another external infrastructure dependency.
- Making replay setups actionable or enabling execution for non-SPX symbols.
- Removing the manual Refresh data control.

## Decisions

### One browser refresh coordinator

Create a single page-level coordinator that owns quote observation, bars, levels, canonical
generation checks, visibility/focus recovery, retries, and request cancellation. The chart consumes
coordinator events rather than maintaining competing lifecycle timers.

This keeps the existing fetch/API architecture and is smaller than introducing WebSockets. Direct
server-sent events remain a later optimization once the coordinated polling contract is proven.

Current and resulting request ownership:

| Lane | Previous owner | Coordinated owner | Authority |
| --- | --- | --- | --- |
| Quote | Hero chart timeout | `marketPulseRefreshCoordinator` quote lane | Observation only |
| Bars | Hero chart timeout plus candle boundary | Coordinator bars lane | Requests canonical reevaluation when completed bars change |
| Levels | Hero chart timeout | Coordinator levels lane | Requests canonical reevaluation when version changes |
| Gamma / permission | Canonical payload and gamma workflow events | Coordinator-triggered canonical lane | Execution dependency |
| Canonical | Page 15-second timeout, visibility, focus, retries | Coordinator canonical lane | Sole execution-authoritative commit |
| Visibility / focus | Separate page and chart listeners | Lane unregister/re-register plus overdue canonical recovery | Lifecycle only |
| Retry | Page-local canonical timeout | Coordinator canonical deadline with bounded jitter | Last-valid generation remains authoritative |

### Cached generation check before full payload

Automatic checks shall request cached generation metadata without forcing provider work. The client
sends its current generation through `If-None-Match` or an equivalent query/header contract. The
server returns `304`/unchanged metadata when nothing promotable changed and returns the complete
canonical payload only for a newer coherent generation.

Manual Refresh data may request a bounded server-side provider refresh. Server single-flight remains
authoritative so multiple tabs cannot multiply provider requests.

### Cadence follows data semantics

- Quote observation: target 3 seconds while visible and open; never authorizes execution.
- Bars: target 10 seconds plus an immediate check just after the selected candle boundary.
- Levels/gamma metadata: target 30–45 seconds unless the server advertises a newer generation.
- Canonical heartbeat: target 15 seconds, but run immediately after a new completed bar or changed
  required-component version.
- Hidden/closed pages: retain existing slower server-provided intervals and refresh immediately on
  visibility/focus recovery when overdue.

Cadence stays server-provided so market phase and provider limits can change without redeploying
browser constants.

### Execution reevaluates only from meaningful dependencies

The client can patch quote labels from observation data. It must not change regime, permission,
scenario, checklist, action, invalidation, target, or ladder authority until the server promotes a
validated canonical generation. A completed-bar or required-component version change requests an
immediate canonical check; unchanged observations do not cause full reconciliation.

### Additive freshness and diagnostics contract

Canonical responses expose component versions/timestamps, generation ETag, server next-check hints,
market phase, promotion reason, blocking components, and whether data is observation-only. Existing
payload fields remain compatible. The UI shows one quiet `Live · updated Ns ago` state and expands
component diagnostics only for delay, partial, or locked states.

The quiet status includes a one-second client countdown derived from the coordinator's actual next
canonical-check deadline. It reads `Refresh in Ns` while healthy and `Retry in Ns` after a partial,
unchanged-in-flight, or failed result. The countdown is display-only: it never creates a second timer
that can initiate requests, and it resets from the scheduler's authoritative deadline after focus,
visibility, cadence, or retry changes.

### Replay uses a distinct historical marker

Replace the letter `S` with a hollow diamond replay marker. Color still communicates historical
direction, but the legend/detail text says `Replay setup` and `Historical · not a live entry`.
Marker ordering remains best-to-least in the replay panel and chronological on the chart.

## Risks / Trade-offs

- [Coordinator regression could pause one data lane] → Add deterministic scheduler tests, watchdog
  timing, focus/visibility tests, and keep manual forced recovery.
- [Conditional response caches stale data incorrectly] → Use canonical generation ids as strong
  validators, `no-store` for forced refreshes, and retain component timestamps in every response.
- [Quote looks live while execution is older] → Explicitly label quote-only updates as observation
  and keep canonical last-valid time visible when the distinction matters.
- [Immediate bar-boundary requests arrive before provider completion] → Keep the existing grace
  interval and retry once using a bounded server hint.
- [Multiple tabs increase load] → Retain server single-flight and add small client jitter to routine
  heartbeats without delaying boundary-triggered checks.
- [SSE would be more immediate] → Defer it until coordinated polling metrics establish whether push
  is necessary; the API design leaves room for generation notifications later.

## Migration Plan

1. Add conditional generation metadata and cached-check behavior without changing the client.
2. Add coordinator tests and place the existing chart/canonical calls behind the coordinator.
3. Remove superseded independent timers after parity checks pass.
4. Replace replay markers and add the legend/detail contract.
5. Rebuild locally, verify health, inspect authenticated Market Pulse during open/closed phases, and
   retain the old asset version for rollback.

Rollback restores the previous JavaScript asset and leaves additive API metadata harmless.

## Open Questions

- Whether measured provider latency justifies Server-Sent Events after coordinated polling ships.
- Whether the same coordinator should later power Dashboard observation data; this change remains
  scoped to Market Pulse.
## Compact control and timestamp follow-up

- The three header utilities use icon-only controls with persistent accessible labels and tooltips.
- The existing refresh button loading state owns the refresh-icon rotation; no presentation timer can start provider work.
- The countdown moves to a compact top-right pill but continues to render the coordinator's deadline.
- `last_completed_candle_time` from the canonical completed-bar source is the only header-time authority. Quote observations, gamma reconciliation, and chart rendering may update values but cannot replace that timestamp.
- The countdown sits immediately before the related utility icons. This keeps polling state visually subordinate to Spot, Session, and Regime and prevents overlap with those market-state cards.
