## 1. Refresh Contract

- [x] 1.1 Trace and document current quote, bars, levels, gamma, canonical, visibility, focus, and retry request ownership
- [x] 1.2 Add cached canonical generation validators, component versions/timestamps, market phase, promotion reason, blockers, and next-check hints
- [x] 1.3 Make automatic generation checks avoid provider-forced work and return an unchanged conditional response when current
- [x] 1.4 Preserve bounded server single-flight provider refresh for the manual Refresh data action

## 2. Coordinated Browser Scheduler

- [x] 2.1 Implement one Market Pulse refresh coordinator with server-provided open, closed, and hidden cadence
- [x] 2.2 Route quote observation, bars, levels, and canonical checks through coordinator-owned single-flight request lanes
- [x] 2.3 Trigger canonical checks after completed candle boundaries and changed required-component versions
- [x] 2.4 Prevent quote-only observations from changing execution-authoritative fields
- [x] 2.5 Add bounded retry with jitter, overdue focus/visibility recovery, and lifecycle cleanup
- [x] 2.6 Remove or disable superseded independent timers after coordinator parity is verified

## 3. Refresh Presentation and Replay Markers

- [x] 3.1 Replace prominent routine automatic-refresh copy with a quiet live age indicator and expandable delayed-component diagnostics
- [x] 3.2 Add a one-second display countdown bound to the coordinator's next refresh or retry deadline without creating another request timer
- [x] 3.3 Replace Setup Replay `S` chart markers with distinct hollow replay markers
- [x] 3.4 Add replay legend/detail language stating `Historical · not a live entry` and preserve best-to-least panel ranking
- [x] 3.5 Preserve chart viewport, drawings, timeframe, ticker, ladder selection, and disclosure state through coordinated updates

## 4. Verification

- [x] 4.1 Add service/API tests for conditional generations, cached automatic checks, provider single-flight, component timing, and retry hints
- [x] 4.2 Add JavaScript contract tests for scheduler cadence, boundary reevaluation, quote-only isolation, recovery, and timer cleanup
- [x] 4.3 Add chart/replay tests proving historical markers cannot be confused with live entry or sweep confirmation
- [x] 4.4 Run focused pytest, JavaScript tests and syntax checks, Python lint/format checks, strict OpenSpec validation, and `git diff --check`
- [x] 4.5 Rebuild the local Podman app, verify `/healthz`, and inspect authenticated Market Pulse receiving behavior in visible and recovered-tab states

## 5. Open-Session Polling Resilience

- [x] 5.1 Honor partial-response `next_retry_seconds` and cap every visible open-session canonical deadline at 30 seconds
- [x] 5.2 Re-arm the canonical lane after every response, failure, tab restore, focus return, and stale coordinator state
- [x] 5.3 Add a watchdog that repairs a missing or overdue visible open-session deadline without issuing duplicate requests
- [x] 5.4 Add JavaScript and template contract regressions proving open-session polling cannot silently drift to 30 minutes
- [x] 5.5 Rebuild and verify the served page reports and follows a bounded refresh or retry countdown

## 6. Truthful Closed-Session Timestamp

- [x] 6.1 Centralize the Market Pulse header snapshot label around the server refresh contract and canonical last-valid timestamp
- [x] 6.2 Prevent quote-only stream rendering from labeling retained after-hours data as live
- [x] 6.3 Render Planning and Last valid immediately when a conditional response reports a closed phase
- [x] 6.4 Add focused regressions for closed-session label ownership and timestamp preservation
- [x] 6.5 Rebuild and verify the deployed page, health endpoint, and served asset contract

## 7. Initial-render Session Authority

- [x] 7.1 Make the server market-hours contract authoritative over retained chart mode during initial render
- [x] 7.2 Add a template regression for a closed session with a cached live-session chart
- [x] 7.3 Rebuild and verify the after-hours page cannot initially present Live execution

## 8. Compact Controls And Candle-Time Authority

- [x] 8.1 Convert sticky summary, manual refresh, and Candle Opens utilities to accessible icon controls
- [x] 8.2 Move the coordinator-owned countdown into a compact top-right status pill without adding a timer
- [x] 8.3 Make every header renderer use the last completed candle timestamp instead of quote, snapshot, or server time
- [x] 8.4 Add refresh-spin, timestamp ownership, and compact-control regressions and rebuild the local app

## 9. Countdown Placement Refinement

- [x] 9.1 Place countdown inside the related utility cluster instead of over market-state metrics
- [x] 9.2 Shorten closed and hidden states while retaining full explanatory tooltips
- [x] 9.3 Add placement regression, rebuild, and verify the deployed asset
