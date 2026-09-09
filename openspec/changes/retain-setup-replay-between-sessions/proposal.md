## Why

Setup Replay defaults to the calendar date, so Sunday filters out Friday's retained candles and
setups. Users need the last available trading session to remain available for weekend study.

## What Changes

- Resolve the default replay session from available completed regular-session candles rather than
  the calendar date alone; retain the prior session through weekends, holidays, and premarket.
- Label retained results with their actual session date and `Review only`, including the last
  evaluated candle so a partial cached session is not presented as a complete day.
- Switch to the new session when its first completed regular-session candle is available, even
  when the new session has no eligible setups yet.
- Preserve explicit `session_date` requests and point-in-time Gamma/level evidence.
- Keep review requests read-only and prevent retained replay results from issuing live alerts.
- Non-goals: a multi-day archive/date-picker, fetching missing historical candles, changing setup
  classification, targets, scoring, live freshness rules, or rewriting runtime data.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-scenario-ranking`: Define retained-session selection and dated review-only replay.

## Impact

- Affected code: replay endpoint/source selection in `services/core.py`, replay session helpers,
  Market Pulse replay rendering and chart-marker integration, and focused tests.
- Data: existing canonical completed candles, session calendar, and session-scoped durable level
  observations. No new financial assumptions or dependencies.
- Acceptance: on Sunday Aug 30, available Friday Aug 28 setups remain visible after reload with
  Friday's date and coverage timestamp; holidays behave likewise; a new completed session switches
  Replay without mixing dates; explicit missing dates return unavailable instead of another day;
  retained results do not trigger notifications or alter live execution permissions.
