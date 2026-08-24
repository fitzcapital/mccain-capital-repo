## Why

Market Pulse currently updates quotes, chart bars, and the Gamma Ladder independently while the
authoritative playbook can remain on a prior-day generation. That correctly locks execution, but it
cannot fulfill the page's purpose of guiding a live trade; it also gives no reliable, timestamp-safe
review of valid setups that developed earlier in the session.

## What Changes

- Publish one atomic live-execution generation containing quote, completed bars, gamma context,
  ranked scenarios, evidence, permission, and timestamps.
- Prevent overlapping refresh work, cap provider concurrency, and expose component-level refresh
  outcomes without treating a partial update as a successful playbook refresh.
- Keep the last valid generation visible on failure, but clearly separate observation-only prices
  from execution-authoritative data and preserve the locked state.
- Add a live execution guide that states the current location, exact trigger, confirmation required,
  action, invalidation, and next target only when the atomic generation is fresh.
- Add an intraday Setup Replay that identifies completed potential setups from earlier in the day,
  using only evidence that existed at each candle close and labeling outcome separately from the
  original signal.
- Show why each replayed setup qualified or failed, its timestamp, direction, entry zone, stop,
  target path, maximum favorable/adverse excursion, and whether it remains relevant.
- Retain manual refresh as a forced fallback while automatic, non-overlapping refresh is primary.
- **Non-goals:** automatic order placement, broker execution, changing the user's strategy rules,
  fabricating missing bars or gamma, and grading historical setups with future information.

## Capabilities

### New Capabilities
- `market-pulse-live-execution`: Atomic freshness, bounded refresh orchestration, permission gating,
  and a live decision guide derived from one synchronized market generation.
- `market-pulse-setup-replay`: Point-in-time-safe detection and presentation of potential intraday
  setups that were available earlier in the selected session.

### Modified Capabilities
- `market-pulse-scenario-ranking`: Ranked scenarios and execution plans must consume and refresh as
  one canonical generation rather than mixing independently updated components.

## Impact

- Primary areas: Market Pulse runtime/coordinator, market-data and gamma services, scenario and
  candle-evidence evaluators, Market Pulse context APIs, template, JavaScript refresh controller,
  chart annotations, and focused contract/service tests.
- Data sources remain the configured quote, completed-bar, and options/gamma providers. Every live
  decision and replay record will expose source time, observed time, market session, ticker, and
  generation identity.
- The change introduces no new financial assumption: execution remains advisory and confirmation
  based. Missing or mixed-generation inputs remain non-actionable.
- Acceptance requires in-place automatic refresh without overlap, consistent values across all
  execution surfaces, honest partial-failure messaging, bounded process/thread usage, and replay
  results proven not to read candles or gamma observations from after their signal timestamp.
