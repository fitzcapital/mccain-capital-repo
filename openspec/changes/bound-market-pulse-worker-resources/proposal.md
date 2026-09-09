## Why

The local app reached its 2,048-task container limit after roughly 22 hours, with the two Gunicorn
workers holding 1,052 and 994 threads. The page remained reachable, but Gamma refresh could no longer
start and Market Pulse stayed locked on `Synchronizing` / `Waiting on Gamma`.

## What Changes

- Bound Gamma refresh concurrency and prevent overlapping forced/background refresh work.
- Avoid creating per-refresh option-chain thread pools by using bounded serial fetching for the
  small configured expiry set.
- Cap native numerical-library thread counts in the container runtime.
- Recycle Gunicorn workers after a bounded number of requests with jitter so any third-party thread
  retention cannot grow until the container task ceiling.
- Expose process thread pressure and Gamma refresh failure reason in operational health diagnostics.
- Publish Gamma staleness/failure and worker-pressure state transitions as timestamped Netdata
  warning/critical events, including recovery, without duplicate event noise.
- Replace generic `Synchronizing` with the actual blocker, such as `Waiting on Gamma`, when known.
- Preserve the last trustworthy Gamma snapshot and fail execution closed during refresh failure.
- Non-goals: increasing the 2,048-task ceiling, weakening data freshness gates, changing Gamma
  calculations or providers, or modifying persistent trading data.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-operational-resilience`: Bound worker resource growth, surface task pressure, and
  preserve controlled recovery when Gamma refresh cannot start.
- `market-pulse-live-state-coherence`: Display the specific required-data blocker rather than a
  generic session synchronization label.

## Impact

- Affected runtime: Gamma worker coordination, Tradier chain fetching, Gunicorn startup arguments,
  container environment, health diagnostics, and Market Pulse status presentation.
- Data sources remain Tradier, canonical SPX candles, and the last-good Gamma snapshot.
- Acceptance criteria: repeated Gamma refreshes do not grow worker thread counts; simultaneous
  refresh callers coalesce; numerical-library threads remain capped; worker recycling is graceful;
  health reports task pressure before exhaustion; Gamma failure retains last-good data while locking
  execution; Netdata records actionable alert and recovery events; the page names Gamma as the
  blocker; rebuilt app remains healthy through a focused refresh soak.
