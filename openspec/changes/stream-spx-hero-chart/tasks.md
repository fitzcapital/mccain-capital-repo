## 1. Stream Transport Integration

- [x] 1.1 Add stream-source, freshness, and fallback state to the reusable micro-tape helper
- [x] 1.2 Subscribe the hero chart to the existing shared Market Pulse stream payload event
- [x] 1.3 Build an independent compact tape canvas below the five-minute candle chart
- [x] 1.4 Feed valid SPX stream observations into bounded five-second visual buckets without touching
  the candle chart time scale
- [x] 1.5 Retain the three-second quote lane as automatic stale-stream fallback

## 2. Lifecycle and Authority Safety

- [x] 2.1 Pause and resume stream visualization correctly across visibility and market-phase changes
- [x] 2.2 Reject invalid, stale, timestamp-regressing, and closed-session stream ticks
- [x] 2.3 Keep stream observations isolated from completed five-minute setup confirmation
- [x] 2.4 Present compact streaming, polling fallback, interrupted, paused, and closed states

## 3. Verification

- [x] 3.1 Add JavaScript tests for stream coalescing, stale fallback, recovery, point bounds, and candle
  viewport isolation
- [x] 3.2 Add receiving-contract tests proving the existing single EventSource is reused
- [x] 3.3 Run focused Market Pulse, setup-authority, syntax, formatting, and OpenSpec checks
- [x] 3.4 Rebuild the local app and verify stream-first chart updates plus polling fallback on the deployed page
