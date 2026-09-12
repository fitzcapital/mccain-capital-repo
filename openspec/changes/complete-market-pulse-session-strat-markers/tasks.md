## 1. Chart Coverage

- [x] 1.1 Preserve Strat marker objects for every classifiable active-session candle
- [x] 1.2 Frame all 78 regular-session five-minute candles by default
- [x] 1.3 Add JavaScript regression coverage for full-session markers and viewport policy

## 2. Replay Freshness

- [x] 2.1 Overlay valid current-session completed bars onto replay source candidates
- [x] 2.2 Preserve cached fallback and explicit historical replay behavior
- [x] 2.3 Add Python tests for live overlay, provider failure, and historical isolation

## 3. Verification

- [x] 3.1 Run focused Market Pulse tests, JavaScript syntax checks, and strict OpenSpec validation
- [x] 3.2 Deploy through K8s and verify pods, full-session candle boundaries, replay cutoff, and page UI
