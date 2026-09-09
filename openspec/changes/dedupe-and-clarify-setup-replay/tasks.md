## 1. Canonical Setup Construction

- [x] 1.1 Add canonical completed-pattern identity independent of anchor level
- [x] 1.2 Group compatible same-completion anchors with deterministic primary selection
- [x] 1.3 Preserve distinct completion timestamps and exclude continuation candles from reversal events
- [x] 1.4 Expose primary and supporting level metadata for Live and Replay consumers

## 2. Score, Target, and Language Coherence

- [x] 2.1 Compute a complete canonical score and matching grade for every eligible replay event
- [x] 2.2 Exclude the anchor cluster from directional target selection and enforce minimum target space
- [x] 2.3 Mark events without a meaningful target non-actionable with diagnostic reasons
- [x] 2.4 Generate level-aware sweep/reclaim and sweep/reject titles and trigger descriptions

## 3. Live and Replay Parity

- [x] 3.1 Route Live and Replay through the same canonical grouped event output
- [x] 3.2 Enforce the inclusive 3:15 PM ET new-setup cutoff on both surfaces
- [x] 3.3 Preserve post-cutoff lifecycle resolution for setups admitted by the cutoff
- [x] 3.4 Reconcile legacy same-completion duplicates at read/render time without rewriting durable data

## 4. Verification

- [x] 4.1 Add focused replay tests for same-completion deduplication, separate completions, and continuation exclusion
- [x] 4.2 Add score/grade, clustered-target, unavailable-target, and contextual-label tests
- [x] 4.3 Add Live/Replay parity, stable identity, alert deduplication, and 3:15 boundary tests
- [x] 4.4 Reproduce the audited 10:05 AM case and verify one card with level confluence
- [x] 4.5 Run focused pytest, scoped Ruff, and `git diff --check`
- [x] 4.6 Rebuild the Podman app, verify `/healthz`, and inspect the deployed Market Pulse receiving surface

## 5. Timestamped Outcome Semantics

- [x] 5.1 Resolve replay invalidation from completed candle close instead of intrabar wick
- [x] 5.2 Preserve directional target-touch resolution and same-candle ambiguity
- [x] 5.3 Expose terminal event time and unresolved evaluation-through time in replay outcomes
- [x] 5.4 Render target, invalidation, ambiguity, and still-open timestamps without conflating MFE/MAE
- [x] 5.5 Add focused wick-versus-close, target-time, invalidation-time, ambiguity-time, and open-as-of tests
- [x] 5.6 Run focused tests, scoped Ruff, `git diff --check`, Podman rebuild, `/healthz`, and deployed replay verification

## 6. Compact Estimated TP Ladder

- [x] 6.1 Compute +15%, +20%, and +30% estimated SPX targets from disclosed $750 and 0.40-delta assumptions
- [x] 6.2 Render compact TP1, TP2, TP3, Runner, assumptions, and dealer-Gamma separation copy
- [x] 6.3 Add bullish/bearish calculation, Gamma-unavailable, and receiving-surface tests
- [x] 6.4 Run focused checks, rebuild Podman, verify `/healthz`, and audit the deployed 10:05 setup

## 7. Narrow Key-Level Proximity

- [x] 7.1 Add a fixed 0.25-point SPX key-level proximity tolerance to pattern location matching
- [x] 7.2 Preserve exact 2-2/2-1-2 classification and continuation exclusion
- [x] 7.3 Add boundary, outside-tolerance, continuation, and audited 11:30 AM replay tests
- [x] 7.4 Run focused checks, rebuild Podman, verify `/healthz`, and audit the deployed 11:30 setup
