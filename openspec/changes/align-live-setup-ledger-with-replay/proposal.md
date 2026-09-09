## Why

Setup Replay can reconstruct a qualifying completed-candle setup that the Live Setup Monitor never recorded because live persistence currently follows only the single highest-ranked current candidate. That makes historical study disagree with what the trader was told in real time and can silently lose valid secondary setup events.

## What Changes

- Evaluate every qualifying newly triggered scenario on each newly completed five-minute candle, not only the current primary candidate.
- Persist each unique live setup event in the durable setup ledger using the same trigger boundary and event identity semantics as Setup Replay.
- Keep ranking as a presentation concern: one setup remains Primary, while other newly triggered setups remain visible in secondary/recent history rather than being discarded.
- Reconcile missed completed candles after a bounded worker/page gap so valid events are backfilled as historical records without generating late real-time alerts.
- Preserve point-in-time inputs, completed-candle safety, cutoff rules, freshness locks, terminal-state monotonicity, acknowledgement state, and alert deduplication.
- Reconcile each persisted event's frozen family and forward lifecycle from the same canonical event
  and outcome semantics used by Replay; the currently ranked Primary MUST NOT overwrite history.
- Add parity tests proving that every replay-eligible setup has a corresponding live-ledger record when the live evaluator has processed the same completed candles.
- Non-goals: changing STRAT pattern definitions, confluence weights, grading, target/invalidation rules, level selection, primary ranking, provider data, or retroactively sending alerts for backfilled events.

### Acceptance Criteria

- When two or more scenarios trigger on the same completed candle, each receives one durable ledger record and the highest-ranked setup alone is displayed as Primary.
- A qualifying setup cannot disappear merely because another candidate ranked above it during that evaluation.
- After a bounded evaluation gap, missed completed-candle setups are reconstructed into the ledger and marked review-only; no stale real-time alert is emitted.
- Live and replay use stable shared event identity and trigger attribution, so processing identical point-in-time bars produces matching eligible setup events.
- Repeated refreshes, worker overlap, and candidate reordering produce no duplicate records or alerts.
- The audited September 1 events retain `failed_high`: 9:40 resolves invalidated and 10:40
  remains open without being relabeled as `breakdown` or downgraded to `ARMED`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Require completed-candle live evaluation and durable persistence for every qualifying triggered setup, independent of which setup is selected as Primary.
- `market-pulse-scenario-ranking`: Require Live Setup and Setup Replay to share eligible-event identity and point-in-time trigger semantics while retaining ranking only for display order.

## Impact

- Affected services: `market_pulse_live_setup.py`, `market_pulse_setup_replay.py`, scenario evaluation helpers, and their call sites in the Market Pulse canonical refresh path.
- Affected persistence: the existing local live-setup JSON ledger schema and deduplicated in-app alert delivery records; no database migration or financial-ledger change.
- Affected UI/API: Live Setup Monitor primary, secondary, and recent payloads; Setup Replay output remains historical and non-authoritative.
- Affected tests: live setup lifecycle, replay parity, simultaneous triggers, gap recovery, cutoff behavior, duplicate suppression, and deployed Market Pulse verification.
- Data sources and financial assumptions remain unchanged: existing canonical completed SPX five-minute bars, levels, gamma context, scenario rules, and authoritative execution permission are reused.
