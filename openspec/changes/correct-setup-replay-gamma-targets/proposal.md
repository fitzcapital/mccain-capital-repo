## Why

Setup Replay preserves historical wall prices but not the Gamma regime that was actually known at
each signal, causing valid Gamma evidence to appear unavailable and allowing scores to drift when a
newer snapshot replaces the current cache. Runner validation is also measured from the anchor rather
than the entry, while estimated option TPs do not distinguish a live Tradier NTM quote from fallback
assumptions clearly enough.

## What Changes

- Persist timestamped Gamma regime and structural-level observations from canonical live generations.
- Resolve each replay signal against only the latest Gamma evidence known at or before that signal.
- Preserve the selected level's source timestamp on Replay output and receiving surfaces.
- Require structural Runners to be directional and at least five SPX points from actual entry.
- Use a current Tradier NTM contract quote and delta for estimated TP prices when valid; retain the
  disclosed $750/0.40 fallback when unavailable.
- Keep dealer Gamma as confluence evidence only; it does not directly calculate option-return TPs.
- Non-goals: changing Strat definitions, entry triggers, invalidation rules, the 3:15 PM cutoff,
  contract execution, or recorded P&L.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-scenario-ranking`: Make Replay Gamma, Runner selection, target estimates, and their
  provenance point-in-time accurate and explicit.
- `market-pulse-live-state-coherence`: Retain canonical Gamma observations durably so Live and Replay
  consume the same historical evidence after refreshes and restarts.

## Impact

- Affected code: Setup Replay evaluation, live setup ledger/observation persistence, Market Pulse API
  assembly, Tradier NTM estimate integration, Replay rendering, and focused tests.
- Data sources: canonical Gamma snapshots, completed SPX candles, durable setup evidence, and current
  Tradier option-chain quotes.
- Financial assumptions: estimates remain illustrative, never realized P&L. Live NTM inputs require
  a valid quote timestamp, premium, and delta; fallback inputs remain $750 premium and 0.40 absolute
  delta and are labeled as fallback.
- Acceptance: a signal cannot receive future Gamma evidence, historical scores remain stable after a
  later refresh, every dynamic level exposes its observation time, every Runner clears entry by five
  points in direction, and TP output identifies live versus fallback inputs.
