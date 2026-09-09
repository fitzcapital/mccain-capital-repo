## Why

Market Pulse currently promotes some generic five-minute breaks, holds, and directional flips as
Strat-confirmed setups. That can create false live callouts and misleading replay results because the
user's proven SPX setup universe is specifically 2-1-2 Up/Down and 2-2 Reversal at CDH or another
recognized key level.

## What Changes

- Add exact completed-candle classification for inside bars, directional 2 Up/Down bars, and outside
  bars, then detect only valid 2-1-2 Up/Down and opposing 2-2 Reversal sequences.
- Require every live or replay setup to be anchored to CDH or another canonical Market Pulse level
  touched by the pattern; mid-range candle sequences remain non-actionable diagnostics.
- Make the exact pattern, direction, anchor level, completion time, and evidence provenance explicit
  in ranked scenarios, the live setup monitor, and Setup Replay.
- Attribute every supported pattern to its actual STRAT signal candle and trigger boundary. The
  opposing second candle triggers a 2-2 Reversal at the first candle's boundary; the third candle
  triggers a 2-1-2 at the inside candle's boundary. Later candles are follow-through and outcome
  evidence, never a second entry gate or a relabeled pattern trigger.
- Reconstruct Current-Day High and Current-Day Low from only the candles available at each replay
  timestamp so a later session extreme cannot erase an earlier valid key-level setup.
- Remove 3-1-2 and generic “Strat confirmation” language from live execution surfaces without
  removing educational material elsewhere in the application.
- Preserve hard safety gates: completed five-minute candles only, fresh canonical data, no score or
  gamma override, and no automatic trade execution.
- Acceptance is measurable: focused fixtures must prove valid and invalid 2-1-2 and 2-2 sequences,
  reject outside-bar and mid-range lookalikes, keep live and replay classifications identical, and
  align replay timestamps and excursion metrics to the actual pattern trigger.

## Capabilities

### New Capabilities

- `spx-key-level-strat-patterns`: Exact five-minute Strat classification and key-level anchoring for
  the supported SPX setup families.

### Modified Capabilities

- `market-pulse-scenario-ranking`: Require an exact supported Strat pattern before a scenario can
  become active and expose its pattern evidence.

## Impact

- Affects Market Pulse candle evidence, scenario ranking, live setup monitoring, replay payloads,
  and their user-facing labels.
- Uses existing canonical SPX five-minute bars and canonical level rows; no new market-data source,
  dependency, database migration, or financial projection is introduced.
- Non-goals: supporting 3-1-2, other tickers, intrabar prediction, buy/sell certainty, order routing,
  or allowing gamma/confluence scores to substitute for candle confirmation.
