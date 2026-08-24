## Why

Market Pulse currently promotes one nearest active level and one failed-sweep path, so a distant
pending setup can make the whole page say `WAIT` while price is confirming a different breakdown,
breakout, or Local Flip opportunity. The page needs to evaluate multiple structural scenarios at
once, rank them with visible evidence, and plot session VWAP so the execution read follows current
price rather than one abandoned location.

## What Changes

- Evaluate five scenario families across supported levels: failed high, failed low, bearish
  breakdown, bullish breakout, and Local Flip loss/reclaim.
- Score every candidate from explicit confluence evidence, including completed five-minute closes,
  retests, Strat confirmation, gamma regime, level clustering, session timing, data freshness,
  target space, and VWAP relationship when those inputs are available.
- Rank candidates into `Active now`, `Alternative`, and `Dormant` lanes while preserving mutually
  exclusive bullish/bearish confirmation for each level interaction.
- Replace page-wide waiting language with scenario-specific states and exact location, trigger,
  action, target, and cancellation conditions.
- Derive the displayed letter grade and numeric confluence score from one scenario-scoring contract,
  with visible grade bands and no legacy page-wide grade mixed into the trade decision.
- Add a session VWAP series and labeled chart overlay using current intraday OHLCV data. For SPX,
  use an explicitly labeled `SPY VWAP Proxy` when native compatible volume is absent; never present
  proxy data as native SPX VWAP.
- Refresh scenarios, scores, VWAP, chart overlays, and decision copy from one canonical generation
  without reloading the entire page.
- Preserve the current six-step failed-sweep checklist as evidence for reversal candidates while
  adding path-specific evidence for continuation and Local Flip candidates.
- Do not add order placement, automated trading, position sizing, account controls, predictive
  commentary, new market-data providers, or fabricated confirmation from incomplete inputs.

Acceptance requires deterministic ranking, no actionable state from stale or incomplete evidence,
VWAP agreement with fixture calculations, coherent in-place refresh, and focused service, route,
template, and chart-contract tests.

## Capabilities

### New Capabilities

- `market-pulse-scenario-ranking`: Multi-level scenario generation, confluence scoring, ranking,
  evidence-safe execution plans, canonical refresh behavior, and decision-lane presentation.
- `market-pulse-vwap-overlay`: Session VWAP calculation, availability semantics, payload contract,
  chart rendering, labeling, and in-place refresh behavior.

### Modified Capabilities

None. No synchronized main specification currently exists for the earlier failed-sweep change; its
behavior is treated as compatibility input to the new scenario-ranking capability.

## Impact

- Domain/services: extend the Market Pulse strategy evaluator and adapters under
  `mccain_capital/services/` from one active result to ranked candidate results.
- API/view model: add scenario and VWAP fields to the canonical Market Pulse context payload while
  retaining existing strategy fields during migration.
- UI: update `mccain_capital/templates/core/market_pulse.html`, Market Pulse JavaScript, chart code,
  and `static/css/market_pulse.css`.
- Inputs and assumptions: use only existing quote, intraday OHLCV, Strat, gamma, wall/flip, day-level,
  session, and freshness inputs. Session VWAP resets at the regular-session boundary; SPX may use
  same-session SPY OHLCV only when labeled as a proxy with source and timestamp.
- Tests: add deterministic evaluator/ranking fixtures, VWAP fixtures, payload/refresh contracts, and
  focused rendered-page/chart assertions. No database migration or runtime-data write is required.
