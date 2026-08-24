## Context

The existing failed-sweep evaluator is deterministic and evidence-safe, but it selects one nearest
active level before evaluation and returns one strategy result. The Market Pulse decision card then
treats that single result as the page-wide opportunity. Existing inputs already include supported
walls/flips, current/prior day levels, intraday bars, Strat evidence, gamma regime, session state,
freshness, and directional targets. The chart consumes canonical overlays but does not expose VWAP.

This change must preserve the existing evaluator as the reversal-path authority while allowing
other levels and continuation paths to compete without duplicating strategy logic in JavaScript.

## Goals / Non-Goals

**Goals:**

- Generate and evaluate candidates for failed highs, failed lows, breakdowns, breakouts, and Local
  Flip loss/reclaim across all valid supported levels.
- Separate eligibility and confirmation gates from a transparent 0–100 confluence score.
- Rank confirmed, armed, and dormant candidates deterministically and expose a compact top set.
- Provide exact location, trigger, action, target, and cancellation fields for every candidate.
- Calculate session VWAP from compatible intraday OHLCV bars and render it as a canonical overlay.
- Keep server render and in-place refresh on one backend-produced contract.

**Non-Goals:**

- Automated execution, trade authorization, position sizing, stops sent to a broker, or account
  risk controls.
- Machine-learned ranking, generated strategy prose, historical backtesting, or profitability
  claims.
- A new data provider or any proxy silently presented as native SPX volume.
- Allowing confluence score to bypass required completed-candle or path evidence.

## Decisions

### 1. Candidate generation precedes evaluation

Normalize every finite supported structural level, deduplicate clustered identical values, and
generate only directionally meaningful candidates around spot. Each candidate has a stable id,
family, level key/value, approach direction, and evidence bundle. Failed-high/failed-low candidates
delegate their ordered evidence to the existing failed-sweep evaluator. Breakout, breakdown, and
Local Flip candidates use a sibling pure evaluator with explicit completed-close and retest inputs.

Alternative considered: repeatedly mutate the existing one-level adapter. Rejected because it
would hide candidate identity, carry incompatible evidence between levels, and make ranking depend
on call order.

### 2. Hard gates and confluence score are separate

Freshness, valid location, and path-specific completed-candle evidence determine candidate state;
score never makes an unconfirmed candidate actionable. Eligible candidates receive a fixed,
auditable 100-point score: location 10, completed boundary close 15, retest outcome 20, Strat
confirmation 15, gamma alignment 10, structural level cluster 10, VWAP alignment 10, and clear
target space 10. Missing optional evidence receives zero for that component and remains named in
the breakdown. Weights live in one Python constant and are serialized with earned points.

Alternative considered: normalize by available inputs. Rejected because the same market condition
would receive a higher score merely because evidence was missing.

### 3. State determines lane; score orders within a lane

`ACTIVE_NOW` requires a fully confirmed path and safe data. `ALTERNATIVE` contains valid candidates
that are at/near a level or awaiting one defined confirmation. `DORMANT` contains valid but distant
or prerequisite-deficient candidates. Invalid, contradictory, and stale candidates remain
non-actionable and are either disclosed separately or omitted from the compact top set. Sort order
is lane priority, score descending, distance ascending, then stable level/family priority.

This prevents a high-scoring dormant Current-Day High from suppressing a confirmed Local Flip
breakdown.

### 4. Every scenario is a structured execution plan

The backend returns structured `wait`, `trigger`, `action`, `target`, and `cancel` fields plus state,
direction, evidence, and score. The UI renders these values without reconstructing strategy copy.
The primary decision card consumes the highest ranked active candidate, otherwise the best
alternative. Two compact secondary lanes expose the next alternative and dormant watch without
making them appear executable.

Alternative considered: client-authored narrative helpers. Rejected because the current page has
already shown contradictions when legacy JavaScript overwrote server decisions.

### 5. VWAP uses native OHLCV or an explicit SPY proxy

For each regular-session bar with finite positive volume, calculate typical price as
`(high + low + close) / 3` and cumulative VWAP as cumulative typical-price-volume divided by
cumulative volume. Reset at 09:30 America/New_York for each session and do not extend a prior
session into the next. Pre/post-market bars do not alter regular-session VWAP. Use native selected-
symbol volume when available. When SPX lacks compatible volume, calculate from same-session SPY
OHLCV and label every chart/UI surface `SPY VWAP Proxy`, including source symbol and timestamp. If
neither source is valid, return `available: false`; never silently use a proxy.

The chart receives timestamp/value points, renders one labeled non-interactive line, and updates it
in place without resetting zoom, timeframe, drawings, or selected gamma levels.

### 6. One canonical payload owns scenarios and VWAP

Extend the Market Pulse context response with `scenario_rankings`, `primary_scenario`, and `vwap`.
Retain the existing `strategy` object as a compatibility projection of the primary candidate until
all consumers migrate. Server-rendered Jinja and refresh JavaScript bind to the same structured
fields. A refresh applies only a complete, valid generation; otherwise the last valid generation
remains visible with its timestamp and lock state.

### 7. Grade labels the same confluence score

Remove the legacy context grade from the Trade Decision surface. Map the primary scenario's fixed
0–100 score to one grade: A (85–100), B (70–84), C (55–69), D (40–54), and F (0–39), with
plus/minus subdivisions within each band. Display the grade and score together, such as
`Grade A · 92/100 confluence`. Confirmation remains independent: a high grade can still be `WAIT`
when a hard trigger is missing, and the UI must state that distinction explicitly.

## Data Flow

1. Existing quote, level, gamma, session, freshness, and intraday OHLCV inputs are normalized.
2. The candidate generator emits stable candidate contexts for all supported levels and families.
3. Pure path evaluators return evidence-safe states and structured execution plans.
4. The scorer attaches fixed-weight component results; the ranker assigns lanes and ordering.
5. The VWAP calculator emits session points and availability metadata from the same bars generation.
6. The canonical view model serializes rankings and VWAP to Jinja and the context API.
7. The page patches decision lanes and chart VWAP from one response without a page reload.

## Failure and Compatibility Behavior

- Critical stale data locks all candidates regardless of score and preserves last-valid values.
- Missing completed candles, retest evidence, Strat data, or volume remains explicitly unavailable;
  no action or VWAP is inferred.
- Contradictory acceptance/rejection evidence invalidates that candidate without poisoning other
  levels.
- Existing chart and ladder overlays, controls, and viewport remain untouched by scenario refresh.
- Existing `strategy` fields remain populated from the ranked primary candidate during migration.

## Risks / Trade-offs

- [Many candidates create visual overload] → Render one primary, one alternative, and a collapsed
  dormant watch list; retain full breakdown in diagnostics.
- [Score looks like certainty or win probability] → Label it `Confluence`, expose component points,
  and state explicitly that confirmation gates control actionability.
- [SPX bars may not contain trustworthy volume] → Show VWAP unavailable with the source reason and
  never substitute a proxy silently.
- [Evidence may be associated with the wrong nearby level] → Bind evidence to candidate id,
  timestamp, approach direction, and tolerance; discard incompatible carryover.
- [Refresh creates mixed generations] → Stage and validate the entire canonical payload before one
  DOM/chart commit.
- [Financial harm from stale or overstated output] → Freshness is a hard gate, cancellation is
  mandatory, targets require direction, and no score can override missing confirmation.

## Migration Plan

1. Add candidate, continuation, scoring, ranking, and VWAP domain tests without UI routing.
2. Extend the service adapter and canonical payload while retaining the legacy strategy projection.
3. Migrate the decision card to ranked structured plans and add compact secondary lanes.
4. Add VWAP chart rendering and canonical in-place refresh reconciliation.
5. Run focused Python/JavaScript/template tests, rebuild the local Podman app, and verify health and
   receiving-page contracts; the user performs subjective visual inspection.
6. Roll back by removing additive ranking/VWAP payloads and restoring the compatibility projection;
   no persisted-data rollback is needed.

## Open Questions

None required before implementation. If the active SPX feed has no compatible volume, the first
release will show VWAP unavailable rather than choosing an undisclosed proxy.
