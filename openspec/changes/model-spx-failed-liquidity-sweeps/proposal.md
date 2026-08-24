## Why

The SPX Playbook currently reduces level interactions to broad planning bias and a manual,
unordered trigger checklist. That can misstate a touch or move beyond a gamma level as an
actionable directional idea without proving the failed-liquidity-sweep sequence the strategy
requires.

## What Changes

- Add a deterministic SPX failed-liquidity-sweep evaluation contract that orders location,
  sweep, close-back-inside, five-minute 2-2, and trigger-break evidence.
- Distinguish rejection/reversal from acceptance/continuation so the page never recommends a
  fade after confirmed acceptance and never treats a later signal as proof of an earlier step.
- Interpret positive gamma as mean-reversion/scalp management, negative gamma as expansion in
  whichever direction gains control, and the Gamma Flip as a reduced-confidence transition.
- Expand the supported active-level set to Call Wall, Put Wall, Gamma Flip, New Call Wall, New
  Put Wall, prior-day high/low, and current-day high/low when those values and interaction
  evidence are available.
- Replace the current four-item manual checklist with the compact six-stage strategy checklist,
  using Pending, Confirmed, Failed, or Unavailable from structured evidence.
- Update the compact decision card with the active interaction, ordered state, direction,
  rejection/acceptance status, regime interpretation, deterministic targets, and missing
  evidence.
- Consolidate the competing refresh affordances into one clearly labeled `Refresh data` control
  that refreshes the canonical quote, gamma, strategy, guardrail, and page-state contract.
- Enforce a single decision hierarchy so locked, unavailable, or pending data can never render
  simultaneously as actionable or trigger-confirmed.
- Handle missing, stale, or malformed strategy inputs without fabricated confirmation, malformed
  JSON rendering, or a runtime crash.
- Add focused state-engine, service/view-model, component-contract, and visual regression checks.
- Preserve the existing McCain Capital dark layout, chart, gamma ladder, navigation, providers,
  and other working controls.
- Explicitly exclude risk controls, calculators, loss/account locks, contract sizing, and any
  broader application redesign.

### Acceptance Criteria

- Every state transition and invalid ordering is deterministic and covered for bullish and
  bearish reversals.
- Reversal-ready and acceptance/continuation outputs are mutually exclusive.
- The four supplied strategy examples produce the specified states, directions, targets, and
  management language.
- Missing required levels, candles, regime data, or confirmation remain pending/unavailable and
  cannot create a ready signal.
- Changing the active level, direction, timeframe evidence, or gamma regime recomputes the whole
  result without carrying incompatible confirmation forward.
- The receiving SPX Playbook remains compact and usable at desktop and narrow viewports, with no
  risk-management UI and no regression to the chart or gamma ladder.
- A successful manual refresh updates every shared spot, timestamp, guardrail, decision, and setup
  surface coherently; failure preserves the last valid data and reports the failure inline.
- OpenSpec validation, focused tests, type checks, lint, production build, and relevant component
  or integration checks pass using repository commands.

## Capabilities

### New Capabilities

- `spx-failed-liquidity-sweep-playbook`: Defines ordered failed-sweep evaluation,
  rejection-versus-acceptance outcomes, gamma-aware management, deterministic targets, missing
  data behavior, and the compact SPX Playbook presentation contract.

### Modified Capabilities

None. The repository currently has no synchronized main capability specs; this change introduces
the behavioral contract as a new capability while preserving existing integrations.

## Impact

- Affected areas: the Market Pulse/SPX Playbook domain and service/view-model layer, its Jinja
  decision and checklist surface, narrowly related JavaScript/types and CSS, and focused tests.
- Data sources: existing gamma snapshots and level maps, SPX spot/ohlc series, current/prior-day
  levels, and any existing five- and fifteen-minute candle/Strat evidence. Unsupported evidence
  remains unavailable rather than inferred.
- APIs/contracts: the internal playbook payload gains typed strategy-state, evidence, target, and
  checklist fields while existing chart, ladder, quote, and refresh contracts remain compatible.
- Dependencies: no new runtime library or data provider is planned.
- Financial assumptions: this is deterministic strategy interpretation, not a projection or
  order/risk system. It does not size contracts, authorize trades, alter account values, or imply
  fills. Targets are existing observable liquidity/structural levels, and stale or absent inputs
  fall back to pending/unavailable.
