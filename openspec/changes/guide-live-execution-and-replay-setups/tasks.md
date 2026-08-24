## 1. Baseline and refresh model

- [x] 1.1 Add focused regression tests reproducing mixed prior-day canonical context with current
  quote, bars, and Gamma Ladder observations.
- [x] 1.2 Define the immutable canonical generation envelope and component refresh outcome models.
- [x] 1.3 Add generation validation for ticker, session date, timestamps, required freshness, and
  cross-component coherence.

## 2. Bounded live refresh orchestration

- [x] 2.1 Trace and remove the repeated provider-worker/thread growth under automatic refresh.
- [x] 2.2 Implement per-ticker single-flight refresh with a bounded shared provider work limit.
- [x] 2.3 Apply finite provider deadlines and guarantee worker capacity is released on timeout,
  exception, and cancellation.
- [x] 2.4 Make automatic and manual refresh share the same orchestration while preserving manual
  forced refresh as a fallback.
- [x] 2.5 Return `promoted`, `partial`, `unchanged`, or `failed` with component results, last-valid
  time, attempt time, and next retry.

## 3. Canonical execution context

- [x] 3.1 Assemble quote, completed bars, gamma, levels, evidence, ranked scenarios, and permission
  into one server-side generation before cache promotion.
- [x] 3.2 Prevent partial or mixed-generation inputs from replacing the last valid canonical context.
- [x] 3.3 Derive the live guide fields for location, evidence, trigger, confirmation, action,
  invalidation, and target from the promoted primary scenario.
- [x] 3.4 Add operational health diagnostics and tests proving repeated refresh remains bounded and
  request handling survives provider timeouts.

## 4. Live Market Pulse presentation

- [x] 4.1 Update the in-place refresh controller to commit all execution nodes atomically from one
  generation.
- [x] 4.2 Preserve timeframe, viewport, drawings, chart toggles, ticker, and selected ladder strike
  across canonical refresh.
- [x] 4.3 Replace ambiguous success copy with distinct live-ready, partial observation, unchanged,
  and failed states including blocking components and timing.
- [x] 4.4 Present the live execution guide prominently and remove action language whenever freshness
  or confirmation is insufficient.

## 5. Point-in-time Setup Replay engine

- [x] 5.1 Build as-of-time input slices from completed session bars and timestamped level/gamma
  observations without future data.
- [x] 5.2 Reuse live scenario ranking and candle-evidence evaluators for each completed five-minute
  decision point.
- [x] 5.3 Record only first confirmation per stable candidate until invalidation and expose rejected
  candidates only as optional diagnostics.
- [x] 5.4 Freeze signal-time eligibility before calculating target/invalidation order, MFE, MAE, and
  end-of-window outcome.
- [x] 5.5 Mark same-bar target/invalidation ordering ambiguous when bar resolution cannot prove which
  happened first.
- [x] 5.6 Add a read-only replay API for ticker and session date with no ledger or journal writes.

## 6. Setup Replay presentation

- [x] 6.1 Add a collapsed intraday replay summary below the live guide with count and most recent
  signal time.
- [x] 6.2 Add an expanded chronological setup view separating “Known at signal” from “What happened
  next.”
- [x] 6.3 Add direction, scenario-family, and qualified/rejected filters with plain-language reason,
  entry zone, stop, targets, and data-availability labels.
- [x] 6.4 Add accessible chart markers for qualified historical setup timestamps without obscuring
  live levels or Strat numbers.

## 7. Verification and delivery

- [x] 7.1 Add focused service, endpoint, template-contract, refresh-controller, and no-look-ahead
  replay tests.
- [x] 7.2 Run focused pytest, Ruff/Black checks for changed Python, JavaScript syntax checks, and
  strict OpenSpec validation.
- [x] 7.3 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and confirm stable process and
  thread counts across repeated market-style refreshes.
- [x] 7.4 Verify the authenticated Market Pulse receiving surface for atomic timestamps and values,
  honest partial failure, preserved chart state, actionable live guidance, and historical replay.

## 8. Replay layout and quality ranking refinement

- [x] 8.1 Move Setup Replay below Alternative and Dormant Watch as a full-width collapsed disclosure.
- [x] 8.2 Bound expanded replay height with internal scrolling and retain direction/family controls.
- [x] 8.3 Sort qualified setups strongest-to-weakest by signal-time confluence with deterministic ties.
- [x] 8.4 Add focused contracts, rebuild, and verify the authenticated receiving surface.
