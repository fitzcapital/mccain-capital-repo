## 1. Domain Contracts and Fixtures

- [x] 1.1 Add typed scenario family, lane, state, evidence, plan, and score-component contracts.
- [x] 1.2 Add deterministic fixtures covering failed highs/lows, breakouts/breakdowns, and Local Flip loss/reclaim.
- [x] 1.3 Add same-symbol regular-session OHLCV fixtures with independently calculated VWAP results.

## 2. Candidate Generation and Path Evaluation

- [x] 2.1 Normalize, deduplicate, and stably identify all supported structural level candidates.
- [x] 2.2 Generate directionally valid candidate families without carrying evidence between levels.
- [x] 2.3 Reuse the existing ordered failed-sweep evaluator for failed-high and failed-low candidates.
- [x] 2.4 Implement pure completed-close and retest evaluation for breakout and breakdown candidates.
- [x] 2.5 Implement Local Flip loss and reclaim evaluation with explicit direction and cancellation.
- [x] 2.6 Reject contradictory, stale, malformed, and prerequisite-skipping candidate evidence.

## 3. Confluence Scoring and Ranking

- [x] 3.1 Implement the fixed 100-point confluence component table in one Python constant.
- [x] 3.2 Score location, boundary close, retest, Strat, gamma, cluster, VWAP, and target-space evidence with breakdown metadata.
- [x] 3.3 Assign active, alternative, and dormant lanes from hard evidence gates before score ordering.
- [x] 3.4 Add deterministic lane, score, distance, level, and family tie-breaking.
- [x] 3.5 Build exact wait, trigger, action, target, and cancellation fields for each ranked candidate.
- [x] 3.6 Add exhaustive focused unit tests for scoring, ranking, lane precedence, and mutual exclusion.

## 4. Session VWAP

- [x] 4.1 Implement a pure regular-session VWAP calculator from finite same-symbol OHLCV bars.
- [x] 4.2 Reset VWAP by America/New_York session and exclude pre/post-market bars from accumulation.
- [x] 4.3 Return explicit VWAP availability and source-reason metadata for missing or malformed volume.
- [x] 4.4 Add VWAP calculation, session-boundary, malformed-input, and no-proxy unit tests.

## 5. Canonical Service and API Integration

- [x] 5.1 Adapt existing quote, level, gamma, Strat, session, freshness, and bars data into candidate inputs.
- [x] 5.2 Add ranked scenarios, primary scenario, score breakdowns, and VWAP to the canonical Market Pulse view model.
- [x] 5.3 Preserve the legacy single-strategy projection from the ranked primary candidate during migration.
- [x] 5.4 Stage and validate ranking and VWAP generation coherence before returning refresh payloads.
- [x] 5.5 Add focused service and route tests for healthy, stale, unavailable, conflicting, and refresh-failure contracts.

## 6. Scenario Decision Interface

- [x] 6.1 Bind the primary five-stage decision card exclusively to backend structured scenario fields.
- [x] 6.2 Add compact alternative and collapsed dormant lanes with non-actionable visual semantics.
- [x] 6.3 Add a transparent confluence breakdown that distinguishes score from confirmation state.
- [x] 6.4 Update the checklist to preserve reversal evidence and expose path-specific continuation evidence.
- [x] 6.5 Reconcile all scenario nodes from one in-place canonical refresh without legacy text overrides.
- [x] 6.6 Add rendered-template and JavaScript contract tests for lane clarity, exact fields, and stale locks.

## 7. VWAP Chart Integration

- [x] 7.1 Add one theme-compatible labeled VWAP series to the existing hero chart.
- [x] 7.2 Reconcile VWAP points and unavailable state without recreating the chart or resetting user state.
- [x] 7.3 Feed VWAP relationship into only the documented confluence component.
- [x] 7.4 Add chart contract tests proving overlay isolation, timestamp alignment, and viewport preservation.

## 8. Verification and Delivery

- [x] 8.1 Run focused scenario, service, route, template, and chart tests plus JavaScript syntax checks.
- [x] 8.2 Run Ruff or equivalent focused Python checks and `git diff --check` on intended files.
- [x] 8.3 Rebuild with `./scripts/run_podman_app.sh` and verify `/healthz` plus canonical Market Pulse payload behavior.
- [x] 8.4 Verify desktop and narrow receiving-surface structure without claiming subjective visual approval.
- [x] 8.5 Update the OpenSpec task evidence, validate strictly, sync the completed specs, and prepare the change for archive.

## 9. SPX VWAP Proxy and Score Clarity

- [x] 9.1 Fetch compatible same-session SPY OHLCV only when SPX native VWAP is unavailable.
- [x] 9.2 Add VWAP source symbol, proxy status, source timestamp, and explicit `SPY VWAP Proxy` labeling.
- [x] 9.3 Map the primary scenario score to deterministic letter-grade bands in the backend contract.
- [x] 9.4 Replace the legacy Trade Decision grade with the scenario grade and explain score versus confirmation.
- [x] 9.5 Add proxy provenance, unavailable fallback, grade-band, and UI coherence tests.
- [x] 9.6 Run focused checks, rebuild the local app, verify health, sync specs, and strictly validate the change.
