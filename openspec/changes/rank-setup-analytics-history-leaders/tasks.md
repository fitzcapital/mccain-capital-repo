## 1. Ranking Model

- [x] 1.1 Add reusable cohort aggregation for setup family, 30-minute New York window, and
  setup-family/window combinations using completed outcomes only.
- [x] 1.2 Add Wilson lower-bound ranking, evidence maturity labels, movement-quality tie-breakers,
  and stable deterministic ordering.
- [x] 1.3 Add focused tests for minimum samples, tiny perfect samples, ties, missing excursions, and
  no-completed-outcome selections.

## 2. Analytics Contract

- [x] 2.1 Add the three leaders, ranking inputs, evidence labels, horizon label, and missing-evidence
  explanation to the setup analytics response.
- [x] 2.2 Ensure Today, session presets, All History, exact dates, and advanced filters produce leaders
  from the same normalized selected rows as all existing analytics panels.
- [x] 2.3 Add response-contract tests for all-history winners, provisional winners, mixed directions,
  and empty evidence.

## 3. Decision Surface

- [x] 3.1 Add a compact "What worked best" strip with Best Setup, Best Time, and Best Setup + Time
  cards near the top of Setup Analytics.
- [x] 3.2 Show target fraction/rate, completed sample, evidence maturity, median favorable move, and
  median adverse move without truncated titles.
- [x] 3.3 Add concise ranking-method disclosure and clearly state that hypothetical option profit is
  excluded from ranking.
- [x] 3.4 Add leader drill-down controls that reuse the existing filters and preserve the active date
  horizon, plus a clear return action.
- [x] 3.5 Add JavaScript and template contract tests for updating, filtering, long labels, and empty
  states.

## 4. Verification

- [x] 4.1 Run focused Setup Analytics Python and JavaScript tests, Ruff/Black checks for changed
  modules, JavaScript syntax checks, OpenSpec strict validation, and `git diff --check`.
- [x] 4.2 Rebuild the local Podman app and verify `/healthz` plus Today and All History receiving
  states at desktop and narrow widths.

## 5. Canonical Counting and Artifact Hygiene

- [x] 5.1 Add deterministic read-time canonicalization for duplicate actionable setup rows without
  deleting stored history.
- [x] 5.2 Expose excluded-duplicate counts and keep every analytics surface on the canonical row set.
- [x] 5.3 Add regression tests for legacy duplicate IDs, overlapping pattern aliases, distinct setups,
  and canonical evidence selection.
- [x] 5.4 Audit tracked/runtime image writers, confirm normal analytics requests are write-free, run
  focused and Market Pulse tests, rebuild, and verify deployed counts and health.
