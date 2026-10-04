## 1. Weekday Analytics Service

- [x] 1.1 Add deterministic New York weekday normalization and filter validation.
- [x] 1.2 Aggregate canonical filtered setups into weekday metrics with explicit unavailable-date
  reconciliation.
- [x] 1.3 Build per-weekday setup-family and family-plus-time cohorts using the existing evidence,
  Wilson-adjusted rate, excursion, and tie-break ranking rules.
- [x] 1.4 Add weekday leader, gamma-coverage, drill-down, and reconciliation fields to the additive
  analytics payload without changing existing field meanings.
- [x] 1.5 Extend analytics cache keys and filter options so weekday selections cannot reuse results
  from another cohort.

## 2. Weekday Study Interface

- [x] 2.1 Add a compact Monday-through-Friday study to the Setup Analytics Overview with target
  rate, completed fraction, evidence maturity, best setup, and best time.
- [x] 2.2 Add concise keyboard-accessible helpers or expandable details for coverage, MFE/MAE,
  adjusted ranking, and gamma sufficiency.
- [x] 2.3 Add weekday selection and leader drill-down controls that preserve the active horizon and
  keep all page sections synchronized.
- [x] 2.4 Add empty, no-completed-outcome, invalid-date, and insufficient-gamma states without
  presenting missing evidence as a zero-percent result.
- [x] 2.5 Verify responsive layout prevents clipped titles, overlap, and page-level horizontal
  overflow at desktop and narrow widths.

## 3. Reliability Tests

- [x] 3.1 Add service tests for weekday assignment, target-rate denominators, open/unavailable
  outcomes, duplicate exclusion, and total reconciliation.
- [x] 3.2 Add leader tests covering single examples, early evidence, established evidence, Wilson
  ranking, excursion tie-breaks, deterministic ties, and no-winner states.
- [x] 3.3 Add API-contract tests for the additive payload, weekday filter propagation, cache
  separation, gamma coverage, and backward compatibility.
- [x] 3.4 Add template and JavaScript contract tests for compact labels, accessible helpers,
  drill-down behavior, and responsive states.

## 4. Verification and Delivery

- [x] 4.1 Run focused Setup Analytics pytest coverage, JavaScript syntax or node tests, formatting
  checks for touched files, and `git diff --check`.
- [x] 4.2 Run the normal local deployment workflow and verify `/healthz` before receiving-page QA.
- [x] 4.3 Verify Today and All History in the deployed Setup Analytics page, including weekday
  reconciliation, leader drill-down, clear-filter behavior, helpers, and narrow viewport layout.
- [x] 4.4 Record the observed sample limitations and gamma-coverage behavior in the completed change
  evidence without modifying runtime data.
