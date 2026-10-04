# Verification Evidence

Verified against the deployed local Kubernetes application on October 2, 2026.

## Checks

- Focused Python analytics coverage: 35 tests passed.
- JavaScript analytics coverage: 16 tests passed.
- Ruff, Black check, JavaScript syntax check, and `git diff --check` passed.
- Local Kubernetes deployment completed and `/healthz` returned `status: ok`.
- Desktop rendering showed five compact weekday cards without clipped titles or page-level
  horizontal overflow. Responsive CSS contract coverage verified the three-column and one-column
  narrow-width layouts.
- Today rendered only the active Friday cohort and kept weekdays without completed outcomes neutral.
- All History rendered all five weekdays. Selecting Wednesday synchronized the page to 62 setups
  across 6 sessions; clearing the weekday restored all 326 setups.

## Observed Sample

- 326 canonical setups across 27 sessions.
- 288 setups had completed outcomes; open and historically unavailable outcomes were excluded from
  target-rate denominators.
- Seven duplicate records were excluded before aggregation.
- Weekday session coverage ranged from four Monday sessions to six sessions for Wednesday,
  Thursday, and Friday. Results are useful for study but remain sensitive to the limited number of
  independent sessions.
- Signal-time gamma was captured for 12 of 326 setups (3.7%). The interface reports this coverage
  explicitly and does not infer missing historical gamma from a current snapshot. Weekday gamma
  conclusions remain insufficient where fewer than five signal-time samples exist.
