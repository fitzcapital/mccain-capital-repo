## Verification

- Replay accepts only an explicit Gamma observation at or before the signal timestamp.
- Existing setup rows migrate additively and retain their prior evidence and outcomes.
- Outcome updates preserve captured signal-time Gamma; unavailable rows can be enriched only by a
  valid same-event, pre-signal observation.
- Analytics keeps unavailable Gamma setups in overall frequency totals but excludes them from
  named-regime performance comparisons.

## Coverage limitation

Legacy setups without an explicit regime and eligible observation timestamp remain `unavailable`.
They are never backfilled or inferred from a current Gamma snapshot, price action, or timestamp-only
evidence. Coverage will increase only as newly recorded setup signals capture contemporaneous Gamma.

## Checks

- Focused Python tests: Replay, Setup Analytics, migrations, and page/API contracts.
- JavaScript contract tests and syntax validation.
- Ruff, Black, `git diff --check`, and strict OpenSpec validation.
- Local Kubernetes deployment completed with both workloads ready, `/healthz` healthy, the four
  Gamma columns and regime index present, and the deployed filter/coverage assets confirmed.
- The 182 pre-change setup records correctly remain unavailable; captured coverage begins with new
  setup signals that have an eligible contemporaneous Gamma observation.
