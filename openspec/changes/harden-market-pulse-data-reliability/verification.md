# Reliability verification baseline

Verified on August 20, 2026 after rebuilding the local Podman app.

## Automated checks

- `93` focused Python reliability, snapshot-integrity, replay, and gamma-workflow tests passed.
- `3` JavaScript refresh-coordinator tests passed.
- Targeted Ruff checks, Python syntax checks, JavaScript syntax checks, and `git diff --check`
  passed.
- Forced missing quote, completed-bars, and gamma timestamps each named the failed component and
  locked execution.
- An invalid newer candidate could not replace the last verified generation.
- Shared-cache tests confirmed atomic promotion, monotonic ordering, and newest-worker adoption.

## Rebuilt runtime

- `./scripts/run_podman_app.sh` completed successfully.
- `/healthz` returned `status: ok`, `safe_mode: false` at `2026-08-20T17:27:37-04:00`.

## Deployed polling baseline

- The signed-in Market Pulse page retained the `SPX` ticker, `5m` timeframe, sticky-summary state,
  URL, and locked execution state across repeated in-place polls.
- The visible countdown tracked the actual retry deadline (`Retry in 4s`, then `Retry in 2s`).
- No new canonical timeout, abort, or generation-rejection warning appeared across multiple
  post-rebuild polling intervals.
- The after-hours generation identified `SPX`, session `2026-08-20`, and the exact required blocker:
  completed bars were stale while spot and gamma were current. Execution remained fail-closed.
- Automatic stale polls returned the last verified generation immediately with
  `worker_recovery_pending`; slow provider recovery remained assigned to the background market
  worker. Manual refresh retained a bounded 25-second request timeout.

This is the reliability baseline for later ladder-ranking and time-of-day enhancements.
