## Automated Verification

- `python -m pytest -q`: 676 passed.
- Focused Market Pulse refresh/coherence suite: 200 passed.
- `python -m ruff check .`: passed.
- `python -m black --check --config pyproject.toml .`: 151 files unchanged.
- `python -m compileall -q mccain_capital`: passed; no standalone type-check command is
  configured in this repository.
- `node --check static/js/market_pulse_gamma_context.js`: passed.
- `openspec validate model-spx-failed-liquidity-sweeps --type change --strict --no-interactive`:
  passed.
- `./scripts/run_podman_app.sh`: production image rebuilt and container started successfully.
- `GET /healthz`: returned HTTP success with `status: ok`.
- `GET /market-pulse?ticker=SPX`: returned HTTP success after redirects; the local auth gate was
  rendered because the smoke client was not signed in. Authenticated receiving-surface rendering
  is covered by Flask component tests.
- Authenticated manual refresh: the labeled `Refresh data` control entered a disabled
  `Refreshing…` state, completed a canonical page reload, advanced the page timestamp from
  10:41:45 AM ET to 10:42:10 AM ET, and updated SPX consistently from 7,743.84 to 7,747.01 across
  the header and execution narrative.
- Authenticated decision check: valid data rendered `Healthy`, while the incomplete ordered
  strategy rendered `Not actionable yet` instead of conflicting actionable/locked language.

## Data Limitations

- Current aggregate quote/gamma data can select the nearest active level and identify the current
  approach side.
- Five-minute sweep, close-back-inside, Strat 2-2, trigger-break, acceptance/retest, and optional
  fifteen-minute runner confirmation are consumed only from explicit `strategy_evidence` fields.
  When the live feed does not provide them, the UI reports Pending or Unavailable and cannot emit
  `REVERSAL_READY`.

## Visual Verification

Per user direction, desktop and narrow-viewport visual inspection was not performed by Codex and
remains the single open task. The rebuilt site is available at `http://localhost:5001`.
