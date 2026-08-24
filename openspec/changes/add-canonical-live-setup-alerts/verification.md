# Verification

## Automated evidence

- `30 passed` — live setup lifecycle, identity, cutoff, terminal monotonicity, stale pause,
  persistence, concurrency, acknowledgement, API generation, and Market Pulse HTML contracts.
- `15 passed` — existing canonical refresh and Setup Replay isolation tests.
- `7 passed` — JavaScript revision ordering, countdown, exactly-once delivery, secondary-watch
  ranking, and refresh single-flight behavior.
- Python syntax, Ruff checks, JavaScript syntax, and `git diff --check` passed.
- OpenSpec strict validation passed.

## Local receiving surface

- Rebuilt with `./scripts/run_podman_app.sh`.
- `/healthz` returned `status: ok`.
- The authenticated Market Pulse page rendered one `Live Setup Monitor` with no browser console
  errors after the rebuild.
- At the verification time the regular session had ended; the monitor correctly showed the
  unconfirmed setup as `Expired` and actionability as paused.

## Reliability behavior

- The monitor consumes only the canonical SPX generation and completed five-minute evidence.
- Alerts require SPX, score 76 or higher, coherent required data, canonical `ACTIVE`/`ready`
  permission, completed-candle confirmation, and confirmation no later than 3:15 PM ET.
- Server delivery state is durable and lock-protected; client delivery ids provide a second
  reload guard. Persistence failure pauses alerts closed.
- Setup Replay remains a separate read-only endpoint and cannot produce a live alert.
- Browser notifications and the short sound are opt-in. In-app notification-center delivery is
  automatic for a newly confirmed eligible setup.

## Configuration and rollback

- Cutoff: `MARKET_PULSE_LIVE_SETUP_CUTOFF_ET` (default `15:15`).
- Feature flag: `MARKET_PULSE_LIVE_SETUP_ALERTS_ENABLED` (default enabled). Set it false to remove
  server monitoring and alert delivery while preserving existing Market Pulse behavior.

## Provider-dependent limitation

- A real-market confirmation can only alert when the existing canonical spot, completed-bar,
  gamma, level, and strategy inputs are current. If any required input becomes stale, the monitor
  deliberately pauses instead of guessing or replaying old evidence.
