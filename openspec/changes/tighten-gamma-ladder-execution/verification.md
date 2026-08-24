# Verification

Verified on 2026-08-10 ET.

## Automated checks

- Focused Gamma and Market Pulse targets: 163 passed.
- Full test suite: 703 passed.
- Ruff, Black check, Python compilation, JavaScript syntax, and `git diff --check`: passed.
- `openspec validate tighten-gamma-ladder-execution --type change --strict --no-interactive`:
  passed.

## Rebuild and runtime

- Rebuilt with `./scripts/run_podman_app.sh`.
- Container image: `8572fdf290ad2118dbf660e5602445ac67d445fc37ddb50e12d73672944e4d9e`.
- `/healthz`: `status=ok`, `safe_mode=false`.
- Authenticated Market Pulse check at `/market-pulse?ticker=SPX` after hours:
  - selected next listed expiration and labeled it `1DTE` / `Next expiry`;
  - rendered the server-backed stabilization map and planning-only guardrail;
  - rendered nine focused rows with ten secondary rows hidden;
  - expanded to 19 rows and returned to nine without navigation;
  - canonical refresh retained the URL, accepted rows, and produced no console warnings/errors;
  - narrow 390 x 844 check kept the map, disclosure control, rows, and selected-level inspector visible.

## Limits

- Verification used the after-hours live-data path. Live-session behavior is covered by deterministic
  service tests but was not available for a same-session runtime check.
- Visual/design approval is intentionally left to the user; verification here is functional only.
