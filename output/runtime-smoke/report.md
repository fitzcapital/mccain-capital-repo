# Runtime Smoke Report

Date: 2026-03-03
Branch: codex/visual-gate-hardening
Container: mccain-capital-app

## Restart + Health
- Restart executed: `podman restart mccain-capital-app`
- Health endpoint after restart: `/healthz` returned `status=ok`

## Persistent Data Integrity (Before vs After Restart)
- DB path: `persistent-data/journal.db`
- SHA256 unchanged: `true`
- Size unchanged: `true`
- Table counts unchanged: `true`

Counts snapshot:
- trades: 26
- entries: 8
- trade_reviews: 45
- settings: 6

## Authenticated Smoke Checks
Authenticated session cookie was generated from app signing config and used for read-only GET checks.

Route status:
- `/dashboard` -> 200
- `/calendar` -> 200
- `/analytics?tab=behavior` -> 200
- `/trades/upload/statement?ws=upload` -> 200
- `/trades/upload/statement?ws=reconcile` -> 200

Content markers verified:
- Calendar: `dayPreviewButton`, `calendarPreview`, `calendarPreviewBackdrop`, `commandCalendarPreview`
- Analytics behavior: `Setup Expectancy Heatmap by Time Block`, `No heatmap data in this range.`
- Upload workspace: `Import Workspace`, `Upload Statement`, `statement-upload-form`
- Reconcile workspace: `Reconcile Import Batches (30D)`, `Unresolved Batches`

## Evidence Files
- `output/runtime-smoke/pre.json`
- `output/runtime-smoke/post.json`
- `output/runtime-smoke/comparison.json`
- `output/runtime-smoke/calendar.html`
- `output/runtime-smoke/analytics_behavior.html`
- `output/runtime-smoke/upload_ws.html`
- `output/runtime-smoke/reconcile_ws.html`
