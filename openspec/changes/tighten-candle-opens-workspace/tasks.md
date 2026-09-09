## 1. Navigation

- [x] 1.1 Add Candle Opens immediately after Market Pulse in the full desktop navigation.
- [x] 1.2 Remove the duplicate desktop Tools-menu destination while preserving responsive and mobile access.
- [x] 1.3 Add focused navigation-order and single-destination contract coverage.

## 2. Workspace hierarchy

- [x] 2.1 Replace the expanded Today Snapshot cards with a compact today-orientation strip and select-today action.
- [x] 2.2 Keep the calendar and selected-day profile as the dominant scan-and-inspect workspace.
- [x] 2.3 Preserve month navigation, calendar selection, keyboard behavior, day notes, reset cycles, and macro details.

## 3. Redundancy cleanup

- [x] 3.1 Consolidate catalyst summaries into date-deduplicated cards with merged reason badges.
- [x] 3.2 Limit the default catalyst view to three unique dates and expose remaining dates through expansion.
- [x] 3.3 Merge monthly totals and cycle legends into one collapsed Timing Reference section.
- [x] 3.4 Retain the separate collapsible macro-event agenda and remove obsolete duplicate wrappers.

## 4. Responsive presentation

- [x] 4.1 Add page-scoped desktop styling that emphasizes the calendar without changing the global canvas width.
- [x] 4.2 Add compact-desktop, tablet, and mobile stacking rules with no horizontal page overflow.
- [x] 4.3 Verify labels, controls, focus states, and selected-day content remain legible at supported widths.

## 5. Verification

- [x] 5.1 Update Candle Opens template-contract tests for the consolidated hierarchy and retained details.
- [x] 5.2 Run focused route, navigation, and Candle Opens tests plus relevant static syntax checks.
- [x] 5.3 Run `git diff --check`, rebuild with `./scripts/run_podman_app.sh`, and verify `/healthz`.
- [x] 5.4 Inspect the deployed Candle Opens page at desktop and responsive widths and record any visual review left to the user.
