# Dashboard modernization compatibility inventory

## Operating sequence

1. Discipline rail: trading state, session mode, one-loss rule, pressure reset.
2. Session command deck: state, gate, alignment, next action, five-day behavior memory.
3. Prepare: ticker context, mission and invalidators, readiness, priority action,
   permission checklist, live upload sync, import and scope actions.
4. Plan: decision refresh, pre-trade gate, bias/risk/plan, gamma structure, Daily Brief.
5. Foundation: intention, routine, alignment, and session behavior controls.
6. Monitor: live tape status, timeframe, refresh, symbol search, SPX/VIX chart lanes.
7. Support and Health: wake lock, milestone, risk/trust, status orbit, reflection.
8. Performance and accounts: account scope, broker metrics, uploads, reconciliation,
   performance range, diagnostics and behavior destinations.
9. Forward pace: timeframe, settings, projection details.
10. Daily P/L calendar: lazy fragment, day preview, selected-day Trades navigation.

## Behavioral hooks that must remain stable

- Root and navigation: `dashboardModeShell`, `data-dashboard-mode`,
  `data-selected-ticker`, `data-dashboard-ticker-switch`.
- Discipline and reset: `data-discipline-state`, `data-discipline-mode`,
  `dashboardResetTrigger`, `dashboardResetModal`, `data-reset-close`,
  `data-reset-action`.
- Readiness and command: `data-dashboard-readiness`, `data-readiness-*`,
  `data-dashboard-live-sync`, `data-dashboard-sync-*`, `data-readiness-item`.
- Planning: `dashboardPlanningRefreshBtn`, `data-trade-gate-toggle`,
  `dashboardGammaStrip`, `data-gamma-key`, `data-gamma-popover`.
- Tape: `dashboardTapeStreamStatus`, `dashboardTapeRefreshBtn`,
  `data-role="tape-window-*"`, `data-watch-symbol`, `data-tape-lane`,
  `data-role="tape-symbol-*"`.
- Health and behavior: `dashboardWakeLockBtn`, `dashboardHealthSurface`,
  `data-intention-preset`, `data-routine-check`, `data-alignment-check`,
  `data-reflection-answer`, `data-dashboard-optional`.
- Broker and accounts: `data-dashboard-account-check`,
  `data-dashboard-account-select-all`, `data-dashboard-account-clear`,
  `dashboardDriftRefreshBtn`, broker metric forms and diagnostic disclosures.
- Calendar: `advancedDashboardWidgets`, `data-calendar-endpoint`,
  `dashboardCalendarLazy`, `dayPreviewButton`, `calendarPreview`,
  `calendarPreviewOpen`, and each day's `data-open-url`.

## Endpoint and persistence contract

- Keep current form actions and links for planning refresh, behavior/reflection updates,
  statement upload, live sync, account metrics, manual broker metrics, milestone, pace,
  backups, diagnostics, alerts, Market Pulse, Trade Gate, Calendar, and Trades.
- Preserve local-storage keys used by the Dashboard controller for discipline state,
  session mode, trade gates, optional mode, health disclosure, and interaction state.
- Preserve server-rendered stale, delayed, unavailable, failed, disabled, and diagnostic
  states; presentation must not upgrade their trust level.

## CSS cascade audit

- Dashboard styles occur in several generations throughout `static/css/app.css`, with
  late cinematic-nebula selectors carrying enough specificity to flatten earlier states.
- Outcome-specific calendar rules and live-tape positive/negative states must remain after
  generic surface rules or use narrowly scoped state selectors in the final layer.
- The modernization layer belongs at the end of `app.css`, scoped to `.page-dashboard`, so
  it does not change other pages and wins without broad `!important` usage.
- Responsive rules must preserve intentional internal scrolling for tape and calendar data
  while preventing document-level horizontal overflow.

## Focused verification map

- Route and control rendering: `tests/test_dashboard_modernization_contract.py` and focused
  Dashboard cases in `tests/test_app_core.py`.
- Ticker behavior: `tests/test_dashboard_playbook_tickers.py`.
- Planning/gamma behavior: `tests/test_dashboard_execution_alignment.py`.
- Account/calendar scope: `tests/test_auth_scope_calendar_e2e.py` and Dashboard calendar cases
  in `tests/test_app_core.py`.
- Broker metric protection: focused Dashboard cases in `tests/test_trades_feature_upgrades.py`.
- Client syntax: `node --check static/js/dashboard_command_center.js`.
- Visuals: `scripts/capture_dashboard_redesign.py` at four representative widths.

## Baseline status

The local app rebuild and `/healthz` check succeeded. Because no reusable authenticated
visual-smoke session or credentials were available, baseline and final captures used the
application's isolated test mode with authentication disabled and all storage rooted in a
temporary directory. The baseline capture served the committed `HEAD` Dashboard CSS while the
final capture served the modernization layer. Both sets cover 390, 768, 1440, and 1920 pixel
viewports without modifying runtime or personal data.

Each capture contains five stage sections, has no document-level horizontal overflow, and has no
visible control without an accessible name. The final inventory contains 342 controls at tablet,
laptop, and wide-desktop widths and 343 at mobile width (the existing mobile disclosure accounts
for the extra control). The same-run baseline contains six additional economic-calendar controls:
five Candle Opens news links and one news-detail button supplied by the time-sensitive news feed.
The stable control inventory is otherwise identical; CSRF token values are expected to differ
between isolated app instances. No static control, endpoint, persistence key, or behavior hook was
deliberately removed or renamed.

One focused baseline assertion remains unrelated to this presentation-only change:
`test_dashboard_scope_rebases_stored_balance_for_drift_check` expects a legacy starting-balance
label that the current Dashboard does not render. It fails alone and no financial or scope logic
was changed to mask it. The selected-day receiving-page regression passes and proves the selected
trade is present while adjacent-day trades are absent.
