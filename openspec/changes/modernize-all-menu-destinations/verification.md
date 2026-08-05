# Verification Record

## Scope and parity

- Route matrix: `route-matrix.md` covers every deduplicated internal destination from the
  desktop primary navigation, desktop Menu, tablet drawer, and mobile menu.
- The modernization is isolated by `body.appModernizedPage`; `/dashboard` deliberately does
  not load the shared page-modernization stylesheet.
- The capture harness uses isolated temporary application storage and performs GET-only page
  inspection. It does not mutate production runtime data.
- Baseline and final captures cover all 30 internal destinations at 1440px. Representative
  routes from every page family are additionally captured at 390px, 768px, and 1920px.

## Passing checks

- Shared navigation, route, anchor, form-action, CSRF, and Dashboard-exclusion contracts:
  21 passed.
- Executive, planning, and strategy focused checks: 29 passed.
- Market Pulse and Candle Opens focused checks: 50 passed.
- Trades, Journals, Life Alignment, Analytics, Calendar, and Planner focused checks: 50 passed.
- Forward Pace, Payouts, and Goals focused checks: 14 passed.
- Operations, account, authentication, Self-Control, backup, restore, and library focused
  checks: 42 passed; one unrelated baseline copy assertion is recorded below.
- JavaScript syntax checks pass for `app_shell.js`, `spx_hero_chart.js`,
  `market_pulse_gamma_context.js`, `symbol_search_control.js`, `gamma_ladder.js`, and
  `analytics_dashboard.js`.
- The selected-day trade receiving-page regression passes, proving the selected day remains
  present and adjacent-day rows remain absent.
- Broker metric refresh success, failure diagnostics, and manual metric update checks pass;
  failure paths do not write broker metrics.
- The normal `./scripts/run_podman_app.sh` rebuild completed successfully. The rebuilt container
  reports `status: ok` and `safe_mode: false` from `http://127.0.0.1:5001/healthz`.

## Visual and accessibility checks

- Every captured route returns HTTP 200, except the intentional `/auth/passkeys` setup redirect,
  whose final destination returns HTTP 200.
- No captured viewport reports document-level horizontal overflow.
- Baseline and final visible-control counts match for every route and viewport.
- Shared focus-visible treatments, minimum control heights, reduced-motion handling, and
  responsive navigation states are scoped to modernized pages.
- Visible date, password, and restore-file controls discovered without programmatic names now
  have explicit `aria-label` values; field names, values, form actions, and methods are unchanged.

## Baseline drift kept separate

The following failures are in behavior or copy untouched by this change. No assertion was
weakened and no unrelated implementation was changed to conceal them:

1. `test_login_page_includes_passkey_cta_when_passkeys_exist` still finds the working
   `Use Passkey` CTA, but the untouched login template no longer includes the exact sentence
   `Passkey sign-in is ready` expected by the test.
2. `test_dashboard_tape_refresh_returns_series_points` expects a legacy four-series response;
   the current untouched service response contains the present SPX/VIX series contract.
3. `test_dashboard_live_tape_compact_labels_and_guardrails` expects the legacy
   `dashboardGapLine` template marker, which is absent from the current untouched Dashboard.
4. `test_dashboard_scope_rebases_stored_balance_for_drift_check` expects legacy Dashboard label
   copy that differs from the current untouched template.

These baseline items should be resolved in their own focused change because they concern
Dashboard behavior/copy, not the menu-destination modernization.
