## 1. Baseline and Compatibility Inventory

- [x] 1.1 Capture authenticated full-page Dashboard screenshots at mobile, tablet, laptop, and wide-desktop widths without modifying runtime data.
- [x] 1.2 Inventory every Dashboard link, button, form, disclosure, dialog, ID, `data-*` hook, endpoint, persisted key, lazy fragment, and visible loading/error state.
- [x] 1.3 Map the current DOM order and user journey across Prepare, Plan, Monitor, Support/Health, Performance, Pace, and Calendar.
- [x] 1.4 Record the focused pytest cases and JavaScript checks that protect the inventoried functionality, adding missing high-value assertions before moving markup.

## 2. Dashboard Visual Foundation

- [x] 2.1 Audit the late Dashboard and theme overrides in `static/css/app.css`, identifying rules that flatten state-specific styles or conflict across breakpoints.
- [x] 2.2 Add Dashboard-scoped color, surface, spacing, radius, shadow, typography, focus, motion, and control-size variables without changing other pages.
- [x] 2.3 Implement consistent primary, secondary, quiet, danger, toggle/segmented, and icon-only control treatments with hover, focus, active, disabled, and loading states.
- [x] 2.4 Verify existing control hooks and ARIA state still work, then run `node --check static/js/dashboard_command_center.js` and the focused control-rendering tests.

## 3. Primary Operating Zone

- [x] 3.1 Modernize the discipline rail and session command deck while preserving state/mode persistence, Reset behavior, and mobile disclosure behavior.
- [x] 3.2 Modernize the Prepare hero, ticker switcher, readiness meter, priority action, Pressure dialog trigger, and permission-to-trade disclosure without changing destinations or state logic.
- [x] 3.3 Verify keyboard order, focus treatment, touch targets, dialog behavior, readiness updates, ticker links, live-sync controls, and command-checklist behavior.
- [x] 3.4 Capture and review desktop and mobile screenshots of the primary operating zone before continuing.

## 4. Planning and Market Monitoring

- [x] 4.1 Modernize the Today's Decision, trade gate, gamma structure, and Daily Brief surfaces while retaining every ID, data hook, refresh state, and Market Pulse/Calendar destination.
- [x] 4.2 Modernize the Market Tape header, timeframe picker, ticker search, live/delayed/missing indicators, chart lanes, and refresh control without changing polling or data semantics.
- [x] 4.3 Verify planning hydration, trade-gate state, gamma popovers, ticker switching, tape polling, stale/missing states, symbol search, and asynchronous loading behavior.
- [x] 4.4 Run focused planning and tape tests plus JavaScript syntax validation, then capture desktop and mobile screenshots of Plan and Monitor.

## 5. Supporting Operations

- [x] 5.1 Modernize Support/Health, reflection, status details, and optional disclosures while preserving wake-lock, routine, alignment, reset, and reflection persistence.
- [x] 5.2 Modernize broker/account actions, milestone controls, performance ranges, diagnostics links, pace controls, and projection disclosures without changing forms, endpoints, or stored settings.
- [x] 5.3 Preserve diagnostic-first broker refresh behavior and prove failed refreshes do not overwrite manual metrics.
- [x] 5.4 Modernize the Dashboard calendar container and preview while keeping lazy loading, rebinding, selected-date navigation, and dashboard-only day styling intact.
- [x] 5.5 Run focused health, behavior, broker, account, performance, pace, and calendar tests, including selected-day presence and adjacent-day absence.

## 6. Responsive, Accessibility, and Cleanup

- [x] 6.1 Verify the complete Dashboard at representative mobile, tablet, laptop, and wide-desktop widths; remove page overflow, clipping, overlap, detached controls, and excessive empty space.
- [x] 6.2 Verify keyboard traversal, accessible names, semantic pressed/expanded states, dialog focus behavior, visible focus, reduced motion, and minimum touch targets.
- [x] 6.3 Remove only Dashboard CSS and markup proven obsolete by targeted search, automated checks, and before/after comparison; do not refactor unrelated pages.
- [x] 6.4 Run `git diff --check`, JavaScript syntax checks, focused Dashboard pytest modules, and an authenticated `/dashboard` route smoke.
- [x] 6.5 With approval, rebuild through the normal container path and verify `/healthz`, then capture final full-page screenshots for functional and visual signoff.
- [x] 6.6 Compare the final control inventory with the baseline and document any deliberate markup-only changes; resolve every missing or behaviorally different control before completion.
