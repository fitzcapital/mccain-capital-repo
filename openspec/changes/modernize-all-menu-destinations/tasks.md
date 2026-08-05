## 1. Navigation Inventory and Baselines

- [x] 1.1 Build a deduplicated route matrix from desktop primary navigation, desktop Menu, tablet drawer, and mobile menu, recording every internal and external destination.
- [x] 1.2 Map each internal route to its template, body/page classes, styles, JavaScript controllers, forms, endpoints, query parameters, persisted keys, authentication rules, and focused tests.
- [x] 1.3 Inventory every visible link, button, form, field, disclosure, dialog, table action, upload, export, filter, pagination control, lazy fragment, loading/error state, and destructive safeguard for each page family.
- [x] 1.4 Classify routes by implementation phase and risk, explicitly identifying financial, broker, upload, authentication, backup, restore, and destructive workflows.
- [x] 1.5 Extend the isolated visual-capture tooling to record route result, control count, section count, unnamed visible controls, and document overflow at 390, 768, 1440, and 1920 pixel widths without runtime-data writes.
- [x] 1.6 Capture representative before-state screenshots for every page family and record any baseline route or test failures without weakening assertions.

## 2. Shared Application Visual System

- [x] 2.1 Audit global, theme, page, and responsive cascade layers to identify selectors that conflict with a shared modernization layer.
- [x] 2.2 Define shared application tokens for surfaces, borders, spacing, radii, shadows, typography, focus, motion, and control sizes derived from the Trading Dashboard.
- [x] 2.3 Implement page-family-scoped primary, secondary, quiet, danger, segmented, toggle, icon-only, disabled, loading, success, and failure control treatments.
- [x] 2.4 Implement page-family-scoped hierarchy patterns for page headers, command bars, cards, section summaries, forms, tables, disclosures, dialogs, empty states, diagnostics, and status messaging.
- [x] 2.5 Preserve and visually align desktop primary navigation, desktop Menu, tablet drawer, and mobile menu active, focus, expanded, locked, external-link, and authentication states.
- [x] 2.6 Add contract tests for shared navigation destinations, active states, accessible names, and representative page-family anchors before changing individual pages.

## 3. Executive, Planning, and Strategy Pages

- [x] 3.1 Modernize Executive while preserving command-summary data, treasury status, Month Workspace placement, and all links and controls.
- [x] 3.2 Modernize The Plan and Trading Window while preserving filters, state, action hierarchy, and receiving-page behavior.
- [x] 3.3 Modernize The Strat, Playbook, and Strategies while preserving strategy content, navigation, disclosures, and persisted interactions.
- [x] 3.4 Verify Executive, planning, and strategy routes, controls, filters, state, keyboard order, and responsive captures before continuing.

## 4. Market Pages

- [x] 4.1 Modernize Market Pulse headers, ticker switcher, spot and level metrics, state panels, charts, feeds, refresh controls, and stale/missing indicators without changing market-data semantics.
- [x] 4.2 Modernize Candle Opens navigation, market calendar, event cards, filters, chart actions, and news links without changing date or symbol scope.
- [x] 4.3 Verify ticker defaults and switching, live and delayed data, refresh behavior, chart controls, event/date navigation, notifications, and external-link safety.
- [x] 4.4 Run focused Market Pulse and Candle Opens tests, JavaScript syntax checks, and responsive visual captures before continuing.

## 5. Trades, Journal, Analytics, Calendar, and Planner

- [x] 5.1 Modernize Trades list/detail surfaces, scope and date filters, pagination, row actions, open positions, reconciliation, and statement/upload workspaces without changing data selection or form behavior.
- [x] 5.2 Modernize Journal and Life Journal indexes, editors, filters, linked trades, drafts, reflections, and save/delete actions while preserving content and persistence.
- [x] 5.3 Modernize Life Alignment while preserving routines, goals, reflections, status, and stored settings.
- [x] 5.4 Modernize Analytics pages, tabs, ranges, charts, diagnostics, behavior views, and session replay without changing calculations or filters.
- [x] 5.5 Modernize Calendar navigation, day states, previews, and trade links while preserving selected-day presence and adjacent-day absence.
- [x] 5.6 Modernize Planner/Calculator inputs, results, validation, reset behavior, and numerical assumptions without changing calculations.
- [x] 5.7 Run focused route, upload, form, persistence, filter, pagination, receiving-page, calculation, calendar-scope, JavaScript, and responsive visual checks for the complete review family.

## 6. Business and Projection Pages

- [x] 6.1 Modernize Forward Pace controls, projection summaries, timelines, settings, and disclosures while preserving source inputs, trading-day timing assumptions, account allocation, and fallback behavior.
- [x] 6.2 Modernize Payouts while preserving account scope, treasury inputs, allocations, timing assumptions, status, and existing calculations.
- [x] 6.3 Modernize Income Tracker/Goals while preserving targets, progress, forms, date ranges, persistence, and calculations.
- [x] 6.4 Verify financial labels, manual inputs, account scope, persisted settings, calculation outputs, receiving pages, keyboard behavior, and responsive captures before continuing.

## 7. Operations, Account, Authentication, and Library Pages

- [x] 7.1 Modernize Live Upload and Ops Alerts while preserving upload validation, sync diagnostics, account selection, alerts, and failure states.
- [x] 7.2 Modernize Auto Backups, backup download, and restore surfaces while preserving CSRF, authorization, confirmations, disabled states, and file validation.
- [x] 7.3 Modernize Profile, Passkeys, Login Setup, and related authentication surfaces while preserving credentials, sessions, profile settings, redirects, and security messaging.
- [x] 7.4 Modernize Self-Control while preserving active blocks, lock states, confirmations, emergency behavior, and diagnostic visibility.
- [x] 7.5 Modernize Books and remaining internal admin destinations while preserving uploads, downloads, metadata, navigation, and authorization.
- [x] 7.6 Verify broker refresh failure does not overwrite manual metrics and successful existing refresh behavior remains unchanged.
- [x] 7.7 Run focused upload, sync, backup, restore, profile, passkey, auth, self-control, books, authorization, CSRF, JavaScript, and responsive visual checks without executing destructive production actions.

## 8. Cross-Page Responsive, Accessibility, and Completion

- [x] 8.1 Verify every internal destination is reachable from each navigation presentation where it is intended to appear, with correct active and expanded states.
- [x] 8.2 Verify representative pages from every family at 390, 768, 1440, and 1920 pixel widths; resolve document overflow, clipping, overlap, detached controls, and excessive empty space.
- [x] 8.3 Verify keyboard traversal, visible focus, semantic pressed/expanded state, dialog behavior, accessible names, minimum touch targets, and reduced motion across all page families.
- [x] 8.4 Compare final control inventories with baselines and resolve every missing or behaviorally different control, documenting only time-sensitive or explicitly approved differences.
- [x] 8.5 Remove only styles and wrappers proven obsolete by targeted search, focused tests, and before/after visual comparison; keep unrelated pages and dirty-worktree edits untouched.
- [x] 8.6 Run `git diff --check`, syntax checks for every touched JavaScript file, all focused page-family pytest slices, and authenticated route/form smoke checks.
- [x] 8.7 With approval, rebuild through the normal container path, verify `/healthz`, and capture final representative screenshots for every page family.
- [x] 8.8 Document baseline failures separately, verify no assertion was weakened to conceal them, and complete the final route, control, accessibility, and functional-parity signoff.
