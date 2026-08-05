# Internal menu destination matrix

## Coverage rule

This matrix is deduplicated from desktop primary navigation, desktop Menu, tablet drawer, mobile
menu, and quick actions in `mccain_capital/templates/base.html`. Every internal GET destination is
in scope. External destinations retain their application-side link treatment and safety attributes
but their websites are out of scope. The Trading Dashboard is the reference implementation and is
not redesigned again.

The capture inventory in `artifacts/menu-page-modernization/<phase>/metrics.json` records each
route's final URL, body class, response status, controls, forms and field names, `data-*` hooks,
scripts, accessible names, and document-overflow state against isolated temporary storage.

## Route families

| Phase | Family | Internal destinations | Primary contracts | Risk |
| --- | --- | --- | --- | --- |
| Reference | Trading Dashboard | `/dashboard` | Existing command-center behavior, hooks, calendar rebinding | Reference only |
| 3 | Executive | `/executive` | Command summary, treasury status, Month Workspace, tabs | Medium, financial display |
| 3 | Planning | `/the-plan`, `/ops/trading-window` | Filters, state, action destinations | Medium |
| 3 | Strategy | `/strat`, `/playbook`, `/strategies` | Disclosures, strategy CRUD links, persisted interactions | Medium |
| 4 | Market | `/market-pulse`, `/market-pulse/feed`, `/candle-opens` | Tickers, polling, charts, event/date links, stale states | High, live async data |
| 5 | Trades | `/trades`, `/trades/new`, `/trades/open-positions`, `/trades/upload/statement` | Scope, filters, pagination, forms, uploads, reconciliation | High, financial mutation/upload |
| 5 | Journal | `/journal`, `/journal/life`, resolved `new_entry` route | Editors, drafts, linked trades, save/delete | High, personal data |
| 5 | Life | `/life-alignment` | Routines, reflections, stored daily state | Medium, personal data |
| 5 | Analytics | `/analytics`, `/analytics?tab=performance` | Tabs, ranges, charts, diagnostics, calculations | High, derived data |
| 5 | Calendar | `/calendar` | Month navigation, day preview, selected-day trade scope | High, receiving-page scope |
| 5 | Planner | `/calculator` | Inputs, validation, results, reset, calculations | High, financial calculation |
| 6 | Projection | `/forward-pace` | Inputs, timing assumptions, output, PDF | High, financial projection |
| 6 | Payouts | `/payouts` | Account scope, allocations, timing, calculations | High, financial projection |
| 6 | Goals | `/goals` | Targets, daily inputs, date ranges, persistence | High, financial/personal input |
| 7 | Operations | `/ops/alerts`, `/ops/notifications-test` | Diagnostics, acknowledgement, resolution, notifications | High, state mutation |
| 7 | Backup | `/ops/backups`, `/admin/restore` | CSRF, authorization, confirmation, file validation | Critical, destructive/recovery |
| 7 | Account | `/profile`, `/auth/passkeys`, `/setup`, `/login`, `/logout` | Profile settings, credentials, sessions, redirects | Critical, authentication |
| 7 | Guardrails | `/self-control` | Locks, confirmations, emergency behavior | Critical, system controls |
| 7 | Library | `/books` | Private files, open/download links, metadata | High, private files |

## Shared navigation and action contracts

- Desktop primary: Executive, Trading Dashboard, Market Pulse, Candle Opens, Trades, Journal,
  Analytics, and Planner.
- Desktop Menu adds The Plan, The Strat, Playbook, Strategies, Life Journal, Life Alignment,
  Forward Pace, Calendar, Live Upload, Ops Alerts, Profile, and Login Setup.
- Tablet/mobile navigation additionally exposes Trading Window, Self-Control, Payouts, Income
  Tracker, Auto Backups, Restore, Passkeys, and Books.
- Quick actions retain Add Trade, Add Journal, Upload Statement, and Open Positions.
- External links retain `target="_blank"` and `rel="noopener"`: TradingView, X.com, All Time High,
  Crystal Academy, Vanquish Trader, and Vanquish Dashboard.

## Persistence, failure, and security inventory

- Preserve all form methods, actions, names, hidden fields, CSRF injection, redirects, validation,
  filters, query strings, pagination, and receiving-page scope.
- Preserve local-storage behavior for shell theme/performance/navigation state and existing
  page-specific controllers; no persisted key is renamed by this change.
- Preserve live, stale, delayed, missing, manual, failed, disabled, locked, unauthenticated, and
  diagnostic states. Styling does not upgrade their trust level.
- Financial pages retain visible source inputs, timing assumptions, account scope/allocation, and
  fallback behavior. Broker failures never overwrite manual metrics.
- Backup, restore, auth, delete, lock, and sign-out controls retain authorization, CSRF,
  confirmation, disabled, and danger semantics.

## CSS cascade audit

- `static/css/app.css` contains several generations of global, theme, page, and breakpoint rules;
  broad rewrites of `.card`, `.btn`, or `.toolbar` would risk every route.
- Market Pulse also loads `static/css/market_pulse.css` after `app.css`, so its modernization needs
  explicit page-family authority or coordinated selectors rather than accidental cascade order.
- The shared modernization layer belongs at the end of `app.css`, scoped to non-Dashboard
  `.pageContent-*` containers and explicit body page classes. Specialized tables, charts,
  calendars, editors, upload workspaces, and admin forms retain page-local exceptions.
- Existing outcome, danger, stale, diagnostic, and disabled selectors must remain visually
  stronger than generic surfaces.
- Dense modules may scroll internally, but the document root must not overflow horizontally.
