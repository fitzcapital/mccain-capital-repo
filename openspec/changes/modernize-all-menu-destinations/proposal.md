## Why

The Trading Dashboard now has a clearer, more modern command-center visual system, but the other
internal menu destinations still use several generations of page layouts, cards, buttons, forms,
and responsive behavior. Extending the same quality and interaction clarity across the application
will make navigation feel cohesive without removing the specialized workflows on each page.

## What Changes

- Establish a shared application-page visual system derived from the Trading Dashboard for page
  headers, section hierarchy, cards, buttons, segmented controls, forms, tables, disclosures,
  dialogs, status states, empty states, focus treatment, spacing, and responsive layouts.
- Modernize every internal destination reachable from the desktop primary navigation, overflow
  Menu, tablet drawer, and mobile menu, including executive, market, planning, strategy, review,
  journal, analytics, business, operations, account, backup, authentication, and library surfaces.
- Preserve the Trading Dashboard as the reference implementation; apply only shared compatibility
  fixes to it rather than redesigning it again.
- Inventory every page family before editing and preserve routes, links, forms, endpoints, IDs,
  `data-*` hooks, query parameters, filters, pagination, persisted settings, lazy fragments,
  uploads, exports, and destructive-action safeguards.
- Deliver the modernization in page-family phases, with focused tests and before/after captures at
  mobile, tablet, laptop, and wide-desktop widths for each phase.
- Keep stale, missing, delayed, failed, disabled, diagnostic, and manually entered data states
  truthful; presentation must not imply that unavailable information is live or successful.

### Non-goals

- No changes to trading, treasury, projection, P&L, analytics, broker, journal, or authentication
  business rules.
- No endpoint removals, route renames, data migrations, persisted-key changes, or runtime-data edits.
- No frontend framework, CSS framework, or third-party component library.
- No redesign of external destinations such as TradingView, Discord, X.com, or Vanquish Trader.
- No removal or consolidation of menu destinations solely to simplify the visual design.
- No unrelated refactor of service, repository, or handler architecture.

### Acceptance criteria

- Every internal desktop and mobile menu destination remains reachable and functionally equivalent.
- Every inventoried page control retains its destination, submission, event binding, query state,
  or persistence behavior, including authenticated and destructive operations.
- Existing focused route, form, upload, filter, pagination, API, persistence, and receiving-page
  tests pass without weakening assertions.
- Shared primary, secondary, quiet, danger, segmented, and icon-only controls have consistent hover,
  focus, active, disabled, and loading states while preserving page-specific semantics.
- Representative pages in every family have no document-level horizontal overflow at 390, 768,
  1440, and 1920 pixel widths, and every visible control has an accessible name.
- Financial and broker pages retain source labels, timing assumptions, account allocation, fallback
  behavior, and manual-value protection.
- Before/after captures and control inventories show no missing page families, sections, or actions.

## Capabilities

### New Capabilities

- `application-page-experience`: Defines the shared modern visual system, responsive behavior,
  accessibility standards, phased page-family coverage, and functional-parity contract for all
  internal menu destinations outside the already-modernized Trading Dashboard.

### Modified Capabilities

None. No synced OpenSpec capabilities currently define these application pages.

## Impact

- Primary presentation surfaces: `mccain_capital/templates/base.html`, internal page templates and
  partials under `mccain_capital/templates/`, and scoped rules in `static/css/app.css`.
- Interaction review: existing page JavaScript under `static/js/`; changes are limited to markup
  compatibility or accessibility-state synchronization required by the redesign.
- Page families: Executive; Market Pulse and Candle Opens; The Plan, The Strat, Playbook, and
  Strategies; Trades and uploads; Journal, Life Journal, and Life Alignment; Analytics, Calendar,
  Planner, and Forward Pace; Payouts and Income Tracker; Ops, backups, profile, passkeys, setup,
  self-control, books, and related internal admin destinations.
- APIs and data: no intentional changes to endpoint contracts, market-data sources, broker data,
  financial calculations, stored settings, uploaded files, or personal/runtime data.
- Verification: focused pytest modules, JavaScript syntax checks, authenticated route/form smoke,
  control inventories, and responsive visual captures for each page family.
