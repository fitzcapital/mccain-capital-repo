## Context

The application uses a shared Jinja shell with four overlapping navigation presentations: desktop
primary navigation, desktop overflow Menu, tablet drawer, and mobile menu. Those controls reach a
large set of specialized Flask-rendered pages backed by existing services, repositories, partials,
and vanilla JavaScript. The Trading Dashboard now demonstrates the desired modern command-center
quality, but other pages have independent CSS generations and cannot safely be restyled through one
unqualified global override.

The worktree already contains unrelated changes, and the completed Dashboard OpenSpec change remains
active. This change therefore treats the Dashboard as a reference and compatibility boundary, keeps
each page-family diff reviewable, and avoids runtime or personal data.

## Goals / Non-Goals

**Goals:**

- Give every internal menu destination a coherent modern visual language derived from the Trading
  Dashboard while preserving its specialized information architecture.
- Standardize hierarchy, controls, forms, tables, disclosures, dialogs, status states, loading and
  empty states, focus, spacing, and responsive behavior.
- Preserve every route, form action, endpoint, query parameter, filter, persisted setting, upload,
  export, lazy fragment, and destructive-action safeguard.
- Verify the receiving page and persisted result for workflows that navigate, submit, upload,
  mutate settings, or scope data.
- Deliver page families independently so regressions can be isolated and rolled back.

**Non-Goals:**

- Changing application business rules, financial calculations, broker behavior, authentication,
  data models, route names, or API contracts.
- Replacing Jinja or vanilla JavaScript with a frontend framework.
- Making every page structurally identical or forcing domain-specific data into generic cards.
- Redesigning external websites or removing destinations from any navigation presentation.
- Reworking the Trading Dashboard beyond shared-shell or compatibility adjustments.

## Decisions

### 1. Define coverage from navigation, not from a hand-picked page list

Build a route matrix from `base.html` covering every unique internal destination in desktop primary
navigation, desktop Menu, tablet drawer, and mobile menu. Record the owning template, page class,
forms, scripts, endpoints, persisted state, authentication requirement, destructive actions, and
focused tests for each route.

This is preferred over modernizing only the eight visible top tabs because the user's application
experience also includes strategy, life, business, operations, account, backup, and library pages.
External links are recorded for navigation parity but excluded from page redesign work.

### 2. Use shared tokens with page-family scopes

Extract the proven Dashboard visual values into shared application tokens for surfaces, borders,
spacing, radii, shadows, typography, focus, motion, and control sizes. Apply components beneath
explicit page or family classes, with page-local exceptions for tables, charts, calendars, editors,
upload workspaces, and administrative forms.

This is preferred over broad `.card`, `.btn`, and `.toolbar` overrides because the existing cascade
is large and global selectors could silently alter auth, mobile, or data-dense pages. It is also
preferred over copying the full Dashboard CSS into every page because duplication would drift.

### 3. Modernize in place and preserve behavioral markup contracts

Keep existing templates, DOM order where behavior depends on it, IDs, `data-*` hooks, form methods,
actions, names, hidden fields, ARIA state, and JavaScript entry points. Markup may be wrapped or
grouped only when a control inventory and focused tests prove equivalent behavior.

This minimizes regression risk compared with template rewrites and keeps server-rendered failure,
authorization, and data-trust states intact.

### 4. Roll out by risk-aware page family

Implement in the following phases:

1. Shared shell and low-risk read-oriented pages: Executive, The Plan, strategy/playbook pages.
2. Market pages: Market Pulse and Candle Opens.
3. Review pages: Trades, Journal, Life Journal, Life Alignment, Analytics, Calendar, and Planner.
4. Business and projections: Forward Pace, Payouts, and Income Tracker.
5. Operations and account pages: uploads, Ops Alerts, backups, profile, passkeys, setup,
   self-control, books, and internal admin/restore surfaces.
6. Cross-page responsive, accessibility, navigation-parity, and cleanup pass.

High-risk forms, uploads, auth, backups, restores, and financial pages come after the shared system
has stabilized on read-oriented pages. Each phase is independently testable and visually reviewable.

### 5. Treat data truth and destructive operations as visual invariants

Stale, delayed, missing, manual, failed, disabled, locked, unauthenticated, and diagnostic states
retain their current meaning and prominence. Financial projections continue to display their source
inputs, timing assumptions, account allocation, and fallback behavior. Manual broker values remain
primary unless an existing refresh succeeds. Destructive and security-sensitive controls retain
confirmation, CSRF, authorization, disabled, and danger treatments.

This avoids a cosmetic redesign making uncertain or risky actions appear safer than they are.

### 6. Verify behavior and visuals with a route matrix

For each route family, capture baseline and final screenshots at 390, 768, 1440, and 1920 pixel
widths using isolated or authenticated test state that does not mutate runtime data. Record control
counts, unnamed visible controls, document overflow, and route result. Run focused tests for links,
forms, filters, uploads, persistence, permissions, and receiving-page state, plus JavaScript syntax
checks for every touched controller.

This is preferred over screenshot-only approval because functional loss can look visually correct.

## Risks / Trade-offs

- [Risk] The scope is large enough for an unreviewable CSS diff. → Keep one page family per phase,
  use explicit scopes, and verify each phase before continuing.
- [Risk] Shared tokens still lose to late legacy theme rules. → Place a documented modernization
  layer after legacy rules and use the narrowest page-family selectors required.
- [Risk] Wrapping controls breaks event delegation or fragment rebinding. → Preserve hooks and DOM
  order, test lazy fragments and receiving pages, and avoid markup movement without coverage.
- [Risk] Simplification hides infrequent admin or backup actions. → Preserve navigation and use
  labeled disclosures rather than removal.
- [Risk] Dense tables and charts create mobile overflow. → Permit intentional component-level
  scrolling while forbidding document-level overflow.
- [Risk] Auth or destructive flows are accidentally exercised during visual capture. → Use isolated
  test storage and read-only route states; never submit destructive controls in visual automation.
- [Trade-off] Page-family scoping adds CSS selectors. → Accept limited explicit duplication to avoid
  destabilizing unrelated pages; consolidate only after visual and behavior parity is proven.

## Migration Plan

1. Freeze the route/control matrix and capture reproducible baselines without runtime-data writes.
2. Add shared tokens and shell-compatible components without changing page structures.
3. Apply and verify each page-family phase in the order above.
4. Run cross-navigation, responsive, accessibility, and focused behavior regression checks.
5. Rebuild through the normal container path with approval and capture final signoff screenshots.
6. Remove only selectors and wrappers proven obsolete by search, tests, and visual comparison.

Rollback is phase-level and presentation-only: revert the latest page-family template/CSS/JavaScript
diff while retaining earlier verified families. No database or settings migration is required.

## Open Questions

- Whether large legacy page styles should remain in `app.css` or move into family stylesheets will be
  decided after the route/cascade inventory; the default is explicit family layers with no loading
  order change.
- Exact density may vary by page family: trading and analytics pages can remain information-dense,
  while journal, profile, setup, and backup pages should favor calmer forms and clearer whitespace.
