## Why

The Trading Dashboard and Market Pulse already have a strong visual system, but their
primary decisions compete with secondary context, repeated status signals, and long
mobile layouts. This change will make both pages A-grade decision surfaces: immediately
legible, progressively disclosed, responsive, and free of shared-shell runtime errors.

## What Changes

- Reorder and consolidate the Trading Dashboard so trading state, permission, risk,
  market context, and the next action dominate the initial viewport.
- Reduce repeated dashboard metrics and place secondary diagnostics, history, and review
  material behind clear disclosure controls without removing access to them.
- Reorganize Market Pulse so regime, spot, decisive gamma levels, trigger, invalidation,
  and data freshness read as one execution narrative.
- Visually distinguish market-data health from trading permission and make live, stale,
  unavailable, and locked states unambiguous.
- Reduce mobile page length and guarantee that fixed navigation does not cover actionable
  content at a 390-pixel viewport.
- Fix the shared notification initialization error caused by formatting a last-read value
  before the Eastern-time formatter is initialized.
- Add focused contract and interaction coverage for hierarchy, disclosure, responsive
  clearance, state semantics, and shared-shell initialization.
- Preserve the existing dark command-center identity, financial calculations, data-source
  selection, account allocation, and fallback behavior.

### Acceptance Criteria

- A user can identify the current state and primary next action from the first desktop
  viewport of either page without expanding secondary content.
- Dashboard presents trading state, gate, account risk, market context, and next action
  before historical or diagnostic content.
- Market Pulse presents regime, spot, decisive levels, trigger, invalidation, freshness,
  and permission as distinct but connected signals.
- Secondary sections remain reachable and keyboard-operable through explicit disclosure.
- At 390 pixels, neither page has horizontal overflow or fixed-navigation overlap, and
  collapsed secondary content materially shortens the default page.
- Loading, live, stale, unavailable, and locked states remain truthful and visually
  distinct.
- The shared shell loads without the `formatEt` initialization exception.
- Focused Python tests, JavaScript syntax checks, OpenSpec validation, and authenticated
  desktop/mobile browser checks pass.

### Non-Goals

- Changing P&L, risk, consistency, forward-pace, gamma, trigger, or trade-gate formulas.
- Replacing manual broker metrics or changing refresh/fallback precedence.
- Adding new market-data providers, modifying feed timing, or changing financial
  assumptions.
- Redesigning unrelated application pages or replacing the current visual identity.
- Removing detailed information that is still useful after intentional disclosure.

## Capabilities

### New Capabilities

- `dashboard-decision-clarity`: Defines the dashboard hierarchy, progressive disclosure,
  responsive behavior, and preservation of existing financial and trading semantics.
- `market-pulse-decision-clarity`: Defines the Market Pulse execution narrative, signal
  separation, freshness states, disclosures, and responsive behavior.
- `shared-shell-runtime-reliability`: Defines error-free shared-shell initialization and
  mobile fixed-navigation clearance required by both target pages.

### Modified Capabilities

None. The repository currently has no synchronized main specifications for these
behaviors; this change introduces the living contracts without rewriting completed
historical change artifacts.

## Impact

- Templates: `mccain_capital/templates/dashboard.html`,
  `mccain_capital/templates/core/market_pulse.html`, shared fragments, and
  `mccain_capital/templates/base.html`.
- Styles and interactions: targeted dashboard, Market Pulse, adaptive-shell, and shared
  JavaScript/CSS assets under `static/`.
- Tests: focused page-contract and workflow tests under `tests/`.
- Data and APIs: existing payloads and routes remain authoritative; no schema migration,
  provider change, or runtime-data rewrite is expected.
- Financial assumptions: all displayed values retain their current sources, timestamps,
  account scope, calculation rules, and failure fallbacks. Manual broker metrics remain
  primary unless an existing automated refresh succeeds.
