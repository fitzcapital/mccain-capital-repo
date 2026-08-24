## Why

Market Pulse and Dashboard expose the right trading information, but too many equally weighted
cards, controls, borders, and repeated states make the primary decision harder to find. The two
pages need one coherent command-surface system that makes decision, reason, next action, and data
trust immediately legible while preserving the existing Galaxy identity and financial logic.

## What Changes

- Introduce shared visual hierarchy, spacing, surface, typography, and semantic-state rules for
  Market Pulse and Dashboard.
- Consolidate the Market Pulse execution read, active level, next evidence, invalidation, session,
  Gamma health, and strategy state into one connected command surface.
- Move the Market Pulse execution chart into the first desktop viewport by collapsing diagnostics
  and moving secondary utilities behind progressive disclosure.
- Simplify the Market Pulse header around ticker, spot, session, regime, freshness, and refresh;
  move non-primary actions into a compact utility menu.
- Make Dashboard lead with one authoritative Today decision, its reason, and one next action.
- Consolidate Dashboard session/state controls and progressively disclose secondary operations,
  broker metrics, history, diagnostics, and review tools.
- Preserve current trading rules, market-data sources, refresh behavior, Gamma calculations,
  chart interactions, account values, and user-entered discipline state.
- Verify desktop hierarchy at 1280, 1440, and 1920 CSS pixels, then verify supported compact and
  mobile layouts without horizontal overflow or inaccessible controls.
- Non-goals: changing financial calculations, scenario ranking, execution permission, market-data
  providers, chart candle logic, Gamma levels, account persistence, navigation destinations, or
  the Galaxy background identity.

## Capabilities

### New Capabilities

- `unified-market-command-surfaces`: Shared hierarchy, density, semantic emphasis, progressive
  disclosure, and responsive behavior across Market Pulse and Dashboard.

### Modified Capabilities

- `dashboard-decision-workflow`: Make one authoritative Today decision and next action dominate the
  Dashboard while preserving existing decision inputs and controls.
- `dashboard-primary-market-canvas`: Reposition market context as supporting evidence beneath the
  primary decision and keep its charts and controls responsive.

## Impact

- Affects `mccain_capital/templates/core/market_pulse.html`,
  `mccain_capital/templates/dashboard.html`, shared/base templates where necessary, and scoped CSS
  and JavaScript under `static/`.
- Adds no dependency, endpoint, persistence, migration, market-data source, or financial assumption.
- Acceptance requires a materially shorter path to the primary decision, chart visibility in the
  first Market Pulse desktop viewport, fewer simultaneously prominent controls, preserved in-place
  refresh/chart state, responsive containment, focused contract tests, strict OpenSpec validation,
  and a healthy rebuilt local app.
