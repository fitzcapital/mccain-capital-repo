## Why

The Market Pulse Gamma Ladder contains useful dealer-positioning data, but its dense control area,
large persistent legend, and mostly static rows make spot, flip, dominant nodes, and meaningful
changes slower to recognize during trading. This change will turn the ladder into an institutional
depth map whose hierarchy and restrained motion communicate what changed without distracting from
the chart or altering the underlying Gamma calculations.

## What Changes

- Recompose the ladder into a compact status and control header, a key-level summary, the existing
  strike board, and a sticky selected-level readout.
- Strengthen the center-axis depth visualization so strike, positive/negative positioning, net
  Gamma, spot, flip, support, resistance, acceleration, and dominant-node roles are readable at a
  glance and are not communicated by color alone.
- Add event-driven animations for refreshed bar values, spot movement, regime transitions,
  dominant-node changes, row selection, and detail expansion.
- Keep ambient motion restrained: no continuously breathing data bars or looping decorative glow;
  only loading indicators may loop indefinitely.
- Preserve ticker search and shortcuts, strike-window and DTE selectors, refresh, loading, empty,
  stale, error, tooltip, row-detail, chart-selection, keyboard, and mobile behaviors.
- Move the full legend into an accessible "How to read this" disclosure while retaining concise
  labels on the board itself.
- Add responsive behavior that keeps spot and the selected strike visible while the ladder scrolls,
  with a reduced three-column row on narrow screens.
- Make the page-level Playbook header non-sticky by default and add a persistent user control that
  can pin or unpin it without changing the ladder or execution workflow.
- Make the depth board visibly distinct through explicit above-spot, at-spot, and below-spot
  sections, stronger center-axis framing, and structural-node shapes for spot, flip, and dominant
  strikes.
- Replace the repeated two-pill signal treatment on every strike with a quieter hierarchy: minor
  roles become inline supporting text, while badges are reserved for actionable structural levels
  such as magnet, primary, flip, and current.
- Rebuild the oversized control header as a compact command bar: keep symbol shortcuts and search
  visible, combine Window and DTE into one current-settings trigger and popover, and retain one
  separate refresh action with complete control parity.
- Add reduced-motion, rendering-performance, data-race, control-parity, and responsive visual
  contracts.
- Do not change quote/options providers, Gamma formulas, strike selection, regime classification,
  trade recommendations, API payloads, persistence, or other application pages.

Acceptance requires zero lost controls or destinations, no fabricated values, no document-level
overflow at 390/768/1440/1920 pixels, visible semantic state labels, safe stale/error behavior, and
focused tests proving refresh diffs and animations cannot overwrite newer payloads.

## Capabilities

### New Capabilities

- `gamma-ladder-depth-visualization`: Defines the institutional Gamma depth-map hierarchy,
  event-driven motion, interaction states, responsive behavior, accessibility, and preservation of
  existing ladder data and controls.

### Modified Capabilities

None.

## Impact

- Primary files: `mccain_capital/templates/core/market_pulse.html`,
  `static/css/market_pulse.css`, and `static/js/gamma_ladder.js`.
- Coordination boundary: the existing `market-pulse:gamma-level-selected` chart event remains
  compatible; no provider or API contract changes are planned.
- Tests and visual tooling will gain Gamma Ladder-specific DOM, state-transition, control-parity,
  reduced-motion, and responsive checks.
- No new dependencies, schema changes, background jobs, financial projections, or runtime-data
  writes are required.
