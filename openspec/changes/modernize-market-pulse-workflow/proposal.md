## Why

Market Pulse contains the Gamma Ladder, execution chart, structure read, decision state, tape,
and news, but its current vertical order separates the gamma levels from the chart used to
interpret them and repeats decision context across several panels. The page should follow the
user's actual workflow: gamma establishes the map, the chart confirms price behavior, and one
decision panel translates both into an actionable or wait state.

## What Changes

- Replace the repeated top summary with a compact, sticky market-status bar containing ticker,
  spot, session/data freshness, timestamp, symbol selection, and refresh controls.
- Establish an above-the-fold Gamma and Chart Cockpit with the existing execution chart as the
  primary surface and a compact Gamma Level Deck alongside it on desktop.
- Surface Main Flip, Local Flip, Call Wall, Put Wall, spot, nearest important level, distance,
  gamma classification, and data validity in the Level Deck without changing their source data.
- Synchronize gamma-level selection with chart overlays so selecting or hovering an available
  level identifies the same level in both representations; preserve the existing chart drawing,
  marker, level, day-level, timeframe, symbol, DTE, window, and refresh controls.
- Place a compact structure-location map immediately after the cockpit, followed by one
  authoritative Trade Decision panel and then the existing four-step entry checklist.
- Move tape, calendar/news context, diagnostics, replay, and advanced gamma explanation below the
  primary workflow in accessible disclosure sections without removing them.
- Provide a responsive flow that stacks status, Level Deck, chart, decision, and checklist in that
  order on narrow screens, with secondary sections collapsed by default where appropriate.
- Preserve explicit loading, unavailable, delayed, stale, degraded, and failure states. Color
  remains supplementary to visible text labels.
- Preserve all current market-data semantics, calculations, APIs, ticker defaults, external-link
  safety, and receiving-page behavior.

Acceptance criteria:

- Gamma levels and the execution chart are visible together at 1440px without horizontal page
  overflow and appear before the Trade Decision and trigger checklist.
- Every currently visible control and destination remains available with an accessible name.
- The same spot and level values are shown consistently across the status bar, Level Deck, chart,
  structure map, and decision context.
- Selecting a supported gamma level makes its corresponding chart overlay visually identifiable;
  unavailable levels fail safely without implying a valid price.
- Existing Market Pulse focused tests, JavaScript syntax checks, route/control inventories, and
  390/768/1440/1920 responsive captures pass without weakening business assertions.

Non-goals:

- No changes to quote, gamma, options, news, or chart data providers.
- No changes to gamma calculations, execution grading, trigger rules, or trading recommendations.
- No new trading automation, order entry, broker mutation, or financial persistence.
- No redesign of Dashboard, Candle Opens, Trades, or other menu destinations.

## Capabilities

### New Capabilities

- `market-pulse-gamma-workflow`: Defines the Gamma-first Market Pulse hierarchy, synchronized
  chart/level interaction, consolidated decision flow, preserved controls, state handling, and
  responsive behavior.

### Modified Capabilities

None.

## Impact

- Primary template: `mccain_capital/templates/core/market_pulse.html`.
- Primary styling: `static/css/market_pulse.css` and the scoped shared modernization layer where
  necessary.
- Primary interactions: `static/js/gamma_ladder.js`, `static/js/spx_hero_chart.js`, and narrowly
  scoped Market Pulse coordination code.
- Existing Market Pulse APIs and view-model/service outputs remain the data contract.
- Focused Market Pulse tests and isolated responsive/control-inventory capture tooling will be
  extended. No dependency or database migration is expected.
