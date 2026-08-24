## Why

Market Pulse leaves execution-monitoring space unused on desktop and can stop reconciling after a
tab is backgrounded or restored from the browser back-forward cache. The page needs to remain
compact, recover without a full reload, and avoid wasting market-data requests while unattended.

## What Changes

- Stretch the alternative, dormant, live monitor, and replay region across the full execution
  cockpit width while retaining compact collapsed summaries.
- Suspend canonical client polling while the page is hidden and immediately run one single-flight
  reconciliation when the page becomes visible, focused, online, or is restored from page cache.
- Re-arm the visible countdown and coordinator after page restoration so an unattended tab cannot
  remain permanently disconnected.
- Preserve in-place updates, chart viewport, drawings, timeframe, and current last-valid state.
- Keep the visible steady-state cadence at 15 seconds and avoid hidden-tab background polling.

Non-goals: changing market-data providers, server promotion rules, strategy ranking, or adding
full-page automatic reloads.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Define hidden-tab suspension and immediate, single-flight
  recovery after visibility, connectivity, focus, or page-cache restoration.
- `market-pulse-scenario-ranking`: Require the compact scenario-monitoring region to use the full
  execution cockpit width without obscuring the chart or ladder.

## Impact

- Affects `mccain_capital/templates/core/market_pulse.html`, Market Pulse CSS, and focused contract
  tests.
- Uses existing canonical context API and refresh coordinator; no API, dependency, provider, or
  financial-calculation changes.
- Acceptance: monitoring cards fill the cockpit width on desktop; hidden tabs issue no scheduled
  canonical polls; returning after more than one cadence triggers one immediate validation; BFCache
  restoration restarts countdown/polling; visible polling remains bounded at 15 seconds.
