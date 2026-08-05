# Market Pulse Baseline

## Existing flow

1. Playbook command header with ticker search, snapshot, repeated level strip, regime,
   actionability, refresh, and Candle Opens.
2. Session/data-state strip.
3. Execution hero containing repeated market state, chart, and Trade Decision.
4. Four-step entry checklist.
5. Full Gamma Ladder.
6. Structure-location and distance cards.
7. Core tape.
8. Feed/news and dialogs.

The primary usability problem is the separation between the chart in step 3 and Gamma in step 5,
plus repeated spot, regime, bias, and decision values in steps 1 and 3.

## Stable behavior inventory

- Template: `mccain_capital/templates/core/market_pulse.html`.
- Styles: `static/css/market_pulse.css`, `static/css/app.css`, and the scoped shared page layer.
- Controllers: `spx_hero_chart.js`, `market_pulse_gamma_context.js`,
  `symbol_search_control.js`, `gamma_ladder.js`, and `app_shell.js`.
- Forms: two GET symbol-search forms plus the authenticated global backup POST form.
- Primary query parameters: `ticker`, `feed`, and Gamma `symbol` search state.
- Primary APIs: hero bars, levels, quote, stream session, Gamma Ladder, and Market Pulse tape.
- Modal destinations: replay, Strat levels, bias, levels/legend, and health/diagnostics.
- External destinations retain `target="_blank"` and `rel="noopener"` through the app shell.
- High-risk hooks: every `marketPulse*`/`spxExecutionHero*` ID, `data-hero-chart-*`,
  `data-gamma-*`, symbol-search hooks, modal hooks, and tape hooks.

## Isolated responsive capture

Captured from temporary application storage under
`artifacts/market-pulse-gamma-workflow/baseline/`.

| Viewport | HTTP | Controls | Visible | Unnamed visible | Overflow |
| --- | ---: | ---: | ---: | ---: | --- |
| 390 | 200 | 208 | 87 | 0 | No |
| 768 | 200 | 208 | 112 | 0 | No |
| 1440 | 200 | 208 | 114 | 0 | No |
| 1920 | 200 | 208 | 114 | 0 | No |

The isolated Gamma API returned its explicit unavailable state during capture. That state is part
of the required verification surface and was not replaced with fabricated data.
