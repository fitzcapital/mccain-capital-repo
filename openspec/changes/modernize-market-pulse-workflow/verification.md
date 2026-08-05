## Verification

### Workflow and behavior

- Status, Gamma/chart cockpit, structure, Trade Decision, entry checklist, Gamma Ladder,
  tape, and collapsed Market Feed render in the specified order.
- Main Flip, Local Flip, Call Wall, Put Wall, and supported advanced-ladder strikes emit
  symbol-scoped, timestamped selection events. Invalid, stale, and mismatched-symbol events are
  ignored.
- Pinned selection survives chart timeframe changes for the page session. Hover and keyboard focus
  preview another level and restore the prior pin on exit.
- Existing ticker search, refresh, chart controls, Gamma controls, Candle Opens link, tape, feed,
  modal, and diagnostics hooks remain present.

### Automated checks

- JavaScript syntax: `node --check` passed for `market_pulse_gamma_workflow.js`,
  `spx_hero_chart.js`, and `gamma_ladder.js`.
- Focused workflow, ticker, snapshot, runtime, and Gamma tests: 87 passed; one unrelated existing
  expectation failed because invalid Gamma symbols currently normalize to `SPY` while the test
  expects `SPX`.
- Focused Market Pulse route/receiving-surface tests: 15 passed.
- Focused Gamma context, hero chart, execution model, and Tradier tests: 42 passed; one unrelated
  existing expectation failed because the default hero symbol is currently `SPY` while the test
  expects `SPX`.
- `git diff --check` passed.

### Responsive and control parity

Final captures are in `artifacts/market-pulse-gamma-workflow/final-cascade/`.

| Viewport | HTTP | Overflow | Unnamed visible controls | Controls |
| --- | ---: | --- | ---: | ---: |
| 390 | 200 | No | 0 | 212 |
| 768 | 200 | No | 0 | 212 |
| 1440 | 200 | No | 0 | 212 |
| 1920 | 200 | No | 0 | 212 |

Baseline inventory contained 208 controls. No baseline control is missing or renamed. The four
intentional additions are the Main Flip, Local Flip, Call Wall, and Put Wall Level Deck buttons.

### Container verification

- `./scripts/run_podman_app.sh` completed successfully and started image
  `localhost/mccain-capital-app:latest` on port 5001.
- `/healthz` returned HTTP 200 with `status: ok`.
- `/market-pulse` and `/candle-opens` returned the expected unauthenticated HTTP 302 redirects to
  `/login` with their original `next` destinations.
- `/static/js/market_pulse_gamma_workflow.js` returned HTTP 200 from the rebuilt container.
