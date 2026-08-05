## Gamma Ladder Baseline

### Existing workflow and compatibility surface

The current ladder is rendered inside `#marketPulseGammaLadderCard` after the entry checklist. Its
DOM order is header controls, metadata, summary, structure/top-level panels, loading/error/board,
tooltip, and a permanently visible seven-item legend.

Stable controls and hooks preserved by this change include:

- Symbol tabs, ticker search form and quick buttons.
- `tight`, `standard`, and `wide` strike-window tabs.
- `0DTE`, `1DTE`, `3DTE`, `7DTE`, and `All` DTE tabs.
- Refresh, loading, empty/error, board, rows, tooltip, row-detail, structure-summary, top-level, and
  legend hooks under `data-gamma-*`.
- `GET /api/gamma-ladder` parameters `symbol`, `window`, `dte`, and the force-refresh cache buster.
- Payload inputs including symbol, spot, regime, regime label, updated time/label, expiration,
  available DTE options, focused row counts, and row strike/call/put/net Gamma plus spot-nearest,
  flip, strongest, and classification inputs.
- `market-pulse:gamma-level-selected` and `market-pulse:gamma-updated` document events.

The controller already aborts superseded requests and rejects responses whose request ID is no
longer current. Existing errors preserve prior data when available and expose an explicit error
summary.

### Responsive capture

Captures and control inventories are stored under
`artifacts/gamma-ladder-depth-visualization/baseline/`.

| Viewport | HTTP | Overflow | Controls | Visible controls | Unnamed visible |
| --- | ---: | --- | ---: | ---: | ---: |
| 390 | 200 | No | 210 | 91 | 0 |
| 768 | 200 | No | 210 | 116 | 0 |
| 1440 | 200 | No | 210 | 118 | 0 |
| 1920 | 200 | No | 210 | 118 | 0 |

The isolated capture currently receives HTTP 503 from `/api/gamma-ladder`, so these screenshots
also establish the unavailable-state baseline without writing runtime market data.
