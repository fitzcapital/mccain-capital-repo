# Verification

## Behavioral and numeric inventory

- Preserved the existing Gamma Ladder API endpoint and the `symbol`, `window`, `dte`, and force
  refresh query parameters.
- Preserved every existing control and stable `data-gamma-*` integration hook. The only new static
  control is the accessible `How to read this depth map` disclosure summary; data-bearing states
  add one row button per accepted strike as before.
- Preserved the existing row values, financial classifications, tooltip/detail values, and
  `market-pulse:gamma-level-selected` event fields. The presentation snapshot reads accepted values
  but does not modify or recalculate them.
- Added presentation-only hooks for the status area, key levels, depth board, selected inspector,
  and disclosure. The helper script loads before the existing controller.

## Automated checks

- `tests/test_gamma_ladder_visualization_contract.py`: 6 passed.
- `tests/test_market_pulse_gamma_workflow_contract.py`: 9 passed.
- Focused Gamma route/API/controller checks: 16 passed with 177 deselected.
- Gamma service checks excluding the pre-existing invalid-symbol default assertion: 38 passed.
- Focused Gamma API and template checks in `tests/test_app_core.py`: 4 passed.
- `node --check` passed for `gamma_ladder.js` and `gamma_ladder_presentation.js`.
- `node --check` also passed for `market_pulse_gamma_workflow.js`, including the Playbook pin
  preference controller.

## Visual matrix

The deterministic capture harness intercepted only `/api/gamma-ladder` and did not read or write
runtime market data. It captured complete, partial, stale, empty, and error states at 390, 768,
1440, and 1920 pixels under `artifacts/gamma-ladder-depth-visualization/final/`.

- 20 of 20 captures had no document-level horizontal overflow.
- Complete, partial, and stale states rendered the expected row counts and one selected row with a
  synchronized inspector.
- Empty and error states rendered zero rows, no selected row, readable unavailable values, and a
  reachable refresh/disclosure surface.
- Baseline captures also had no overflow and no unnamed visible controls at all four widths.

## Playbook pinning and segmented ladder revision

- The Playbook status header now uses normal document flow by default instead of sticking during
  page scroll.
- The accessible `Pin header` toggle reports `aria-pressed`, switches the header to sticky only
  when enabled, and saves the preference in local storage.
- The browser capture verified all three states: default was unpinned with `position: relative`,
  enabled was pinned with `position: sticky`, and a full reload restored the pinned state.
- The depth board now labels above-spot, at-spot, and below-spot sections and uses distinct geometric
  hooks for spot, Gamma flip, and dominant nodes. No financial values, roles, or calculations were
  changed.
- The revised deterministic visual matrix retained zero document-level horizontal overflow across
  all 20 state-and-width captures. A dedicated default-header screenshot is stored at
  `artifacts/gamma-ladder-depth-visualization/final/playbook-header--default.png`.

## Quieter heat-map signal hierarchy

- Removed the repeated two-pill `MINOR` and role treatment from ordinary rows and removed the
  dedicated Signal column.
- Minor support, resistance, acceleration, and level roles now render as quiet inline context near
  the strike. Magnet, Current, Flip, and Primary structure retain at most one compact badge.
- The row remains the only interactive control. Selected details now include importance, role, and
  status together, so no information was discarded.
- The desktop and narrow layouts use three columns—strike/context, dealer-positioning depth, and
  Net GEX—giving the heat-map lane more room.
- The final 20-capture matrix reported no horizontal overflow at 390, 768, 1440, or 1920 pixels.

## Compact Gamma Ladder command bar

- Replaced the three-row control stack with a two-zone command bar that keeps SPX, SPY, QQQ,
  search, current settings, and refresh visible.
- Tight, Standard, Wide, 0DTE, 1DTE, 3DTE, 7DTE, and All remain available through one settings
  popover using the original controller hooks and pressed or disabled states.
- The closed command bar exposes six visible buttons instead of thirteen. Its trigger correctly
  reported `Standard · 0DTE`, `aria-expanded=false`, and a hidden popover.
- The open state exposed three Window and five DTE choices. Escape restored the closed state and
  keyboard focus to the settings trigger.
- The full 20-capture state matrix retained zero document-level horizontal overflow at 390, 768,
  1440, and 1920 pixels. Closed and open command-bar references are stored as
  `gamma-command-bar--closed.png` and `gamma-command-bar--open.png` in the final artifact folder.
- The normal Podman workflow rebuilt `localhost/mccain-capital-app:latest`, replaced the running
  container, and returned `status: ok` from `/healthz`. Container inspection confirmed the new
  template, controller, and command-bar stylesheet are present in the running image.

## Unrelated baseline failure

`tests/test_gamma_map_service.py::test_normalize_gamma_ladder_symbol_accepts_search_symbols` expects
an invalid symbol to normalize to `SPX`, while the current application implementation and API tests
normalize it to `SPY`. This change does not touch symbol normalization or weaken that assertion.

## Container rebuild

The preserved `podman-machine-applehv` runtime recovered and the approved
`./scripts/run_podman_app.sh` workflow successfully built
`localhost/mccain-capital-app:latest`, replaced the existing container, and started Gunicorn with
two healthy workers on port 5001. `/healthz` returned `status: ok`; the rebuilt container includes
the simplified Gamma Ladder renderer, structural-badge styles, and three-column template. Container
logs showed a clean startup without application errors.
