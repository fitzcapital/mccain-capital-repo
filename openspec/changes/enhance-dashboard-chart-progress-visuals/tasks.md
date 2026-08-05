## 1. Current Visual and Data Contract

- [x] 1.1 Inventory the initial and refreshed Market Tape payload fields, both SVG renderers, every
  supported timeframe, symbol switching, and fallback source behavior.
- [x] 1.2 Inventory milestone, alignment, balance/goal, performance pace, consistency, and discipline
  progress calculations, markup, scripts, ARIA state, and responsive styling.
- [x] 1.3 Record representative current-session, early-session collision, stale, missing, close-only,
  and prior-session chart fixtures before changing rendering.

## 2. Current-Session Level Data

- [x] 2.1 Add a focused tape-service helper that validates current-session OHLC rows and derives
  numeric/formatted CDH and CDL plus source and freshness metadata.
- [x] 2.2 Ensure stale, prior-session, cached, synthetic, and close-only sources cannot be labeled as
  current-day levels.
- [x] 2.3 Extend timeframe payload construction so every selected window receives the same
  symbol-specific session-level payload without changing windowed point/percent calculations.
- [x] 2.4 Add the optional session-level fields to initial Dashboard tape rows and the
  `/api/dashboard/tape` response while preserving all existing keys and fallbacks.
- [x] 2.5 Add service and handler tests for valid extrema, symbol isolation, malformed rows, recency,
  missing data, fallback sources, and backward compatibility.

## 3. Server-Rendered CDH/CDL Chart Overlay

- [x] 3.1 Extend the Python Market Tape chart scale to include valid CDH/CDL values with bounded
  padding and the existing minimum candle-domain floor.
- [x] 3.2 Render distinct dashed CDH and CDL rules, complete price tags, and connectors in the initial
  SVG without obscuring candles or the current-price point.
- [x] 3.3 Implement deterministic label clamping and collision separation for close levels and chart
  boundaries.
- [x] 3.4 Add visible or assistive chart context containing symbol, timeframe, CDH/CDL values or
  unavailability, and the current price state.
- [x] 3.5 Add focused SVG snapshot/contract tests across up, down, flat, out-of-window, collision,
  missing-level, and close-only fixtures.

## 4. Live Chart Parity

- [x] 4.1 Extend the browser chart scale and SVG builder with the same level-domain, line, tag,
  connector, clamping, and collision rules as the server renderer.
- [x] 4.2 Update chart cards, accessible descriptions, and freshness together after successful tape
  refreshes and ticker changes.
- [x] 4.3 Preserve full-session CDH/CDL while switching 15M, 30M, 1H, 6H, and 24H candle windows.
- [x] 4.4 Preserve the last confirmed candles and session levels when refresh or replacement data is
  invalid, stale, or fails.
- [x] 4.5 Add JavaScript/DOM coverage comparing initial and refreshed level classes, formatted values,
  scale bounds, collision positions, timeframe behavior, and failure preservation.

## 5. Shared Progress Visual Components

- [x] 5.1 Define a reusable semantic Dashboard ring component with clamped visual percentage,
  endpoint marker, tone, center label, and progressbar ARIA attributes.
- [x] 5.2 Migrate milestone, alignment, and balance/goal completion to the shared ring while preserving
  their authoritative numeric values, statuses, controls, and calculations.
- [x] 5.3 Define a reusable linear progress component with track, fill, endpoint, tone, labels, and
  optional target or threshold markers.
- [x] 5.4 Migrate performance pace, consistency, discipline signal, and remaining applicable progress
  tracks to the shared linear component without changing thresholds or comparison logic.
- [x] 5.5 Remove only redundant visual wrappers and styles after confirming no text, state, form,
  deep-link, or responsive behavior depends on them.
- [x] 5.6 Add template and calculation-regression tests proving every pre-enhancement numeric result
  and status remains unchanged, including values below zero and above one hundred.

## 6. Styling and Restrained Motion

- [x] 6.1 Add Dashboard-scoped CDH/CDL rule, tag, connector, ring, track, endpoint, and state-tone
  styles in a focused companion stylesheet.
- [x] 6.2 Add an idempotent visual-state controller using `IntersectionObserver` for one-time reveal
  and successful-value-change transitions, including lazy-fragment rebinding.
- [x] 6.3 Keep confirmed text and controls immediately available while animating only visual arcs,
  fills, level opacity, and point position for short durations.
- [x] 6.4 Add complete `prefers-reduced-motion: reduce` behavior with final states visible and no
  interpolation or continuous pulsing.
- [x] 6.5 Verify the styles remain compatible with existing Dashboard themes, focus indicators,
  operation feedback, and the shared modal/drawer layer.

## 7. Responsive and Accessibility Verification

- [x] 7.1 Verify price tags, connectors, candles, current-price points, ring centers, endpoints, and
  labels at supported desktop, tablet, and mobile widths with no horizontal page overflow.
- [x] 7.2 Verify CDH/CDL remain distinguishable by text, line pattern, and shape without relying on
  teal/rose color alone.
- [x] 7.3 Verify chart descriptions and progressbar roles expose symbol, timeframe, values, bounds,
  and state with concise accessible names.
- [x] 7.4 Verify keyboard controls, ticker/timeframe menus, refresh, milestone editing, and all existing
  progress-related interactions remain unchanged.

## 8. Regression, Deployment, and Evidence

- [x] 8.1 Run JavaScript syntax checks and focused pytest slices for tape services, refresh handlers,
  Dashboard rendering, financial calculations, and the new visual contracts.
- [x] 8.2 Run authenticated browser smoke tests for initial chart levels, every timeframe, symbol
  switching, refresh behavior, progress rings/tracks, responsive layouts, and reduced motion.
- [x] 8.3 Review the scoped diff against the dirty worktree and confirm no runtime data, financial
  rules, routes, or unrelated page styles changed.
- [x] 8.4 Rebuild and restart with `./scripts/run_podman_app.sh`, verify `/healthz` at
  `http://127.0.0.1:5001`, and repeat relevant checks against deployed assets.
- [x] 8.5 Record final screenshots or browser measurements for representative desktop, tablet, and
  mobile chart and progress states before marking the change complete.

## 9. Visual Review Rollback

- [x] 9.1 Record the failed visual-review outcome and prevent the rejected change from being synced
  or archived as the production contract.
- [x] 9.2 Restore the original Market Tape payload, scale, candle rendering, and live browser chart.
- [x] 9.3 Restore the original Dashboard milestone, alignment, balance, pace, consistency, capital,
  and discipline visuals without changing their calculations.
- [x] 9.4 Remove the rejected Dashboard metric stylesheet, animation controller, and focused tests.
- [x] 9.5 Rebuild the container and verify the restored Dashboard behavior.
