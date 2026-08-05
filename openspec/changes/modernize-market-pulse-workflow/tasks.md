## 1. Baseline and Contracts

- [x] 1.1 Inventory the current Market Pulse section order, stable IDs, data hooks, controls, forms, query parameters, scripts, modals, external links, and API endpoints.
- [x] 1.2 Capture isolated baseline screenshots and control inventories at 390, 768, 1440, and 1920 pixel widths without writing runtime data.
- [x] 1.3 Add template contracts for the Gamma-first section order, preserved stable hooks, one authoritative status surface, and one Trade Decision surface.
- [x] 1.4 Add interaction contracts for valid, missing, stale, and mismatched-symbol Gamma level payloads before changing the controllers.

## 2. Status and Gamma-Chart Cockpit

- [x] 2.1 Recompose the command header into a compact status bar with ticker, spot, session/data state, freshness, symbol selection, refresh, and diagnostics.
- [x] 2.2 Build the desktop Gamma and Chart Cockpit around the existing execution chart and a new compact Gamma Level Deck.
- [x] 2.3 Populate Main Flip, Local Flip, Call Wall, Put Wall, spot, nearest level, distance, classification, and validity labels from existing server-provided structure data.
- [x] 2.4 Preserve the existing chart timeframe, drawing, undo, clear, marker, Gamma-level, and day-level controls with their current IDs and behavior.
- [x] 2.5 Preserve the full Gamma Ladder symbol search, ticker shortcuts, strike window, DTE, refresh, legend, error, loading, and advanced board interactions.
- [x] 2.6 Remove or demote repeated summary values only after proving the authoritative status and cockpit surfaces expose the same information.

## 3. Gamma and Chart Coordination

- [x] 3.1 Define normalized stable level keys and validity metadata shared by the Level Deck, Gamma controller, and chart coordinator.
- [x] 3.2 Dispatch idempotent Gamma update and selection events containing ticker, level key, numeric price, classification, timestamp, and validity state.
- [x] 3.3 Add chart-side level highlight handling that ignores invalid, stale, or mismatched-symbol events and leaves independent chart rendering intact.
- [x] 3.4 Implement click-to-pin and hover/focus-to-preview behavior, restoring the pinned selection when preview interaction ends.
- [x] 3.5 Preserve the pinned level across supported timeframe changes for the current page session without adding persistent storage.
- [x] 3.6 Add focused JavaScript or DOM interaction tests for selection, preview, timeframe retention, refresh races, unavailable levels, and symbol changes.

## 4. Structure, Decision, and Secondary Context

- [x] 4.1 Move the structure-location map directly below the cockpit and preserve its wall, flip, price, zone, nearest-level, and status calculations.
- [x] 4.2 Consolidate state, bias, best look, required trigger, invalidation, grade, score, and execution status into one authoritative Trade Decision surface.
- [x] 4.3 Place the existing four-step entry checklist after the Trade Decision and retain all progress, pressed-state, and next-step behavior.
- [x] 4.4 Move tape, calendar/news context, diagnostics, replay, and advanced Gamma explanation below the primary workflow using accessible disclosures where appropriate.
- [x] 4.5 Keep active guardrails, data-quality warnings, stale states, and critical failures visible without requiring a secondary disclosure to be opened.
- [x] 4.6 Verify Candle Opens, replay, diagnostics, ticker search, refresh, tape, news, and external destinations still reach their original receiving surfaces safely.

## 5. Responsive, Accessibility, and Completion

- [x] 5.1 Implement the desktop chart/Level Deck ratio and the narrow-screen order of status, Level Deck, chart, structure, decision, and checklist.
- [x] 5.2 Verify keyboard order, visible focus, semantic pressed/expanded/loading/disabled states, reduced motion, and text labels independent of color.
- [x] 5.3 Compare final and baseline control inventories and resolve every missing control, changed action, unnamed control, or unexpected destination.
- [x] 5.4 Run focused Market Pulse ticker, snapshot, runtime, chart, Gamma, route, external-link, and regression tests plus syntax checks for every touched JavaScript file.
- [x] 5.5 Capture final screenshots at 390, 768, 1440, and 1920 pixels and resolve horizontal overflow, clipping, overlap, detached overlays, and unreadable values.
- [x] 5.6 Run `git diff --check`, document unrelated baseline failures separately, and verify no assertion was weakened to conceal them.
- [x] 5.7 With approval, rebuild through `./scripts/run_podman_app.sh`, verify `/healthz`, and perform a final receiving-page smoke check.
