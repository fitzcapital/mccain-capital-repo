## 1. Baseline and Contracts

- [x] 1.1 Inventory the current Gamma Ladder DOM order, stable IDs, data hooks, controls, query parameters, API fields, chart event, tooltip, detail, loading, stale, empty, and error states.
- [x] 1.2 Capture isolated baseline screenshots and control inventories at 390, 768, 1440, and 1920 pixel widths without writing runtime data.
- [x] 1.3 Add failing template contracts for compact status, key-level summary, depth board, selected-level inspector, explanation disclosure, and preserved stable hooks.
- [x] 1.4 Add failing pure-state contracts for unchanged, changed, inserted, removed, reordered, spot-crossed, regime-changed, and strongest-node-changed payload diffs.
- [x] 1.5 Add failing interaction contracts for stale responses, mismatched contexts, unavailable levels, filter changes, reduced motion, and chart-selection compatibility.

## 2. Institutional Ladder Hierarchy

- [x] 2.1 Recompose the existing Gamma Ladder header into compact status and control zones while preserving ticker search, shortcuts, DTE, window, expiration, and refresh behavior.
- [x] 2.2 Add a key-level summary for spot, regime, freshness, strongest strike, flip, and nearest support or resistance using accepted payload values only.
- [x] 2.3 Strengthen row hierarchy for strike, center-axis depth, net Gamma, strength, spot location, and explicit structural-role labels without relying on color alone.
- [x] 2.4 Add a ladder-local sticky selected-level inspector populated from the same normalized row used by the detail panel and chart-selection event.
- [x] 2.5 Convert the full legend into an accessible "How to read this" disclosure while retaining concise board labels and existing tooltip explanations.
- [x] 2.6 Preserve loading, stale, error, empty, unavailable, and prior-valid-data presentations within the new hierarchy.

## 3. Accepted-Payload Diff and Motion State

- [x] 3.1 Define a presentation snapshot keyed by active symbol, DTE, expiration, window, and strike without changing the financial data model.
- [x] 3.2 Implement a pure accepted-payload diff that identifies material value, sign, order, spot, crossing, regime, classification, and strongest-node changes.
- [x] 3.3 Apply presentation diffs only after the existing request-sequence and active-context guards accept a response.
- [x] 3.4 Clear obsolete presentation and selected-level state when symbol, DTE, expiration, or strike window changes.
- [x] 3.5 Add bounded one-shot transition state cleanup using `animationend` with a safe timeout fallback.
- [x] 3.6 Verify unchanged accepted payloads do not replay attention animations or mutate selected state.

## 4. Data-Driven Animation System

- [x] 4.1 Implement batched pre-render and post-render row measurements for compositor-safe strike insertion, removal, and reorder transitions.
- [x] 4.2 Animate changed positive, negative, and net Gamma bar dimensions from accepted prior values to current values without recalculating them.
- [x] 4.3 Implement smooth spot relocation and a one-shot labeled pulse only when spot crosses displayed structure.
- [x] 4.4 Implement one-shot regime transition and dominant-node transfer treatments that end in the accepted static state.
- [x] 4.5 Implement row selection, sticky inspector, and detail expansion transitions while preserving `aria-expanded` and the existing chart event payload.
- [x] 4.6 Keep all static data bars and decorative surfaces motionless between accepted changes; allow indefinite motion only for loading or refresh progress.

## 5. Responsive, Accessibility, and Performance

- [x] 5.1 Implement the desktop four-column depth board and narrow three-column strike/depth/net-Gamma board without document-level overflow.
- [x] 5.2 Keep spot and the selected-level inspector discoverable within the ladder scroll container without covering focused rows.
- [x] 5.3 Preserve keyboard order, visible focus, pressed, expanded, loading, disabled, and disclosure semantics for all controls and rows.
- [x] 5.4 Implement a reduced-motion path that skips measurement-driven travel, morphs, pulses, and sweeps while rendering identical final values.
- [x] 5.5 Limit animation work to visible focused rows and batch layout reads and writes to avoid repeated forced layouts.
- [x] 5.6 Verify structural roles, crossings, strength, selection, stale state, and errors retain explicit text or shape independent of color.

## 6. Verification and Completion

- [x] 6.1 Run focused Gamma Ladder route, service, API, symbol, window, DTE, expiration, failure-state, and chart-coordination tests.
- [x] 6.2 Run JavaScript syntax checks and focused presentation-diff, refresh-race, selection, filter-change, reduced-motion, and animation-idempotence tests.
- [x] 6.3 Compare final and baseline control, hook, API-field, and numeric-value inventories and resolve every undocumented difference.
- [x] 6.4 Capture complete, partial, stale, empty, and error states at 390, 768, 1440, and 1920 pixels and resolve overflow, clipping, detached sticky surfaces, and unreadable values.
- [x] 6.5 Run `git diff --check`, document unrelated baseline failures separately, and verify no assertion or financial calculation was weakened.
- [x] 6.6 With approval, rebuild through `./scripts/run_podman_app.sh`, verify `/healthz`, and smoke-check Market Pulse, Gamma refresh, row selection, chart highlighting, and filter controls in the rebuilt app.

## 7. Playbook Pin Control and Stronger Visual Differentiation

- [x] 7.1 Add contracts for default-off persistent Playbook pinning, accessible pressed state, market-location section labels, and distinct spot, flip, and dominant marker hooks.
- [x] 7.2 Add a Playbook utility toggle that pins the header only when enabled and restores the saved preference without changing ladder-local sticky behavior.
- [x] 7.3 Segment rendered rows into above-spot, at-spot, and below-spot sections and strengthen the center-axis lane plus spot, flip, and dominant-node geometry.
- [x] 7.4 Run focused syntax, DOM, interaction, responsive, and visual checks for the revised header and ladder without changing financial assertions.

## 8. Quieter Heat-Map Signal Hierarchy

- [x] 8.1 Add contracts that ordinary minor rows use inline role text while scan-critical rows retain one structural badge and all selection details.
- [x] 8.2 Replace the repeated stacked importance and role pills with an importance-aware signal renderer without changing the row button or data hooks.
- [x] 8.3 Rebalance desktop and narrow row columns so the depth lane gains space and supporting text remains readable without overflow.
- [x] 8.4 Run focused syntax, interaction, numeric-identity, responsive, and visual checks for the simplified signal hierarchy.

## 9. Compact Gamma Ladder Command Bar

- [x] 9.1 Add contracts for the compact identity/control zones, current-settings label, accessible popover state, and preserved Window/DTE hooks.
- [x] 9.2 Recompose the header so symbol shortcuts and search remain visible while Window and DTE choices move into one settings popover beside refresh.
- [x] 9.3 Synchronize the trigger label, expanded state, dismissal behavior, and focus handling with the existing Window and DTE controller state.
- [x] 9.4 Rebalance desktop and narrow header layouts to remove excess empty space and prevent overflow without hiding status context.
- [x] 9.5 Run focused DOM, JavaScript, interaction, responsive visual, and container rebuild checks for the revised command bar.
