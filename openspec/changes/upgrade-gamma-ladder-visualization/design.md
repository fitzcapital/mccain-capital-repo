## Context

The Market Pulse Gamma Ladder is server-framed by Jinja and populated by
`static/js/gamma_ladder.js` from the existing Gamma Ladder API. Each response is normalized into
strike rows with positive/negative bars, net Gamma, spot proximity, strength, flip, and structural
classifications. The controller already protects against out-of-order requests, supports symbol,
window, DTE, expiration, refresh, tooltip, and detail interactions, and dispatches the existing
chart-selection event.

The current page-scoped CSS contains visual state tokens and animation keyframes, but most data-row
animations are deliberately disabled. The upgrade must add meaningful motion without creating a
constant animated trading surface, changing financial values, introducing a rendering framework,
or weakening stale/error behavior.

## Goals / Non-Goals

**Goals:**

- Make spot, flip, dominant nodes, support/resistance, and net Gamma direction immediately readable.
- Animate accepted state changes rather than continuously decorating static data.
- Keep the selected strike and its chart relationship obvious.
- Preserve all current data, filters, controls, APIs, accessibility semantics, and failure states.
- Maintain smooth behavior on desktop and constrained mobile hardware.
- Let the user opt into a pinned Playbook header while keeping normal document flow as the default.

**Non-Goals:**

- Changing provider acquisition, Gamma calculations, strike selection, regime classification, or
  trade recommendations.
- Adding Canvas/WebGL, a client framework, dependencies, persisted visualization preferences other
  than the Playbook pin preference, or background refresh jobs.
- Redesigning the primary execution chart, Gamma Level Deck, or other application pages.
- Using constant breathing, pulsing, or glowing animations for valid static values.

## Decisions

### Recompose the existing ladder instead of replacing its controller

The Jinja shell and existing controller remain the compatibility boundary. The template will gain
stable containers for the compact status, key-level summary, depth board, selected-level readout,
and accessible explanation disclosure. Existing IDs, data attributes, buttons, query behavior,
tooltip content, and row-detail fields remain intact.

Alternative considered: build a Canvas or client-rendered ladder. Rejected because it would reduce
native accessibility, duplicate current row semantics, complicate responsive rendering, and add no
data capability.

### Derive animation state from accepted payload diffs

The controller will keep a small in-memory presentation snapshot keyed by symbol, DTE, expiration,
window, and strike. Only after the existing request-sequence guard accepts a response will it diff
spot, regime, strongest strike, row order, numeric values, sign, and classifications. The snapshot
contains display inputs only and never becomes a financial source of truth.

Rows receive one-shot semantic transition states such as `is-entering`, `is-moving`,
`is-value-changed`, `is-spot-crossed`, and `is-new-strongest`. State classes are removed on
`animationend` or a bounded timeout. An unchanged accepted payload renders without replaying them.

Alternative considered: replay all animations after every refresh. Rejected because it obscures
which values actually changed and causes unnecessary motion.

### Use DOM measurement and compositor-safe transitions

Before replacing row markup, the controller records visible row positions by strike. After render,
surviving rows begin at their prior vertical delta and transition to zero using transforms. Bar
width and opacity changes use SVG/CSS transitions or the Web Animations API against accepted old
and new dimensions. The implementation avoids animating layout-heavy properties, large blurred
shadows, or filters across the entire board.

Alternative considered: introduce an animation library. Rejected because the required transitions
are small, browser-native APIs are sufficient, and a dependency would enlarge the performance and
maintenance surface.

### Treat spot, regime, and dominant-node motion as finite events

Spot movement uses a short transform between accepted positions. Crossing a displayed structural
level adds one pulse and a text label. Regime changes use one restrained surface sweep, and the
dominant-node marker transfers once to the new strongest strike. Loading shimmer and refresh spin
are the only continuously looping animations.

No motion implies actionability. Trade permission remains owned by the existing Trade Decision
surface; Gamma motion communicates data change only.

### Add a sticky inspector without duplicating financial state

Selecting a row continues to open the existing detail panel and dispatch
`market-pulse:gamma-level-selected`. The same normalized row object updates a sticky selected-level
readout containing strike, classification, distance, and net Gamma. The inspector clears when a
new context no longer includes the strike. It does not persist beyond the page session.

Alternative considered: create a second detail data model. Rejected because duplicated values could
drift from the selected row and chart event.

### Move explanation behind an accessible disclosure

The seven-item legend becomes a concise `details` disclosure labeled "How to read this". Board
headers and structural chips retain enough text and shape to understand the current visualization
without opening it. Existing tooltip explanations remain available.

### Use responsive information reduction, not horizontal compression

Desktop and narrow layouts use three primary columns—strike, depth, and net Gamma—and place
supporting importance and role context near the strike and in the activated detail. Spot and
the selected-level inspector use sticky positioning within the ladder, not the document viewport.
The board must not create page-level overflow.

### Make Playbook pinning explicit and opt-in

The page-level Playbook header remains in normal document flow by default. A `Pin header` toggle in
the Playbook utility area applies the existing sticky positioning only when pressed and saves the
choice in local storage. The control exposes `aria-pressed`, retains a visible focus state, and does
not affect the ladder-local selected-level inspector.

### Add market-location bands and structural node geometry

The depth board groups consecutive rows into explicit `Above spot`, `At spot`, and `Below spot`
sections. The common axis gains a stronger zero spine and symmetric negative/positive lane framing.
Spot, flip, and dominant strikes receive different marker geometry and labels rather than relying
on brighter versions of the same row. All grouping and geometry derive from the existing normalized
rows and do not change ordering, calculations, or event payloads.

### Reserve badges for meaningful structural levels

The row remains the single interactive control. Ordinary minor support and resistance roles move
into quiet inline text near the strike instead of rendering as two stacked button-like pills in a
dedicated signal column. Magnet, primary, flip, and current levels retain a compact badge because
they represent scan-critical structure; secondary roles may use a restrained text marker. Strength
remains paired with Net GEX, and the selected-level detail retains the complete importance, role,
status, and behavior values.

Alternative considered: keep every importance and role value in matching pills. Rejected because
the repeated containers look interactive, consume horizontal space, and give minor rows the same
visual weight as structural levels.

### Consolidate the control stack into one command bar

The ladder header becomes a compact two-zone command bar. The identity zone contains the Gamma
Ladder label, live description, active symbol and spot, and a concise freshness/regime line. The
control zone keeps SPX, SPY, QQQ, and search immediately available, then presents one settings
trigger whose label always reflects the accepted Window and DTE selections, such as
`Standard · 0DTE`. Activating it opens a keyboard-accessible popover containing the existing Window
and DTE buttons. Refresh remains one separate icon action.

The settings popover closes on outside click, Escape, or after a selection, while the trigger keeps
`aria-expanded` and `aria-controls` synchronized. Existing `data-gamma-window-pill` and
`data-gamma-dte-pill` hooks and selection behavior remain unchanged inside the popover.

Alternative considered: replace all filters with native selects. Rejected because it weakens rapid
comparison and changes established control semantics. Keeping the segmented choices inside a
popover preserves fast selection without displaying thirteen simultaneous buttons.

### Make reduced motion an equivalent final-state path

`prefers-reduced-motion: reduce` disables travel, sweeps, morph duration, and pulses while keeping
all final values, role labels, selection states, and loading/error messages. JavaScript also avoids
measurement-driven animation work when reduced motion is active.

## Risks / Trade-offs

- [Risk] Frequent DOM measurement could cause layout thrashing. → Measure old rows in one read
  phase, render once, measure new rows in one read phase, then batch writes in
  `requestAnimationFrame`; limit work to the existing focused strike window.
- [Risk] Refresh responses could animate stale data over a newer context. → Diff only after the
  existing request ID and active-context checks accept the response; discard presentation state
  when symbol, DTE, expiration, or window changes.
- [Risk] Motion could distract during volatile trading. → Use short one-shot transitions, reserve
  pulses for actual crossings or state transfers, and prohibit looping ambient data animation.
- [Risk] Sticky surfaces could obscure rows on mobile. → Bound them to the ladder board, keep their
  height compact, and verify 390/768 pixel captures with keyboard focus.
- [Risk] Restructuring the legend or controls could break selectors. → Inventory stable hooks first,
  retain existing names and data attributes, and add DOM/control-parity contracts before edits.
- [Trade-off] Native DOM/SVG animation is less visually elaborate than Canvas. → It preserves
  accessibility, existing behavior, and maintainability while meeting the functional visual goal.

## Migration Plan

1. Capture baseline Gamma Ladder screenshots, controls, data hooks, and representative complete,
   stale, empty, and error states in isolated storage.
2. Add failing DOM and pure-state contracts for the new hierarchy and accepted-payload diff model.
3. Recompose the template and page-scoped styles while preserving stable hooks.
4. Add the presentation snapshot, one-shot transition classes, sticky inspector, and chart-event
   compatibility to the existing controller.
5. Run focused route, Gamma service/API, controller, accessibility, and responsive visual checks.
6. Rebuild through the normal container workflow with approval and verify the receiving page.

Rollback consists of reverting the template, page-scoped CSS, controller presentation state, and
tests. No schema, API, persistent data, or provider rollback is required.

## Open Questions

No blocking questions. Implementation should use the institutional hybrid direction: strong static
hierarchy with restrained event-driven motion.
