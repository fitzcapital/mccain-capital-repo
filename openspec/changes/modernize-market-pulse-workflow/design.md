## Context

Market Pulse already has the data and controls required for the desired workflow, but the DOM
order currently places the execution summary and trigger checklist before the Gamma Ladder. Spot,
regime, bias, tradeability, and decision context are repeated in the command header and execution
hero. The Gamma Ladder and execution chart are implemented by separate JavaScript controllers,
while server-rendered structure values provide the initial state.

The redesign must operate within the existing Flask/Jinja, page-scoped CSS, and static JavaScript
architecture. Market-data providers, API payloads, calculations, execution rules, and persistence
are compatibility boundaries. Existing element IDs and data hooks are high-risk because focused
tests and JavaScript controllers depend on them.

## Goals / Non-Goals

**Goals:**

- Make Gamma levels and the execution chart the primary, shared workspace.
- Reduce repeated summary content and establish one authoritative status and decision hierarchy.
- Coordinate level selection between the Level Deck and chart without coupling data providers.
- Preserve all current controls, states, destinations, endpoints, and diagnostics.
- Provide a deliberate desktop cockpit and a usable stacked mobile workflow.

**Non-Goals:**

- Changing quote, options, Gamma, tape, chart, or news data acquisition.
- Changing Gamma calculations, trade grading, trigger validation, or trading recommendations.
- Adding order entry, broker actions, background jobs, dependencies, migrations, or persisted user
  preferences for selected levels.
- Redesigning other application pages.

## Decisions

### Recompose existing surfaces instead of replacing controllers

The template will move and regroup existing blocks while preserving stable IDs, data attributes,
form fields, links, and modal targets. Reusing the existing chart and Gamma controllers minimizes
behavior risk and keeps rollback mechanical.

Alternative considered: rebuild the page as a new client-rendered application. Rejected because it
would duplicate server view-model logic, enlarge the regression surface, and add no value to the
Gamma-first hierarchy.

### Use one cockpit with chart and Level Deck peers

At desktop widths the cockpit uses a dominant chart column and a narrower Level Deck column. The
Level Deck contains the principal server-rendered structure values immediately, then accepts live
updates from existing Gamma responses. The full ladder remains available as an advanced section
within or directly below the cockpit rather than being removed.

At narrow widths CSS reorders the Level Deck before the chart so the user sees the map before price
confirmation. Core chart controls remain attached to the chart surface.

Alternative considered: put Gamma and chart behind tabs. Rejected because the user needs to compare
them continuously and tabs would preserve the current context-switching problem.

### Coordinate controllers through a small page-level event contract

The Gamma controller will expose normalized available level records through DOM data and dispatch
a narrowly scoped custom event when level data changes or a level is selected. A Market Pulse
coordinator will map stable level keys (`main_flip`, `local_flip`, `call_wall`, `put_wall`, and
supported ladder strikes) to chart overlays. The chart controller will expose a focused highlight
method or consume the event without owning Gamma data acquisition.

Pinned selection is page-session state only. Hover and focus are temporary and restore the pinned
selection on exit. Invalid, missing, or non-numeric levels never create overlays.

Alternative considered: merge `gamma_ladder.js` and `spx_hero_chart.js`. Rejected because the
controllers have separate data lifecycles and should remain independently testable.

### Preserve a single semantic value source

The initial Level Deck uses the same server-provided structure snapshot already consumed by the
header and chart. Live Gamma refreshes update named level nodes through the event contract. Duplicate
headline values are removed or demoted rather than recalculated in the template or JavaScript.
Formatting can differ by context, but numeric identity and validity state cannot.

### Use progressive disclosure only for secondary context

Tape, news, calendar context, diagnostics, replay, and educational Gamma detail move below the
decision workflow into accessible `details` or existing dialog surfaces. Gamma levels, chart,
structure, decision, and checklist remain visible without opening disclosures.

### Verify behavior through route, DOM, controller, and visual contracts

Before editing, capture current controls, IDs, forms, scripts, and screenshots using isolated
storage. Add template contracts for section order and preserved hooks, JavaScript tests or focused
DOM checks for level/chart synchronization, and retain existing Market Pulse behavioral tests.
Final captures at 390, 768, 1440, and 1920 pixels must show no document overflow and exact control
parity unless a difference is explicitly approved.

## Risks / Trade-offs

- [Risk] Moving large template blocks can break selectors or initialization order. → Preserve IDs
  and data hooks, inventory scripts before editing, and add DOM-order/controller contracts first.
- [Risk] Chart and Gamma refreshes can race. → Make events idempotent, include ticker and validity
  metadata, and ignore stale or mismatched-symbol events.
- [Risk] Repeated spot or level values can drift during partial refresh. → Update named nodes from
  one normalized event payload and retain explicit timestamps/state labels.
- [Risk] A side-by-side cockpit can become cramped. → Use a bounded desktop column ratio and stack
  Level Deck before chart below the responsive breakpoint.
- [Risk] Collapsing secondary content can hide alerts. → Keep active guardrails, stale states, and
  critical failures visible in the status/decision workflow; collapse only supporting detail.
- [Trade-off] The full Gamma Ladder may remain tall. → Prioritize a compact Level Deck above it and
  keep the full board available for deeper inspection.

## Migration Plan

1. Capture a baseline route/control/state inventory in isolated application storage.
2. Add structural and interaction contracts for the proposed order and preserved hooks.
3. Recompose the template and apply page-scoped responsive styling.
4. Add the minimal Gamma/chart coordination event contract with missing/stale safeguards.
5. Run focused Market Pulse tests, JavaScript syntax checks, and responsive captures.
6. Rebuild through the normal container path with approval and verify `/healthz`.

Rollback consists of reverting the Market Pulse template, page-scoped CSS, and coordinator changes;
no data or schema rollback is required.

## Open Questions

No blocking questions. Implementation should treat the compact Level Deck as the primary scan
surface while retaining the full Gamma Ladder for advanced inspection.
