## 1. Current Behavior Inventory

- [x] 1.1 Inventory every Dashboard action, trigger, destination, modal, disclosure, lazy fragment,
  and asynchronous request so the modernization preserves the complete action surface.
- [x] 1.2 Map the Pressure Check, behavior-update, calendar, trade-day, broker-metrics, planning,
  import, and diagnostic data flows to their existing templates, JavaScript handlers, services, and
  routes.
- [x] 1.3 Identify whether session-inspector detail and Pressure Check notes can use existing
  endpoints and persistence fields; document any minimal backward-compatible additions required.

## 2. Shared Interaction Foundation

- [x] 2.1 Add reusable server-rendered modal, drawer, and sheet markup conventions with labelled
  headers, scrollable bodies, stable action footers, and data-driven triggers.
- [x] 2.2 Implement an idempotent Dashboard interaction controller for open/close lifecycle, one-open
  enforcement, focus capture and restoration, focus containment, Escape/backdrop handling, and body
  scroll locking.
- [x] 2.3 Add compact, standard, wide, desktop-drawer, and narrow-sheet styles consistent with the
  current Dashboard theme and safe-area constraints.
- [x] 2.4 Add reduced-motion styles and ensure spinners and state transitions retain clear non-motion
  feedback.
- [x] 2.5 Convert lazy-fragment initialization to rebind new interaction triggers exactly once after
  calendar or planning fragment replacement.
- [x] 2.6 Add focused JavaScript tests or DOM harness coverage for keyboard lifecycle, focus return,
  one-open behavior, narrow-sheet behavior, and fragment rebinding.

## 3. Guided Pressure Check

- [x] 3.1 Replace the static Pressure Check body with steps for trigger category and note, reset
  interval, rule checklist, and explicit operating outcome.
- [x] 3.2 Reuse the existing discipline state, A+ Only/Done for Day modes, trade-gate checks, and
  planning-scroll behavior for reset outcomes.
- [x] 3.3 Implement the client-side reset timer with pause-safe completion state and reduced-motion
  presentation.
- [x] 3.4 Connect reset completion to the existing behavior-update flow, including trigger details
  when supported and retryable feedback when behavior logging fails.
- [x] 3.5 Prevent Proceed Aligned from enabling new risk unless the operating mode and every required
  trade-gate item allow it.
- [x] 3.6 Add regression coverage for Return to Plan, Done for Day, incomplete gate rejection, local
  conservative state on logging failure, and behavior-trend updates.

## 4. Calendar Session Inspector

- [x] 4.1 Replace the Dashboard calendar preview overlay with the shared wide session-inspector
  surface while keeping lazy calendar rendering and active-day feedback.
- [x] 4.2 Populate the inspector with selected-date summary, P/L, trade count, record, balance, and
  available trade, journal, debrief, and reconciliation state.
- [x] 4.3 Add or extend the smallest focused read endpoint needed for optional session detail by
  composing existing repositories/services rather than duplicating calculations.
- [x] 4.4 Generate Open Day, Start Debrief, Open Journal, and Reconcile actions with the explicit ISO
  date and graceful unavailable-detail messaging.
- [x] 4.5 Verify the receiving Trades page treats inspector navigation as a one-day scope and excludes
  adjacent-day trades unless the user explicitly changes scope.
- [x] 4.6 Add template, endpoint, JavaScript, and receiving-route tests for populated days, missing
  optional detail, lazy rebinds, and selected-day isolation.

## 5. Broker Metrics Drawer

- [x] 5.1 Move the current broker metrics editor into the shared drawer while preserving existing
  form actions, server validation, account selection, and financial calculations.
- [x] 5.2 Display manual, statement-derived, and confirmed refreshed sources distinctly with last
  successful update, current connection state, and stale indicators.
- [x] 5.3 Add explicit refresh, retry, diagnostics, and headed session-seeding controls using existing
  broker automation behavior where available.
- [x] 5.4 Ensure failed authentication, parsing, refresh, or seed attempts retain prior manual or
  confirmed metrics and show actionable diagnostics.
- [x] 5.5 Add focused service/handler and browser-level tests proving successful updates, validation
  failures, stale states, and non-destructive refresh failure behavior.

## 6. Command Palette

- [x] 6.1 Add a visible Dashboard command trigger and Command/Ctrl+K shortcut that ignores editable
  fields.
- [x] 6.2 Build a searchable client-side registry mapping commands to the same links or handlers used
  by visible Pressure Check, Market Pulse, trade, import, journal, planning, and refresh controls.
- [x] 6.3 Implement arrow-key navigation, Enter activation, Escape close, active-option semantics,
  empty results, and clear search behavior.
- [x] 6.4 Add state predicates that prioritize relevant actions and visibly explain commands blocked
  by the current session or discipline mode.
- [x] 6.5 Add JavaScript/DOM coverage for shortcut conflicts, search, keyboard selection, handler
  reuse, and unavailable-action explanations.

## 7. Unified Operation Feedback

- [x] 7.1 Define reusable idle, loading, success, stale, and error rendering helpers for Dashboard
  asynchronous controls and live status regions.
- [x] 7.2 Apply the shared feedback to planning refresh, market-tape refresh, synchronization/import,
  broker refresh, and session-seeding actions without clearing confirmed content on failure.
- [x] 7.3 Protect active operations from duplicate submission and expose completion timestamps, retry
  actions, and diagnostics when available.
- [x] 7.4 Add regression coverage for loading animation, duplicate protection, successful replacement,
  stale data, and failure with preserved content.

## 8. Dashboard Flow and Responsive Polish

- [x] 8.1 Group existing sections into Command, Today, Decision Tools, and Review & Reference layers
  while preserving stable IDs, deep links, destinations, and the complete action inventory.
- [x] 8.2 Keep command and current-session content immediately visible and summarize or collapse long
  historical/reference content until requested.
- [x] 8.3 Verify modal, drawer, palette, sticky actions, and existing Dashboard cards at supported
  desktop, tablet, and mobile widths with no horizontal overflow.
- [x] 8.4 Verify reduced-motion behavior, visible focus states, accessible names, live-region updates,
  color contrast, and keyboard-only completion of each workflow.

## 9. Verification and Deployment

- [x] 9.1 Run JavaScript syntax checks and focused pytest slices for Dashboard rendering, behavior,
  calendar scope, broker metrics, and any new focused endpoint.
- [x] 9.2 Run an authenticated browser smoke test covering Pressure Check, session inspection, broker
  drawer, command palette, asynchronous feedback, and every previously inventoried action.
- [x] 9.3 Rebuild and restart with `./scripts/run_podman_app.sh`, verify `/healthz` on
  `http://127.0.0.1:5001`, and repeat the relevant workflows against deployed assets.
- [x] 9.4 Review the final diff for unrelated dirty-worktree changes and record verification evidence
  before marking the OpenSpec change complete.
