## Context

The Dashboard is a server-rendered Flask/Jinja page enhanced by
`static/js/dashboard_command_center.js`. It combines persistent discipline state, lazy-loaded
calendar and planning fragments, live market requests, broker metrics forms, behavior logging,
and links into trades, journal, analytics, and Market Pulse. Its current interactions use a mix of
custom hidden overlays, inline event handlers, disclosure elements, and direct links. That mixture
makes keyboard behavior, loading feedback, fragment rebinding, and mobile presentation
inconsistent.

This change introduces a shared interaction layer while retaining the current server routes,
financial calculations, manual broker values, and page destinations. It must coexist with lazy
fragment replacement and must not mutate runtime or personal data except through existing,
explicit user actions.

## Goals / Non-Goals

**Goals:**

- Provide a reusable accessible modal, drawer, and mobile sheet foundation without adding a UI
  framework dependency.
- Turn Pressure Check, calendar inspection, and broker metric management into focused workflows.
- Provide a command palette for discovering and starting frequent Dashboard actions.
- Make asynchronous activity visibly progress through loading, success, stale, and error states.
- Preserve Dashboard state, selected-day trade scope, manual broker fallbacks, and existing routes.
- Keep the Dashboard readable by presenting command and current-session information before review
  and reference material.

**Non-Goals:**

- Redesigning destination pages such as Market Pulse, Trades, Journal, or Analytics.
- Changing P/L, account balance, drawdown, gamma, or market-data calculations.
- Introducing a new front-end framework, client-side router, or external component library.
- Automatically placing trades or silently changing discipline or broker values.
- Migrating or rewriting existing personal/runtime data.

## Decisions

### 1. Use one declarative interaction controller

Add a small reusable controller to the existing Dashboard JavaScript. Triggers identify a target
and interaction type through data attributes; the controller owns open/close state, focus capture
and restoration, Escape handling, backdrop handling, scroll locking, and one-open-surface-at-a-time
behavior. Dialog markup remains server-rendered so it works with Jinja data and existing routes.

Use native `<dialog>` semantics where supported and a guarded fallback for the existing supported
browser surface. This avoids a dependency while improving keyboard and assistive-technology
behavior. Lazy fragments call an idempotent initializer after replacement, matching the existing
calendar rebinding pattern.

Alternative considered: keep separate handlers for each popup. Rejected because it preserves the
current accessibility and lifecycle inconsistencies.

### 2. Model surfaces by task rather than by card

Use three presentation variants backed by the same controller:

- compact modal for confirmation and short decisions;
- standard or wide modal for guided workflows and session inspection;
- right-side drawer on desktop and bottom/full-height sheet on narrow screens for persistent
  editing such as broker metrics.

Every surface uses a labelled header, scrollable body, and stable action footer. Informational
cards remain inline; only tasks requiring input, decision, or deeper context open a surface.

Alternative considered: convert all Dashboard details panels into modals. Rejected because it
would hide glanceable information and create excessive interruption.

### 3. Extend the existing discipline and behavior state for Pressure Check

The guided reset reuses the current discipline-state persistence, three-part trade gate, behavior
update endpoint, and `dashboard:urgency-check` event. It records a trigger category, optional note,
timer completion, checklist results, and selected outcome. Applying an outcome updates the same
state used by the discipline rail; it never enables new risk unless all gate requirements and the
current mode permit it.

The timer runs client-side and does not pretend to provide server authority. A failed behavior-log
request leaves the chosen local discipline mode intact and shows a retryable warning.

Alternative considered: create a separate reset-state store. Rejected because it could drift from
the discipline rail and recent behavior trend.

### 4. Enrich calendar inspection without changing date-scope semantics

The lazy calendar fragment supplies the selected ISO date and summary data. The session inspector
may request additional trade, journal, debrief, and reconciliation state for that date, either from
an existing focused endpoint or a small Dashboard read endpoint composed from existing services.
All links generated from the inspector carry the explicit ISO date. The receiving Trades route
must continue treating `d=YYYY-MM-DD` as a one-day custom scope unless the user explicitly chooses
another scope.

Alternative considered: embed full trade payloads in every calendar day. Rejected because it
increases initial page size and duplicates server formatting.

### 5. Define the command palette as a registry of existing actions

The palette uses a static client-side registry for available Dashboard actions, labels, keywords,
destinations, and optional state predicates. It opens with Command/Ctrl+K, supports arrow-key
navigation and Enter, and invokes the same links or functions as visible controls. It does not
create a second implementation of imports, refreshes, or discipline changes.

State-aware suggestions may prioritize actions, but unavailable actions remain explained rather
than silently omitted when the reason helps the user.

Alternative considered: server-side command search. Rejected because the command set is small and
does not require another request.

### 6. Preserve manual broker metrics through an explicit drawer workflow

Move the current broker editor and diagnostics into a drawer while retaining the existing form and
server-side validation. The drawer distinguishes manual values, statement-derived values, and live
refresh results; shows last successful refresh and diagnostics; and exposes headed session seeding
as a secondary recovery action. A refresh updates displayed broker metrics only after the response
confirms success. Failure keeps the current/manual values visible and records diagnostic feedback.

Alternative considered: optimistic metric replacement while refresh runs. Rejected because it can
display unverified financial values.

### 7. Use a shared operation-status contract

Dashboard refresh, synchronization, import, and seed actions expose a common client state:
`idle`, `loading`, `success`, `stale`, or `error`. The initiating control receives a busy state and
spinner; a compact status region reports the current step, timestamp, outcome, retry action, and
diagnostics link when available. Existing content remains visible when refreshes fail.

No global blocking overlay is used for operations that can safely run in context. A blocking state
is reserved for operations whose next step would otherwise create duplicate submissions.

### 8. Keep hierarchy changes semantic and incremental

The page groups existing sections into Command, Today, Decision Tools, and Review & Reference.
Initial command and current-session content remains expanded. Long review/reference content stays
collapsed or summarized until requested. Existing section IDs and destination URLs remain stable
where practical to preserve deep links and scripts.

## Data Flow and Failure Behavior

1. A Dashboard trigger opens a registered surface and captures the invoking element.
2. The surface renders existing server data immediately and requests optional detail only when
   needed.
3. Mutating actions call existing form/API endpoints with the current CSRF and authentication
   behavior.
4. Successful responses update the originating Dashboard card, the open surface, and relevant live
   regions.
5. Failed responses preserve the prior visible data, show retry/diagnostic guidance, and return
   focus to a useful control.
6. Closing restores focus to the invoking control and removes scroll lock.

No market, trade, or broker response is persisted by the interaction layer itself. Server handlers
remain authoritative for validation and storage.

## Risks / Trade-offs

- [Risk] A shared controller could break lazy calendar rebinding. → Use idempotent initialization,
  fragment-scoped queries, and replacement regression tests.
- [Risk] Large modals can simply move the Dashboard's density into overlays. → Limit each surface
  to one task, use progressive disclosure, and keep glanceable status inline.
- [Risk] Keyboard shortcuts can conflict with text entry. → Ignore shortcuts while editable fields
  are active and provide a visible palette trigger.
- [Risk] Pressure Check state and behavior logging can diverge on a network error. → Apply the
  conservative local operating mode, mark logging as pending, and expose retry feedback.
- [Risk] Session details could accidentally broaden trade scope. → Pass an explicit ISO date and
  assert selected-day presence plus adjacent-day absence at the receiving route.
- [Risk] Live broker refreshes can overwrite trusted manual values. → Render refresh results only
  after confirmed success and retain manual values and diagnostics on every failure path.
- [Risk] Responsive drawers can obscure navigation. → Use a full-height mobile sheet with an
  explicit close control, safe-area spacing, and restored focus.

## Migration Plan

1. Add the controller, shared styles, and automated accessibility/lifecycle checks without changing
   current triggers.
2. Convert Pressure Check and verify discipline state and behavior logging.
3. Convert calendar preview to the session inspector and verify one-day receiving-page scope.
4. Convert broker metrics to the drawer and verify manual-value preservation on refresh failure.
5. Add the command palette and shared operation feedback.
6. Apply the four-level page hierarchy and run responsive/browser regression checks.
7. Rebuild the application container, verify `/healthz`, and inspect the deployed Dashboard flows.

Rollback is file-level: restore the previous Dashboard template, JavaScript, and CSS while leaving
existing server data untouched. New optional read endpoints can remain unused or be reverted
independently.

## Implementation Discovery

- The calendar fragment already carries the selected ISO date, P/L, record, balance, session state,
  and one-day Trades URL. The inspector can link that date into existing Trades, Journal, Analytics
  Replay, and reconciliation routes without adding a new financial-data endpoint.
- Pressure Check can reuse the existing reflection urgency field and behavior update endpoint. The
  workflow will copy its category/note into that field and request the existing debounced save,
  avoiding a persistence migration.
- Existing planning and tape refresh handlers already preserve confirmed content on failure. The
  shared operation layer will add consistent visible status around those handlers instead of
  duplicating their requests.
