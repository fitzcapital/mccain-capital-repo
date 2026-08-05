## Context

The authenticated Trading Dashboard and Market Pulse share a mature dark command-center visual
system and a large common shell. Browser audit at 1292 x 964 found no horizontal overflow, but the
Dashboard rendered roughly 12,600 pixels of content with 150 buttons and 171 links. At 390 x 844,
the default Dashboard remained roughly 12,400 pixels tall and fixed navigation overlapped content.
Market Pulse has a stronger overall hierarchy but allows data health, permission, gamma status,
levels, and supporting analysis to compete in the initial view.

The audit also found the shared notification initializer calling `formatEt` before the `const`
formatter is initialized, producing a `ReferenceError` on every inspected primary page. The
current financial, account, market-data, gamma, and trading-gate behavior is authoritative and
must not change.

## Goals / Non-Goals

**Goals:**

- Make each page's current state and next correct action legible within the first viewport.
- Use progressive disclosure to preserve detail while reducing default cognitive load.
- Establish one state vocabulary for live, stale, unavailable, loading, and locked conditions.
- Shorten default mobile flows and guarantee fixed-navigation clearance.
- Eliminate the shared notification initialization error.
- Preserve existing source authority, calculations, refresh precedence, and fallbacks.

**Non-Goals:**

- Recalculate or reinterpret financial, risk, consistency, gamma, or trading-gate values.
- Change routes, providers, API schemas, persistent data, or manual-value precedence.
- Redesign unrelated pages or replace the current visual identity.
- Delete advanced analysis that remains useful through intentional disclosure.

## Decisions

### 1. Recompose existing sections before inventing new components

Implementation will first reorder, consolidate, and summarize existing Jinja fragments and cards.
The first viewport will be treated as a decision sequence rather than an inventory.

**Rationale:** Existing content and styling are already strong, and reusing them minimizes
regression risk.

**Alternative considered:** A new unified frontend component layer. Rejected because it adds
architecture and migration risk without being necessary for this information-architecture change.

### 2. Use native disclosure semantics where practical

Secondary diagnostics, history, calendar detail, and advanced gamma context will use existing
tabs or native `details`/`summary`-style behavior enhanced by minimal JavaScript only where the
current interaction model requires it. Expanded state must remain keyboard-operable and clearly
labeled.

**Rationale:** Native disclosure reduces custom focus and state-management code.

**Alternative considered:** Hide secondary content at mobile breakpoints. Rejected because hiding
would remove access rather than reduce default density.

### 3. Keep server and service payloads authoritative

Templates and client code will reorganize presentation around the current payload fields.
No new frontend calculation will reinterpret P&L, consistency, account scope, gamma levels,
freshness, permission, or trigger state.

**Rationale:** This separates visual cleanup from financial and trading business rules.

**Alternative considered:** Build a new aggregate endpoint tailored to each first viewport.
Rejected unless implementation proves the existing payloads cannot express the required state.

### 4. Separate freshness, availability, and permission

Market Pulse will render three independent concepts:

- freshness: loading, live, or stale;
- availability: value present or unavailable;
- permission: permitted or locked by existing trading rules.

Dashboard state and gate messaging will use the same conceptual separation where applicable.

**Rationale:** Healthy data does not imply permission to trade, and unavailable data is not zero.

**Alternative considered:** Continue using composite status badges. Rejected because combined
badges caused ambiguity during the audit.

### 5. Fix formatter ordering at the shared-shell source

The Eastern-time formatter will be declared before any last-read initialization path can invoke
it, or converted to an ordering-safe function declaration. A focused regression contract will
anchor the ordering.

**Rationale:** The defect is shared-shell initialization, not a page-specific notification issue.

### 6. Treat mobile clearance and default length as contracts

Shared shell CSS will expose one bottom-clearance rule derived from the fixed navigation height
and `env(safe-area-inset-bottom)`. Target pages will collapse secondary sections by default rather
than merely stacking all desktop content.

**Rationale:** Additional padding alone fixes overlap but not excessive mobile scanning.

## Data Flow and Failure Behavior

Existing Flask handlers and services continue producing page contexts and JSON payloads. Jinja
renders the initial authoritative state; existing JavaScript refresh paths update the same
semantic fields. Presentation code maps those existing states to explicit labels and disclosures.

On broker-refresh failure, manual dashboard metrics remain unchanged. On market or gamma failure,
the current fallback payload remains in force and the UI labels its freshness and availability
truthfully. No cleanup path writes to runtime or personal data.

## Risks / Trade-offs

- **[Risk] Collapsing content hides a signal an expert expects to scan continuously.**
  → Keep immediate-decision signals expanded, preserve all supporting content, and verify the
  classification in authenticated browser checks.
- **[Risk] Reordering cards breaks selectors or fragment rebinding.**
  → Preserve stable IDs and data attributes, update focused contracts, and verify lazy/refresh
  paths at the receiving page.
- **[Risk] New disclosure controls increase interaction cost.**
  → Collapse only secondary content and keep expanded state obvious and keyboard-operable.
- **[Risk] Existing dirty-worktree changes overlap target templates and CSS.**
  → Make narrow edits, inspect diffs before each patch, and avoid reverting unrelated changes.
- **[Risk] Mobile page-height assertions are brittle across real data.**
  → Test structural default-collapse contracts and overlap/overflow behavior rather than a fixed
  pixel-height ceiling.

## Migration Plan

1. Fix and test the shared-shell initialization order and mobile clearance.
2. Recompose Dashboard hierarchy and disclosures without changing its payloads.
3. Recompose Market Pulse hierarchy and state vocabulary without changing its payloads.
4. Run focused pytest contracts and JavaScript syntax checks.
5. Rebuild the local app through the established Podman workflow and verify authenticated desktop
   and 390-pixel behavior, including stale/unavailable/locked cases available in the local state.
6. Roll back by reverting only the targeted template, CSS, JavaScript, and test changes; no data
   migration is required.

## Open Questions

None blocking. During implementation, any section whose immediate-decision importance is
ambiguous will remain visible by default until authenticated browser evidence supports collapsing
it.
