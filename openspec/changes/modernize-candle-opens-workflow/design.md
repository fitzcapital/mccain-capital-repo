## Context

The current Candle Opens template renders a month hero, Today Snapshot, five metric cards, focus
cards, cluster summaries, cycle details, macro details, and finally the calendar workspace. The
route already provides all values needed by the redesigned page, and the existing calendar markup
contains the data attributes used by day-selection behavior. This change is therefore a presentation
and information-architecture update, not a calculation or persistence change.

The implementation must coexist with a large shared stylesheet and a dirty worktree. New rules will
be scoped to `body.page-candle-opens` and a dedicated workflow root so other pages and unrelated
changes remain untouched.

## Goals / Non-Goals

**Goals:**

- Put the month timing map and selected-day intelligence at the center of the first workflow view.
- Consolidate repeated summary information while retaining every server-provided value.
- Create a compact, responsive command header and a clear Today action.
- Preserve all URLs, data attributes, event anchors, keyboard interactions, and mobile weekly folds.
- Add observable structure hooks and focused regression coverage.

**Non-Goals:**

- Recalculate resets, macro impact, market holidays, or importance.
- Introduce client-side persistence, new data fetching, or a new dependency.
- Change the meaning of any trading or timing signal.
- Modify global navigation or another menu destination.

## Decisions

### 1. Reorder existing sections instead of duplicating data

The template will establish five workflow regions: command header, today decision strip, primary
calendar workspace, catalyst strip, and macro/cycle reference. Existing route values and existing
calendar/day-profile markup will be moved into those regions. This avoids a new view model and keeps
server behavior stable.

Alternative considered: build a second compact calendar above the existing page. Rejected because
it would duplicate controls, IDs, and day-selection state.

### 2. Keep one canonical Today summary

The Today Snapshot becomes a compact decision strip. Trading-day totals and cycle definitions move
into a collapsed reference disclosure near the macro agenda. Focus and cluster cards remain
available after the calendar as the catalyst strip, but repeated cluster rows are visually grouped
rather than presented as competing primary cards.

Alternative considered: remove the metric and cycle content entirely. Rejected because the user
requested that no functionality or information be lost.

### 3. Preserve existing interaction hooks

Classes and `data-candle-day` attributes that existing behavior depends on remain intact. New
workflow classes provide layout hooks without renaming receiving anchors or link destinations.
Month navigation remains server-routed and day inspection remains client-side.

Alternative considered: replace the calendar with a new JavaScript component. Rejected because it
would add unnecessary regression risk to mature date and macro behavior.

### 4. Use a desktop workspace and progressive mobile folds

At desktop widths the calendar and Day Profile remain a two-column workspace with a sticky profile
rail. At narrower widths the page becomes one column and retains the existing weekly disclosures.
Calendar tags are visually limited through existing overflow summaries rather than removing data.

### 5. Use motion only as interaction feedback

Hover lift, selected-day emphasis, and Day Profile updates may use short transitions. Motion will
respect `prefers-reduced-motion`, and no ambient animation will distract from trading information.

## Risks / Trade-offs

- **Shared CSS cascade can override the new workflow** → Place a final, page-scoped Candle Opens
  workflow block in the existing stylesheet and add contract assertions for its marker and hooks.
- **Template reordering can break day-selection JavaScript** → Preserve IDs, classes, data
  attributes, and DOM content expected by existing handlers; run focused page and visual tests.
- **Sticky profile can consume space on short screens** → Enable stickiness only on sufficiently
  wide desktop layouts and disable it for tablet/mobile widths.
- **Consolidation can make reference details less obvious** → Use a labeled Cycle Reference
  disclosure with accessible summary text instead of deleting the information.
- **Stale or missing macro data can weaken the summary** → Continue rendering the existing fallback
  text and unavailable states supplied by the route.

## Migration Plan

1. Add workflow landmarks and reorder existing template sections.
2. Add isolated Candle Opens workflow styling and responsive rules.
3. Add contract tests for hierarchy, preserved controls, and layout hooks.
4. Run focused Flask tests, CSS/HTML checks, and authenticated visual captures.
5. Rebuild the application container and confirm `/healthz`.

Rollback is a template/CSS revert; no data migration or schema rollback is required.

## Open Questions

None. The user approved the calendar-first direction and immediate implementation.
