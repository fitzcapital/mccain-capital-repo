## Context

The first dashboard consolidation established a single Command Center and Execution Plan. The
remaining Prepare surface still spreads the same preparation state across a hero, behavioral strip,
readiness meter, priority bar, checklist, and sync rail. The global Ops Band also renders verbose
healthy guidance that should only be needed during an exception.

## Goals / Non-Goals

**Goals:**

- Present session preparation as one coherent readiness card.
- Retain blockers and the next action above the disclosure boundary.
- Make healthy operational status quiet and attention states explicit.
- Use one four-stage vocabulary throughout the dashboard.

**Non-Goals:**

- Recalculate readiness, permission, sync, discipline, or strategy state.
- Remove any action or diagnostic detail.
- Change persisted data, endpoints, background work, or other pages.

## Decisions

1. **Consolidate through composition, not new state.** Existing readiness, checklist, priority, and
   sync values remain authoritative and are reorganized into one surface. A new readiness service
   was rejected because no business-rule change is needed.
2. **Use a visible summary plus native disclosure.** Score, blockers, priority, and next action remain
   visible. Full behavioral guidance, permission items, and sync controls live inside an accessible
   `details` region.
3. **Drive Ops Band disclosure from existing tone/state.** Healthy state renders as a compact summary;
   warning/error states remain expanded or prominent. Hiding the component entirely was rejected
   because status must remain discoverable.
4. **Standardize only user-visible workflow terms.** Existing element IDs and functional hooks stay
   stable where possible to avoid breaking navigation or client behavior.

## Risks / Trade-offs

- [Actions become one click deeper] -> Keep the primary next action and blockers outside the fold.
- [Ops state may be misclassified] -> Reuse existing tone/status attributes and keep warning/error
  content visible.
- [Existing JavaScript depends on hero nodes] -> Preserve readiness and checklist hooks while moving
  their containing markup.
- [Responsive layout regresses] -> Add focused mobile grid rules and receiving-page checks.

## Migration Plan

Deploy as template, CSS, and small interaction updates. No data migration is required; rollback is
limited to the affected presentation files.

## Open Questions

None.
