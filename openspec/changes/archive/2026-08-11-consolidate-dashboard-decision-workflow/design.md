## Context

The dashboard currently renders the same decision state through several independently composed
panels. The duplication increases page length and permits status drift, particularly around import
readiness. The existing Flask context, Jinja templates, and lightweight client interactions remain
the appropriate architecture.

## Goals / Non-Goals

**Goals:**

- Establish one scan-first command hierarchy and one execution-plan presentation.
- Render import readiness from one resolved state and reuse it everywhere it is referenced.
- Preserve detail through accessible native disclosure controls.
- Keep current responsive behavior and progressive enhancement.

**Non-Goals:**

- Change trading strategy, permission, market, risk, or import business logic.
- Add persistence, endpoints, dependencies, or background jobs.
- Redesign market charts or other pages.

## Decisions

1. **Consolidate at the template boundary.** Existing context values will feed one Command Center
   and one Execution Plan rather than creating another client-side view model. This minimizes state
   duplication and keeps initial rendering authoritative. A client-rendered replacement was rejected
   because it adds loading and synchronization complexity without changing the underlying data.
2. **Resolve import readiness once.** The server context will expose or normalize one canonical
   import status whose label, tone, detail, and action are reused. Template-local interpretations are
   removed. A CSS-only reconciliation was rejected because it would hide rather than fix conflicting
   semantics.
3. **Use native disclosure.** Supporting content will use `details`/`summary` or the established
   accessible disclosure pattern so information remains keyboard accessible without more JavaScript.
4. **Use exception-first copy.** Healthy values remain compact; blockers retain a short cause and
   next action. This preserves safety information while reducing routine text.
5. **Keep one CTA row.** Execution navigation appears once within the Execution Plan; duplicate
   navigation groups are removed.

## Risks / Trade-offs

- [Users may initially miss moved supporting content] -> Keep descriptive disclosure labels and
  visible status summaries.
- [Existing JavaScript may target removed duplicate nodes] -> Search selectors and add focused
  rendering/interaction regression tests before removal.
- [Import normalization could alter legacy wording] -> Preserve the existing underlying states and
  actions; change only their single presentation.
- [Dirty worktree overlap] -> Limit edits to dashboard-specific files and avoid unrelated changes.

## Migration Plan

Deploy as a template/static-asset update with no data migration. Roll back the affected dashboard
template, context helper, and CSS if receiving-page checks fail.

## Open Questions

None. The user approved the three consolidation priorities and will perform subjective visual review.
