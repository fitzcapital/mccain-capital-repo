## Context

The route already supplies all required month, day, cycle, macro, and catalyst data. The problem is
information architecture: the template renders several derived summaries beside the canonical
calendar/day-profile pair, causing the same date, reset count, macro count, and cluster status to
appear repeatedly. Candle Opens is also accessible through Tools but absent from the desktop primary
navigation.

The change is intentionally presentation-only. Existing server calculations and route data remain
authoritative.

## Goals / Non-Goals

**Goals:**

- Put Candle Opens directly after Market Pulse in the primary desktop workflow.
- Make calendar scanning and selected-day inspection the visual center of the page.
- Preserve context while reducing repeated labels, cards, and expanded sections.
- Keep the page readable at desktop, compact desktop, tablet, and mobile widths.

**Non-Goals:**

- Recalculate timing cycles or reinterpret macro data.
- Add execution signals, alerts, polling, persistence, or external data sources.
- Change routes or remove access to any underlying timing detail.

## Decisions

### 1. Promote one desktop navigation entry

Add Candle Opens after Market Pulse in the desktop primary navigation. Remove only the duplicate
desktop Tools-menu link; keep the responsive drawer/mobile destinations because those layouts do not
show the full desktop row.

Alternative considered: leave Candle Opens under Tools. Rejected because the user treats it as a
regular preparation workspace, not an occasional utility.

### 2. Keep the calendar and day profile as the canonical detail pair

Calendar cells remain the scanning surface. Selecting a cell updates the existing Day Profile, which
remains the only expanded description of that day. The Today strip is reduced to date, importance,
reset count, macro count, and a jump/select-today action.

Alternative considered: remove the Today strip entirely. Rejected because a stable current-session
orientation is useful when another date is selected.

### 3. Deduplicate catalysts by date before presentation

Build a short display collection from existing catalyst summaries, keyed by session date. Merge
reason labels such as reset cluster, macro collision, or largest cluster into badges on one date card.
Show the next three unique dates, with the remainder available through expansion.

Alternative considered: retain one card per derived statistic. Rejected because several statistics
can legitimately resolve to the same date and create visually identical cards.

### 4. Collapse reference material, not operational context

Monthly counts and day/week/month cycle legends become one Timing Reference disclosure. The macro
agenda remains a separate disclosure because it contains date-specific events rather than static
legend material.

### 5. Scope layout behavior to Candle Opens

Use page-scoped classes and existing canvas tokens. Avoid shared global width overrides. At narrower
desktop widths the calendar and profile stack predictably; no card is allowed to create horizontal
page overflow.

## Risks / Trade-offs

- [A catalyst can have several meanings] -> Merge meanings into badges while preserving the full
  underlying source list in the disclosure.
- [Removing a Tools entry could reduce discoverability in compact layouts] -> Remove it only from the
  full desktop Tools menu and retain responsive/mobile access.
- [Template-only grouping could duplicate server logic] -> Keep calculations in existing context and
  limit presentation code to ordering, date-keyed grouping, and labels.
- [Existing contract tests expect old region names] -> Update focused tests to assert the new hierarchy
  and retained details rather than deleted wrapper names.

## Migration Plan

1. Update navigation and template hierarchy behind existing routes.
2. Add scoped responsive styles and minimal disclosure behavior.
3. Run focused route/template/navigation tests and syntax checks.
4. Rebuild the Podman app, verify `/healthz`, and inspect desktop plus responsive renders.
5. Roll back by reverting the template/style/test commit; there is no data migration.

## Open Questions

None. The default is three unique catalyst dates before expansion and retention of all underlying
timing data.
