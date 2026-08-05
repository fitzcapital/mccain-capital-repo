## Context

The shared `.wrap` currently acts as both the application-chrome frame and each page's content
frame. Most routes inherit a 1280px cap, while Dashboard and Executive override it to 1440px and
the dormant standalone Budget template can reach 1600px. `/budget` currently redirects to
Executive. Because the header, navigation, and content share the same container,
changing a page's workspace width also moves the application chrome.

The implementation must preserve the existing Jinja route shell, page-specific grids, responsive
breakpoints, controls, data hooks, and dirty-worktree changes. This is a layout-contract change;
there are no APIs, persistence, data-source, security, or financial-calculation changes.

## Goals / Non-Goals

**Goals:**

- Keep desktop header and navigation edges aligned to a 1440px frame across routes.
- Assign every route an explicit standard, wide, or dense content mode.
- Preserve 1280px readability for text, form, journal, profile, and configuration workflows.
- Give Dashboard, Executive, Market Pulse, Candle Opens, and Analytics a 1440px workspace.
- Preserve the dense 1500-1600px mode for a directly rendered Budget template without changing
  the current redirect.
- Retain existing tablet/mobile collapse behavior and prevent horizontal overflow.
- Verify representative receiving pages and their controls, not only CSS selectors.

**Non-Goals:**

- Redesigning cards, forms, tables, charts, or navigation.
- Changing route destinations, business rules, financial assumptions, or data loading.
- Making every child card fill its available width.
- Introducing JavaScript-based sizing or a new CSS framework.

## Decisions

### Separate application chrome from workspace width

The outer `.wrap` will provide enough room for the selected workspace, while `.topbar` will receive
a stable 1440px maximum width and centered margins. `#pageShell` will receive its own mode-specific
maximum width. This uses the existing DOM and avoids adding wrapper elements around every page.

Alternative considered: set every `.wrap` to 1440px. Rejected because standard-page content would
also stretch and Budget would lose its useful dense-width exception.

### Use explicit shell-mode hooks and shared CSS tokens

`base.html` will assign a stable `shellMode-standard`, `shellMode-wide`, or `shellMode-dense` body
class from the active route. CSS variables will define the chrome, standard, wide, and dense caps.
Page-specific legacy width overrides will be consolidated or made subordinate to this contract.

Alternative considered: maintain a growing list of page selectors in multiple CSS sections.
Rejected because the current repeated overrides are the source of drift and make new menu pages
easy to classify incorrectly.

### Classify by information density

- Wide: Dashboard, Executive, Market Pulse, Candle Opens, and Analytics.
- Dense: a directly rendered Budget template; the current `/budget` redirect remains unchanged.
- Standard: remaining reading, form, journal, planning, profile, and operations destinations.

Exceptional charts, ladders, calendars, or tables may use the full width of their assigned page
shell, while prose continues to use existing character-based line-length constraints.

Alternative considered: classify all menu pages as wide. Rejected because forms and prose become
harder to scan and many cards gain empty space rather than useful information density.

### Keep responsive behavior CSS-only

Below the established desktop breakpoints, all shell modes will resolve to the available viewport
width with existing padding and single-column rules. No viewport JavaScript or persisted user
setting will be introduced.

## Risks / Trade-offs

- [Legacy selectors override the new tokens] -> Place one authoritative shell contract late enough
  in the cascade, remove only conflicting width declarations, and assert computed widths in browser
  checks.
- [Standard pages still contain isolated wide tables] -> Keep their page shell standard by default
  and use existing overflow/table containment rather than widening the entire route.
- [Dense Budget content misaligns with the header] -> Center the 1440px topbar independently inside
  the wider dense outer frame.
- [Responsive regressions] -> Check representative standard, wide, and dense pages at desktop,
  tablet, and mobile widths for document-level horizontal overflow.
- [Unrelated functionality changes during broad page work] -> Limit implementation to shell hooks,
  shared CSS, and focused tests; assert representative routes, controls, and form contracts.

## Migration Plan

1. Add shell-mode classification and CSS tokens without removing existing route functionality.
2. Consolidate conflicting width rules after computed-width tests confirm the new contract wins.
3. Run focused pytest contracts, including synthetic dense-mode rendering, and representative live
   browser viewport checks for standard and wide routes.
4. Rebuild the application container and verify `/healthz` after user approval.

Rollback is limited to reverting the shell-mode classes and shared CSS contract; no data migration
or persistence rollback is required.

## Open Questions

None. The agreed mode model is standard 1280px, wide 1440px, and dense up to 1600px when a dense
template is directly rendered, with a consistent 1440px application-chrome frame.
