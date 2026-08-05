## Context

The Dashboard is a large Jinja-rendered operating surface backed by Flask view models and a
substantial vanilla-JavaScript controller. It combines session discipline, readiness, planning,
gamma context, live tape, broker and account controls, performance, health, pace, reflection,
and a lazy-loaded calendar. Its behavior is already covered by focused route and interaction
tests, but its visual language has accumulated many Dashboard-specific rules in `app.css` and
multiple generations of button, card, and hierarchy treatments.

The redesign must improve the interface without treating the existing markup as disposable.
IDs, `data-*` hooks, form actions, links, ARIA state, local storage keys, endpoint contracts, and
lazy-fragment rebinding are compatibility boundaries. Existing unrelated worktree changes must
remain untouched.

## Goals / Non-Goals

**Goals:**

- Establish a modern, coherent Dashboard visual hierarchy and control system.
- Make the operating sequence and next action obvious while reducing perceived density.
- Preserve every current capability and all server/client integration contracts.
- Improve responsive behavior, keyboard use, focus visibility, touch targets, and state clarity.
- Make implementation reviewable through phased diffs, focused tests, and visual captures.

**Non-Goals:**

- Rebuild Dashboard business logic or API contracts.
- Introduce a frontend framework or design-system dependency.
- Redesign other application pages or global navigation.
- Change financial, broker, market-data, or trading calculations.
- Modify runtime data or migrate persisted user settings.

## Decisions

### 1. Modernize in place behind Dashboard-scoped presentation rules

Retain the Jinja and vanilla-JavaScript architecture and introduce a small set of reusable
Dashboard component classes and CSS custom properties scoped under `.page-dashboard`.

This is preferred over a framework rewrite because it limits regression risk, preserves
server-rendered behavior, and avoids maintaining two UI stacks. A framework migration was
rejected as unrelated to the visual outcome and too risky for functional parity.

### 2. Preserve behavioral hooks as explicit compatibility contracts

Create a pre-change inventory of links, buttons, forms, disclosures, dialogs, IDs, `data-*`
hooks, endpoint URLs, persisted keys, and lazy-loaded fragments. Markup may be regrouped for
hierarchy, but behavioral hooks SHALL not be renamed or removed unless the corresponding
JavaScript and focused regression coverage are deliberately updated together.

This is preferred over recreating handlers after the redesign because the current controller
contains stateful, async, accessibility, and rebinding behavior that is easy to lose visually.

### 3. Use an operating-sequence information architecture

Organize emphasis around Prepare, Plan, Monitor, Support/Health, Performance, Pace, and Review
without inventing new workflows. Discipline, readiness, permission, and the next action remain
above supporting metrics. Lower-frequency controls use existing disclosures with clearer
summaries rather than being removed.

This is preferred over a generic card grid because the Dashboard is an operational workflow,
not a collection of equally important widgets.

### 4. Standardize controls by intent

Define Dashboard variants for primary, secondary, quiet, danger, toggle/segmented, and icon-only
controls. All variants share minimum target size, focus ring, disabled state, loading state,
icon alignment, and responsive wrapping behavior. Existing `.btn` semantics remain compatible;
new classes refine Dashboard context rather than globally restyling the application.

This is preferred over one universal button treatment because action hierarchy and risk need
clear visual differentiation.

### 5. Deliver in visually verifiable phases

Implementation proceeds through baseline inventory, tokens and controls, upper operating zone,
planning and monitoring, support/performance/calendar, then responsive and accessibility polish.
Each phase runs narrow automated checks and produces authenticated desktop/mobile captures.

This is preferred over a single large template rewrite because phased review makes missing
functionality and CSS regressions easier to isolate and roll back.

### 6. Keep data flow and failure behavior unchanged

Server-rendered values continue to come from current view models. JavaScript continues to use
the existing planning, tape, sync, behavior, broker, and calendar endpoints. Loading, stale,
missing, failed, and diagnostic states remain explicit. Manual broker values are never replaced
unless the existing refresh or seed workflow succeeds.

## Risks / Trade-offs

- [Risk] Moving markup breaks selectors or event binding. → Inventory hooks first, preserve IDs
  and `data-*` attributes, and add focused assertions for critical controls.
- [Risk] New CSS loses to late theme overrides in the large stylesheet. → Keep final
  Dashboard-scoped rules in one documented layer and test all outcome/state variants.
- [Risk] Visual simplification hides low-frequency functionality. → Use labeled progressive
  disclosure and verify every inventoried control remains reachable.
- [Risk] A large redesign diff becomes difficult to review. → Implement and verify in phases,
  keeping behavior changes separate from presentation changes where possible.
- [Risk] Live data appears fresh when stale. → Preserve current tone, timestamp, disabled,
  diagnostic, and failure semantics in both markup and tests.
- [Risk] Mobile stacking changes keyboard or reading order. → Keep DOM order aligned with the
  operating sequence and verify keyboard traversal at narrow and wide widths.
- [Trade-off] Retaining the current architecture limits radical component reuse. → Favor low-risk
  consistency now; extract broader shared components only in a separately specified change.

## Migration Plan

1. Capture the existing authenticated Dashboard at representative viewport sizes and record the
   complete control/behavior inventory.
2. Add scoped tokens and control variants without moving sections; verify functional parity.
3. Modernize and regroup one operating region at a time, running focused tests and captures
   after each phase.
4. Remove only Dashboard CSS rules proven obsolete by targeted search and visual verification.
5. Run the focused Dashboard regression set, JavaScript syntax check, authenticated route smoke,
   and final visual comparison.

Rollback is file-level: revert the latest presentation phase while retaining prior verified
phases. No database or settings migration is required.

## Open Questions

- Final visual preference—restrained professional command center versus a more luminous trading
  cockpit—will be resolved from the first visual implementation review without changing the
  functional specification.
- Whether to extract Dashboard CSS into a dedicated page stylesheet will be decided after the
  baseline cascade audit; the default is to keep a single documented Dashboard-scoped layer in
  the existing stylesheet to minimize loading and ordering changes.
