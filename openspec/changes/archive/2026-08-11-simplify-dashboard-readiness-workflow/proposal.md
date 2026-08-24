## Why

The dashboard's remaining Prepare area still repeats readiness, priority, behavioral guidance, and
permission information, while a verbose healthy Ops Band competes with decision-critical content.
The next pass should make preparation and navigation faster without weakening any guardrail.

## What Changes

- Consolidate Operating Zone, behavioral guidance, readiness progress, priority, and permission
  checklist into one Session Readiness surface.
- Keep the score, current blockers, priority, and next action immediately visible; disclose the full
  checklist and sync controls on demand.
- Collapse the Ops Band to a quiet health indicator when healthy and automatically expose useful
  detail when attention is required.
- Standardize dashboard workflow language to `Command → Prepare → Execute → Review`.
- Preserve existing readiness calculations, permission controls, sync behavior, market/account
  inputs, and user-entered discipline state.
- Non-goals: changing trading eligibility, strategy logic, import execution, persistence, market
  data, financial calculations, or other pages.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `dashboard-decision-workflow`: Extend the dashboard hierarchy contract with consolidated session
  readiness, attention-aware Ops Band disclosure, and canonical workflow labels.

## Impact

- Dashboard Jinja template, shared Ops Band template/state attributes, dashboard CSS/JavaScript,
  and focused rendering/interaction tests.
- No API, database, dependency, or financial-assumption changes.
- Acceptance: one Session Readiness surface, healthy Ops Band reduced to a compact summary,
  attention states still explicit, and the four workflow labels rendered consistently.
