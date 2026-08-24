## 1. Date projection model

- [x] 1.1 Add explicit date and weeks horizon parsing with bounded validation and compatibility defaults.
- [x] 1.2 Calculate inclusive weekday sessions, partial-week schedule rows, equivalent trading weeks, and date-window metadata.
- [x] 1.3 Add optional target-balance required pace and status calculations.
- [x] 1.4 Add conservative, base, and stretch scenarios that reconcile to primary totals.
- [x] 1.5 Extend PDF output with date assumptions, target analysis, and scenario outcomes.

## 2. Planning interface

- [x] 2.1 Add compact horizon-mode controls, target date, and optional target balance inputs.
- [x] 2.2 Add date-window, target pace, scenario, assumption, and validation output sections.
- [x] 2.3 Update browser rendering and horizon synchronization while retaining the last valid projection on failure.
- [x] 2.4 Add responsive styling for the expanded planning workspace and partial-week schedule.

## 3. Verification

- [x] 3.1 Add focused service and API tests for partial weeks, boundaries, invalid ranges, targets, scenarios, and legacy weeks mode.
- [x] 3.2 Add page/frontend contract coverage for new controls, output hooks, and error handling.
- [x] 3.3 Run focused pytest, JavaScript syntax checking, `git diff --check`, and strict OpenSpec validation.
- [x] 3.4 Rebuild the local application, verify `/healthz`, verify the live desktop planner and responsive CSS contract, and leave subjective visual inspection to the user.

## 4. Explicit update redesign

- [x] 4.1 Group inputs into compact Account, Projection Window, and Tax & Reserve sections.
- [x] 4.2 Add a primary Update Projection submit action with dirty, updating, success, and error states.
- [x] 4.3 Remove per-keystroke projection requests while preserving one initial calculation and last-valid-result failure behavior.
- [x] 4.4 Tighten the responsive visual hierarchy and move PDF export into the projection action/status area.
- [x] 4.5 Add focused frontend contracts and rerun verification against the rebuilt live page.

## 5. Visual planning tool

- [x] 5.1 Add goal gap, verdict, adjustment, and scenario completion-date outputs to the server projection.
- [x] 5.2 Replace the output hierarchy with a target-first verdict and compact key metrics.
- [x] 5.3 Add balance trajectory, pace comparison, and scenario outcome SVG charts using server results.
- [x] 5.4 Collapse assumptions and the full schedule by default while preserving reconciliation detail.
- [x] 5.5 Add focused tests, rebuild, and verify target and no-target states on the live page.
