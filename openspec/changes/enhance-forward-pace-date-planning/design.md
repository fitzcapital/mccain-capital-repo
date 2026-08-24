## Context

The standalone planner sends a compact input payload to `/api/forward-pace/projection`, where the server calculates weekly tax, buffer, net pace, schedule rows, and totals. Today the server loops over an integer week count, so partial date windows and deadline-driven planning are not representable. The browser only renders server results and PDF export calls the same calculator; that server-authoritative pattern should remain.

## Goals / Non-Goals

**Goals:**

- Make a start-to-target date window the primary detailed planning mode.
- Keep weeks-based projections backward compatible and useful as a quick control.
- Derive all target, scenario, schedule, and export values from one server calculation.
- Explain enough timing and financial assumptions for the numbers to reconcile.
- Keep the interface compact and responsive while making projection updates explicit.

**Non-Goals:**

- Import market calendars, broker activity, or journal results.
- Persist planner inputs.
- Replace the existing tax model or claim filing accuracy.
- Change the separate Dashboard Forward Pace model.

## Decisions

### 1. Use explicit horizon modes

The payload will accept `horizon_mode` as `date` or `weeks`. Date mode uses `start_date` and `target_date`; weeks mode derives its target date from `weeks`. Existing requests without a mode remain weeks-based for compatibility.

Treating any supplied target date as silently authoritative was considered, but an explicit mode prevents stale hidden fields from unexpectedly changing calculations.

### 2. Prorate by inclusive weekdays

Date mode will count Monday-Friday dates inclusively and calculate each day as one fifth of the weekly gross, tax, buffer, and net amounts. Schedule rows group those daily values by Monday-based calendar week, so partial first and last weeks reconcile exactly to totals.

Rounding a partial window up to full weeks was rejected because it overstates outcomes. Exchange holidays will not be removed because the project has no authoritative market-calendar dependency; the UI and export will say so.

### 3. Keep tax rate derivation annualized from the weekly plan

The existing weekly gross remains the annualization basis for federal and state planning rates. Date-window totals prorate the resulting weekly tax across weekday sessions. This preserves existing tax behavior while changing only the selected horizon.

### 4. Make target balance optional

When `target_balance` is above the base balance, the server will calculate the remaining amount and required net pace per weekday session and per five-session week. Status compares the current modeled net pace with required pace and returns `ahead`, `on_track`, or `behind`. A missing or non-actionable target produces a neutral state instead of invented requirements.

### 5. Compare fixed pace multipliers

Scenario cards will use 75% conservative, 100% base, and 125% stretch multipliers applied to the calculated net daily pace for the same session count. The multipliers will be returned by the server and visible in the UI and PDF.

### 6. Validate before returning projection output

The service will raise a projection validation error for an invalid date, a target before the start, a date window with no weekdays, or a window beyond two years. API and PDF handlers will return a clear 400 response rather than replacing valid results with defaults. Invalid optional numeric values continue to use existing bounded defaults where compatibility requires it.

### 7. Use an explicit projection commit interaction

The form will load one initial server projection, then treat later edits as a draft. Editing any input marks the workspace as having unapplied changes and enables a prominent Update Projection button. Submitting the form fetches one authoritative projection, redraws all result regions, and records a visible update time. This avoids continuous network requests and makes it clear which inputs produced the displayed result.

Auto-updating on every keystroke was considered but rejected because the resulting numbers can appear authoritative before a multi-field edit is complete.

### 8. Group controls by planning intent

Inputs will be grouped into Account, Projection Window, and Tax & Reserve cards. The primary submit action and its status sit in a fixed footer within the input panel, while PDF export remains a secondary output action. This creates a clear input-to-action-to-result flow without increasing page density.

### 9. Render decision charts without a new chart dependency

The browser will render accessible SVG charts from the server projection: an account-balance line with starting and target reference values, a two-bar current-versus-required pace comparison, and scenario outcome bars. Native SVG keeps the page lightweight and makes every plotted value traceable to the same API response.

### 10. Derive scenario completion dates on the server

For each pace multiplier, the service will divide the target gap by scenario daily net pace, round up to a whole weekday session, and advance from the selected start date while skipping weekends. The estimate will be labeled as weekday-based and may extend beyond the selected projection window.

### 11. Lead with verdict and progressively disclose detail

When a target exists, the primary result will state projected surplus or shortfall, current and required weekly pace, and a direct adjustment. When no target exists, it will prompt for one. Tax assumptions and the full schedule remain available inside collapsed detail regions so they support verification without dominating the initial page.

## Risks / Trade-offs

- **Weekday counts can differ from actual market sessions** -> Label the calculation as Monday-Friday with holidays not excluded.
- **Daily prorating can create cent-level differences across rows** -> Round display values but derive totals from the unrounded daily rate.
- **Two horizon controls can feel ambiguous** -> Use a visible mode switch and disable the inactive horizon input.
- **Long windows can make schedules unwieldy** -> Cap date mode at two years and retain a scrollable compact schedule.
- **Target status could be mistaken for a trading recommendation** -> Describe it strictly as cashflow pace against a user-entered balance goal.

## Migration Plan

No data migration is required. Deploy service, template, JavaScript, CSS, and focused tests together. Existing weeks-only API consumers continue to work. Rollback restores the previous page assets and calculator without stored-data changes.

## Open Questions

None. The first version intentionally uses weekday sessions and visibly excludes exchange-holiday awareness.
