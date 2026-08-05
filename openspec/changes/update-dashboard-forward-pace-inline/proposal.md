## Why

Saving Forward Pace settings from the Dashboard currently redirects and reloads the entire page, interrupting the user's position and making a small planning adjustment feel expensive. The Dashboard should save and redraw only the Forward Pace card while preserving the existing projection rules and a reliable non-JavaScript fallback.

## What Changes

- Submit the Dashboard Forward Pace form asynchronously when JavaScript is available.
- Persist the same daily pace, pass buffer, start date, target date, and live-pace reset settings used today.
- Return an authoritative server-rendered Forward Pace card fragment after a successful save and replace only that card in the Dashboard.
- Show compact saving, success, and failure feedback without moving the user away from the card.
- Distinguish the actual projected account balance from the amount available above the configured pass buffer by adding an `Available Balance After Buffer` output.
- Preserve the current full form submission and redirect as progressive-enhancement fallback behavior.
- Preserve all existing projection sources, trading-day timing assumptions, account allocation, and manual-value behavior while making the existing buffer treatment explicit and preventing double subtraction.

### Non-goals

- Changing tax calculations, trading-day rules, live-pace derivation, or the meaning of the existing Projected Balance and Projected Profit outputs.
- Introducing automatic polling or background broker refreshes.
- Redesigning the standalone `/forward-pace` planner.
- Refreshing unrelated Dashboard cards after a Forward Pace save.

### Acceptance Criteria

- Saving or resetting Forward Pace with JavaScript enabled does not perform a full-page navigation or change the Dashboard scroll position materially.
- The updated card reflects the server-persisted values and recalculated projection immediately after success.
- Invalid dates and server failures produce accessible inline feedback and do not replace the existing card with partial or untrusted state.
- The form still saves through the existing redirect workflow when JavaScript is unavailable.
- Existing Forward Pace calculations and manually entered values remain unchanged unless the save succeeds.
- Projected Balance remains the expected account balance before reserving the pass buffer, Projected Profit remains profit after the buffer, and Available Balance After Buffer equals Projected Balance minus the buffer exactly once.

## Capabilities

### New Capabilities

- `dashboard-forward-pace-inline-update`: Asynchronous persistence and authoritative partial rendering of the Dashboard Forward Pace card with accessible status feedback, progressive fallback, and explicit actual-versus-available balance presentation.

### Modified Capabilities

None.

## Impact

- Dashboard route handling and Forward Pace view-model/fragment rendering under `mccain_capital/`.
- `mccain_capital/templates/dashboard.html` or a focused extracted Dashboard partial.
- Dashboard JavaScript that intercepts and submits the Forward Pace form.
- Focused Flask and frontend contract tests for success, reset, validation, failure, and fallback behavior.
- Focused view-model and rendering coverage for projected balance, projected profit after buffer, and available balance after buffer.
- No new third-party dependencies, database schema changes, runtime-data edits, or external service changes.
