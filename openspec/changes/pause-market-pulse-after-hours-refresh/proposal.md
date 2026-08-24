## Why

Market Pulse continues advertising and running its live-session polling cadence after the
regular market closes, even though the API labels the phase closed. This creates misleading
"live" updates, unnecessary provider work, and weakens trust in execution-state timing.

## What Changes

- Make the canonical refresh contract phase-aware and explicitly pause automatic market-data
  refresh outside the regular session.
- Preserve the last verified session snapshot after hours instead of presenting it as live data.
- Keep manual refresh available for deliberate diagnostics without restarting automatic polling.
- Resume automatic polling when the next regular session begins, including when a page remains
  open overnight.
- Show a clear market-closed state instead of a live refresh countdown.
- Add focused server and browser-contract tests for open, closed, manual, and resume behavior.
- Non-goals: adding extended-hours execution, changing market-data vendors, or changing strategy
  calculations and gamma thresholds.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Automatic refresh scheduling must respect regular-session
  phase and clearly distinguish retained after-hours data from live execution data.

## Impact

- Affects the Market Pulse context API refresh contract, page-side canonical scheduler, status
  copy, and focused tests.
- Uses the existing Eastern-time regular-session rule and cached canonical snapshot. No financial
  values, strategy logic, persistence schema, or external dependencies change.
- Acceptance: after 4:00 PM ET the page performs no recurring provider refresh, labels the data
  retained/closed, allows a manual diagnostic refresh, and automatically resumes polling during
  the next regular session.
