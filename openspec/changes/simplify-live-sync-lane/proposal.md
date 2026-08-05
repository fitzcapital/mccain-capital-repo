## Why

The Dashboard and Live Sync lane can report contradictory outcomes for the same run, and the
Dashboard quick action can silently reuse stale dates, broker-account context, or diagnostic-only
settings. The primary desktop workflow should be safe, understandable, and consistent before the
recovery tooling is visually refined.

## What Changes

- Introduce one canonical Live Sync view state shared by the Dashboard and full Live Sync lane.
- Distinguish ready, running, completed, no-new-trades, diagnostic-only, cancelled, failed, and
  recovery-required outcomes; a cancelled or diagnostic-only run never counts as a completed
  import.
- Replace the opaque Dashboard "Run Last Sync" behavior with a transparent "Sync Today" action
  that previews the selected ledger, broker account, ET date, mode, and import intent.
- Validate that the selected local ledger maps to the broker account before starting an import.
- Default manual Dashboard syncs to today's ET date and normal import mode instead of silently
  inheriting a stale date window or diagnostic-only setting.
- Simplify the desktop Live Sync lane around one primary workflow, while moving diagnostics,
  recovery, artifacts, and destructive controls into contextual advanced sections.
- Clarify the scheduled after-market sync state, last attempt, last successful import, next run,
  and whether manual action is still needed today.
- Align reliability counts and failure guidance with the canonical outcome model.

## Capabilities

### New Capabilities

- `live-sync-lane`: Defines safe broker-to-ledger sync initiation, canonical run outcomes, shared
  Dashboard/lane presentation, automation visibility, and contextual recovery behavior.

### Modified Capabilities

None. The repository currently has no main capability specifications for Live Sync.

## Impact

- Flask sync orchestration and Dashboard state services under `mccain_capital/services/`.
- Dashboard and Live Sync Jinja templates plus their JavaScript and CSS behavior.
- Existing broker sync job/history records remain the data source; no credential, ledger, trade,
  or financial projection schema change is intended.
- The selected local ledger, its configured broker account identifier, the ET calendar date, the
  requested sync mode, and explicit import/debug intent become required preflight inputs.
- Existing saved requests remain reusable only as visible suggestions; they cannot override the
  safety defaults for a new Dashboard sync.

## Non-Goals

- Replacing broker automation, credential storage, trade parsing, or ledger accounting.
- Adding mobile-first layouts; this change prioritizes the desktop application.
- Automatically mutating manual drawdown or other manually maintained account values.
- Removing recovery or diagnostic capabilities; they are reorganized and shown contextually.
- Changing after-market scheduling policy beyond making its state and next action clear.

## Acceptance Criteria

- Dashboard and full Live Sync lane render the same canonical outcome for the same latest run.
- Cancelled, failed, and diagnostic-only runs are never labeled "Import complete" or counted as a
  successful import.
- A Dashboard sync shows its account, date, mode, and import intent before execution.
- A Dashboard sync cannot start when the selected ledger and broker account do not match.
- A Dashboard sync uses today's ET date and normal import mode unless the operator explicitly
  chooses another option in the full lane.
- Normal desktop operation exposes one primary sync action; recovery controls appear only through
  Advanced or when the canonical state requires recovery.
- The desktop lane uses exactly three top-level visual regions: compact status, primary Sync Today
  workspace, and one collapsed Advanced/Recovery drawer; it does not repeat status in separate
  hero, operator, feedback, failure, and tools cards.
- The Dashboard permission-to-trade panel presents sync as one compact readiness item with a
  concise primary action, while detailed preflight, history, recovery, and account controls link
  to the full Live Sync lane instead of forming a second dashboard inside the checklist.
- Targeted tests cover the outcome mapping, date/default behavior, account mismatch rejection,
  diagnostic-only handling, and consistent Dashboard/full-lane presentation.
