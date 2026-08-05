## Context

Live Sync currently derives user-visible state in more than one place. The server-side Dashboard
summary understands some terminal statuses, while Dashboard JavaScript can replace that status
using only whether the last timestamp is today. The full lane separately renders system health,
last run, reliability, failure guidance, and automation state. This permits one cancelled job to
appear as completed, failed, and non-actionable at the same time.

The Dashboard quick action also treats the last request as an executable preset. That request can
contain an old date range, broker account, selected ledger context, headless/debug capture options,
or debug-only intent. Account selection and saved request state can therefore drift independently.

The existing broker worker, job records, credential storage, import parser, and recovery tooling
are mature enough to retain. The smallest safe design is to centralize interpretation and preflight
validation while simplifying how those capabilities are exposed.

## Goals / Non-Goals

**Goals:**

- Produce one immutable canonical view model from sync configuration, selected ledger, latest job,
  last successful import, and automation schedule.
- Make a Dashboard sync predictably mean a normal import for today in Eastern Time.
- Reject account ambiguity before any broker session or import starts.
- Present one desktop-first normal workflow and reveal operational tools contextually.
- Preserve all existing diagnostic, manual recovery, and reliability capabilities.

**Non-Goals:**

- Redesign broker automation or parsing internals.
- Change credentials, trade, ledger, or financial data schemas.
- Add mobile-first behavior.
- Auto-edit manual drawdown or account metadata.
- Change the configured after-market schedule.

## Decisions

### 1. Centralize state in a server-owned Live Sync view model

A service function will map persisted job facts to a fixed outcome enum: `ready`, `running`,
`completed`, `no_new_trades`, `diagnostic_only`, `cancelled`, `failed`, or `needs_recovery`. It will
also expose last attempt, last successful import, ran-today flags, automation state, and the safe
next action. Both templates and Dashboard polling responses will consume this same model.

Client JavaScript may render the returned model but must not infer a terminal outcome from a date
or timestamp. This is preferred over duplicating the mapper in JavaScript because server records
are authoritative and a single Python mapper is directly testable.

### 2. Separate attempt, import success, and daily completion

`last_attempt` describes the most recent terminal or active job. `last_successful_import` describes
the most recent job that actually completed an import, including a valid no-new-trades result.
`import_completed_today` is derived from the latter in Eastern Time. Cancellation, failure, and
diagnostic-only execution can set `attempted_today` but cannot set `import_completed_today`.

This is preferred over one overloaded "last run" field because reliability, operator guidance,
and automation each need different facts.

### 3. Treat Dashboard sync as a new safe request, not replay

The Dashboard action will construct a request from today's ET date, the currently selected local
ledger, its configured broker identifier, broker-fill import mode, and `debug_only=false`. Saved
credentials and non-destructive runtime defaults may be reused, but an old date range, broker ID,
or debug-only intent may not be inherited.

The UI will show this preflight summary before execution. Operators who need historical dates,
snapshot mode, or diagnostic-only execution will use the full lane, where those choices are
explicit.

### 4. Fail closed on account identity

Before starting a worker, the service will normalize and compare the selected ledger's configured
broker account identifier with the request broker identifier. Missing or mismatched identifiers
will return a validation response and no job will be created. Account identifiers will remain
masked in ordinary UI text while still giving enough ledger context to correct the selection.

This is preferred over silently rewriting one side because importing into the wrong ledger is a
higher financial-data risk than requiring an operator correction.

### 5. Use progressive disclosure for the desktop lane

The desktop lane will use three top-level visual regions and no additional peer status cards:

1. A compact horizontal status strip containing canonical outcome, today state, last attempt, and
   automation timing.
2. One primary `Sync Today` workspace containing preflight, selected ledger, credential readiness,
   the primary action, and current job progress/result.
3. One `Advanced & Recovery` drawer containing account editing, credentials, diagnostic test,
   historical/snapshot mode, artifacts, HTML recovery, reliability, automation configuration, and
   force/reset controls. The drawer opens automatically only for `needs_recovery`.

The page hero will provide navigation and orientation only; it will not repeat sync status. The
former Operator Deck, Run Feedback, Failure Guide, and Tools cards will be consolidated into the
primary workspace or the single drawer instead of remaining separate top-level cards.

Existing tools remain reachable. This reduces normal-path cognitive load without removing the
evidence needed for broker failures.

### 6. Derive reliability from canonical outcomes

Reliability counts will classify completed/no-new-trades imports as successes, failed/recovery
outcomes as failures, and cancelled/diagnostic-only runs as separate neutral categories. The UI
will display the denominator and category counts so 0% cannot be mistaken for an import failure
when only a diagnostic test or cancellation occurred.

### 7. Keep monitoring read-only

Health and monitoring endpoints may consume the canonical state but will not start, cancel,
recover, or modify a sync. This preserves separation between observability and broker behavior.

### 8. Keep Dashboard sync subordinate to permission-to-trade readiness

The Dashboard will render Live Sync as one compact readiness item alongside mindset and debrief.
Its default view will show the canonical outcome, today's import need, last-attempt context, and one
`Sync Today` action. A secondary link opens the full Live Sync lane for preflight detail, history,
account scope, diagnostics, and recovery.

The Dashboard will not repeat the full lane's progress hero, stacked status prose, preflight
paragraph, automation paragraph, and vertical navigation buttons. This preserves safe execution
while keeping the permission-to-trade checklist visually balanced and scannable.

## Data Flow

1. The service reads selected-ledger metadata, credential readiness, latest job history, latest
   successful import, and after-market schedule.
2. The canonical mapper returns outcome, timestamps, daily flags, preflight inputs, disabled reason,
   and recommended next action.
3. Dashboard and full-lane endpoints serialize the same fields.
4. On `Sync Today`, the server rebuilds and validates a fresh request before creating a job.
5. Polling returns the canonical model; the browser renders it without independently reclassifying
   success or failure.

## Failure Behavior

- Missing credentials: disable execution and link to credential setup.
- Missing or mismatched broker account: reject before worker start and link to account settings.
- Active job: expose progress and prevent duplicate execution.
- Cancelled or diagnostic-only attempt: retain the attempt record, keep import incomplete, and
  explain the next safe action.
- Failed job with recovery evidence: set `needs_recovery` and reveal relevant recovery controls.
- State-mapping error: show an unavailable state and preserve the underlying last-known records;
  never claim completion as a fallback.

## Risks / Trade-offs

- [Existing status strings contain legacy variants] -> Normalize them in one explicit mapper and
  lock representative variants with focused tests.
- [Changing quick-run defaults may surprise operators who relied on replay] -> Rename the action,
  show preflight details, and retain explicit historical/recovery execution in the full lane.
- [A configured ledger may lack a broker identifier] -> Fail closed with a direct configuration
  link; do not guess from the previous request.
- [Large template cleanup can obscure correctness work] -> Implement and test the service/state
  contract first, then refactor presentation in small steps.
- [Reliability history may not distinguish all old diagnostic runs] -> Classify using available
  persisted intent; label unknown legacy entries separately instead of treating them as success.

## Migration Plan

1. Add the canonical mapper and tests without changing persisted data.
2. Update Dashboard endpoints and rendering to consume the mapper.
3. Replace quick replay with validated `Sync Today` request construction.
4. Update the full lane and reliability/failure presentation.
5. Reorganize advanced controls and verify the authenticated desktop workflow.

Rollback consists of reverting application/template changes; no data migration or destructive
rollback is required.

## Open Questions

None blocking. Historical sync, snapshot mode, and diagnostic-only execution remain explicit
advanced workflows.
