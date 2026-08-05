## 1. Canonical State Contract

- [x] 1.1 Inventory persisted Live Sync job statuses and intent fields used by current Dashboard,
  lane, reliability, failure-guide, and automation summaries.
- [x] 1.2 Add a server-owned canonical outcome mapper for ready, running, completed,
  no-new-trades, diagnostic-only, cancelled, failed, and needs-recovery states.
- [x] 1.3 Add last-attempt, last-successful-import, attempted-today, import-completed-today,
  automation timing, disabled reason, and recommended-next-action fields to the shared view model.
- [x] 1.4 Remove duplicate or conflicting Dashboard state fields and make Dashboard/full-lane
  endpoints consume the canonical view model.
- [x] 1.5 Add focused service tests for terminal status variants, ET day boundaries, diagnostic-only,
  cancellation, no-new-trades, unknown legacy status, and an earlier success followed by failure.

## 2. Safe Sync-Today Execution

- [x] 2.1 Add a preflight builder that uses today's ET date, the selected ledger, its configured
  broker identifier, broker-fill import mode, and normal import intent.
- [x] 2.2 Validate normalized selected-ledger and requested broker identifiers before job creation,
  returning actionable missing/mismatch errors without starting the worker.
- [x] 2.3 Replace Dashboard last-request replay with the validated Sync Today request while retaining
  saved credentials and safe runtime defaults only.
- [x] 2.4 Ensure historical dates, snapshot mode, and diagnostic-only execution remain explicit
  full-lane actions and cannot leak into Dashboard Sync Today.
- [x] 2.5 Add endpoint/service tests for correct account, mismatched account, missing identifier,
  stale saved dates, saved diagnostic-only intent, active-job protection, and missing credentials.

## 3. Dashboard Live Sync Card

- [x] 3.1 Render the canonical outcome, last attempt, last successful import, daily completion, and
  next automation time without client-side timestamp reclassification.
- [x] 3.2 Replace Run Last Sync with a Sync Today preflight showing ledger, masked broker account,
  date, mode, import intent, and specific disabled reason.
- [x] 3.3 Update Dashboard polling and completion rendering so cancelled, failed, diagnostic-only,
  and no-new-trades outcomes retain their server classifications.
- [x] 3.4 Add focused Dashboard DOM or response tests for canonical labels, preflight content,
  disabled states, and cancellation regression behavior.

## 4. Desktop Live Sync Lane

- [x] 4.1 Replace duplicated header, system, job, failure, and automation summaries with one
  canonical status region and one primary Sync Today workflow.
- [x] 4.2 Rename normal execution to Normal Import and diagnostic-only execution to Diagnostic Test —
  No Import, with explicit intent shown before submission.
- [x] 4.3 Move historical window, balance snapshot, diagnostics, artifacts, HTML recovery, force/reset,
  and reliability detail into Advanced or state-triggered recovery sections.
- [x] 4.4 Reveal relevant failure guidance and recovery controls when needs-recovery is active while
  preserving job evidence and manual recovery access.
- [x] 4.5 Show after-market automation enabled state, next ET run, latest attempt, latest successful
  import, and whether today's import remains pending.
- [x] 4.6 Add focused template/endpoint tests for the normal, active, completed, cancelled,
  diagnostic-only, failed, and recovery-required lane states.

## 5. Reliability and Passive Monitoring

- [x] 5.1 Reclassify reliability history into successful imports, failures, cancellations,
  diagnostic-only runs, and unknown legacy results with an explicit calculation denominator.
- [x] 5.2 Update failure guidance so cancelled and diagnostic-only attempts do not appear as either
  successful imports or unexplained zero-percent failures.
- [x] 5.3 Expose only redacted canonical state to monitoring and verify monitoring reads cannot mutate
  sync, broker, credential, or job state.
- [x] 5.4 Add focused reliability and monitoring tests for mixed outcomes and cancellation-only windows.

## 6. Verification

- [x] 6.1 Run the targeted Live Sync and Dashboard pytest modules and address regressions.
- [x] 6.2 Run JavaScript syntax checks and repository-configured formatting/lint checks for changed files.
- [x] 6.3 Rebuild the local application and verify authenticated desktop behavior on both Dashboard
  and the full Live Sync lane without modifying personal runtime data.
- [x] 6.4 Verify a cancelled attempt, diagnostic-only completion, normal no-new-trades result,
  account mismatch, and ready state render consistently on both receiving surfaces.
- [x] 6.5 Validate the OpenSpec change strictly and record the completed implementation tasks.

## 7. Desktop Visual Consolidation

- [x] 7.1 Restructure the Live Sync template into exactly three top-level regions: compact canonical
  status, one primary Sync Today workspace, and one Advanced & Recovery drawer.
- [x] 7.2 Consolidate Operator Deck, Run Feedback, Failure Guide, Tools, reliability, automation,
  credentials, account editing, and recovery controls into the primary workspace or drawer.
- [x] 7.3 Add focused desktop styling for the new hierarchy, compact status strip, preflight grid,
  primary action area, and drawer without prioritizing mobile behavior.
- [x] 7.4 Update client-side selectors and status updates so the consolidated layout retains job
  progress, diagnostic intent, recovery guidance, and automation behavior.
- [x] 7.5 Add template contract tests that reject repeated peer status surfaces and require the
  three-region hierarchy.
- [x] 7.6 Run targeted tests and syntax/lint checks, rebuild the local app, and visually verify the
  authenticated desktop Dashboard and Live Sync lane.
- [x] 7.7 Strictly validate the revised OpenSpec change and record all visual tasks complete.

## 8. Dashboard Sync Consolidation

- [x] 8.1 Replace the oversized Dashboard sync workspace with one compact readiness item inside the
  permission-to-trade checklist.
- [x] 8.2 Preserve canonical outcome polling and safe Sync Today execution while reducing visible
  copy to outcome, concise guidance, last-attempt context, and one primary action.
- [x] 8.3 Move preflight detail, account scope, history, diagnostics, and recovery navigation behind
  one link to the full Live Sync lane.
- [x] 8.4 Add focused desktop styling and contract tests for the compact Dashboard treatment.
- [x] 8.5 Run targeted tests and syntax/lint checks, rebuild, visually verify the authenticated
  Dashboard, and strictly validate the revised change.
