## 1. Canonical Generation and Freshness Contract

- [x] 1.1 Add normalized component timestamp, age, threshold, status, oldest-required-input, and
  stable generation metadata to the canonical Market Pulse service payload.
- [x] 1.2 Make missing, malformed, and stale required-component metadata feed the existing guardrail
  and authoritative execution permission without masking last-valid values.
- [x] 1.3 Expose the canonical metadata consistently from page render and context API paths while
  preserving existing payload compatibility fields.
- [x] 1.4 Add focused service/API tests for stable and changed generations, independent component
  ages, session-aware thresholds, and stale-component locks.

## 2. Completed-Candle Evidence Extraction

- [x] 2.1 Add typed normalization for five- and fifteen-minute OHLC bars, completed-interval checks,
  session identity, evidence identity, ordering, deduplication, and malformed-input handling.
- [x] 2.2 Implement deterministic bullish and bearish location, sweep, completed close-back-inside,
  five-minute Strat 2-2, and later trigger-break evidence with timestamps and provenance.
- [x] 2.3 Implement mutually exclusive acceptance/hold/retest evidence and optional post-trigger
  fifteen-minute runner confirmation.
- [x] 2.4 Reset or reject incompatible evidence across ticker, session, active level/value,
  direction, timeframe, and out-of-order transitions.
- [x] 2.5 Integrate derived evidence through the existing failed-sweep adapter while preserving
  explicit evidence compatibility and keeping ambiguous inputs Pending or Unavailable.
- [x] 2.6 Add exhaustive unit tests for completed versus unfinished bars, bullish/bearish symmetry,
  invalid ordering, acceptance exclusivity, session/level changes, stale bars, and malformed data.

## 3. Automatic Synchronization Coordinator

- [x] 3.1 Add one browser coordinator for 15-second visible cached-generation checks, hidden-tab
  pause, immediate visibility/focus catch-up, and bounded retry status.
- [x] 3.2 Share one concurrency gate across automatic checks and forced manual refresh, preventing
  overlapping requests while keeping `Refresh data` authoritative.
- [x] 3.3 Treat unchanged generations as no-ops and advance newer generations through one atomic
  receiving boundary without forcing providers.
- [x] 3.4 Preserve stable page state when a changed generation requires canonical reload, including
  ticker, execution/research mode, folds, chart timeframe, and practical scroll position.
- [x] 3.5 Show compact component freshness and synchronization feedback without adding another
  competing refresh control.
- [x] 3.6 Add JavaScript/component-contract tests for timers, visibility/focus behavior, generation
  comparisons, concurrency, success, failure, no-op, and state restoration.

## 4. Authoritative Execution Hierarchy

- [x] 4.1 Add one canonical verdict view model containing permission, active level/distance, path,
  confirmed evidence, next evidence, invalidation, management, target, generation, and freshness.
- [x] 4.2 Consolidate repeated actionability presentation into a compact persistent execution strip
  and one detailed decision card driven only by the canonical verdict.
- [x] 4.3 Keep the ordered checklist and gamma ladder in the primary execution flow while placing
  tape, news, flow, and extended structure behind secondary research disclosure.
- [x] 4.4 De-emphasize contract ideas until a ready terminal strategy state and ensure supporting
  sections cannot render a verdict conflicting with the canonical decision.
- [x] 4.5 Preserve ticker controls, chart interactions, navigation, manual refresh, gamma ladder,
  responsive usability, and explicit risk/order-system exclusions.
- [x] 4.6 Add Jinja/service/browser-contract tests for locked, pending, reversal-ready, continuation,
  exact next-evidence copy, target withholding, and absence of competing verdicts.

## 5. Verification and Delivery

- [x] 5.1 Run focused freshness, candle-extractor, strategy, context API, and component test targets,
  resolving regressions without weakening evidence or stale-data safety.
- [x] 5.2 Run `python -m pytest -q`, Ruff, Black, Python compilation, JavaScript syntax checks, and
  strict OpenSpec validation.
- [x] 5.3 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and perform authenticated
  Market Pulse functional checks for automatic generation advance, focus catch-up, forced manual
  refresh, component ages, and coherent decision state.
- [x] 5.4 Perform desktop and narrow-viewport receiving-surface checks for sticky-strip readability,
  primary workflow order, chart/ladder usability, secondary disclosure, overflow, and console errors.
- [x] 5.5 Record commands, results, live-data limitations, and receiving-surface evidence in the
  change verification notes before synchronization/archive.

## 6. In-Place Canonical Refresh

- [x] 6.1 Replace canonical navigation/reload behavior with one validated in-place payload
  application boundary shared by automatic and manual refresh.
- [x] 6.2 Update the execution strip, header status, verdict, active level/distance, component ages,
  strategy card/checklist, structure levels, and compatible chart context from the same generation.
- [x] 6.3 Preserve the existing chart instance, chart drawings/state, scroll, folds, mode, ticker,
  selected timeframe, focus, and controls; remove navigation and reload calls from refresh paths.
- [x] 6.4 Keep the last valid generation unchanged when validation or application fails and report
  the synchronization failure inline.
- [x] 6.5 Add browser-contract and service tests proving no navigation, one-generation application,
  unchanged-generation no-op, preserved interaction state, and manual/automatic shared behavior.
- [x] 6.6 Run focused and full verification, strict OpenSpec validation, production rebuild, health
  check, and authenticated receiving-surface checks demonstrating in-place value changes.

## 7. Above-the-Fold Summary Cleanup

- [x] 7.1 Replace the verbose Gamma Data status metric with the direct current gamma regime while
  retaining a compact data-health state.
- [x] 7.2 Reduce the authoritative execution strip to verdict/lock reason, active level/distance,
  next evidence, and invalidation; remove duplicate spot, availability, path, freshness, generation,
  and generic helper copy.
- [x] 7.3 Make the existing persistent pin preference control the execution strip, default off, and
  expose an unambiguous Sticky summary On/Off label and pressed state.
- [x] 7.4 Update in-place refresh targets and focused contracts for the simplified summary, direct
  regime label, and shared sticky toggle.
- [x] 7.5 Run focused and full verification, strict validation, rebuild, health check, and functional
  receiving-surface checks without claiming subjective visual approval.

## 8. Data Lock Diagnostics

- [x] 8.1 Add one collapsed diagnostics disclosure after the execution strip with a concise lock or
  healthy summary and no additional refresh control.
- [x] 8.2 Render canonical spot, bars, gamma, options, and strategy rows with status, age, threshold,
  last successful timestamp, required/optional role, and source/cache context.
- [x] 8.3 Show blocking required components, last canonical success time, and automatic retry cadence
  while keeping missing metadata explicit and avoiding inferred provider-health claims.
- [x] 8.4 Update the diagnostics through the existing validated in-place generation boundary and add
  focused render/contract tests for locked, healthy, optional, missing, and changed payload states.
- [x] 8.5 Run focused/full verification, strict validation, rebuild, health and authenticated
  functional checks; leave subjective visual approval to the user.

## 9. Canonical Strategy Chart Overlays

- [x] 9.1 Add a structured invalidation level to the canonical verdict only when it resolves to a
  validated market-structure level; preserve the existing display label for compatibility.
- [x] 9.2 Reconcile Active, Invalidation, and first valid Target overlays on the existing chart from
  initial and in-place canonical generations, merging exact-price role collisions.
- [x] 9.3 Preserve chart instance, visible range, timeframe, drawings, gamma selection, and existing
  level collections; omit unavailable prices and never parse browser-visible prose.
- [x] 9.4 Add focused backend and JavaScript/browser-contract tests for structured values, omission,
  collision handling, generation updates, and isolated chart mutations.
- [x] 9.5 Run focused/full verification, strict validation, rebuild, health and authenticated
  functional checks; leave subjective visual approval to the user.

## 10. Confirmed-Path Target Eligibility

- [x] 10.1 Withhold primary and expansion targets in the domain evaluator unless the strategy is
  `REVERSAL_READY` or `CONTINUATION_ACTIVE`.
- [x] 10.2 Label eligible chart targets by their confirmed reversal or continuation path and ensure
  pending/after-hours planning cannot merge an invalidation with a premature target.
- [x] 10.3 Add focused domain and chart-contract tests for pending, acceptance-only, reversal-ready,
  continuation-active, and same-price behavior.
- [x] 10.4 Run focused/full verification, strict validation, rebuild, health and authenticated
  functional checks; leave subjective visual approval to the user.
