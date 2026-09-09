## 1. Analytics Event Store

- [x] 1.1 Add the normalized setup-event table and indexes through the existing additive database initialization path.
- [x] 1.2 Implement a repository that idempotently upserts stable events, freezes signal facts, and advances lifecycle/outcome fields monotonically.
- [x] 1.3 Map canonical Live/Replay events into analytics records without changing setup evaluation or alert behavior.
- [x] 1.4 Add a conservative, idempotent durable-ledger backfill that preserves unavailable historical fields as null.
- [x] 1.5 Add repository tests for duplicate suppression, frozen facts, terminal monotonicity, backfill retry, and session retention.

## 2. Filtered Analytics Service and API

- [x] 2.1 Define and validate the normalized filter contract, New York session-time handling, bounded date range, sorting, and pagination.
- [x] 2.2 Implement consistent headline metrics and sample sizes from the filtered population.
- [x] 2.3 Implement occurrence trend, 30-minute time-bucket outcome mix, family outcome mix, and MFE-versus-MAE chart series.
- [x] 2.4 Return paginated ledger rows, filter options, coverage boundaries, evaluated-through time, and missing-data counts from one response.
- [x] 2.5 Add revision-aware response caching that invalidates when analytics event data advances.
- [x] 2.6 Add authenticated read-only endpoint tests for valid filters, invalid inputs, bounds, empty results, unavailable data, and filter consistency.

## 3. Dedicated Setup Analytics Page

- [x] 3.1 Add the Setup Analytics route and responsive page shell using existing Market Pulse styling and charting patterns.
- [x] 3.2 Add compact KPI cards with explicit terminal denominators and MFE, MAE, and progress sample sizes.
- [x] 3.3 Add synchronized filters and the four required charts with loading, empty, unavailable, and retry states.
- [x] 3.4 Add the sortable paginated ledger, expandable frozen evidence, and session-scoped Replay links.
- [x] 3.5 Add coverage and interpretation copy stating that SPX setup outcomes are not option fills, returns, or realized profit.
- [x] 3.6 Add a compact `Analyze setups` link to Setup Replay without increasing execution-page density.

## 4. Parity, Performance, and Deployment Verification

- [x] 4.1 Add fixtures proving analytics rows match Live/Replay event ids, families, targets, timestamps, and outcomes across open and terminal setups.
- [x] 4.2 Add daylight-saving and 30-minute boundary tests plus responsive template and JavaScript contract checks.
- [x] 4.3 Verify bounded query behavior and inspect the query plan for indexed date, family, pattern, and outcome filtering.
- [x] 4.4 Run focused Python and JavaScript tests, Ruff, syntax checks, `git diff --check`, and strict OpenSpec validation.
- [x] 4.5 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, inspect the deployed analytics page at desktop and narrow widths, and compare sampled rows against Live/Replay.

## 5. Research Cockpit Refinement

- [x] 5.1 Add outcome coverage, time-heatmap, family-comparison, and evidence-qualified insight data to the bounded analytics response.
- [x] 5.2 Add the prominent insight banner and compact KPI/coverage strip.
- [x] 5.3 Move synchronized filters into a collapsible drawer and keep incomplete outcomes out of performance evidence by default.
- [x] 5.4 Add the timing heatmap centerpiece, family comparison cards, and cleaner expandable ledger with explicit legacy labels.
- [x] 5.5 Add Setup Analytics to desktop and mobile Market Pulse navigation paths.
- [x] 5.6 Add focused service/UI tests, validate OpenSpec, rebuild, verify health, and inspect desktop and narrow receiving surfaces.

## 6. Modern Terminal Workspace

- [x] 6.1 Combine the hero and primary insight into a compact command header and convert KPI cards into a metric ribbon.
- [x] 6.2 Add accessible Overview, Timing, Families, and Ledger tabs that preserve filters without refetching.
- [x] 6.3 Build the horizontal session timeline, four-family Overview preview, and full Timing and Families views.
- [x] 6.4 Replace the wide ledger table with compact expandable event rows while preserving audit facts and Replay links.
- [x] 6.5 Add modern responsive styling, restrained interaction motion, and reduced-motion support.
- [x] 6.6 Update focused UI contracts, validate, rebuild, and verify deployed desktop and narrow layouts.

## 7. Chart Readability and Full Labels

- [x] 7.1 Remove title ellipsis and guarantee wrapping for page, panel, family, and ledger titles.
- [x] 7.2 Replace timing cards with a frequency-profile chart using outcome-rate color and complete bucket labels.
- [x] 7.3 Make Timing charts full width and rebuild the session chart with readable month/day labels, grid lines, and full-date detail.
- [x] 7.4 Change the Overview family preview to a spacious ranked list while retaining the full Families comparison view.
- [x] 7.5 Add label-visibility and chart contract checks, rebuild, and verify deployed desktop and narrow receiving surfaces.

## 8. Analytical Label Clarity

- [x] 8.1 Replace bare coverage, MFE, and MAE family labels with captured counts and plain-language SPX-point movement labels.
- [x] 8.2 Pair every family target percentage with the reached-target count and evaluated denominator.
- [x] 8.3 Update UI contracts, rebuild, and verify the clarified labels on Overview and Families views.

## 9. Excursion Legend, Profit Estimate, and Families Layout

- [x] 9.1 Add disclosed Replay-aligned contract-cost, delta, multiplier, and take-profit assumptions to the analytics response.
- [x] 9.2 Add a visible MFE/MAE legend and estimated median profit opportunity with clear non-realized-P&L caveats.
- [x] 9.3 Rebuild the Families view as a full-width, two-column comparison grid with vertical metrics and a full-width excursion chart.
- [x] 9.4 Add focused service/UI contracts, validate, rebuild, and verify desktop and narrow layouts.
