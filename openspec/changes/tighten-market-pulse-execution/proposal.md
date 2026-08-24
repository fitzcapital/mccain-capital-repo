## Why

Market Pulse has capable quote, options, gamma, chart, and failed-sweep services, but the receiving
page can remain on an old canonical generation until the user manually refreshes. It also depends
on externally supplied strategy evidence for signals that completed candles can prove, while
repeating the execution conclusion across several competing surfaces.

## What Changes

- Add automatic, visibility-aware delivery of canonical Market Pulse generations without forcing
  upstream providers on every browser check.
- Add a generation contract with component timestamps for spot, bars, gamma, options, strategy,
  and the oldest required execution input.
- Keep `Refresh data` as a forced fallback, prevent overlapping work, catch up after a hidden tab
  returns, and preserve the last valid state when refresh fails.
- Deterministically extract supported failed-sweep evidence from completed five- and
  fifteen-minute candles, including interaction, sweep, close-back-inside, Strat 2-2, trigger,
  acceptance, retest, and optional runner evidence when the bars prove it.
- Reject unfinished, stale, out-of-order, cross-session, or incompatible candle evidence rather
  than fabricating confirmation.
- Consolidate the repeated actionability surfaces into one authoritative execution hierarchy:
  safety and freshness, active level and strategy path, next required evidence, invalidation, and
  targets.
- Preserve the chart, gamma ladder, ticker controls, research data, providers, and manual refresh,
  while moving secondary research below the primary execution workflow.
- Add focused domain, service, API, browser-contract, and receiving-surface verification.

### Acceptance Criteria

- While the page is visible, it detects a newer canonical server generation within 15 seconds and
  updates all execution-critical surfaces atomically without forcing provider refreshes or
  navigating/reloading the document.
- A hidden page pauses automatic checks and catches up immediately when visible if its generation
  is behind; automatic and manual refresh work never overlaps.
- Spot, bars, gamma, options, strategy, and oldest-required-input ages are independently visible,
  and stale required inputs lock actionable output.
- Only completed, ordered, same-session candles can confirm strategy evidence; malformed or
  incomplete bars remain Pending or Unavailable.
- The six-step strategy checklist advances automatically for evidence proven by candles and resets
  incompatible evidence when the active level, direction, session, or regime changes.
- Exactly one authoritative execution verdict is prominent, with coherent permission, active
  level, distance, next evidence, invalidation, and first target.
- The first viewport names the current gamma regime directly, removes duplicate explanatory copy,
  and keeps only execution-critical summary fields above the chart.
- One explicit sticky-summary toggle controls the authoritative verdict strip, defaults off,
  persists locally, and clearly reports On or Off.
- A collapsed Data Lock Diagnostics disclosure explains every canonical component's status, age,
  threshold, last successful timestamp, source/cache context, and automatic retry cadence without
  competing with the execution verdict.
- The existing chart shows canonical Active, Invalidation, and first valid path-specific Target
  overlays from the same generation; targets remain withheld until reversal or continuation is
  confirmed, unavailable levels are omitted, and refresh never resets chart interaction state.
- Automatic and manual refresh preserve the existing document, chart instance, scroll position,
  open disclosures, selected timeframe, and interaction state.
- Focused and repository-wide tests, lint, formatting, JavaScript syntax, strict OpenSpec
  validation, production build, health check, and authenticated receiving-surface checks pass.

### Non-Goals

- Order entry, trade authorization, contract sizing, loss limits, account locking, or risk
  calculators.
- New market-data providers, predictive commentary, or inference from unfinished candles.
- Replacement of the charting library, gamma ladder, navigation, or broad application redesign.
- Promoting contract ideas before the underlying strategy becomes ready.
- Removing safety locks or hiding the reason execution is locked.
- Adding provider controls or a second refresh path inside diagnostics.
- Adding discretionary chart levels or deriving prices from browser-visible prose.

## Capabilities

### New Capabilities

- `market-pulse-canonical-freshness`: Defines generation-aware automatic delivery, component-level
  freshness, hidden-tab behavior, forced manual fallback, atomic application, and failure safety.
- `spx-completed-candle-evidence`: Defines deterministic extraction and validation of ordered SPX
  failed-sweep evidence from completed five- and fifteen-minute candles.
- `market-pulse-execution-hierarchy`: Defines one authoritative, compact execution verdict and the
  priority and disclosure rules for supporting Market Pulse information.

### Modified Capabilities

None. No capability has been synchronized into `openspec/specs/`; these contracts extend the
active SPX Playbook work without changing unrelated application capabilities.

## Impact

- Affected code: Market Pulse runtime/coordinator, canonical context API and view-model assembly,
  failed-sweep adapter/evidence services, Jinja execution surfaces, narrowly related JavaScript and
  CSS, and focused tests.
- Data sources: existing quote worker, completed chart bars, gamma snapshot, options snapshot, and
  current/prior-session level maps. No new provider or dependency is planned.
- API contracts: the canonical Market Pulse payload gains a stable generation identifier,
  component timestamps/ages, refresh metadata, and derived candle-evidence provenance while
  retaining existing compatibility fields.
- Financial assumptions: all conclusions remain deterministic interpretations of observable
  market data. No price projection, fill assumption, risk amount, or trade permission is added.
  Last-valid values remain visible only with explicit degraded/stale labeling.
