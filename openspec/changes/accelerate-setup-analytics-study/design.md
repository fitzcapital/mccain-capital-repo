## Context

Setup Analytics uses one filter query for every KPI, graph, family card, and ledger row. Its date
defaults now open on today, but switching to common research horizons requires manual date entry.
The service also converts SPX MFE into a hypothetical option return using a fixed $750 premium and
0.40 absolute delta. The application already maintains a Tradier-only options snapshot containing
SPX/SPXW contract labels, mid prices, delta, volume, open interest, spread, liquidity, and snapshot
time, so analytics can consume that bounded cache without adding another provider request path.

“No outcomes” currently combines different evidence conditions: no terminal setup in the selected
bucket, legacy records without post-signal evaluation, and potentially open setups. Those are not
equivalent and must not be presented as one trading result.

## Goals / Non-Goals

**Goals:**

- Provide one-click, exchange-session-aware study horizons.
- Keep one normalized filter contract across every analytics surface.
- Separate open, unevaluated legacy, and zero-terminal-sample states in plain language.
- Anchor projections to a current, complete Tradier contract quote when available.
- Disclose the contract, quote time, premium source, delta, and fallback assumptions.
- Avoid extra provider calls on each analytics request.

**Non-Goals:**

- Historical option-price reconstruction or implied fills at setup timestamps.
- Contract recommendations, order entry, realized P&L, or execution signals.
- New providers, raw chain persistence, or changes to setup evaluation.

## Decisions

Family cards show the latest occurrence time and level price, with up to five latest occurrences
in collapsed details. Each uses frozen event prices, pattern, direction, and resolution time, not
today's levels. Missing fields display a dash. Previews are sorted newest first independently of
ledger sorting, and explicitly labeled when limited to five. No outcome rules change.

### 1. Presets resolve on the server using exchange sessions

Add a `preset` query value with `today`, `last_3_sessions`, `this_week`, `last_20_sessions`, and
`all_history`. The server resolves session-aware boundaries using the shared market-session calendar;
weekends and exchange holidays do not consume session counts. Exact `start_date` and `end_date`
remain authoritative for Custom mode. Preset buttons set the filter query and all panels reload from
the same response.

Calendar-day shortcuts were rejected because “last 3 days” on Monday would mostly select the
weekend. Independent client-side date arithmetic was rejected because it would diverge from the
server’s exchange calendar.

### 2. Outcome evidence is modeled separately from outcome result

The payload reports total setups, terminal/evaluated setups, open setups, and legacy/unevaluated
setups. Presentation uses “Awaiting resolution” for open setups, “Historical outcome not captured”
for legacy rows, and “No completed outcomes in this selection” for an evaluated denominator of
zero. Every rate continues to include its numerator and denominator.

Treating missing outcomes as losses, wins, or zero movement was rejected because it contaminates
performance analysis.

### 3. Premium projections consume the existing Tradier options snapshot

Analytics reads the cached options snapshot rather than fetching a chain on each page load. A
representative planning contract is eligible only when the snapshot is current, its mid is positive,
its bid/ask are not crossed, it includes a usable delta, and its strike can be compared with the
snapshot's current SPX spot. Selection requires a near-the-money strike and then prefers SPXW,
nearest non-expired expiry, tight/OK liquidity, premium nearest $7.50, meaningful volume/open
interest, and absolute delta near 0.40. Bullish and bearish cohorts use calls and puts respectively;
mixed aggregate views display a neutral source card rather than pretending one directional contract
fits both.

The cached options shortlist retains both an NTM call and an NTM put so analytics does not mistake a
liquidity-ranked but distant strike for the representative contract. Premium and delta do not outrank
strike distance; a $3.04 NTM contract is valid market data, while a $7.50 far-OTM contract is not an
NTM substitute.

The quote-based projection uses `mid × 100` as premium and the quoted absolute delta in the existing
MFE approximation. The response includes `pricing_mode=tradier_current_quote`, contract label,
strike, current spot, spot distance, premium, mid, delta, spread, DTE, and `as_of`. If eligibility
fails it returns
`pricing_mode=fallback_estimate` with the existing $750 and 0.40 assumptions plus a reason code.

Direct chain calls per analytics request were rejected due to latency, rate limits, and inconsistent
multi-panel results. Using Tradier’s last price ahead of a valid two-sided mid was rejected because
stale prints can distort projections.

### 4. Quote projections are current planning anchors, not historical results

The page labels the projection “current Tradier premium anchor” and explicitly states that MFE is
underlying SPX movement while the option projection excludes historical IV, theta, gamma path,
spread, slippage, and fills. Historical setup rows never inherit a claim that this was their actual
contract cost.

### 5. Bounded caching and failure behavior stay fail-soft

The existing options worker owns provider refresh. Setup Analytics reads a copy of its cache and
never blocks setup analytics when Tradier is missing. Stale or malformed quotes downgrade only the
projection to fallback mode; setup statistics remain available. No credentials or raw responses are
returned to the browser.

### 6. The sanitized options snapshot is shared across web workers

The options worker atomically writes only the existing sanitized SPX snapshot to the persistent
data directory. Every Gunicorn worker reads that shared snapshot before consulting its process-local
copy. A file lock permits only one worker to perform an empty-cache recovery refresh, preventing
duplicate Tradier chain calls while making Setup Analytics independent of which worker served Market
Pulse. Setup Analytics starts the refresh loop and may perform this one bounded recovery only when
both shared and local snapshots are empty; normal analytics loads remain cache reads.

Raw provider responses and credentials are never written. A failed refresh preserves the last good
shared snapshot and keeps the explicit fallback estimate available.

## Risks / Trade-offs

- [Current premium is mistaken for historical setup cost] → Label it as a current planning anchor on
every card and never attach it to a setup’s realized outcome.

Family cards use a progressive-disclosure result block. The always-visible layer shows only the
estimated total trade gain for an editable quantity (default 3), estimated percentage gain versus the current anchor premium,
and the median SPX move used in the approximation. It says “Illustrative, not realized P&L.” Exact
contract, spot, distance, mid, spread, delta, DTE, timestamp, exclusions, and fallback reason move
behind “How this was estimated.” This keeps the research result scannable without hiding its source.
Quantity is a page-local integer from 1 to 1000, independent of study filters. Valid edits rerender
both family views without fetching data. Total gain and reference cost scale with quantity; return
percentage does not. Per-contract gain and cost remain in the expanded details. Invalid input keeps
the last valid estimate and displays a validation message. Reload defaults to 3.
- [Representative contract selection looks like a recommendation] → Use “illustrative contract,”
  show the deterministic eligibility rules, and exclude execution language.
- [Premium targeting selects the wrong moneyness] → Require NTM first and use proximity to the
  $7.50 planning premium only after strike distance, expiry, and liquidity.
- [Cached quote becomes stale] → Enforce a freshness threshold and expose quote time/fallback reason.
- [All History grows expensive] → Keep indexed ticker/session queries, bounded pagination, and
  aggregate only retained canonical setup rows.
- [Preset dates conflict with manual dates] → Exact dates switch the UI to Custom and remain
  authoritative until another preset is selected.
- [Gunicorn workers disagree about options state] → Atomically share the sanitized snapshot and
  serialize empty-cache recovery refreshes with a cross-process lock.

## Migration Plan

1. Add server-side preset normalization and tests without changing the default Today response.
2. Add compact preset controls and explicit outcome evidence copy.
3. Add a source-labeled premium-anchor adapter over the existing options snapshot.
4. Update projection cards and tests for current, stale, malformed, and unavailable quotes.
5. Rebuild, verify `/healthz`, and inspect today/history/mixed-direction layouts.

Rollback removes the controls and quote adapter while preserving the existing date filters and fixed
planning fallback. No schema or stored-data migration is required.

## Open Questions

None required for implementation. The initial Tradier freshness threshold will reuse the options
worker cadence with a conservative multiple and can be tuned after observing live sessions.
