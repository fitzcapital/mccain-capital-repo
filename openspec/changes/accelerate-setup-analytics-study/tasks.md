## 1. Session-Aware Study Presets

- [x] 1.1 Add normalized preset values and exchange-session boundary resolution for Today, Last 3 Sessions, This Week, Last 20 Sessions, All History, and Custom.
- [x] 1.2 Keep explicit custom dates authoritative and expose the active preset plus resolved dates in the analytics payload.
- [x] 1.3 Add compact, accessible preset controls that update the whole page through the existing single-query refresh path.
- [x] 1.4 Add preset tests covering weekends, exchange holidays, week boundaries, all retained history, custom dates, and Reset-to-Today behavior.

## 2. Outcome Evidence Clarity

- [x] 2.1 Add explicit completed, open, and historical-unevaluated counts to chart, family, coverage, and ledger payloads.
- [x] 2.2 Replace “No outcomes” and vague unavailable labels with “No completed outcomes,” “Awaiting resolution,” and “Historical outcome not captured” as applicable.
- [x] 2.3 Show numerator and denominator beside every outcome rate and preserve frequency when evaluation evidence is missing.
- [x] 2.4 Add Python and JavaScript tests proving missing evidence is never interpreted as a win, loss, or zero move.

## 3. Tradier Premium Anchor

- [x] 3.1 Add a narrow read-only adapter over the existing options snapshot that validates freshness, positive mid, non-crossed spread, usable delta, direction, DTE, and liquidity.
- [x] 3.2 Implement deterministic illustrative call/put selection favoring SPXW, nearest expiry, liquid contracts, and absolute delta near 0.40 without issuing a new chain request per analytics load.
- [x] 3.3 Return source mode, contract label, mid, premium, delta, spread, DTE, quote timestamp, and sanitized fallback reason in the analytics payload.
- [x] 3.4 Preserve the $750 and 0.40 fallback for missing, stale, malformed, mixed-direction, or incomplete Tradier evidence and label it as an estimate.
- [x] 3.5 Add adapter tests for live call/put selection, stale quote, crossed market, missing delta, empty cache, mixed direction, and bounded-cache behavior.

## 4. Projection Presentation

- [x] 4.1 Update family and excursion cards to display the current Tradier premium anchor or the explicit fallback assumption without implying historical fills.
- [x] 4.2 Add a concise projection legend covering current quote timing, underlying MFE, IV/theta/gamma-path exclusions, spread, slippage, and fills.
- [x] 4.3 Ensure contract labels, timestamps, fallback reasons, and preset buttons wrap without clipped text or horizontal page overflow.
- [x] 4.4 Add receiving-surface tests for live, fallback, empty, and narrow-layout states.

## 5. Verification and Deployment

- [x] 5.1 Run focused analytics, options-adapter, route, and JavaScript tests plus Ruff, syntax checks, strict OpenSpec validation, and `git diff --check`.
- [x] 5.2 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and inspect Today, historical preset, Tradier-quoted, fallback, desktop, and narrow layouts.

## 6. Near-the-Money Contract Selection

- [x] 6.1 Preserve current SPX spot and balanced NTM call/put candidates in the cached Tradier options shortlist.
- [x] 6.2 Require minimum strike distance before premium and delta tie-breakers in the analytics anchor adapter.
- [x] 6.3 Disclose strike, current spot, and spot distance beside the illustrative contract.
- [x] 6.4 Add tests proving a farther $7.50 contract cannot outrank an NTM contract and that both call and put anchors remain available.
- [x] 6.5 Run focused checks, rebuild, verify `/healthz`, and inspect the deployed fallback/current-anchor presentation.

## 7. Cross-Worker Tradier Snapshot Reliability

- [x] 7.1 Atomically persist and read the sanitized options snapshot from the shared persistent-data directory.
- [x] 7.2 Add a cross-process lock and one bounded empty-cache recovery refresh without replacing a last-good snapshot on failure.
- [x] 7.3 Ensure Setup Analytics starts the options worker and consumes the shared snapshot regardless of Gunicorn worker routing.
- [x] 7.4 Add tests for cross-worker reads, failed-write preservation, single-writer recovery, and analytics consumption.
- [x] 7.5 Run focused checks, rebuild, verify `/healthz`, and confirm the deployed analytics reason/source state.

## 8. Scannable Profit Projection

- [x] 8.1 Replace the dense family projection paragraph with prominent estimated dollars, percentage gain, and median SPX move.
- [x] 8.2 Label the result “Illustrative, not realized P&L” and move quote metadata plus formula inputs into an expandable explanation.
- [x] 8.3 Style live and fallback projection states for clear hierarchy, wrapping, and narrow layouts.
- [x] 8.4 Add JavaScript receiving-surface tests for visible results, disclosures, and collapsed details.
- [x] 8.5 Run focused checks, rebuild, verify `/healthz`, and inspect the deployed cards.

## 9. Editable Trade Size

- [x] 9.1 Default to 3 editable contracts, show total estimated trade profit, and retain unit details.
- [x] 9.2 Verify quantity validation, proportional dollars, unchanged percentages, and both views.
- [x] 9.3 Validate specs and focused tests; verify deployed assets after approved rebuild.
- [x] 9.4 Add bounded family occurrence details with recorded prices and timestamps; test ordering and missing data.
