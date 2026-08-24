## 1. Session and Expiration Truth

- [x] 1.1 Add a typed server-side Gamma session context covering market phase, session date, next
  session date, expiration lifecycle, and live/after-hours/stale/degraded/unavailable display state.
- [x] 1.2 Preserve independent quote, options-chain, compute, and expiration timestamps with
  session-aware ages and thresholds; prevent quote freshness from masking chain age or expiration.
- [x] 1.3 Resolve live-session 0DTE and after-hours next-tradable-expiration defaults while retaining
  prior-session expiration as an explicit research option.
- [x] 1.4 Add focused tests for live, after-hours, weekend/holiday, expired, missing-next-expiration,
  mismatched timestamp, stale, degraded, and unavailable behavior.

## 2. Local Gamma Flip and Relevance Model

- [x] 2.1 Implement local adjacent-sign-transition Gamma Flip selection with interpolation,
  distance cap, magnitude qualification, deterministic tie-breaking, confidence, and provenance.
- [x] 2.2 Prevent isolated or distant zero-GEX strikes from becoming the primary flip and retain
  rejected/global candidates as secondary diagnostic structure.
- [x] 2.3 Add deterministic strike relevance scoring from proximity, normalized absolute GEX, and
  structural role; mark at most nine default decision-relevant rows without altering source values.
- [x] 2.4 Add domain tests for exact zero, noisy crossing, multiple crossings, missing crossing,
  distance/magnitude boundaries, relevance ranking, tie-breaking, and original strike order.

## 3. Canonical Gamma Execution Map

- [x] 3.1 Add a server-backed execution-map view model with session state, regime, decision level,
  upside level, downside failure, expected range, next evidence, source generation, and unavailable
  fallbacks.
- [x] 3.2 Implement distinct positive-Gamma stabilization and negative-Gamma acceleration language
  without changing or competing with the authoritative Market Pulse execution permission.
- [x] 3.3 Extend Gamma Ladder and canonical Market Pulse payloads with additive truth, local-flip,
  relevance, execution-map, and stable Gamma generation fields while preserving compatibility.
- [x] 3.4 Add service/API/canonical tests proving one-generation coherence, deterministic map
  selection, no fabricated fields, and no actionability claims.

## 4. Receiving-Surface Hierarchy

- [x] 4.1 Replace the duplicated key-level cards, structure summary, and Top Levels presentation
  with one compact Gamma execution map and a collapsed timing/source disclosure.
- [x] 4.2 Render at most nine decision-relevant strikes by default with hidden-count feedback and an
  accessible Show full ladder / Show focused ladder control operating on the accepted row set.
- [x] 4.3 Move distant flip and low-relevance levels into Extended structure while preserving exact
  row detail, selected inspector, chart event, controls, and error/empty states.
- [x] 4.4 Tighten row copy and density without removing strike, distance, net/call/put GEX, strength,
  structural role, keyboard semantics, reduced motion, or responsive usability.
- [x] 4.5 Add Jinja/JavaScript/component contracts for the compact hierarchy, session labels,
  disclosure, focused/full rows, selection clearing, control parity, and absence of duplicate summaries.

## 5. Shared Canonical Refresh

- [x] 5.1 Add compact Gamma generation/map/relevant-row data to the existing Market Pulse canonical
  fingerprint and validated in-place application model.
- [x] 5.2 Route visible Gamma refresh through the shared forced-refresh concurrency gate; keep
  automatic checks cached-only and eliminate competing ladder freshness state.
- [x] 5.3 Preserve last-valid Gamma data on automatic/manual failure and update session truth, map,
  rows, disclosures, and compatible chart selection atomically on success.
- [x] 5.4 Add browser-contract tests for new/unchanged/out-of-order generations, forced refresh,
  failure retention, no document reload, preserved selection, and no replay on unchanged data.

## 6. Verification and Delivery

- [x] 6.1 Run focused Gamma service, expiry, flip, relevance, map, API, canonical-refresh, template,
  controller, chart-coordination, accessibility, and responsive test targets.
- [x] 6.2 Run the full pytest suite, Ruff, Black, Python compilation, JavaScript syntax, strict
  OpenSpec validation, and `git diff --check`.
- [x] 6.3 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and perform authenticated live
  and after-hours functional checks for timing truth, default expiration, flip qualification,
  execution map, focused/full rows, refresh coherence, and console health.
- [x] 6.4 Check desktop and narrow receiving surfaces without claiming subjective visual approval;
  record commands, results, fixtures/live-data limits, image id, and functional evidence in
  `verification.md` before synchronization/archive.
