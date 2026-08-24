## Automated Verification

- Focused freshness, candle evidence, strategy, API, and component suite: 227 passed.
- `python -m pytest -q`: 686 passed.
- `python -m ruff check .`: passed.
- `python -m black --check --config pyproject.toml .`: 155 files unchanged after formatting.
- `python -m compileall -q mccain_capital`: passed.
- `node --check static/js/market_pulse_gamma_context.js`: passed.
- `openspec validate tighten-market-pulse-execution --type change --strict --no-interactive`:
  passed.
- `git diff --check`: passed.

## Production and Receiving Surface

- `./scripts/run_podman_app.sh`: rebuilt image `e9709957f417` and started the local container.
- `GET /healthz`: returned `status: ok`.
- Authenticated desktop Market Pulse rendered the canonical execution strip, component ages,
  active level, permission/state, next evidence, invalidation, chart, checklist, and gamma ladder.
- Forced manual refresh entered the disabled `Refreshing…` state, then returned to `Refresh data`
  and advanced SPX from 7773.33 to 7753.11 with a new active level and strategy state.
- After refresh, spot and gamma ages were 0 seconds while bars remained explicitly stale; the
  verdict stayed `Data locked` and identified bars as the blocking component.
- The automatic 15-second cached-generation check ran without console warnings or errors; unchanged
  state did not leave a busy overlay.
- At 390 by 844, the execution strip, chart, and gamma ladder remained present with no horizontal
  document overflow (`scrollWidth == innerWidth == 390`).

## Live Data Limitations

- Verification occurred after the regular session. Completed bars were correctly reported stale,
  so actionability remained locked even though forced refresh produced current spot and gamma.
- Fifteen-minute runner evidence remains conditional on a valid completed 15-minute input series;
  it never substitutes for the required five-minute sequence.
- Subjective visual preference remains user-owned; checks here cover hierarchy, readability,
  responsiveness, control availability, overflow, and runtime behavior.

## In-Place Canonical Refresh Follow-Up

- Manual and automatic refresh now share one validated in-place application boundary; refresh code
  no longer navigates or reloads the document.
- The application boundary updates the execution strip, header status, decision fields, gamma level
  deck, structure map, strategy card/checklist, and compatible chart context from one payload.
- A generation timestamp gate prevents a cached automatic response from replacing a newer forced
  generation. Invalid, incomplete, or older payloads leave the last valid screen authoritative.
- Authenticated runtime verification advanced generation `9d429d948952fc654960` to
  `9214c57226296a5591a3` while the URL, mounted chart, and selected 5-minute timeframe remained
  unchanged. A separate automatic cycle preserved scroll position at 1144.5 pixels.
- Focused Market Pulse and core application verification: 168 passed.
- Full suite: 687 passed.
- Ruff and Black checks passed; JavaScript syntax, strict OpenSpec validation, and `git diff --check`
  passed.
- Final local image: `6ceaf61f65d2`; `/healthz` returned HTTP 200 with `status: ok`.
- Generated Swift build output is now excluded from the container context so unrelated iOS cache
  files cannot exhaust Podman during application rebuilds.

## Above-the-Fold Summary Cleanup

- The first status row now names the current gamma regime directly and retains compact health text;
  authenticated verification rendered `Positive Ⲅ` and `Healthy`.
- The authoritative strip now contains four blocks only: execution verdict/lock reason, active level
  with distance, next evidence, and invalidation. Duplicate spot, availability, permission/path,
  component-age, generation-hash, and generic helper copy were removed from this strip.
- `Sticky summary: Off` is the persisted default. Functional checks confirmed Off maps to
  `aria-pressed=false` and `position: relative`; On maps to `aria-pressed=true` and
  `position: sticky`, survives reload, and can be returned to Off.
- A new workflow asset revision prevents an older cached pin controller from masking the updated
  interaction after deployment.
- Focused Market Pulse/core verification: 164 passed. Full suite: 687 passed. Ruff, Black,
  JavaScript syntax, strict OpenSpec validation, and `git diff --check` passed.
- Final local image: `1c18de99b4fa`; `/healthz` returned `status: ok`.
- Subjective visual approval remains with the user.

## Data Lock Diagnostics

- Added one collapsed `Data Lock Diagnostics` disclosure immediately after the authoritative strip;
  it contains no separate refresh control or competing verdict.
- Authenticated verification rendered five canonical rows. The closed summary correctly reported
  `Locked · Completed Bars blocking`; the Bars row showed Required, Stale, `121m 30s / 15m`, the
  last successful bar timestamp, and `Provider completed-bar cache`.
- Missing strategy provenance remained explicit as Optional, Unavailable, `— / 15m`, and
  `Unavailable`; it was not misreported as zero-age or added to the required lock.
- Forced refresh kept the disclosure open, preserved the URL, advanced canonical success to
  `Aug 10, 7:04:27 PM EDT`, and returned synchronization to `Last check current · next ≤15s`.
- Focused freshness, workflow, and core verification: 170 passed. Full suite: 689 passed. Ruff,
  Black, JavaScript syntax, strict OpenSpec validation, and `git diff --check` passed.
- Final local image: `b31fd0b71bfe`; `/healthz` returned `status: ok`. Subjective visual approval
  remains with the user.

## Canonical Strategy Chart Overlays

- The canonical verdict now includes `invalidation_level` only when backend display context matches
  both a named validated structure level and its canonical value. Display-only or mismatched copy
  remains unstructured and produces no chart line.
- The existing Lightweight Charts candle series owns a separate managed overlay collection for
  Active, Invalid, and first Target. Exact-price roles merge into one label; canonical reconciliation
  does not refit the chart or touch timeframe, drawings, gamma selection, or other level collections.
- Initial payload and `market-pulse-canonical-update` use the same overlay boundary. A read-only
  runtime state reports generation and normalized overlays for functional diagnostics.
- Focused snapshot/workflow verification: 57 passed. Full suite: 692 passed. Ruff, Black, Python
  compilation, JavaScript syntax, strict OpenSpec validation, and `git diff --check` passed.
- Final local image: `c75aca881a56`; `/healthz` returned `status: ok`. The deployed chart asset was
  confirmed to contain the new overlay runtime. Manual refresh preserved the URL, single chart
  mount, and selected 5-minute timeframe with no console warnings or errors.
- The in-app browser blocked direct execution/inspection of the external chart JavaScript asset,
  so rendered line placement was not claimed as verified. Subjective visual approval remains with
  the user as requested.

## Confirmed-Path Target Eligibility

- The domain evaluator now returns primary and expansion targets only for `REVERSAL_READY` or
  `CONTINUATION_ACTIVE`; pending and acceptance-without-retest states return no targets.
- The canonical assembly boundary applies the same rule to legacy cached snapshots, updates every
  verdict/strategy consumer, and advances the generation when an obsolete target is removed.
- The chart independently requires the matching confirmed state and path, then labels eligible
  lines `REVERSAL TARGET` or `CONTINUATION TARGET`.
- Live after-hours verification rendered `LEVEL_BEING_TESTED`, path `pending`, invalidation
  `CW 7750`, `primaryTarget: null`, and no expansion targets. Forced in-place refresh retained that
  state, the URL, one chart mount, and the selected 5-minute timeframe with no console errors.
- Focused target/strategy/chart verification: 79 passed. Full suite: 695 passed. Ruff, Black,
  Python compilation, JavaScript syntax, strict OpenSpec validation, and `git diff --check` passed.
- Final local image: `e74c5cf8116b`; `/healthz` returned `status: ok`. Subjective visual approval
  remains with the user.
