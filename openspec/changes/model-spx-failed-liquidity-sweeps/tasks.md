## 1. Strategy Domain Model

- [x] 1.1 Add typed strategy enums/models for supported levels, interaction direction, gamma
  regime, ordered states, evidence status, path, targets, and timestamped evidence.
- [x] 1.2 Implement a pure evaluator that enforces every reversal prerequisite in order, branches
  exclusively to acceptance/continuation, and rejects conflicting or invalid transitions.
- [x] 1.3 Implement deterministic directional target ordering and positive-, negative-, and
  transition-gamma management interpretation, including optional 15m runner confirmation.
- [x] 1.4 Normalize missing, stale, non-finite, and malformed inputs to safe pending/unavailable
  results without non-serializable output.

## 2. Market Pulse Integration

- [x] 2.1 Add a thin adapter from current gamma/spot/chart/day-level and explicit candle evidence to
  the strategy evaluator, supporting all specified level types when available.
- [x] 2.2 Add the strategy result to the canonical Playbook view model while preserving existing
  chart, gamma ladder, quote, refresh, and dashboard compatibility fields.
- [x] 2.3 Repair any malformed strategy JSON/template serialization path encountered in the
  receiving feature area and verify malformed inputs still render a valid page/payload.

## 3. Compact Playbook UI

- [x] 3.1 Update the decision card to render active level/test direction, state/path/direction,
  rejection or acceptance, gamma interpretation, targets, and earliest missing evidence from the
  canonical strategy result.
- [x] 3.2 Replace the four-item manual trigger list with the six required ordered checklist items
  and Pending, Confirmed, Failed, or Unavailable presentation.
- [x] 3.3 Update only the necessary refresh bindings and compact CSS, preserving the dark design,
  page density, chart, ladder, navigation, providers, and existing working controls.
- [x] 3.4 Add explicit contract assertions that no risk-control, calculator, daily-loss,
  account-locking, or contract-sizing UI appears in the SPX Playbook change.

## 4. Automated Verification

- [x] 4.1 Add domain unit tests for every state transition, invalid ordering, conflicting paths,
  active-level/regime changes, missing/malformed data, and bullish/bearish symmetry.
- [x] 4.2 Add cases for the four supplied examples, positive-gamma nearest-target management,
  negative-gamma expansion/15m runner behavior, and acceptance continuation targets.
- [x] 4.3 Add focused service/view-model and Jinja/component contract tests for deterministic
  payloads, six checklist statuses, compact card fields, compatibility, and safe rendering.
- [x] 4.4 Run focused pytest targets first, then the repository CI test command
  `python -m pytest -q` after receiving approval for the full suite.
- [x] 4.5 Run the configured quality commands `python -m ruff check .` and
  `python -m black --check --config pyproject.toml .`; record that no standalone static type-check
  command is configured and use Python compilation plus typed-model tests as the available check.
- [x] 4.6 Run JavaScript syntax checks for changed scripts and any repository-native component
  tests that cover the Market Pulse browser contract.
- [x] 4.7 Validate the completed OpenSpec change with
  `openspec validate model-spx-failed-liquidity-sweeps --type change --strict --no-interactive`.

## 5. Build and Receiving-Surface Verification

- [x] 5.1 With approval for the long-running container build, run the repository's production
  Podman build/start path and verify `/healthz` plus the SPX Playbook endpoint.
- [ ] 5.2 User visual inspection: inspect the running SPX Playbook at desktop and narrow viewport widths, confirming readable
  status/checklist text, no material page-length growth, no malformed JSON/runtime error, no risk
  clutter, and usable chart and gamma ladder.
- [x] 5.3 Record executed commands, results, visual evidence, and any live-data limitations for the
  final handoff and OpenSpec verification notes.

## 6. Refresh and Decision Coherence

- [x] 6.1 Make guardrail/data availability and ordered strategy state authoritative over legacy
  actionable or trigger-confirmed presentation fields.
- [x] 6.2 Replace the icon-only and buried competing refresh affordances with one labeled
  `Refresh data` control beside the updated timestamp while retaining ladder-only refresh.
- [x] 6.3 Implement refreshing, success, and failed/last-valid feedback; prevent duplicate clicks
  and reload the page from the canonical refreshed generation after success.
- [x] 6.4 Add backend and component tests for locked, pending, ready, refresh-success, refresh-failure,
  single-control, and cross-surface spot/timestamp coherence behavior.
- [x] 6.5 Run focused and repository-wide tests, lint, formatting, JavaScript syntax, strict OpenSpec
  validation, production rebuild, and authenticated receiving-surface checks.
