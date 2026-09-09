## 1. Shared Setup Event Evaluation

- [x] 1.1 Extract a point-in-time completed-candle evaluator that emits all eligible setup events with frozen signal facts and stable event ids.
- [x] 1.2 Make Setup Replay consume the shared events while retaining its forward MFE, MAE, target, and invalidation analysis.
- [x] 1.3 Add parity fixtures for 2-2 and 2-1-2 setups, multiple same-candle triggers, missing point-in-time evidence, and deterministic event identity.

## 2. Complete Live Ledger Processing

- [x] 2.1 Extend the live ledger additively with the last atomically processed completed-candle checkpoint and stable event ids.
- [x] 2.2 Process every unprocessed completed candle chronologically and upsert all eligible setup events instead of persisting only the current Primary.
- [x] 2.3 Preserve existing terminal state, acknowledgement, revision, and delivery records when repeated evaluation or candidate reordering encounters an existing event.
- [x] 2.4 Mark first-run and gap-recovered historical events review-only while restricting alerts to newly completed current events that pass all existing gates.

## 3. Primary and History Presentation

- [x] 3.1 Continue selecting one current Primary with the existing ranking while exposing other persisted events through secondary and recent payloads.
- [x] 3.2 Update the receiving-page copy only as needed to distinguish current actionable Primary from review-only recovered history.
- [x] 3.3 Add route and template contract tests proving qualifying secondary events remain visible without becoming competing execution instructions.

## 4. Reliability and Verification

- [x] 4.1 Add focused tests for simultaneous triggers, missed-candle recovery, no late alerts, worker overlap, atomic-write retry, duplicate suppression, and terminal-state monotonicity.
- [x] 4.2 Run focused live-setup, replay, scenario, route, syntax, formatting, and `git diff --check` validation.
- [x] 4.3 Rebuild with `./scripts/run_podman_app.sh`, verify `/healthz`, and inspect deployed Live Setup and Replay parity without modifying runtime ledger data.

## 5. Frozen Event Lifecycle Reconciliation

- [x] 5.1 Prevent current Primary persistence from overwriting a stable historical event's family and pattern facts.
- [x] 5.2 Reconcile every stable Live event lifecycle with Replay's completed-candle outcome semantics while preserving monotonic terminal and delivery fields.
- [x] 5.3 Add regressions for Primary-family drift, target/invalidation parity, terminal monotonicity, and repeated reconciliation.
- [x] 5.4 Reproduce September 1 at 9:40 and 10:40, run focused checks, rebuild, verify health, and audit deployed parity.
