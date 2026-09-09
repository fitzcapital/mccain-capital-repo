## Context

Setup Replay walks the session's completed five-minute bars point in time, keeps armed candidates across candles, and emits every eligible trigger. The Live Setup Monitor receives only the latest scenario rankings, sorts them, and persists one `primary` candidate. Secondary candidates are transient summaries, so a valid setup can be omitted from the durable ledger when another setup ranks above it or when evaluation resumes after its trigger candle.

The existing scenario engine, completed-bar contract, JSON setup ledger, canonical generation, and alert delivery ledger remain authoritative. The solution must not mutate runtime ledgers during migration or infer eligibility from future data.

## Goals / Non-Goals

**Goals:**

- Produce one shared point-in-time setup-event stream from completed candles for both live persistence and replay.
- Persist all newly eligible events idempotently while keeping a single ranked Primary for presentation.
- Recover bounded missed-candle events as review-only records.
- Preserve freshness, permission, cutoff, acknowledgement, terminal-state, and alert-delivery safety.

**Non-Goals:**

- Changing scenario qualification, STRAT definitions, scores, grades, ranking, targets, or invalidations.
- Sending historical alerts or treating replay output as execution-authoritative.
- Replacing the existing JSON ledger or adding a database migration.
- Reprocessing sessions outside the currently loaded session.

## Decisions

### 1. Extract one point-in-time setup event evaluator

Create a shared service helper that accepts ordered completed bars plus the existing level, strategy, and gamma inputs and returns deterministic eligible setup events. Each event freezes the signal candle, pattern candle, trigger boundary, candidate/level identity, entry basis, and plan fields at that timestamp.

Both Live and Replay consume this helper. Replay adds forward outcome measurement; Live adds persistence, state transitions, and alert policy. This is preferred over comparing Live to Replay after the fact because it prevents the two paths from drifting at the qualification boundary.

### 2. Event identity is independent of current rank

Use a stable identity composed from session, ticker, candidate/level identity, pattern code, pattern completion, and trigger time. Rank is not part of identity. Candidate ordering may change which event is Primary but cannot create, merge, or discard an event.

### 3. Live processes the completed-bar delta

The live ledger stores the last successfully processed completed-candle timestamp per session. Each canonical evaluation processes bars after that checkpoint, in chronological order, including all events on each candle, then advances the checkpoint only after an atomic ledger write.

On first use with no checkpoint, processing is bounded to the available current-session bar window. Events whose trigger candle predates the current live evaluation are persisted with `late_review_only=true` and never emit an alert. An event on the newest just-completed candle may alert only through the existing freshness, cutoff, permission, score, and delivery-deduplication gates.

### 4. Persistence and presentation are separate

Every event is upserted into `setups`; terminal and acknowledgement fields remain monotonic. The Primary is still chosen by the existing candidate rank from currently actionable candidates. Secondary and recent output include other persisted events so the user can see that a setup occurred even after another candidate replaces it.

### 5. One atomic write covers checkpoint, events, and deliveries

The existing file lock and atomic replacement remain. Events, delivery ids, recent transitions, and the completed-bar checkpoint commit together. A failed write leaves the prior checkpoint intact so retrying is idempotent.

### 6. Persisted events reconcile from frozen events, not the current Primary

After shared events are upserted, Live evaluates their target/invalidation lifecycle against the
same subsequent completed candles as Replay. Event family, pattern, level, and trigger facts remain
frozen. Only lifecycle fields may advance monotonically. The separate current Primary candidate
MUST NOT spread its family or `ARMED/WATCHING` state into a historical event record.

## Risks / Trade-offs

- **More point-in-time evaluations per refresh** → Process only the unprocessed completed-bar delta and bound first-run recovery to the current session.
- **Historical inputs such as gamma may not exist at every signal** → Preserve explicit unavailable evidence; never substitute a later value for a past signal.
- **First deployment can discover earlier same-session setups** → Mark all recovered events review-only and suppress notifications.
- **Identity changes could duplicate existing records** → Preserve legacy setup ids where possible, add event ids additively, and test idempotent adoption of an existing primary record.
- **Concurrent workers process the same delta** → Retain the ledger lock, reread inside the lock, and deduplicate by stable event id before atomic write.
- **Existing ledgers contain drifted mutable fields** → Reconcile only stable event records from
  canonical event facts; preserve acknowledgements and deliveries and do not rewrite other sessions.

## Migration Plan

1. Add the shared event evaluator and parity tests without changing output.
2. Extend the ledger additively with a processed-candle checkpoint and event id.
3. Process all new events and expose persisted secondary/recent records.
4. Rebuild and verify live/replay parity on deterministic fixtures and the deployed page.

Rollback restores primary-only evaluation. Additive checkpoint and event-id fields remain harmless to the older reader; no runtime data rewrite is required.

## Open Questions

None.
