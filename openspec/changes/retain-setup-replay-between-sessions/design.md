## Context

The replay endpoint defaults `session_date` to today's Eastern date. The analyzer correctly
excludes candles from other dates, which unintentionally removes Friday's data on Sunday.
The source audit on Aug 30 found 46 retained Aug 28 candles through 13:15 ET, not a complete day.
Replay reads canonical context/playbook snapshots and durable level observations; it does not
need a new history database to fix this default-date mismatch.

## Goals / Non-Goals

**Goals:** Preserve available session review across closed periods, accurately label coverage,
and transition deterministically when the next session supplies a completed candle.

**Non-Goals:** Historical provider backfill, multi-day archive UI, new storage schemas, live
freshness relaxation, strategy changes, notifications, or runtime-data rewrites.

## Decisions

1. Resolve a replay-only session after selecting a source snapshot. Normalize candle timestamps
   to Eastern time, reject malformed/future/incomplete/non-session rows, and select the latest
   regular-session date represented by valid completed candles. Explicit `session_date` remains
   authoritative. Merely subtracting a day or hard-coding Friday fails holidays and missing data.
2. Select a compatible retained snapshot with bars using existing persisted context/playbook
   sources, independently of live execution freshness. Keep its quote/structural inputs together;
   never combine a prior session's bars with current-day high/low or another day's Gamma context.
   An older review snapshot must not be promoted as fresh execution data.
3. Scope candle filtering and durable level recovery to the resolved session. Preserve existing
   point-in-time Gamma checks, deduplication, targets, and scoring. With no valid source, return
   a clear unavailable reason rather than inventing a session or reporting zero setups as success.
4. Add response metadata for session date, retained-review state, and evaluated-through time.
   Render a compact date and `Review only` badge for retained/closed-session results. Render
   partial coverage honestly; do not imply that the last cached candle is the session close.
5. On receipt of the first valid completed regular-session candle of the new session, use that
   session even if it contains zero setups. Until then, retain the previous session with its date.
   Use the existing market calendar for weekends, holidays, and early-close boundaries.
6. Preserve explicit-date behavior: invalid dates remain validation errors; an unavailable
   requested date is not silently replaced. Review loading remains read-only. Chart markers must
   remain timestamp/session-scoped and must not feed the live alert path.

## Risks / Trade-offs

- [Cache covers only part of Friday] → Display evaluated-through time; backfill is out of scope.
- [Latest snapshot is empty] → Check existing compatible persisted sources before unavailable.
- [Old review appears actionable] → Review-only label; no change to live permission or alerts.
- [New session is delayed] → Preserve prior date visibly, never relabel old bars as today's data.
- [Gamma history is incomplete] → Retain unavailable-at-signal semantics rather than extrapolate.

## Migration Plan

No migration. Add focused resolver/API/render tests, deploy through the normal Podman rebuild,
check health and Sunday replay output, and verify the displayed date and coverage. Roll back only
the scoped code changes if necessary; existing snapshots and setup history remain untouched.

## Open Questions

None for this scope. Multi-day browsing and missing-candle backfill are separate future work.
