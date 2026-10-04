## Context

Market Pulse combines polling health, event-driven strategy state, completed candles, live bars, and tape events. Their different freshness semantics are currently flattened into shared labels and a single degraded verdict.

## Goals / Non-Goals

**Goals:** Keep the execution gate conservative while making health and setup language truthful, compact, and timezone-safe.

**Non-Goals:** Change signal detection, setup scoring, levels, targets, or provider integrations.

## Decisions

- Compute the top-level trust verdict from required feeds only. Optional feeds retain component-level advisory states.
- Treat the strategy feed as event-driven: absence of a recent signal is waiting, not a stale data failure.
- Label completed-candle and live-bar times explicitly instead of forcing them to match.
- Format tape timestamps at render time in `America/New_York`, preserving stored timestamps.
- Keep compact primary labels and place semantics in clearer status copy rather than adding new panels.

## Risks / Trade-offs

- Optional data problems may be less prominent → retain advisory component badges and detail text.
- Timestamp parsing may receive naive values → preserve the existing fallback display when parsing fails.
- Copy changes could drift between server and live refresh → update both initial HTML and JavaScript refresh paths with contract tests.

## Migration Plan

No data migration. Deploy as a presentation and health-classification update; rollback is a code revert.

## Open Questions

None.
