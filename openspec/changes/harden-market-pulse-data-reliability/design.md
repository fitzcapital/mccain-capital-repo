## Context

Market Pulse combines provider quotes, completed candles, gamma calculations, ladder levels,
scenario ranking, and replay output. These values can refresh at different speeds, and the app runs
multiple workers, so a poll can be transport-successful without producing a coherent execution
snapshot. The current client also needs a more rigorous lifecycle after timeouts, tab suspension,
overlapping requests, and unchanged generations.

## Goals / Non-Goals

**Goals:**

- Make the server generation—not the browser poll—the unit of truth.
- Publish execution-facing data only after symbol, session, timestamp, and generation validation.
- Keep all workers and browser updates consistent with the newest verified generation.
- Recover automatically from transient faults without reload loops or indefinite busy states.
- Make freshness, retry timing, and blockers understandable without noisy refresh messages.
- Add enough instrumentation and deterministic tests to prove the behavior repeatedly.

**Non-Goals:**

- Changing SPX strategy, gamma calculations, scenario scores, or setup definitions.
- Supporting additional instruments or market-data vendors.
- Implementing late-session viability or redesigning the Gamma Ladder.
- Treating a faster quote as permission to trade when required gamma or candles are stale.

## Decisions

### 1. Promote immutable canonical generations

The server will assemble a candidate snapshot with a generation id, build start/end times, source
timestamps, session id, symbol, and component statuses. It will promote the candidate only when all
required execution components validate. The last verified generation remains immutable.

This is preferred over independently updating each component because independent updates create the
mixed-state failure the execution lock is intended to prevent.

### 2. Use a shared durable snapshot envelope across workers

The current process cache remains a fast path, but workers will compare it against a shared on-disk
verified envelope and adopt the newest generation before responding. Writes will use an atomic
replace under a single-flight lock; incomplete candidates never replace the verified envelope.

This avoids adding infrastructure such as Redis to a local-only app while addressing worker drift.

### 3. Separate observation freshness from execution freshness

A quote may advance independently for display, but the execution envelope advances only when spot,
completed candles, gamma, and derived guidance validate together. Observation-only values will be
explicitly marked and MUST NOT change the action, trigger, invalidation, target, or last-valid time.

### 4. Make polling an explicit state machine

The coordinator will own `idle`, `refreshing`, `retrying`, `delayed`, and `locked` states. It will use
one request at a time, an abort timeout, bounded exponential backoff with jitter, and immediate
revalidation after tab visibility or network restoration. The countdown uses the coordinator's
actual scheduled-at timestamp rather than an independent timer.

The server will return the recommended next interval and retry classification. Manual refresh joins
or follows the active flight instead of creating competing refreshes.

### 5. Reconcile the page atomically

The browser will validate monotonic generation order and required fields, stage the response, and
commit all execution-facing bindings in one render transaction. Older, malformed, or contradictory
responses are rejected without clearing the current page.

### 6. Instrument reliability without collecting trading secrets

Structured local logs and response diagnostics will record generation id, component age, refresh
duration, outcome, retry reason, and worker adoption. No credentials, raw provider payloads, orders,
or personal financial records will be logged.

## Risks / Trade-offs

- **[Strict validation increases locked time]** → Prefer an honest lock over apparently fresh but
  unsafe guidance; expose the exact blocker and recovery attempt.
- **[Atomic disk envelope can become corrupt]** → Validate before reading, write through a temporary
  file plus atomic replace, and retain the last readable verified envelope.
- **[Retry traffic can amplify an outage]** → Enforce single-flight requests, capped backoff, jitter,
  and server-directed intervals.
- **[Observation quote differs from canonical spot]** → Label it as observation-only and keep the
  canonical execution timestamp and guidance visibly distinct.
- **[System clock drift affects age checks]** → Prefer provider/session timestamps and monotonic
  elapsed time for request lifecycle; reject impossible future timestamps.

## Migration Plan

1. Add the canonical envelope and validators behind the existing context endpoint contract.
2. Add focused server tests for promotion, stale components, worker adoption, and atomic recovery.
3. Upgrade the client coordinator and atomic reconciler while retaining the current manual control.
4. Add deterministic browser tests for overlap, timeout, visibility recovery, and countdown truth.
5. Rebuild the local Podman app, verify health, then observe several live polling cycles and forced
   partial failures. Roll back by restoring the previous coordinator and envelope reader; no data
   migration is required.

## Open Questions

- Freshness thresholds will remain the existing configured values during this phase; threshold
  tuning should be a separately evidenced change.
