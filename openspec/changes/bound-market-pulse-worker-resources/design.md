## Context

The running container is capped at 2,048 tasks. After about 22 hours, Gunicorn workers 6 and 7 held
1,052 and 994 threads, with 2,029 threads blocked in futex waits. New container execution and Gamma
refresh startup failed with `Resource temporarily unavailable`. The current runtime starts one Gamma
loop per Gunicorn process and may also perform forced refreshes from requests. Gamma chain fetching
uses a short-lived thread pool, while native numerical dependencies have no explicit thread caps.

## Goals / Non-Goals

**Goals:**

- Keep process thread counts bounded across repeated background and forced Gamma refreshes.
- Preserve one coordinated Gamma computation across Gunicorn workers.
- Recover gracefully from retained third-party threads before the container limit is reached.
- Expose thread pressure and the precise Market Pulse blocker.
- Surface Gamma and worker-pressure transitions in the local Netdata Events feed.
- Preserve last-good Gamma and execution safety.

**Non-Goals:**

- Raising the container task limit to hide growth.
- Replacing Tradier, Pandas, or the Gamma model.
- Allowing stale Gamma to authorize a trade.
- Modifying persistent trading or journal data.

## Decisions

### Serialize the small Gamma expiry fetch set

Tradier expiry fetching will default to serial requests. The configured Gamma window is small, and
the 12-second network timeout already bounds each request. This removes a per-refresh executor from
the critical path. Explicit parallelism will not be exposed until a persistent bounded executor can
be proven stable under soak.

### Coalesce refresh work inside each process and across processes

The existing file lock remains the cross-process authority. A process-local nonblocking singleflight
gate will prevent a request-triggered force refresh from waiting behind or duplicating its own
background refresh. Callers that lose the gate receive the current last-good snapshot plus an
`in_progress` status rather than creating new work.

### Cap native numerical threads at process startup

The container will set `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, and
`NUMEXPR_NUM_THREADS` to 1 before Python imports numerical libraries. Gamma computation is small and
request latency is dominated by provider I/O, so predictability is more valuable than local BLAS
parallelism.

### Recycle Gunicorn workers as a containment boundary

Gunicorn will use a configurable `max-requests` and jitter. Recycling is graceful and preserves
persistent data and the shared last-good Gamma snapshot. This contains thread retention from
third-party dependencies even if the originating library cannot immediately be eliminated.

### Report thread pressure without making health expensive

Operational diagnostics will read the current process thread count from `/proc/self/status`, expose
it with a configurable warning threshold, and report the last Gamma error/attempt. The page session
label will derive from required-component blockers; Gamma-specific staleness becomes `Waiting on
Gamma`, while healthy aligned data retains the real session label.

### Publish state transitions through Netdata health alarms

The read-only operational-health surface will expose stable numeric dimensions for Gamma freshness,
Gamma refresh failure, and worker thread pressure. Netdata will collect those dimensions locally and
evaluate warning/critical thresholds. Alarm transitions create timestamped Events; recovery creates
a resolved transition. Stable alarm identity and hysteresis prevent a new event on every collection
cycle. Event details identify the affected service, current value, threshold, and concise cause.

## Risks / Trade-offs

- [Serial expiry fetching increases refresh duration] → Keep the existing provider timeout and small
  expiry window; measure refresh duration in focused tests and deployed diagnostics.
- [Worker recycling interrupts an in-flight request] → Use Gunicorn graceful recycling with jitter
  and two workers so requests drain rather than terminate abruptly.
- [Thread caps reduce numerical throughput] → Gamma calculation size is modest; verify refresh timing
  and allow configuration only if measured need appears.
- [Singleflight serves an older snapshot briefly] → Preserve freshness locks and explicitly report
  refresh-in-progress; never promote stale data as current.
- [Transient provider delays create noisy alerts] → Require sustained threshold breach and use
  recovery hysteresis so Netdata records state changes rather than repeated samples.

## Migration Plan

1. Add resource caps, singleflight coordination, serial chain fetch, diagnostics, and Netdata alarms.
2. Add deterministic refresh-concurrency, thread-pressure, and event-contract tests plus a bounded soak.
3. Rebuild the container, which clears the currently exhausted workers without changing volumes.
4. Verify `/healthz`, process thread counts, Gamma refresh completion, Market Pulse blocker copy, and
   Netdata warning/recovery Events.
5. Roll back code/container image if needed; persistent data requires no migration or rollback.

## Open Questions

None. Initial worker recycling and warning thresholds will be conservative and configurable.
