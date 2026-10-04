## Context

The dedicated worker starts long-lived Market Pulse, gamma, options, setup-monitor, trade-sync,
and backup loops in one Python process. The current pod is still touching its heartbeat while
holding 510 threads under a 512-task cgroup limit. Most leaked threads are blocked on futexes and
the process retains many pipe descriptors, which points to a recurring subprocess-backed provider
path rather than the fixed set of named daemon loops. Kubernetes exec probes become impossible at
the ceiling and remain insufficient as the only liveness signal.

The web pod and persistent data are separate. Restarting only the worker preserves the last-good
snapshots and does not interrupt the site, but a restart is containment rather than a root fix.

## Goals / Non-Goals

**Goals:**

- Attribute recurring thread and pipe growth to a specific refresh path.
- Ensure repeated refresh cycles release all provider/browser resources.
- Keep the steady-state worker task count bounded and observable.
- Have the worker exit cleanly before its cgroup ceiling prevents Kubernetes recovery.
- Preserve last-good Market Pulse data and make temporary worker recovery explicit.

**Non-Goals:**

- Change Market Pulse trading logic, gamma interpretation, targets, chart layout, or setup rules.
- Delete or migrate persistent data.
- Replace local kind, Python background workers, or the existing provider integrations.

## Decisions

1. **Measure at the process boundary.** Add a small worker resource snapshot that reads
   `/proc/self/task` and `/proc/self/fd`, with a portable `threading.active_count()` fallback.
   Record baseline, current, and high-water counts without exposing credentials or request data.
   This captures native/library threads that named Python-thread inspection can miss.

2. **Fix the owning refresh path, not the chart.** Exercise each scheduled worker path repeatedly
   in isolation, identify which call increases tasks or pipe descriptors after completion, and
   close/reuse its browser, event-loop, executor, response, or subprocess resources. Add a focused
   regression test around that boundary. Raising the PID limit is rejected because it delays the
   same failure.

3. **Self-protect below the cgroup ceiling.** The worker main loop checks task capacity before
   updating its heartbeat. After a configurable number of consecutive critical samples it stops
   updating, writes a sanitized diagnostic, and exits nonzero. Kubernetes then restarts only the
   worker while last-good snapshots remain readable. A default critical threshold leaves enough
   headroom for probes and shutdown.

4. **Separate readiness from liveness.** `--check` validates both heartbeat age and the last
   capacity sample. Status tooling reports task count, ceiling, high-water mark, and recovery
   reason. Existing exec probes remain, but correctness no longer depends on spawning a probe at
   the exact exhaustion point.

5. **Verify behavior at the receiving surfaces.** Deployment verification checks worker task
   stability over multiple refresh intervals, then confirms `/healthz`, current chart candles,
   gamma/target timestamps, and setup replay data. Unit tests alone are insufficient.

## Risks / Trade-offs

- **[A threshold can restart a worker during a temporary provider spike]** → require consecutive
  critical samples and retain last-good snapshots during restart.
- **[A provider library may own hidden native threads]** → test both task and file-descriptor
  deltas and isolate provider calls rather than relying only on Python thread names.
- **[Self-exit can loop if the leak remains]** → include restart backoff through Kubernetes and
  fail deployment verification if the task count trends upward after replacement.
- **[Diagnostics could expose sensitive provider context]** → persist only counts, component name,
  timestamps, and sanitized reason codes.

## Migration Plan

1. Add diagnostics and reproduce the growth with focused local tests.
2. Correct the identified resource owner and add regression coverage.
3. Add capacity-aware worker health and Kubernetes configuration/tests.
4. Build and deploy the existing local image without touching the persistent volume.
5. Restart only the worker deployment, observe several refresh intervals, and verify Market Pulse.
6. Roll back the image and worker deployment if task counts rise or refresh timestamps regress.

## Open Questions

- **Resolved:** the 15-second server setup-monitor boundary was rebuilding the same completed
  candle through the full provider path. That created five transient provider tasks per cycle and,
  before the cache-first gate, allowed task accumulation to reach 510. The same path also exposed
  a separate SQLite ownership bug: `with db() as conn` completed transactions but Python's native
  SQLite context manager did not close connections, retaining database and WAL descriptors.
- `InstrumentedConnection.__exit__` now closes every application-owned SQLite connection after
  commit or rollback. A short-lived cache-first monitor gate was removed after full-session
  verification proved it could mistake an old cached candle for the current completed candle.
- Deployed soak evidence on 2026-09-28 held the worker at 10 tasks and 5-7 descriptors across
  multiple 15-second monitor intervals, compared with the pre-fix 510 tasks at a 512-task ceiling.
  The configured warning/critical bounds are 300/400 tasks with self-exit after three consecutive
  critical samples.

### Follow-up diagnosis: gamma image renderer

The next full-session observation showed that the short soak had not crossed enough 60-second gamma
refresh intervals. An isolated provider harness proved that `gamma_map_service.get_gamma_snapshot`
on a cold refresh retained exactly five Python threads named `checking_close-*` and
`readwrite_thread-*`. Those threads belong to Choreographer, which Plotly 7 uses for Kaleido image
export. `export_outputs` attempted `Figure.write_image()` every minute, caught renderer failures,
and left Choreographer's manual thread executors alive.

The live page consumes Plotly chart JSON and treats the PNG artifact as optional. Normal gamma
refresh therefore writes the CSV and chart JSON without launching Kaleido. Static PNG generation is
now explicit and opt-in for an operator export, so the trading refresh path owns no browser renderer.
The setup monitor continues to build the current canonical snapshot before comparing candle IDs;
this preserves new-candle detection without relaunching the optional image renderer.

The final 2026-09-29 deployed soak crossed multiple gamma refreshes while holding at 10 tasks and
6-7 file descriptors with zero worker restarts. Gamma timestamps advanced, PNG output remained
disabled, and the canonical page stayed unlocked. During verification, two related accuracy defects
were also corrected: replay storage now retains the full 390-minute regular session instead of the
last 240 minutes, and bar freshness uses the latest completed five-minute candle rather than a
still-forming one-minute display point. The live chart then covered 9:30 AM onward and advanced
completed candles without future-dating them.
