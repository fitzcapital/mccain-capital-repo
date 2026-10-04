## Why

The dedicated Market Pulse worker can accumulate hundreds of threads until it reaches the
pod's 512-task ceiling. Its heartbeat remains current while new processes can no longer start,
so Kubernetes reports a running pod even though chart, gamma, and setup refreshes are no longer
reliably observable or recoverable.

## What Changes

- Identify and eliminate the recurring worker path that leaves threads or subprocess pipes open.
- Add bounded worker resource accounting and an in-process fail-fast threshold below the pod PID
  ceiling so Kubernetes can restart an unhealthy worker before refreshes silently stall.
- Make worker health distinguish a current heartbeat from safe thread/process capacity.
- Add diagnostics and focused soak tests proving repeated Market Pulse refresh cycles do not grow
  worker tasks without bound.
- Verify the deployed worker remains below the warning threshold while the full-session chart,
  gamma context, targets, and setup replay continue to refresh.
- Non-goals: changing trading rules, chart presentation, target calculations, gamma semantics,
  persistent trading data, or production deployment architecture.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-operational-resilience`: Require bounded worker task growth, capacity-aware health,
  and safe self-recovery before PID exhaustion can stale Market Pulse data.

## Impact

- Worker lifecycle and background refresh services under `mccain_capital/`.
- Local Kubernetes worker probes and resource configuration in `k8s/base.yaml`.
- Local status/guard diagnostics and focused worker-resilience tests.
- No API contract or financial assumption changes; existing cached snapshots and persistent data
  remain authoritative during a worker restart.

Acceptance requires a repeatable refresh soak with stable task counts, a worker restart when the
safety threshold is intentionally exceeded, healthy Kubernetes workloads, `/healthz` success, and
current Market Pulse chart/gamma/setup timestamps after deployment.
