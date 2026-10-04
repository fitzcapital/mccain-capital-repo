## Why

The local Kubernetes node exhausted its process/thread capacity, taking down the web pod and preventing probes, mounts, and maintenance jobs from starting. Resource limits currently cover CPU and memory but do not isolate PID growth or provide early node-capacity recovery.

## What Changes

- Configure a per-pod PID ceiling in the local kind cluster so one workload cannot exhaust the node.
- Add an external, conservative health guard that detects sustained endpoint failure or critical node PID usage and restarts only the local kind node after confirming the target.
- Extend the resource monitor to report node PID usage and warn before process creation fails.
- Keep storage maintenance concurrency bounded and preserve all persistent application data.
- Non-goals: no trading-rule, market-data, database, or production-cluster changes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-operational-resilience`: Add local Kubernetes PID isolation, early warning, and bounded recovery behavior.

## Impact

- Affects `k8s/kind-config.yaml.template`, local deployment/monitoring scripts, and focused script tests.
- Applying the PID ceiling requires recreating the local kind cluster; the external `persistent-data` mount is retained.
- Acceptance: each pod receives a finite PID ceiling, the monitor reports node PID pressure, recovery targets only the named local node, and the rebuilt site passes health and data-mount checks.
