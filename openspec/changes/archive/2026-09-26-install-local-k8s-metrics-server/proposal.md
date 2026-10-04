## Why

The local Kubernetes cluster enforces CPU and memory limits but cannot report live usage because
the Metrics API is absent. Installing a pinned, kind-compatible Metrics Server makes resource
pressure visible in the existing monitor and `kubectl top` without changing application data.

## What Changes

- Add a pinned Metrics Server manifest configured for the local kind cluster.
- Deploy and wait for the Metrics API during normal local Kubernetes deployment.
- Verify live pod CPU and memory readings before declaring deployment complete.
- Keep application resource limits authoritative if metrics are temporarily unavailable.
- Non-goals: autoscaling, cloud monitoring, long-term metric storage, or application-data changes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-operational-resilience`: Require repeatable local CPU and memory telemetry for the
  application workloads.

## Impact

This affects the local Kubernetes manifests, deployment script, resource monitor contract tests,
and the cluster `metrics.k8s.io` API. Acceptance requires a healthy Metrics Server deployment and
successful `kubectl top pods` output for the web and worker pods.
