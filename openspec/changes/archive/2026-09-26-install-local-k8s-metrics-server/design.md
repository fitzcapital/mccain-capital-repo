## Context

The kind cluster currently has resource requests and limits but no Metrics API, so the resource
monitor cannot show current CPU or memory consumption. Metrics Server must trust kind's kubelet
certificate and use an address reachable inside the local node network.

## Goals / Non-Goals

**Goals:**

- Pin and vendor the Metrics Server resources used by this repository.
- Configure kubelet TLS and preferred node addressing for kind.
- Make repeat deployments idempotent and wait for metrics readiness.
- Preserve application startup if telemetry takes extra time to become available.

**Non-Goals:**

- Historical metric storage, dashboards, autoscaling, or external monitoring.
- Changes to application resource budgets or persistent data.

## Decisions

- Vendor a repository-owned manifest instead of downloading the latest manifest on every deploy.
  This removes a network and version-drift dependency from normal deployment.
- Use the official Metrics Server image with `--kubelet-insecure-tls` and
  `--kubelet-preferred-address-types=InternalIP,Hostname,ExternalIP`, which is the standard local-kind
  compatibility adjustment. It is restricted to this local development cluster.
- Apply telemetry after application workloads and wait for the Metrics Server rollout. A short,
  bounded poll confirms the Metrics API; telemetry failure is reported clearly without deleting or
  recreating application resources.

## Risks / Trade-offs

- [Local kubelet TLS verification is relaxed] → Scope the manifest to the local kind cluster only;
  do not reuse it for a remote or production cluster.
- [Metrics can take time to populate] → Distinguish installed-but-not-ready from not-installed and
  use bounded retries.
- [Additional resource usage] → Use one replica with conservative CPU and memory requests/limits.
