## Why

The application currently runs as one Podman container, which makes web serving and background
Market Pulse work share one lifecycle and offers limited workload visibility. A local Kubernetes
deployment managed through Freelens will separate responsibilities while preserving localhost
access, persistent data, and bounded laptop disk, CPU, and memory usage.

## What Changes

- Install and configure `kubectl`, kind, and Freelens for an Apple Silicon local cluster using the
  existing Podman machine.
- Add a reproducible local kind cluster configuration with localhost port 5001 and the existing
  `persistent-data/` directory mounted into the cluster node.
- Add two purpose-specific workloads: one web pod and one Market Pulse/background-worker pod.
- Add explicit runtime switches so web workers do not start background loops and the worker pod does
  not serve user traffic.
- Add Kubernetes Service, persistent volume/claim, health probes, resource requests/limits, and
  single-writer safeguards.
- Add build/load/deploy/status/cleanup scripts that reuse one image across pods, bound image-cache
  growth, and retire the standalone container only after Kubernetes is healthy.
- Keep the site available at `http://localhost:5001` with the same application routes and data.
- Non-goals: cloud deployment, multi-node high availability, two concurrent SQLite writers, or a
  database migration in this change.

## Capabilities

### New Capabilities

- `local-kubernetes-deployment`: Reproducible local cluster installation, workload deployment,
  localhost routing, persistent storage, resource controls, Freelens visibility, and cleanup.
- `separated-runtime-roles`: Explicit web and worker roles that prevent duplicate sync, monitor,
  backup, and alert loops while preserving existing application behavior.

### Modified Capabilities

- None.

## Impact

- Affected code: application startup/runtime configuration, `Containerfile`, new Kubernetes manifests
  and lifecycle scripts, focused tests, and operator documentation.
- Local tools: Homebrew-managed `kubectl`, kind, and Freelens; existing Podman remains the container
  provider.
- Persistent data: the current repository `persistent-data/` remains authoritative and is mounted
  through a single-writer persistent volume; no database or user-file migration is allowed.
- Laptop resources: one cluster node inside the existing four-GB Podman VM, one shared application
  image, bounded build cache, CPU requests below one core at idle, explicit CPU/memory limits, and
  removal of the old standalone runtime only after successful cutover.
- Acceptance: both pods become ready, the worker has one active monitor owner, localhost:5001 and
  `/healthz` work, existing data is present, Freelens discovers the cluster, resource limits are
  visible, no dangling app images remain, and rollback to the Podman launcher is documented/tested.
