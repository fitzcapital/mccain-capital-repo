## Context

McCain Capital currently runs in one Podman container with repository data bind-mounted at `/data`.
Application startup also launches trade synchronization, backup, and Market Pulse monitor threads in
every server process. A naive two-replica Deployment would therefore duplicate background work and
create unsafe concurrent SQLite writers. The Mac is Apple Silicon with a four-CPU, four-GB Podman VM;
`kubectl`, kind, and Freelens are not installed.

## Goals / Non-Goals

**Goals:**

- Run a local Kubernetes cluster through the existing Podman machine and manage it in Freelens.
- Preserve `http://localhost:5001`, authentication, routes, and existing persistent data.
- Run exactly one web pod and one purpose-specific background worker pod.
- Bound laptop disk, CPU, and memory consumption and expose actual use in Freelens.
- Provide repeatable install, deploy, status, cleanup, and rollback commands.

**Non-Goals:**

- Cloud hosting, multi-node availability, horizontal web scaling, or PostgreSQL migration.
- Concurrent SQLite writers or copying personal data into container images.
- Running the old standalone container alongside Kubernetes after cutover.

## Decisions

1. **Use kind with its Podman provider.** This reuses the existing Podman VM instead of creating a
   second virtualization stack. Freelens reads kind's kubeconfig. Minikube was rejected because a
   separate VM would add disk and memory overhead.
2. **Use one kind control-plane node with a host port mapping for 5001.** A NodePort mapped by kind
   keeps the existing localhost URL without a permanent `kubectl port-forward` process.
3. **Mount repository `persistent-data/` into the kind node, then expose it as a hostPath PV.** The
   PVC uses single-writer semantics. Only the worker owns background writes; web requests retain
   normal interactive writes through the same single-node filesystem and SQLite locking. No second
   web replica is introduced.
4. **Add explicit runtime roles.** `MCCAIN_RUNTIME_ROLE=web` disables all startup background loops;
   `worker` runs a dedicated process entrypoint that owns synchronization, backup, and setup-monitor
   loops without serving traffic. The default role preserves current Podman behavior for rollback.
5. **Use one built image shared by both pods.** The deploy script builds once, loads once into kind,
   and uses `imagePullPolicy: IfNotPresent`. Pods share node image layers, so a second pod does not
   create another 2-GB image copy.
6. **Set conservative resource controls.** Web requests 300m CPU/512Mi and is limited to 1500m/1Gi;
   worker requests 150m/256Mi and is limited to 750m/768Mi. The existing Podman VM remains at its
   stable four-GB ceiling after a six-GB configuration proved unstable on this host; CPU remains
   four. Requests total 0.45 CPU and 768Mi, while limits prevent a runaway process from exhausting
   the laptop.
7. **Bound storage automatically.** Deployment removes obsolete app images from the kind node after
   readiness succeeds, retains only current and one rollback build reference, and invokes the
   existing host Podman storage manager. Cluster teardown is explicit and does not remove
   `persistent-data/`.
8. **Cut over only after verification.** Kubernetes readiness, `/healthz`, data-path checks, worker
   ownership, and resource declarations must pass before stopping/removing the standalone container.

## Risks / Trade-offs

- [kind with rootless Podman competes for the four-GB VM budget] -> Keep workload requests below one
  CPU and one GB combined, use one cluster node, and verify live utilization before cutover.
- [SQLite is not a horizontally scalable database] -> Keep one web pod and one worker owner; prohibit
  replicas greater than one until a database migration is designed.
- [Both roles can still touch the same SQLite file] -> Preserve WAL/busy-timeout behavior, use one
  node/PVC, and verify write contention under focused tests.
- [Image exists in host and kind stores] -> Share it across pods, prune stale revisions after healthy
  deploys, and expose storage status/cleanup commands.
- [Local cluster failure makes the site unavailable] -> Keep the Podman launcher compatible and
  document a tested rollback that remounts the same persistent directory.
- [Background role separation could miss a loop] -> Add startup contract tests and a worker-health
  record that proves exactly one owner is active.

## Migration Plan

1. Add role switches, dedicated worker entrypoint, manifests, and tests without changing the active
   runtime.
2. Install tools, resize/restart the Podman VM if required, and create the kind cluster.
3. Build/load the image and deploy both pods while the standalone app remains available.
4. Verify pods, worker ownership, persistent data, resource use, and Kubernetes localhost routing.
5. Stop the standalone container, activate port 5001 for Kubernetes, and rerun page/health checks.
6. Connect Freelens to the generated kubeconfig and verify workloads, logs, and resource limits.
7. Roll back by stopping the kind workload and running `scripts/run_podman_app.sh`; data remains in
   the same repository directory.

## Open Questions

- None. Local-only kind/Podman, one web pod, and one worker pod are the selected baseline.
