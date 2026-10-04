## Context

The kind control-plane container currently has no effective PID ceiling and its kubelet does not cap PID use per pod. When task creation was exhausted, Gunicorn could not start threads, kubelet could not run probes or mounts, and in-cluster recovery also failed. Persistent application data is host-mounted outside the cluster.

## Goals / Non-Goals

**Goals:** Isolate PID growth per pod, surface node task pressure, recover conservatively from a confirmed exhausted local node, and preserve persistent data through cluster recreation.

**Non-Goals:** Change application concurrency, production deployment behavior, financial data, or treat ordinary application errors as node exhaustion.

## Decisions

- Set kind kubelet `pod-max-pids` to 512. This is high enough for normal workloads while preventing one pod from consuming the local node.
- Run recovery outside Kubernetes through a macOS LaunchAgent because an exhausted node cannot reliably execute an in-cluster watchdog.
- Require three consecutive failed health checks. Restart the web deployment when the node can still fork; restart the exact kind node only when a lightweight node command also fails.
- Enforce a ten-minute recovery cooldown and log every action. The guard never deletes clusters, images, volumes, or data.
- Report node task count in the existing laptop monitor with warning and critical thresholds below exhaustion.

## Risks / Trade-offs

- [A legitimate workload exceeds 512 tasks] → Kubernetes restarts or throttles only that pod; thresholds remain configurable in the kind template.
- [Transient startup is mistaken for failure] → three-check threshold and cooldown prevent rapid recovery loops.
- [Node restart interrupts the worker briefly] → use node restart only when process creation is already unavailable.
- [Cluster recreation loses runtime state] → verify the external data mount and back up/checksum the database before recreation.

## Migration Plan

Validate scripts, back up the database, delete only the named kind cluster, recreate it from the updated template, deploy the shared image, install the LaunchAgent, and verify health, mounts, PID limits, and workloads. Rollback by removing the kubelet argument and recreating the cluster again; persistent data remains external.

## Open Questions

None.
