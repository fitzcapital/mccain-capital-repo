## Context

The kind cluster runs inside Podman and mounts the repository's persistent data into a PVC. Host image cleanup already runs after healthy deployments and in the laptop monitor. Kubernetes needs only PVC visibility and cleanup of disposable application scratch data.

## Goals / Non-Goals

**Goals:**

- Report PVC capacity on a predictable schedule.
- Remove aged files only from dedicated `/data/tmp` and `/data/cache` roots.
- Keep the job observable, manually runnable, non-overlapping, and resource bounded.

**Non-Goals:**

- Accessing the Podman socket or pruning host images from a pod.
- Deleting the SQLite database, uploads, books, backups, or Market Pulse evidence.
- Running a continuously resident monitoring pod.

## Decisions

Use a Kubernetes CronJob every 30 minutes rather than a resident deployment. It consumes resources only while checking storage. The job mounts the PVC and runs the existing application image, using a small Python maintenance command so path validation and tests are clearer than a shell `find` expression.

Cleanup roots and retention are explicit environment values, but the command rejects any root outside `/data/tmp` and `/data/cache`. It reports filesystem usage before and after, deletes regular files older than seven days, and removes only empty descendant directories. Concurrency is forbidden and history is bounded.

Host Podman cleanup remains in `monitor_laptop_resources.sh`; mounting the host container socket was rejected because it would make the maintenance pod privileged and capable of destructive host-wide operations.

## Risks / Trade-offs

- [A configured path could be unsafe] -> Resolve paths and enforce an immutable allowlist before deletion.
- [A cleanup overlaps another run] -> Set `concurrencyPolicy: Forbid` and an active deadline.
- [The job consumes unnecessary resources] -> Use a CronJob with 10m CPU and 32Mi memory requests, strict limits, and no daemon.
- [PVC data is accidentally classified as scratch] -> Do not include uploads, books, backups, hidden evidence files, or the database in cleanup roots.

## Migration Plan

Apply the manifest with the normal deployment script, inspect the CronJob, and run one manual job. Rollback deletes only the CronJob; application deployments and the retained PVC remain unchanged.

## Open Questions

None.
