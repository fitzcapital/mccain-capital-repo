## Why

Repeated local application rebuilds leave prior Podman image layers behind. The current host reports
104 images with only one active and about 3.6 GB reclaimable, so storage grows without a safe,
repeatable way to inspect and clean it.

## What Changes

- Add a repository-owned Podman storage manager that reports total, active, dangling, and
  reclaimable image storage in plain language.
- Make inspection the default and require an explicit apply flag before deleting anything.
- Protect the image used by a running container, the current application image, and one tagged
  rollback image.
- Add a conservative post-deploy cleanup mode that runs only after the new container passes its
  health check and removes eligible dangling images older than a retention window.
- Report what was removed and the before/after storage totals.
- Add focused tests with a fake Podman executable so cleanup safety is verified without touching
  the developer's real images.
- Non-goals: removing containers or volumes, deleting arbitrary tagged images, pruning active
  images, or changing application/runtime data.

## Capabilities

### New Capabilities

- `podman-image-storage-management`: Safe inspection, retention, cleanup, rollback protection, and
  post-deploy management of local Podman application images.

### Modified Capabilities

- None.

## Impact

- Affected code: `scripts/run_podman_app.sh`, a new script under `scripts/`, focused script tests,
  and concise operator documentation.
- External system: the local Podman machine and its image store.
- Dependencies: existing Bash, Podman CLI, and standard Unix tools only.
- Data safety: persistent data, containers, and volumes remain out of scope; default execution is
  read-only.
- Acceptance: status mode performs no deletion; cleanup requires explicit authorization; active,
  current, rollback, and recent images survive; eligible dangling images are removed only after a
  healthy deployment; failures are visible and do not hide deployment health.
