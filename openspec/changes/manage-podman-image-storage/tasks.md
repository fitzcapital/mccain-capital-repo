## 1. Storage Manager

- [x] 1.1 Add `scripts/manage_podman_storage.sh` with read-only status as the default mode
- [x] 1.2 Add cleanup preview and explicit `cleanup --apply` behavior limited to retained dangling images
- [x] 1.3 Add an applied-maintenance lock and concise Podman-unavailable failure output
- [x] 1.4 Add before/after storage summaries and retention-policy reporting

## 2. Deployment Integration

- [x] 2.1 Preserve the existing current application image under one rollback tag before building
- [x] 2.2 Make failed health checks exit nonzero before any automatic cleanup
- [x] 2.3 Invoke conservative automatic cleanup only after health succeeds and keep cleanup failure nonfatal

## 3. Verification

- [x] 3.1 Add fake-Podman tests proving status and preview modes never delete images
- [x] 3.2 Add tests proving applied cleanup uses dangling and age filters and respects the operation lock
- [x] 3.3 Add deployment contract tests for rollback tagging, post-health cleanup, and failed-health behavior
- [x] 3.4 Run focused tests, Bash syntax checks, and `git diff --check`
- [x] 3.5 Inspect the live cleanup preview, apply the initial cleanup, and verify the running container and `/healthz`

## 4. Operator Guidance

- [x] 4.1 Document status, preview, applied cleanup, retention override, rollback tag, and safety boundaries
