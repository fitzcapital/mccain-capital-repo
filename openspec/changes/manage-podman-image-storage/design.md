## Context

`scripts/run_podman_app.sh` builds the same `localhost/mccain-capital-app:latest` image on every
deployment. Retagging `latest` leaves prior build results dangling, and no repository workflow
currently reports or removes them. Cleanup must be conservative because the local container is the
user's live application and its persistent data must remain untouched.

## Goals / Non-Goals

**Goals:**

- Provide one script for readable status, dry-run cleanup planning, and explicit cleanup.
- Keep the running image, current application tag, and a rollback tag protected.
- Bound normal dangling-image growth after successful deployments.
- Make behavior deterministic and testable without a real Podman store.

**Non-Goals:**

- Removing containers, volumes, persistent data, or arbitrary tagged images.
- General Podman machine maintenance or destructive system-wide pruning.
- Guaranteeing an exact byte estimate before Podman performs deletion.

## Decisions

1. **Use a standalone Bash command with `status`, `cleanup`, and `auto` modes.** Status and cleanup
   without `--apply` are read-only. `cleanup --apply` prunes eligible dangling images; `auto` is
   reserved for post-deploy use and follows the same safety boundary. This is clearer and safer than
   embedding opaque prune commands directly in the deploy script.
2. **Prune dangling images only.** The implementation will use Podman's dangling-image filter plus
   an age retention filter. It will not use `podman system prune` or `podman image prune --all`, which
   could remove unrelated tagged images.
3. **Keep a rollback tag.** Before a new build replaces `latest`, the existing application image is
   tagged as `localhost/mccain-capital-app:rollback`. Tagged current and rollback images are not
   dangling and therefore remain outside cleanup candidates.
4. **Run automatic cleanup only after health succeeds.** A failed build or unhealthy replacement
   leaves cleanup untouched. Cleanup failure is reported but does not turn a healthy deployment into
   a failed deployment.
5. **Default retention is zero hours and is configurable.** Frequent local builds created gigabytes
   of reclaimable layers within one day. The dedicated rollback tag preserves the previous deploy,
   so untagged replaced builds add storage without adding recovery value.
6. **Test through a fake Podman binary.** Focused tests capture invocations and fixtures for image
   inventory output, proving that default modes do not prune and applied cleanup uses only the
   approved filters.

## Risks / Trade-offs

- [Dangling layers can grow during failed builds] -> Show candidates in status output and clean them
  on the next successful deployment.
- [Podman versions can format sizes differently] -> Treat Podman's summary as display text and avoid
  fragile byte parsing for safety decisions.
- [A rollback image adds storage] -> Retain only one stable rollback tag, overwritten before each
  successful new build attempt.
- [Concurrent deploy and manual cleanup could race] -> Use a repository-specific lock directory and
  fail closed when another maintenance operation owns it.
- [Automatic cleanup could fail] -> Log the failure clearly, preserve the healthy container, and
  allow a later manual retry.

## Migration Plan

1. Add the manager and fake-Podman tests.
2. Run status/dry-run against the live store and inspect candidates.
3. Apply one manual conservative cleanup and verify the running container plus `/healthz`.
4. Integrate rollback tagging and post-health auto cleanup into the deployment script.
5. Roll back by removing the deploy hook; the standalone script makes no persistent configuration
   changes.

## Open Questions

- None. Zero-hour retention is safe because current and rollback images are tagged and protected;
  operators can override it per invocation.
