## Context

The Gamma Ladder already receives accepted snapshot rows, source timestamps, role metadata, and Gamma generation identifiers. The surrounding Market Pulse page separately validates a canonical generation. The current presentation keeps lineage collapsed, defaults the inspector to an empty state, and describes the entire negative/positive distribution without converting the nearest boundary into a concise execution sequence.

## Goals / Non-Goals

**Goals:**

- Turn accepted SPX ladder data into a compact immediate decision sequence.
- Keep the depth map price-ordered while exposing deterministic priority levels.
- Make canonical alignment and source age visible before guidance is trusted.
- Remove ambiguity from overlapping decision, failure, magnet, and crossing labels.
- Preserve current controls, refresh behavior, and selected-state stability.

**Non-Goals:**

- No order routing, strategy expansion beyond SPX, provider changes, or Gamma-model changes.
- No replacement of the canonical Market Pulse permission or scenario engine.
- No persistent data migration.

## Decisions

1. **Derive presentation guidance on the client from the accepted payload.** Existing rows already contain distance, role, state, and strength. A pure presentation derivation avoids creating a competing financial decision engine. Alternative: add another server model; rejected because it duplicates canonical strategy authority.

2. **Use the nearest relevant row as `Now`, then derive adjacent confirmation, target, and failure levels by direction and role.** The command rail always includes exact strikes and neutral planning language. It is hidden or locked if data is stale, incoherent, or non-SPX. Alternative: use only the strongest absolute GEX row; rejected because it can be far from spot and not immediately useful.

3. **Keep strike order and add a separate three-item priority strip.** Priority ranking uses proximity first, then decision relevance, role weight, and strength. This preserves spatial market structure while making best-to-least levels scannable.

4. **Treat canonical alignment as a display gate.** The ladder compares its canonical generation identifier with the page generation supplied through a root data attribute/custom event. Mismatch displays `Revalidating` and suppresses live command wording; missing identifiers display `Lineage unavailable`, not an aligned claim.

5. **Scope execution to SPX.** Non-SPX symbols remain available for observation but display `Reference only` and do not receive the SPX command rail. This preserves current research controls without implying strategy validation.

6. **Auto-select without stealing later user intent.** The nearest relevant row is selected only after first load or when the prior selected strike disappears. Subsequent refreshes preserve a valid manual selection.

## Risks / Trade-offs

- **[Risk] Presentation guidance could appear more authoritative than canonical permission.** → Prefix with planning language, honor coherence/freshness gates, and never alter permission.
- **[Risk] Rapid spot movement can reorder the nearest level.** → Recompute `Now` on accepted snapshots while preserving explicit user selection.
- **[Risk] Canonical id is temporarily absent during boot.** → Show `Synchronizing` and retain the last valid ladder rather than claiming alignment.
- **[Risk] More information increases density.** → Use a single compact rail, three priority items, concise badges, and collapsible lineage.

## Migration Plan

Deploy template, CSS, and JavaScript changes together. No data migration is required. Rollback is removal of the added presentation nodes and helper functions; existing ladder rendering and API remain compatible.

## Open Questions

None. The implementation uses existing accepted fields and conservative fallback copy when optional timestamps or crossing metadata are unavailable.
