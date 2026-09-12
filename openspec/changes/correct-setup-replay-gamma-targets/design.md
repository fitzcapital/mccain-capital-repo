## Context

Replay currently combines completed candles with the latest canonical snapshot and level facts
recovered from the live setup ledger. Level prices can therefore be point-in-time safe, but Gamma
regime is represented by one current value/timestamp and historical level provenance is discarded by
normalization. Estimated option-return targets use fixed premium/delta constants even when the app
has a current Tradier NTM reference.

## Goals / Non-Goals

**Goals:**

- Make Gamma scoring deterministic for each signal and stable across later refreshes/restarts.
- Make every structural target directional and meaningful from actual entry.
- Make target-estimate provenance explicit and prefer valid Tradier NTM inputs.
- Preserve current outcome semantics and financial disclaimers.

**Non-Goals:**

- Recalculate dealer Gamma, predict direction from Gamma, or use Gamma in TP arithmetic.
- Change Strat qualification, entry, invalidation, cutoff, execution, or realized P&L.
- Fabricate historical Gamma for sessions where no observation was retained.

## Decisions

### Append canonical Gamma observations to a bounded durable history

Store one normalized observation per ticker/generation with timestamp, session, regime, and all
numeric dynamic levels. Deduplicate by generation/timestamp and retain a bounded number of sessions.
This is preferable to deriving history from setup events because Gamma can change when no setup fires.
Writes use the existing persistent upload-data boundary and atomic JSON helper.

### Resolve one point-in-time Gamma view per signal

Replay will select the latest observation whose timestamp is not later than the signal candle. That
observation supplies both regime and dynamic levels. Legacy durable setup-level observations remain a
fallback, but a newer snapshot is never projected backward. Missing history remains explicitly
unavailable and earns no Gamma score.

### Preserve provenance through normalization

Scenario level normalization will retain `as_of`. The selected primary and target levels will carry
their timestamps into the Replay payload. This avoids UI-only inference and makes API output auditable.

### Validate structural targets from entry

Canonicalization will choose targets in the setup direction relative to actual entry and require at
least five SPX points from entry, while still excluding the complete anchor cluster. Anchor-relative
distance remains useful for clustering but no longer authorizes a Runner.

### Inject target-estimate inputs rather than fetch per setup

The endpoint will obtain at most one current, valid Tradier NTM reference for the selected ticker and
pass normalized premium, absolute delta, contract, and quote timestamp into the replay builder. Every
setup derives its illustrative price ladder from those inputs. Missing/stale/invalid data uses the
existing $750/0.40 fallback. This avoids repeated provider calls and keeps tests deterministic.

Historical sessions will use only a quote retained for that session; the current option quote must
not be projected backward. Gamma remains separate from the TP model in both cases.

## Risks / Trade-offs

- [No historical regime exists before deployment] → Report unavailable; never backfill current Gamma.
- [Gamma history grows indefinitely] → Bound by session count and observations per session.
- [Tradier quote is wide or stale] → Require timestamp, positive midpoint/premium, plausible delta,
  and a bounded spread; otherwise use the labeled fallback.
- [Target behavior changes historical analytics] → Recompute read-only Replay output without rewriting
  journal trades; preserve the old data as legacy analytics until refreshed.
- [Provider outage] → Structural Runner and Replay outcomes continue; only TP estimate source falls back.

## Migration Plan

1. Add history persistence and point-in-time selectors with deterministic fixtures.
2. Add entry-based Runner selection and option-input injection behind existing payload fields.
3. Add provenance fields and compact rendering labels without removing existing fields.
4. Deploy through local K8s, verify health, current Replay Gamma/targets, and persistence after restart.
5. Roll back application code if needed; the additive history file can remain unused safely.

## Open Questions

None. Current NTM selection and quote-quality rules already used by Setup Analytics are authoritative.
