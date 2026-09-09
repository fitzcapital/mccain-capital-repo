## Context

Replay currently materializes eligible scenario candidates after iterating structural levels. Its
identity includes the anchor level, so one completed Strat pattern can produce several cards when
nearby levels all qualify. Immediate candidates can also bypass the scoring value expected by the
card, and target selection can choose another level from the same tight cluster. Live and Replay use
the shared event builder, but their setup-admission windows are not yet identical.

## Goals / Non-Goals

**Goals:**

- Represent one completed reversal pattern as one canonical setup event.
- Retain multi-level information as confluence instead of duplicate cards.
- Keep event identity stable across reruns and Live/Replay surfaces.
- Give every card an honest score, grade, contextual trigger description, and meaningful target.
- Admit new setups through the same 3:15 PM ET boundary everywhere.

**Non-Goals:**

- Redefining Strat `2-2` or `2-1-2` pattern classification.
- Collapsing different pattern completion timestamps into one trade idea.
- Promoting continuation candles or delayed candidates into reversal events.
- Rewriting stored history or sending alerts for historical recovered events.

## Decisions

### Canonical identity follows the completed pattern

The event key will use symbol, session, pattern code, completion timestamp, and direction. Anchor
level will not participate in identity. This matches the trading fact being studied: the completed
Strat reversal is the event, while levels describe its location. The alternative—keeping one event
per anchor—preserves implementation detail but produces misleading duplicates.

### Nearby compatible anchors become a level cluster

All compatible qualifying anchors for that canonical event will be grouped before presentation.
The existing structural clustering distance used by scenario confluence will be reused rather than
introducing another arbitrary threshold. The primary anchor will be selected deterministically from
the anchor that most directly satisfies the point-in-time sweep/reclaim or sweep/reject evidence,
then stable structural-level priority and proximity will break ties. Remaining anchors will be
exposed as supporting confluence with name, value, and role.

This grouping is exact-event only. Patterns completed on different candles remain independent even
when their direction and anchor cluster match.

### Key-level location uses a narrow SPX tolerance

An otherwise exact completed `2-2` or `2-1-2` reversal qualifies as located at a canonical SPX
level when either pattern candle spans the level or comes within 0.25 SPX points of it. This absorbs
quote granularity at levels such as the 11:25 high of 7,769.81 against Call Wall 7,770.00 without
loosening candle classification. Continuations remain excluded and the tolerance does not cluster
or invent a pattern by itself.

### Scores are computed after canonical grouping

The canonical event will be scored once from the selected primary candidate plus the grouped level
cluster. Replay cards will require numeric `quality_score`, score components, and the corresponding
grade. A missing immediate-candidate score will be computed through the same scoring path instead of
rendering an empty grade or borrowing a page-wide grade.

### Targets must clear the anchor cluster

Target selection will exclude every level in the event's anchor cluster and scan in the setup
direction for the nearest eligible structural target that satisfies the existing minimum target
space. If none exists, the event remains reviewable but is not presented as an actionable setup with
an artificial tiny target. The rejected choices will remain available in diagnostics.

### Labels describe both action and location

Presentation copy will be derived from direction and the primary level role: for example, `Sweep and
reject Prior-Day High` or `Sweep and reclaim Current-Day Low`. Generic family labels remain available
for filtering, while the visible title and trigger text name the actual structure.

### One admission cutoff, separate lifecycle continuation

Both Live and Replay will reject a newly completed pattern after 3:15 PM ET. A setup confirmed by the
cutoff may continue to target, invalidation, or expiration under the existing lifecycle rules. This
separates new-signal admission from monitoring and removes the 3:15/3:30 discrepancy.

### Outcome rules follow the execution plan

A target resolves when a later completed candle's directional extreme touches or passes the target:
high for bullish setups and low for bearish setups. Invalidation resolves only when that candle's
close finishes through the anchor in the adverse direction, matching the card's stated completed-close
rule. A wick through the anchor without the required close does not invalidate the setup.

Every terminal outcome will expose its event candle timestamp. If target and close-based
invalidation both occur on the same completed candle, Replay will label the ordering ambiguous and
name that candle. An unresolved setup will expose the timestamp of the last completed candle through
which it was evaluated. MFE and MAE remain excursion measurements; they do not independently set
lifecycle state.

### Compact estimated option-return target ladder

Replay will calculate actual SPX price proxies for +15%, +20%, and +30% option returns from a
disclosed $750 contract-cost and 0.40 absolute-delta assumption. Required SPX movement equals
`(($750 / 100) * return percentage) / 0.40`; bullish targets add that movement to entry and bearish
targets subtract it. The structural target remains the Runner. The card will keep this to compact
one-line targets plus one assumptions line. Dealer Gamma is separate confluence evidence and will be
explicitly identified as unused by this estimate.

## Risks / Trade-offs

- [Primary-anchor selection hides a useful alternate interpretation] → Preserve every grouped level
  and its role in confluence details and diagnostics.
- [Changing identity could duplicate pre-existing durable records] → Do not rewrite stored records;
  canonicalize new evaluation output and reconcile legacy same-completion records read-only in the
  rendered history.
- [Cluster exclusion removes the only nearby target] → Mark target unavailable/non-actionable rather
  than inventing reward space.
- [Cutoff change removes late replay examples] → Keep post-cutoff candles in chart context and
  diagnostics, but do not label them setups that Live could have alerted.
- [Shared builder regression affects Live and Replay] → Add fixture parity, idempotency, boundary,
  continuation, and audited-session regression tests before deployment.
- [Five-minute OHLC cannot establish intrabar ordering] → Preserve `ambiguous` when target touch and
  close-based invalidation first occur on the same candle and display that candle's timestamp.
- [A proximity rule admits loose location matches] → Fix the SPX tolerance at 0.25 points and still
  require an exact supported reversal completion.
- [A newer Gamma snapshot hides an earlier valid wall] → Replay the latest durable Live level
  observation known by each signal candle; never project a later observation backward.

## Migration Plan

1. Add canonical grouping and target/scoring helpers without changing persisted runtime data.
2. Route Replay and Live event construction through the grouped output.
3. Reconcile legacy duplicate records at read/render time while leaving the durable ledger intact.
4. Deploy with focused tests, rebuild the local Podman app, verify `/healthz`, and inspect the Market
   Pulse receiving surface.
5. Roll back application code if needed; no database rollback is required.

## Open Questions

None. The existing scenario cluster distance and 3:15 PM ET live cutoff are authoritative for this
change.
