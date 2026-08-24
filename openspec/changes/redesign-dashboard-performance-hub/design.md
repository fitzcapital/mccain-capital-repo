## Context

Dashboard currently exposes a large execution command model even though Market Pulse already owns
live permission, trigger, invalidation, and scenario decisions. Existing Dashboard context already
contains broker/ledger metrics, goals, calendar results, equity history, trade aggregates, and tape
data, but presentation gives those business outcomes secondary weight.

## Goals / Non-Goals

**Goals:**
- Make month-to-date earnings and goal pace the first Dashboard read.
- Separate financial outcome, trading quality, risk, trend, and recent activity into clear layers.
- Keep market context as a compact handoff to Market Pulse.
- Simplify global navigation around primary destinations and grouped secondary tools.
- Preserve existing sources, calculations, refresh behavior, URLs, and user-entered goals.

**Non-Goals:**
- Change accounting, trade classification, broker import, strategy, or Market Pulse rules.
- Add a data store, external dependency, or automatic financial write.
- Remove direct access to any existing application page.

## Decisions

1. **Dashboard becomes a performance hub.** The first region uses existing authoritative monthly
   values to show net P&L, goal, progress, remaining amount, trading days, pace, and projection.
   Alternative rejected: retaining the command deck and adding performance cards below, because it
   continues the redundancy the change is intended to remove.
2. **Use five information layers.** Performance hero, trading-quality statistics, equity/calendar
   trend, recent breakdowns, then compact market context. This supports a top-down business review
   without producing another dense grid of equal-weight cards.
3. **Projection remains explicitly conditional.** It is shown only when the goal, period, and
   trading-day inputs are valid and includes its basis; otherwise it reports unavailable.
4. **Market Pulse owns execution language.** Dashboard retains only a compact status and link. The
   current permission/trigger/invalidation sequence remains on Market Pulse.
5. **Navigation uses primary destinations plus a Tools menu.** Primary: Dashboard, Market Pulse,
   Trades, Journal. Secondary: Executive, Candle Opens, Analytics, Planner, Calendar, and other
   utilities. Existing URLs and active-state semantics remain unchanged.
6. **Progressive disclosure protects scan speed.** Detailed broker diagnostics, import history,
   health, and extended reports remain accessible but collapsed unless exceptional.

## Risks / Trade-offs

- [Incomplete broker or goal inputs could imply false precision] → Label each figure by source and
  render projections unavailable when required inputs are missing.
- [Existing template context contains overlapping definitions] → Reuse authoritative values and add
  presentation aliases only; do not duplicate calculation logic in Jinja.
- [Navigation regrouping could reduce discoverability] → Use a clearly labeled Tools menu, preserve
  keyboard focus/ARIA, and keep all URLs directly reachable.
- [Dense statistics can recreate the same visual problem] → Limit the initial statistics row to the
  highest-value measures and place deeper breakdowns behind disclosures.
- [Dirty worktree overlap] → Keep edits scoped, preserve existing IDs used by hydration code, and
  verify the deployed receiving page after rebuild.

## Migration Plan

Deploy as a presentation and context-assembly change with no data migration. Retain existing
Dashboard IDs required by live refresh or provide compatible aliases. Roll back by restoring the
prior template/CSS/navigation composition; financial data remains unchanged.

## Open Questions

- None blocking. The first implementation will use existing monthly goal and realized-P&L sources;
  additional configurable benchmark goals can be proposed separately.
