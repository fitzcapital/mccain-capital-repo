## Why

The Dashboard currently repeats Market Pulse execution decisions and gives insufficient priority to
the information needed to run a trading business: earnings, monthly goals, pace, risk, and trading
quality. The two pages need distinct jobs so the Dashboard becomes the financial scorecard and
Market Pulse remains the execution workstation.

## What Changes

- Replace the Dashboard command-center hero with a monthly performance hero showing realized net
  P&L, goal progress, amount remaining, trading days remaining, required pace, and projection.
- Present trading statistics such as win rate, profit factor, expectancy, average winner/loss,
  drawdown, trade count, streak, and best/worst day as the primary supporting layer.
- Prioritize the equity curve, daily P&L calendar, recent sessions, and performance breakdowns by
  setup, ticker, and time of day.
- Reduce Dashboard execution permission and market context to a compact handoff strip linking to
  Market Pulse instead of repeating its full decision model.
- Simplify the main navigation into clear primary destinations with secondary tools grouped under a
  labeled menu; preserve direct URLs and keyboard-accessible behavior.
- Keep broker, ledger, goal, and journal sources explicit. Unavailable inputs display as unavailable
  and never produce invented financial projections.
- Non-goals: changing trade calculations, strategy rules, journal records, broker data, Market Pulse
  execution behavior, or adding a new backend/data store.

## Capabilities

### New Capabilities
- `dashboard-trading-performance`: Monthly earnings, goals, pace, projections, risk, statistics, and
  performance-trend hierarchy for the Dashboard.
- `application-navigation-hierarchy`: Primary workflow destinations and accessible grouping of
  secondary tools in the global menu.

### Modified Capabilities
- `dashboard-decision-workflow`: Replace the Dashboard's primary execution command center with a
  compact Market Pulse handoff and business-performance-first hierarchy.
- `dashboard-primary-market-canvas`: Recast the market canvas as compact supporting context beneath
  trading-business performance rather than a competing primary Dashboard workspace.

## Impact

- Affects Dashboard context assembly, Jinja markup, scoped Dashboard CSS, navigation markup and
  interaction JavaScript, and focused presentation/route contracts.
- Reuses current broker metrics, journal trades, goals, calendar summaries, equity history, and
  market-feed context; no new dependency or persistent data source is introduced.
- Projections must identify the source period, goal input, remaining trading-day assumption, and
  fallback state. Manually entered goals remain authoritative unless a successful update replaces
  them.
- Acceptance: the initial Dashboard viewport leads with month-to-date P&L and goal pace; execution
  language is no longer the dominant page content; all displayed statistics match authoritative
  existing sources; navigation remains keyboard accessible; no desktop or compact-width overflow;
  existing refreshes remain in place without full-page reloads.
