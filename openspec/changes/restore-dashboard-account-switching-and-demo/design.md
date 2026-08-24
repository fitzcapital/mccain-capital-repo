## Context

The dashboard already resolves an account from its query string and calculates account-scoped
performance, but the redesigned header no longer exposes that selection. Existing account and
trade tables can represent the requested sample without a schema change.

## Goals / Non-Goals

**Goals:**
- Restore account switching in the compact header.
- Keep all dashboard metrics scoped to the chosen account.
- Seed an unmistakably labeled demo account whose August metrics exercise projection and 35%
  consistency states.

**Non-Goals:**
- Changing real account data or broker synchronization.
- Creating a general-purpose sample-data framework.

## Decisions

- Use the existing `account_id` dashboard query parameter and repository display deduplication.
  This retains current scoping semantics instead of introducing client-only state.
- Render a compact semantic disclosure button with account-card links. This avoids the clunky
  native select while retaining keyboard behavior and URL-driven server-side account scope.
- Store demo trades under a dedicated `DEMO-AUG-10K-35` account. Realized net P&L totals $10,000;
  the largest winner is $3,500, so the established largest-winner/net-profit calculation returns
  exactly 35%. Include one $1,000 losing trade so Profit Factor and Average Loser are measurable.
- Use dated closed trades from August 3 through August 14, 2026. Dashboard projection continues to
  use the normal recorded-trade calculation and remaining-session assumptions.
- Render each recent trading day as a labeled tile containing month/day and realized dollar P&L;
  bar height remains a secondary relative-magnitude cue.
- Use a bright mint text glow only for positive Net Earned values. Use a stronger coral border,
  glow, status badge, and recovery message when consistency exceeds 30%.
- Promote the existing start/end range inputs into the compact Target Health header. For a custom
  range, compute net and quality from range trades, count remaining weekdays through the selected
  end date, and preserve the range on account switches and target-setting forms.

## Risks / Trade-offs

- [Sample data could be mistaken for live performance] → Prefix the account name and broker ID
  with `DEMO`, and keep it independently selectable.
- [Repeated seeding could duplicate results] → Resolve the stable demo broker ID and insert only
  when the expected demo batch is absent.
- [Account switching could drop dashboard filters] → Preserve year, month, ticker, and timeframe
  as hidden form fields.
