## Why

The compact performance dashboard removed the account-switching control, preventing users from
reviewing performance by account. A clearly isolated demo account is also needed to validate
projection and consistency displays without contaminating real trading records.

## What Changes

- Restore a compact account selector in the performance dashboard header.
- Preserve the selected account in the dashboard URL and reload all account-scoped metrics.
- Add an explicitly labeled demo account with August 2026 realized P&L totaling $10,000 through
  August 12 and a largest winning trade of $3,500, producing 35% consistency.
- Keep demo data isolated from the real Protect account and visibly identify it as sample data.
- Replace ambiguous Recent P&L bars with a compact daily P&L strip showing dates and dollar values.
- Give positive net earnings a controlled neon treatment and elevate consistency-limit warnings.
- Add focused tests for account switching and the demo metric contract.
- Non-goals: changing real account records, broker sync behavior, or Market Pulse execution logic.

## Capabilities

### New Capabilities
- `dashboard-account-scenarios`: Account-scoped dashboard switching and isolated financial demo
  scenarios.

### Modified Capabilities

## Impact

Dashboard service/template/CSS behavior, account/trade repositories, focused dashboard tests, and
explicit local sample rows in the runtime database. Projection uses recorded realized trade P&L
through August 12, 2026; consistency is largest winning trade divided by positive net profit.
