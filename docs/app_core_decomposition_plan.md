# app_core Decomposition Plan

## Goal
Reduce `mccain_capital/app_core.py` from a mixed monolith into:
- domain services (request/response orchestration)
- repositories (data/file access)
- shared utility modules (parsing/formatting/date/math)

This keeps behavior stable while making future changes safer and faster.

## Current Status
- `journal` extracted to service.
- `strategies` and `books` extracted to services + repositories.
- `goals/payouts` moved to service ownership at runtime.
- `trades` moved to service ownership at runtime.
- Repositories introduced for `trades`, `goals`, `strategies`, `books`.
- `app_core` still contains compatibility delegates and shared logic.

## Target Structure
- `mccain_capital/services/`
  - `core.py` (auth/setup/login/logout/health/export/backup/restore/chart/links)
  - `journal.py`
  - `trades.py`
  - `goals.py`
  - `strategies.py`
  - `books.py`
- `mccain_capital/repositories/`
  - `journal.py`
  - `trades.py`
  - `goals.py`
  - `strategies.py`
  - `books.py`
- `mccain_capital/shared/` (new)
  - `time.py`
  - `parsing.py`
  - `formatting.py`
  - `risk_math.py`
  - `calendar_math.py`

## Next Phases

### Phase 1 - Shared Utility Extraction
Move pure functions out of `app_core.py`:
- formatting: `money`, `pct`
- parsing: `parse_float`, `parse_int`, `parse_date_any`, helpers
- time/date: `now_iso`, `today_iso`, week/month helpers
- risk/projection math: `calc_consistency`, projection helpers

Success criteria:
- Services import utilities from `mccain_capital/shared/*`.
- No route behavior changes.

### Phase 2 - Repository Completion
Add missing repositories and move raw SQL from `app_core.py`:
- `journal` repository CRUD
- remaining trade SQL that still lives in `app_core.py`
- export/backup query paths

Success criteria:
- SQL is repository-owned.
- Services no longer execute direct SQL.

### Phase 3 - Core Service Split
Move remaining route handlers in `app_core.py` into `services/core.py`:
- auth + setup/login/logout
- links/chart
- export/backup/restore
- dashboard and analytics (or split into dedicated service module)

Success criteria:
- `app_core.py` only contains temporary compatibility wrappers.

### Phase 4 - Compatibility Trim
Remove wrappers from `app_core.py` once all callers are migrated.

Success criteria:
- `app_core.py` either deleted or reduced to a small compatibility shim.

## Guardrails
- Keep endpoint signatures and URLs unchanged.
- Refactor in small moves with route smoke checks after each phase.
- Avoid broad formatting churn during extraction.
- Add/expand tests for each extracted area before removing wrappers.

