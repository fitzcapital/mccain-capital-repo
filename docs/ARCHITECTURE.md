# 🏗️ McCain Capital Architecture Guide

Welcome to the architecture map for this app.
Use this doc to understand the request flow, where logic lives, and how to study the codebase quickly.

## 🚦 1) End-to-End Request Flow

```text
Browser Request
  -> Flask App (create_app)
  -> Route (routes/*)
  -> Handler (handlers/*)
  -> Service (services/*)
  -> Repository + Runtime Helpers
  -> DB/File I/O
  -> HTML/JSON/Redirect Response
```

## 🧱 2) Layer Responsibilities

| Layer | Purpose | Keep Out |
|---|---|---|
| `mccain_capital/routes/` | URL registration (`app.add_url_rule`) | Business logic |
| `mccain_capital/handlers/` | Thin controller wrappers | SQL, heavy logic |
| `mccain_capital/services/` | Domain orchestration, validation, response rendering | Raw persistence details |
| `mccain_capital/repositories/` | SQLite/file access and query logic | HTTP concerns |
| `mccain_capital/runtime.py` | Shared helpers (db/time/format/parsing/math) | Endpoint orchestration |
| `mccain_capital/app_core.py` | Legacy/compatibility surface during migration | New feature ownership |

## 🎯 3) Current Feature Ownership

- 🏠 Core pages (`/`, `/dashboard`, `/calculator`, auth/export/backup/restore/chart)
  - `mccain_capital/services/core.py` (currently delegates into legacy compatibility functions).

- 📈 Trades
  - `routes/trades.py` -> `handlers/trades.py` -> `services/trades.py`
  - Data/state logic in `repositories/trades.py`
  - OCR/import bridge in `services/trades_importing.py`

- 📝 Journal
  - `services/journal.py` + `repositories/journal.py`

- 🎯 Goals/Payouts
  - `services/goals.py` + `repositories/goals.py` + shared math/helpers from `runtime.py`

- 🧠 Strategies
  - `services/strategies.py` + `repositories/strategies.py`

- 📚 Books
  - `services/books.py` + `repositories/books.py`

## 🔧 4) Startup + Config Sync

App factory: `mccain_capital/__init__.py:create_app()`

What happens at startup:

1. Flask app is configured.
2. Security hooks are registered once.
3. Runtime storage paths are synced to legacy core paths:
   `DB_PATH`, `UPLOAD_DIR`, `BOOKS_DIR`.
4. DB initialization runs.

This sync keeps modular code and legacy compatibility code pointed at the same data.

## 📦 5) Why `repositories/` Exists

`repositories/` gives strict separation of concerns:

- Services decide **what** to do.
- Repositories decide **how** data is read/written.

✅ Benefits:

- safer schema/query refactors
- cleaner services and handlers
- easier mocking in tests
- less coupling to `app_core.py`

## 🔍 6) Real Request Examples

### 📈 Example: `/trades`

1. Route registration in `routes/trades.py`.
2. Handler delegates in `handlers/trades.py:trades_page()`.
3. Service orchestrates in `services/trades.py:trades_page()`.
4. Repository fetches records/review data in `repositories/trades.py`.
5. Service computes metrics and renders the page.

### 📝 Example: `/journal`

1. Route -> handler -> `services/journal.py:journal_home()`.
2. Service calls `repositories/journal.py:fetch_entries(...)`.
3. Service renders journal template with returned rows.

## ✅ 7) CI + Quality Gates

- Ruff lint: `python -m ruff check .`
- Black format check: `python -m black --check --config pyproject.toml .`
- Tests: `python -m pytest -q`
- Workflow: `.github/workflows/ci.yml`

## 🚧 8) Transitional Areas (Known)

- `mccain_capital/app_core.py` is still large (legacy compatibility still present).
- `services/trades_importing.py` still bridges OCR/import functionality to legacy internals.
- `services/core.py` and `services/ui.py` still depend on compatibility functions.

## 🧠 9) Best Study Path

1. Start with `mccain_capital/__init__.py` (app factory + hooks).
2. Read `mccain_capital/routes/*` to see endpoint map.
3. Pick one feature and trace:
   route -> handler -> service -> repository.
4. Compare the same feature in `app_core.py` to understand what has been extracted.

## 🗺️ 10) Quick Navigation

- App factory: `mccain_capital/__init__.py`
- Legacy core: `mccain_capital/app_core.py`
- Runtime helpers: `mccain_capital/runtime.py`
- Routes: `mccain_capital/routes/`
- Handlers: `mccain_capital/handlers/`
- Services: `mccain_capital/services/`
- Repositories: `mccain_capital/repositories/`
