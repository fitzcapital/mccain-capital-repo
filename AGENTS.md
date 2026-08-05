- Work only within this repository root unless the user explicitly asks otherwise.
- Prefer targeted reads and searches over broad scans; use `rg` instead of `find`/`grep` when possible.
- Avoid scanning or traversing heavy/generated directories unless the task requires it: `.git/`, `.venv/`, `node_modules/`, `artifacts/`, `uploads/`, `books/`, `persistent-data/`, `output/`, `tmp/`, `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `.idea/`.
- This project is primarily Python 3.11. Match existing style and keep changes minimal and production-ready.
- Respect configured formatting/lint settings in `pyproject.toml`: line length 100, target Python 3.11.
- Prefer editing application code under `mccain_capital/` before changing legacy entrypoints like `app.py` unless the task specifically needs an entrypoint change.
- Keep business logic in services/repositories/handlers patterns already used by the repo; avoid adding unrelated abstractions.
- Do not modify generated/runtime data such as `journal.db`, files under `persistent-data/`, `uploads/`, `artifacts/`, or local environment files like `.env` unless the user explicitly asks.
- For validation, prefer narrow checks first, such as `pytest tests/<target>` or a small focused command, before suggesting broader test runs.
- Ask before running full test suites, long-running scripts, container builds, or commands that may touch large local datasets.
- If documentation needs updating, keep it focused and consistent with the existing README tone and structure.

## OpenSpec workflow

- Use OpenSpec for new features, financial/business-rule changes, data-model changes, and multi-file behavior changes. Small typo, formatting, or isolated low-risk fixes may proceed directly.
- Start substantial work with `/opsx:propose`; review and agree on the proposal, specs, design, and tasks before implementation.
- Implement approved work with `/opsx:apply`, verify the acceptance criteria and targeted tests, then use `/opsx:sync` and `/opsx:archive` so the living specs match production behavior.
- Keep one coherent change per OpenSpec change folder and one focused branch/commit series per change. Do not mix unrelated dirty-worktree edits.
- Treat `openspec/specs/` as the current behavioral contract and `openspec/changes/` as proposed deltas. Code and tests must remain consistent with both.
