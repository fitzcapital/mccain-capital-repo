"""Run database migrations for McCain Capital."""

from __future__ import annotations

import os

from mccain_capital.migrations import run_migrations


def main() -> int:
    db_path = os.environ.get("DB_PATH", "journal.db")
    applied = run_migrations(db_path)
    if applied:
        print("Applied migrations:")
        for mid in applied:
            print(f"- {mid}")
    else:
        print("No pending migrations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
