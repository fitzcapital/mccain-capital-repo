"""Installable Fitz command-line interface."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fitz",
        description="Fitz command-line tools.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    status_parser = subcommands.add_parser(
        "status",
        help="Launch the live Fitz system status dashboard.",
    )
    status_parser.set_defaults(handler=run_status)

    clean_parser = subcommands.add_parser(
        "clean",
        help="Safely preview or run Mole cleanup from the Fitz wrapper.",
    )
    clean_parser.add_argument(
        "--apply",
        action="store_true",
        help="Run the real cleanup. Without this flag, Fitz uses --dry-run for safety.",
    )
    clean_parser.add_argument(
        "--debug",
        action="store_true",
        help="Pass Mole's --debug flag through to show detailed logs.",
    )
    clean_parser.add_argument(
        "--whitelist",
        action="store_true",
        help="Open Mole's cache whitelist manager instead of the default cleaning flow.",
    )
    clean_parser.set_defaults(handler=run_clean)
    return parser


def _run_status_main() -> int:
    from scripts.fitz_status import main as fitz_status_main

    return fitz_status_main()


def _find_mole_binary() -> str | None:
    for candidate in ("mo", "mole"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def _build_clean_command(args: argparse.Namespace) -> list[str]:
    mole_binary = _find_mole_binary()
    if not mole_binary:
        raise FileNotFoundError(
            "Mole is not installed. Install it first, for example with `brew install mole`."
        )
    command = [mole_binary, "clean"]
    if not args.apply:
        command.append("--dry-run")
    if args.debug:
        command.append("--debug")
    if args.whitelist:
        command.append("--whitelist")
    return command


def _run_subprocess(command: Sequence[str]) -> int:
    completed = subprocess.run(list(command), check=False)
    return int(completed.returncode)


def run_status(_args: argparse.Namespace) -> int:
    return _run_status_main()


def run_clean(args: argparse.Namespace) -> int:
    try:
        command = _build_clean_command(args)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 127
    if not args.apply and "--dry-run" in command:
        print("fitz clean: preview mode enabled by default. Use --apply to execute cleanup.")
    return _run_subprocess(command)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return int(args.handler(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
