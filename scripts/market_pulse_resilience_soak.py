#!/usr/bin/env python3
"""Run the bounded local Market Pulse resilience verification."""

from __future__ import annotations

import argparse
import json

from mccain_capital.services.market_pulse_resilience_soak import run_resilience_soak


def main() -> int:
    parser = argparse.ArgumentParser(description="Market Pulse operational-resilience soak")
    parser.add_argument("--cycles", type=int, default=3, help="deterministic fault cycles")
    parser.add_argument("--live-session", action="store_true", help="run the longer local mode")
    args = parser.parse_args()
    cycles = max(args.cycles, 80) if args.live_session else args.cycles
    report = run_resilience_soak(cycles)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
