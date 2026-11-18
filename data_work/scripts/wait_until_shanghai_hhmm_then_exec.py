#!/usr/bin/env python3
"""
Sleep until a given clock time in Asia/Shanghai, then exec the rest of argv (no shell).

Usage:
  wait_until_shanghai_hhmm_then_exec.py 07:10 -- python3.11 -m src.national_tidal_commuting.run_full_crawl ...

The separator `--` is optional if the command does not start with digits.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")


def _parse_hhmm(s: str) -> tuple[int, int]:
    parts = s.strip().split(":")
    if len(parts) != 2:
        raise SystemExit(f"Expected HH:MM, got: {s!r}")
    h, m = int(parts[0]), int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise SystemExit(f"Invalid time: {s!r}")
    return h, m


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: wait_until_shanghai_hhmm_then_exec.py HH:MM [--] <command> [args...]\n"
            "Example:\n"
            "  cd data_work && python3.11 scripts/wait_until_shanghai_hhmm_then_exec.py 07:05 -- \\\n"
            "    python3.11 -m src.national_tidal_commuting.run_full_crawl --config config/....json \\\n"
            "    --panels AM --strict-live-window --label am_$(date +%Y%m%d)",
            file=sys.stderr,
        )
        raise SystemExit(2)

    hhmm = sys.argv[1]
    rest = sys.argv[2:]
    if rest and rest[0] == "--":
        rest = rest[1:]
    if not rest:
        raise SystemExit("No command after HH:MM")

    h, m = _parse_hhmm(hhmm)
    now = datetime.now(TZ)
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    sec = (target - now).total_seconds()
    print(f"[wait_until_shanghai] now={now.isoformat(timespec='seconds')} target={target.isoformat(timespec='seconds')} sleep_sec={sec:.0f}", flush=True)
    time.sleep(max(0.0, sec))
    os.execvp(rest[0], rest)


if __name__ == "__main__":
    main()
