"""Process-wide minimum interval between Baidu API calls (shared lock file)."""

from __future__ import annotations

import fcntl
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCK_PATH = ROOT / "outputs" / ".baidu_ak_global.lock"


def _lock_ex(fh) -> None:
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)


def _lock_un(fh) -> None:
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def wait_for_ak_slot(
    min_interval_sec: float,
    *,
    lock_path: str | Path | None = None,
) -> None:
    """Block until at least ``min_interval_sec`` since the last Baidu request on this machine."""
    interval = max(float(min_interval_sec), 0.0)
    if interval <= 0:
        return

    path = Path(lock_path) if lock_path else DEFAULT_LOCK_PATH
    if not path.is_absolute():
        path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a+", encoding="utf-8") as fh:
        while True:
            fh.seek(0)
            _lock_ex(fh)
            now = time.monotonic()
            last = 0.0
            fh.seek(0)
            raw = fh.read().strip()
            if raw:
                try:
                    last = float(raw)
                except ValueError:
                    last = 0.0
            if last > now:
                # stamp from before a reboot (monotonic clock restarted): treat as stale
                last = 0.0
            wait_sec = interval - (now - last)
            if wait_sec <= 0:
                fh.seek(0)
                fh.truncate()
                fh.write(f"{now:.6f}\n")
                fh.flush()
                _lock_un(fh)
                return
            _lock_un(fh)
            time.sleep(min(wait_sec, interval))
