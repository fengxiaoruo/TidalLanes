"""
Utilities to configure matplotlib for this project.
"""

import os
from pathlib import Path


def ensure_mpl_cache(output_dir: str) -> Path:
    root = Path(output_dir)
    cache_dir = root / ".mpl_cache"
    xdg_cache_dir = root / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    xdg_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_dir.resolve()))
    os.environ.setdefault("XDG_CACHE_HOME", str(xdg_cache_dir.resolve()))
    return cache_dir
