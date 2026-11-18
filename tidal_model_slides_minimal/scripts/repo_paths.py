from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_REPO_FILE = ROOT / "MAIN_REPO"
DEFAULT_MAIN_REL = ".."


def read_main_rel() -> str | None:
    if not MAIN_REPO_FILE.exists():
        return None
    rel = MAIN_REPO_FILE.read_text().strip()
    return rel or None


def write_main_rel(rel: str) -> None:
    rel = rel.strip()
    if not rel:
        raise ValueError("main repo relative path must be non-empty")
    MAIN_REPO_FILE.write_text(f"{rel}\n")


def resolve_main(explicit: str | None = None) -> Path:
    if explicit:
        main = Path(explicit).expanduser()
        if not main.is_absolute():
            main = (ROOT / main).resolve()
        else:
            main = main.resolve()
    else:
        rel = read_main_rel() or DEFAULT_MAIN_REL
        main = (ROOT / rel).resolve()
    if not (main / "data_work").is_dir():
        raise SystemExit(
            f"not a TidalLanes root: {main}\n"
            f"Set MAIN_REPO (currently {read_main_rel()!r}) or pass --main"
        )
    return main


def main_rel_from_path(main: Path) -> str:
    main = main.resolve()
    rel = os.path.relpath(main, ROOT.resolve())
    if rel == ".":
        return "."
    return rel.replace("\\", "/")


def ensure_main_repo_file(main: Path | None = None) -> Path:
    main = main or resolve_main()
    write_main_rel(main_rel_from_path(main))
    return main
