from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path

from repo_paths import ROOT, resolve_main


def load_map(main: Path) -> dict[str, str]:
    path = ROOT / "docs/FILE_MAP.csv"
    if not path.exists():
        raise SystemExit("docs/FILE_MAP.csv missing; run make sync-from-main first")
    rows = {}
    with path.open() as fh:
        for row in csv.DictReader(fh):
            main_path = row["main_project_path"]
            if main_path != "PACKAGE_LOCAL":
                rows[row["package_path"]] = main_path
    return rows


def push_one(package_path: str, main: Path, mapping: dict[str, str], dry_run: bool) -> None:
    if package_path not in mapping:
        raise SystemExit(f"no main-project mapping for {package_path}")
    src = ROOT / package_path
    dst = main / mapping[package_path]
    if not src.exists():
        raise SystemExit(f"missing package file: {src}")
    print(f"{package_path} -> {dst.relative_to(main)}")
    if dry_run:
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Copy selected package files back to the main repo.")
    parser.add_argument("paths", nargs="+", help="package-relative paths, e.g. tidal.tex")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    main_root = resolve_main()
    mapping = load_map(main_root)
    for package_path in args.paths:
        push_one(package_path, main_root, mapping, args.dry_run)
    print(f"main repo root: {main_root}")


if __name__ == "__main__":
    main()
