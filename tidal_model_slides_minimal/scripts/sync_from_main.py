from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path

from package_manifest import FileSpec, all_specs, managed_package_paths
from repo_paths import ROOT, ensure_main_repo_file, resolve_main


def copy_one(main: Path, spec: FileSpec) -> None:
    if not spec.main_path:
        return
    src = main / spec.main_path
    dst = ROOT / spec.package_path
    if not src.exists():
        raise FileNotFoundError(f"missing source: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    if spec.package_path == "tidal.tex":
        txt = dst.read_text()
        txt = txt.replace(
            "/Users/fxr/Dropbox/Projects/TidalLanes/Documents/L3_figs/speed_eg.png",
            "speed_eg.png",
        )
        # The OD-validation figure lives under data_work/outputs in the main
        # repo; the package carries a copy under figs/.
        txt = txt.replace(
            "../../data_work/outputs/od_validation_aa_style_gate5_bpr_pos05_v1/figures/figure_od_validation_structural_central99.pdf",
            "figs/figure_od_validation_structural_central99.pdf",
        )
        dst.write_text(txt)
    if spec.package_path == "tidal_speaker_script.md":
        txt = dst.read_text()
        txt = txt.replace("`Documents/PreSlides/Tidal0718.pdf`", "`tidal.pdf`")
        txt = txt.replace("`Documents/PreSlides/Tidal0715.tex`", "`tidal.tex`")
        dst.write_text(txt)


def write_file_map(main: Path) -> None:
    out = ROOT / "docs/FILE_MAP.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for spec in all_specs(main):
        rows.append(
            {
                "package_path": spec.package_path,
                "main_project_path": spec.main_path if spec.main_path else "PACKAGE_LOCAL",
                "kind": spec.kind,
                "origin_or_source": spec.origin,
            }
        )
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["package_path", "main_project_path", "kind", "origin_or_source"],
        )
        writer.writeheader()
        writer.writerows(rows)


def prune_unmanaged(main: Path) -> list[str]:
    managed = managed_package_paths(main)
    removed: list[str] = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("outputs/validation_") or rel.startswith("outputs/qa_render/"):
            path.unlink()
            removed.append(rel)
            continue
        if rel.startswith("src/model/") and rel.endswith(".py") and rel not in managed:
            path.unlink()
            removed.append(rel)
            continue
        if rel.startswith("figs/") and rel not in managed:
            path.unlink()
            removed.append(rel)
            continue
        if rel.startswith("outputs/reference_results/") and rel not in managed:
            path.unlink()
            removed.append(rel)
    for directory in sorted((ROOT / "src/model").rglob("__pycache__"), reverse=True):
        shutil.rmtree(directory, ignore_errors=True)
    return removed


def remove_legacy_symlink() -> None:
    legacy = ROOT / "MAIN_REPO"
    if legacy.is_symlink():
        legacy.unlink()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main", default=None, help="main project root or relative path, default from MAIN_REPO")
    parser.add_argument("--write-map-only", action="store_true")
    parser.add_argument("--prune", action="store_true", help="remove package files outside manifest")
    args = parser.parse_args(argv)

    remove_legacy_symlink()
    main_root = resolve_main(args.main)
    ensure_main_repo_file(main_root)

    if not args.write_map_only:
        for spec in all_specs(main_root):
            copy_one(main_root, spec)
        (ROOT / "src/__init__.py").touch()
        if args.prune:
            removed = prune_unmanaged(main_root)
            if removed:
                print(f"pruned {len(removed)} unmanaged files")
    write_file_map(main_root)
    print(f"synced from {main_root}")


if __name__ == "__main__":
    main()
