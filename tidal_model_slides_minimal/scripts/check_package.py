from __future__ import annotations

import csv
import re
from pathlib import Path

import pandas as pd

from repo_paths import read_main_rel, resolve_main
from slide_metric_checks import validate_slide_descriptive_metrics


ROOT = Path(__file__).resolve().parents[1]


def fail(msg: str) -> None:
    raise SystemExit(f"CHECK FAILED: {msg}")


def check_slides() -> None:
    tex = (ROOT / "tidal.tex").read_text()
    if "/Users/fxr/" in tex:
        fail("tidal.tex still contains an absolute /Users/fxr path")
    script = ROOT / "tidal_speaker_script.md"
    if not script.exists():
        fail("missing speaker script: tidal_speaker_script.md")
    images = re.findall(r"\\includegraphics(?:\[[^\]]*\])?\{([^}]+)\}", tex)
    missing = []
    for img in images:
        p = ROOT / img
        if not p.exists():
            p = ROOT / "figs" / img
        if not p.exists():
            missing.append(img)
    if missing:
        fail("missing slide figures: " + ", ".join(missing))
    run_links = re.findall(r"\\href\{run:([^}]+)\}", tex)
    missing_run_links = []
    for item in run_links:
        item = item.removeprefix("./")
        if not (ROOT / item).exists():
            missing_run_links.append(item)
    if missing_run_links:
        fail("missing run-linked assets: " + ", ".join(missing_run_links))
    print(f"slide figures ok: {len(images)} references; run links ok: {len(run_links)}; speaker script ok")


def check_data() -> None:
    data_root = ROOT / "outputs/model_input_edgeproj_v2_lcc_gate5_v1/data"
    expected = [
        "qsm_input_nodes_square.parquet",
        "qsm_input_edges_square.parquet",
        "qsm_input_od_square.parquet",
    ]
    for name in expected:
        if not (data_root / name).exists():
            fail(f"missing aggregate input: {name}")
    nodes = pd.read_parquet(data_root / expected[0])
    edges = pd.read_parquet(data_root / expected[1])
    od = pd.read_parquet(data_root / expected[2])
    print(f"aggregate inputs ok: nodes={len(nodes):,}, edges={len(edges):,}, od={len(od):,}")


def check_file_map() -> None:
    path = ROOT / "docs/FILE_MAP.csv"
    if not path.exists():
        fail("docs/FILE_MAP.csv missing")
    with path.open() as fh:
        rows = list(csv.DictReader(fh))
    missing_package = [r["package_path"] for r in rows if not (ROOT / r["package_path"]).exists()]
    if missing_package:
        fail("FILE_MAP package paths missing: " + ", ".join(missing_package[:10]))
    print(f"file map ok: {len(rows)} rows")


def check_slide_metrics() -> None:
    for line in validate_slide_descriptive_metrics(ROOT):
        print(line)


def check_main_link() -> None:
    rel = read_main_rel()
    if rel is None:
        fail("MAIN_REPO missing; expected a relative path such as ..")
    main = resolve_main()
    print(f"main repo ok: MAIN_REPO={rel!r} -> {main}")


def main() -> None:
    check_slides()
    check_data()
    check_file_map()
    check_main_link()
    check_slide_metrics()


if __name__ == "__main__":
    main()
