"""Write data_work/docs/OUTPUTS_REGISTRY.md: one row per folder in data_work/outputs.

Classification is rule-based (folder name, contents, MAINLINE_MANIFEST.md) and marks
which folders are cited by old-mainline documents or by shared materials. Rerun after
adding outputs:

    python src/diagnostics/build_outputs_registry.py --repo-root .   # from data_work/
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import re

CATEGORIES = [
    ("mainline", "Current mainline", "Listed in `MAINLINE_MANIFEST.md`. Back up; do not move or delete."),
    ("current_spec_experiment", "Current-spec experiments", "Runs on the 1B / 1A networks, 2025 platform data, or the November checks. Context for the mainline; most are robustness or trials."),
    ("od2025_on_gate5", "2025 OD on the gate5 network", "Transition runs (Sept to early Nov 2026) that put 2025 OD on the old gate5 network. Superseded by 1B."),
    ("old_gate5_line", "Old gate5 mainline and its experiments", "2022 OD, anchored lambda = 1.1, BPR trials, Stage 1 and Stage 2. Keep the ones marked as cited; the rest are history."),
    ("pipeline_and_validation", "Pipeline baselines and Baidu validation", "Stage pipeline, centreline match, Baidu route validation."),
    ("evidence_other_city", "Shenzhen, Caitian, national evidence", "Reduced-form evidence lines, currently paused."),
    ("smoke_or_failed", "Smoke tests and named failures", "Safe to delete after a check."),
    ("failed_or_empty", "Failed runs (only run.log)", "Recorded as void in the work log. Safe to delete."),
]

CURRENT_PAT = re.compile(
    r"osm1b|osm1a|alldaytt|osm_|od2025|platform|rn202505|segment_|flow_density|unicom|purpose_share|"
    r"link_time|load_dep|background|beta_c|congestion_floor|grid_chain|road_layer|network_"
)


def corpus(root: str, patterns: list[str]) -> str:
    text = []
    for pat in patterns:
        for f in glob.glob(os.path.join(root, pat), recursive=True):
            if os.path.isfile(f) and f.endswith((".md", ".tex", ".py", ".R", ".json", ".txt")) and os.path.getsize(f) < 5e6:
                with open(f, encoding="utf-8", errors="ignore") as fh:
                    # Skip the "historical" banners added on 2026-11-18: they name current folders.
                    text.append("\n".join(
                        ln for ln in fh.read().splitlines()
                        if not ln.startswith(("> **Historical", "> **历史文档"))
                    ))
    return "\n".join(text)


def classify(name: str, files: list[str], manifest: str) -> str:
    if f"`{name}`" in manifest:
        return "mainline"
    if not [f for f in files if f not in ("run.log", ".DS_Store")]:
        return "failed_or_empty"
    if re.search(r"shenzhen|caitian", name):
        return "evidence_other_city"
    if re.search(r"(^|_)(smoke|test)(_|$)|_smoke|_failed_\d", name):
        return "smoke_or_failed"
    if re.search(r"national_", name):
        return "evidence_other_city"
    if re.search(r"centerline|raw_baidu|grid_pair_baidu|rn_baidu|raw_rebuild|raw_topology|match_projection|manual_centerline", name):
        return "pipeline_and_validation"
    if CURRENT_PAT.search(name) and "gate5_bpr_pos05" not in name:
        if re.search(r"gate5|edgeproj|clv|anchored", name) and not re.search(r"osm1[ab]", name):
            return "od2025_on_gate5"
        return "current_spec_experiment"
    return "old_gate5_line"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".", help="data_work directory")
    args = ap.parse_args()
    dw = os.path.abspath(args.repo_root)
    proj = os.path.dirname(dw)
    out_dir = os.path.join(dw, "outputs")

    with open(os.path.join(dw, "docs", "MAINLINE_MANIFEST.md"), encoding="utf-8") as fh:
        manifest = fh.read()
    old_docs = corpus(proj, [
        "data_work/docs/STAGE2_MAINLINE_GATE5.md",
        "Documents/L2_notes/model_and_results_summary_20260928.md",
        "Documents/L2_notes/travel_cost_lock_in_20260628.md",
    ])
    materials = corpus(proj, [
        "tidal_model_slides_minimal/**/*", "tidal_model_slides_minimal_r/**/*",
        "Documents/PreSlides/*.tex", "Documents/PreSlides/brad_202611/**/*",
        "Documents/L1_manuscripts/*.tex", "Replication_apply/**/*.py",
    ])

    rows: dict[str, list[tuple[str, str, str]]] = {c[0]: [] for c in CATEGORIES}
    for name in sorted(os.listdir(out_dir)):
        path = os.path.join(out_dir, name)
        if not os.path.isdir(path) or name.startswith("."):
            continue
        files = os.listdir(path)
        cited = []
        if name in old_docs:
            cited.append("old-mainline doc")
        if name in materials:
            cited.append("deck / paper / replication")
        rows[classify(name, files, manifest)].append((name, "yes" if "README.md" in files else "", ", ".join(cited)))

    total = sum(len(v) for v in rows.values())
    lines = [
        "# Outputs registry",
        "",
        f"Generated {dt.date.today().isoformat()} by `data_work/src/diagnostics/build_outputs_registry.py`; rerun it after adding outputs. "
        f"{total} folders in `data_work/outputs/`.",
        "",
        "Classification is by folder name and contents, so treat it as a first sort. The current mainline is defined by "
        "`MAINLINE_MANIFEST.md` and the \"Current mainline\" table in `DOCUMENT_INDEX.md`, not by this file. "
        "\"Cited by\" marks folders named in old-mainline documents or in the deck, paper, and replication packages; "
        "keep those even when the category says history. A folder missing here is newer than the last run.",
        "",
        "| Category | Folders | Meaning |",
        "|---|---|---|",
    ]
    for key, title, meaning in CATEGORIES:
        lines.append(f"| {title} | {len(rows[key])} | {meaning} |")
    for key, title, _ in CATEGORIES:
        lines += ["", f"## {title} ({len(rows[key])})", "", "| Folder | README | Cited by |", "|---|---|---|"]
        lines += [f"| `{n}` | {r} | {c} |" for n, r, c in rows[key]]
    with open(os.path.join(dw, "docs", "OUTPUTS_REGISTRY.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print({k: len(v) for k, v in rows.items()})


if __name__ == "__main__":
    main()
