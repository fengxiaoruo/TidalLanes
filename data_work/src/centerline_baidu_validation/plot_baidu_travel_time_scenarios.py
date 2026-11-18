"""
Fine-bin histogram of Baidu API centerline travel time by scenario (crawl panels).

Mirrors ``plot_speed_diagnostics.plot_centerline_travel_time_scenarios`` layout.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.diagnostics.plot_speed_diagnostics import (
    BIN_WIDTH_MIN,
    ROAD_CLASS_COLORS,
    build_centerline_road_class,
    build_scenario_centerline_travel_times,
    load_inputs,
    travel_time_display_cap,
)
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config, resolve_path

SH_TZ = ZoneInfo("Asia/Shanghai")
QC_LO, QC_HI = 0.6, 1.8
PANEL_SCENARIO = {"FREE": "FREE_WORK", "AM": "AMP_WORK", "PM": "PMP_WORK"}
PANEL_ORDER = ["FREE", "AM", "PM"]
PANEL_TO_SCENARIO = {p: PANEL_SCENARIO[p] for p in PANEL_ORDER}
WORK_SCENARIOS = ["FREE_WORK", "AMP_WORK", "PMP_WORK"]
REQUIRED_ROWS = 11890


def load_panel_flat(session_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(session_dir / "results_flat.csv")
    panel = session_dir.name.split("_")[0]
    if "panel" not in df.columns:
        df["panel"] = panel
    else:
        df["panel"] = df["panel"].fillna(panel)
    return df


def enrich_crawl(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["length_m"] = pd.to_numeric(out["length_m"], errors="coerce")
    out["distance_m"] = pd.to_numeric(out["distance_m"], errors="coerce")
    out["duration_sec"] = pd.to_numeric(out["duration_sec"], errors="coerce")
    out["api_status"] = pd.to_numeric(out["api_status"], errors="coerce")
    ok = out["api_status"].eq(0) & out["distance_m"].notna() & out["length_m"].gt(0)
    out["distance_over_length"] = np.where(ok, out["distance_m"] / out["length_m"], np.nan)
    out["bind_ok"] = ok & out["distance_over_length"].between(QC_LO, QC_HI)
    out["baidu_travel_time_min"] = out["duration_sec"] / 60.0
    return out


def merge_inferred(df: pd.DataFrame, inferred: pd.DataFrame, road_class: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["scenario"] = out["panel"].map(PANEL_SCENARIO)
    inf = inferred.rename(columns={"travel_time_min": "inferred_travel_time_min"})
    keys = ["skel_dir", "cline_id", "dir"]
    out = out.merge(
        inf[keys + ["scenario", "inferred_travel_time_min", "cl_speed_kmh", "cl_len_m"]],
        on=keys + ["scenario"],
        how="left",
    )
    return out.merge(road_class, on=keys, how="left")


def parse_args():
    p = argparse.ArgumentParser(description="Fine-bin Baidu travel time histogram by scenario.")
    p.add_argument("--config", default="config/centerline_baidu_validation_beijing_v1.json")
    p.add_argument(
        "--session-label-contains",
        default="formal_week_20260527",
        help="Only include crawl sessions whose directory name contains this substring.",
    )
    p.add_argument(
        "--subset",
        choices=["api_ok", "bind_ok", "bind_ok_inferred"],
        default="bind_ok",
        help="Which rows enter the histogram.",
    )
    return p.parse_args()


def list_complete_sessions(version_root: Path, contains: str) -> list[Path]:
    out = []
    for sd in sorted((version_root / "api" / "crawl_sessions").iterdir()):
        if not sd.is_dir() or contains not in sd.name:
            continue
        jsonl = sd / "responses.jsonl"
        if jsonl.exists() and sum(1 for _ in jsonl.open()) >= REQUIRED_ROWS:
            out.append(sd)
    return out


def load_pooled_crawl(
    session_dirs: list[Path],
    *,
    inferred,
    road_class,
    subset: str,
) -> pd.DataFrame:
    frames = []
    for sd in session_dirs:
        panel = sd.name.split("_")[0]
        if panel not in PANEL_ORDER:
            continue
        df = enrich_crawl(load_panel_flat(sd))
        df = merge_inferred(df, inferred, road_class)
        df["crawl_session"] = sd.name
        df["scenario"] = PANEL_TO_SCENARIO[panel]
        frames.append(df)
    if not frames:
        raise ValueError("No crawl sessions loaded.")
    out = pd.concat(frames, ignore_index=True)
    ok = out["api_status"].eq(0) & out["baidu_travel_time_min"].notna() & out["baidu_travel_time_min"].gt(0)
    out = out[ok].copy()
    if subset == "bind_ok":
        out = out[out["bind_ok"]].copy()
    elif subset == "bind_ok_inferred":
        out = out[out["bind_ok"] & out["inferred_travel_time_min"].notna()].copy()
    out["travel_time_min"] = out["baidu_travel_time_min"]
    return out


def _draw_scenario_panel(ax, sub: pd.DataFrame, *, scenario: str, bins: np.ndarray, class_order: list[str], x_cap: float):
    bottom = np.zeros(len(bins) - 1, dtype=float)
    n_total = len(sub)
    for label in class_order:
        vals = sub.loc[sub["road_class_label"] == label, "travel_time_min"].to_numpy(dtype=float)
        if len(vals) == 0:
            continue
        counts, _ = np.histogram(vals, bins=bins)
        density = counts / (n_total * BIN_WIDTH_MIN)
        ax.bar(
            bins[:-1],
            density,
            width=BIN_WIDTH_MIN,
            align="edge",
            bottom=bottom,
            color=ROAD_CLASS_COLORS.get(label, "#969696"),
            edgecolor="white",
            linewidth=0.35,
            label=label,
        )
        bottom += density
    ax.set_title(f"{scenario} (N={len(sub):,})")
    ax.set_xlim(0, x_cap)
    ax.set_xlabel("Baidu API centerline travel time, minutes")
    ax.set_ylabel("Density")
    ax.grid(True, axis="y", alpha=0.25, linewidth=0.8)


def plot_baidu_work_histograms(fig_dir: Path, tt_df: pd.DataFrame, *, subset: str, n_sessions: int) -> dict[str, Path]:
    tt_df = tt_df[tt_df["scenario"].isin(WORK_SCENARIOS)].copy()
    if tt_df.empty:
        raise ValueError("No WORK scenario rows to plot.")
    tt_df["road_class_label"] = tt_df["road_class_label"].fillna("other")
    x_cap = travel_time_display_cap(tt_df["travel_time_min"])
    bins = np.arange(0.0, x_cap + BIN_WIDTH_MIN, BIN_WIDTH_MIN)
    class_order = [c for c in ["expressway", "arterial", "secondary", "other"] if c in set(tt_df["road_class_label"])]
    excluded = int((tt_df["travel_time_min"] > x_cap).sum())
    note = (
        f"Source: {n_sessions} complete crawl panel(s); subset={subset}. "
        "One observation per directed centerline × crawl session. "
        "Travel time = Baidu duration_sec / 60. "
        "Panel mapping: FREE→FREE_WORK, AM→AMP_WORK, PM→PMP_WORK. "
        f"Bins are {BIN_WIDTH_MIN:g} minutes wide and stacked by road class. "
        f"x-axis truncated at the {99.5:.1f}th percentile ({x_cap:.2f} min; excludes {excluded:,} observations)."
    )

    out_paths: dict[str, Path] = {}
    for scenario in WORK_SCENARIOS:
        sub = tt_df[(tt_df["scenario"] == scenario) & tt_df["travel_time_min"].le(x_cap)].copy()
        if sub.empty:
            continue
        fig, ax = plt.subplots(figsize=(4.8, 4.2), dpi=220)
        _draw_scenario_panel(ax, sub, scenario=scenario, bins=bins, class_order=class_order, x_cap=x_cap)
        if class_order:
            ax.legend(loc="upper right", frameon=False, fontsize=9)
        fig.suptitle("Fine-bin histogram of Baidu centerline travel time", y=1.02, fontsize=12)
        fig.text(0.5, -0.06, note, ha="center", va="top", fontsize=8.5, color="#4b5563", wrap=True)
        fig.tight_layout()
        out_path = fig_dir / f"fig_baidu_finebin_{scenario}.png"
        fig.savefig(out_path, bbox_inches="tight")
        plt.close(fig)
        out_paths[scenario] = out_path
    return out_paths


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config)
    dirs = ensure_version_dirs(version_root)
    ensure_mpl_cache(str(version_root.parent))

    baseline_root = resolve_path(config.get("output_dir", "outputs")) / config["baseline_network_version_id"]
    raw_segments, match, cl_speed, cl_dir = load_inputs(baseline_root)
    inferred = build_scenario_centerline_travel_times(cl_speed, cl_dir)
    road_class = build_centerline_road_class(raw_segments, match, baseline_root)

    sessions = list_complete_sessions(version_root, args.session_label_contains)
    if not sessions:
        raise FileNotFoundError(f"No complete sessions matching {args.session_label_contains!r}")

    tt_df = load_pooled_crawl(sessions, inferred=inferred, road_class=road_class, subset=args.subset)

    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    tag = f"baidu_finebin_{args.session_label_contains}_{stamp}"
    fig_dir = dirs["figures"] / tag
    metrics_dir = dirs["metrics"] / tag
    fig_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    figure_paths = plot_baidu_work_histograms(fig_dir, tt_df, subset=args.subset, n_sessions=len(sessions))

    meta = {
        "sessions": [sd.name for sd in sessions],
        "subset": args.subset,
        "scenarios": WORK_SCENARIOS,
        "n_rows": int(len(tt_df)),
        "by_scenario": {k: int(v) for k, v in tt_df.groupby("scenario").size().items()},
        "by_panel_session": {
            f"{scenario}|{crawl_session}": int(n)
            for (scenario, crawl_session), n in tt_df.groupby(["scenario", "crawl_session"]).size().items()
        },
        "figures": {k: str(v) for k, v in figure_paths.items()},
    }
    (metrics_dir / "plot_meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    tt_df[["request_id", "scenario", "panel", "crawl_session", "travel_time_min", "road_class_label", "bind_ok"]].to_csv(
        metrics_dir / "plot_input.csv", index=False
    )

    for label, path in figure_paths.items():
        print(f"[plot_baidu_travel_time_scenarios] {label} -> {path}")
    print(f"[plot_baidu_travel_time_scenarios] meta -> {metrics_dir / 'plot_meta.json'}")
    print(json.dumps(meta, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
