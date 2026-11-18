"""
Fine-bin stacked histogram of raw-segment travel time from completed API crawl panels.

Weekday panels only: FREE -> FREE_WORK, AM -> AMP_WORK, PM -> PMP_WORK.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.plot_speed_diagnostics import (
    BIN_WIDTH_MIN,
    ROAD_CLASS_COLORS,
    ROAD_CLASS_LABELS,
    travel_time_display_cap,
)
from src.national_tidal_commuting.common import get_version_root, load_config, resolve_path

WEEKDAY_PANEL_SPECS = [
    ("FREE_WORK", "FREE"),
    ("AMP_WORK", "AM"),
    ("PMP_WORK", "PM"),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Plot weekday panel travel-time histograms for raw validation.")
    parser.add_argument("--config", required=True, help="JSON config path.")
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--session-label", default="formal_week_20260528_d01", help="Filter sessions by label suffix.")
    parser.add_argument("--require-bind-ok", action="store_true", help="Keep rows with distance/length in [0.6, 1.8].")
    parser.add_argument("--min-rows", type=int, default=1000, help="Minimum rows to treat a panel session as usable.")
    return parser.parse_args()


def pick_latest_session(sessions_root: Path, panel: str, label: str, *, min_rows: int) -> Path | None:
    candidates = sorted(sessions_root.glob(f"{panel}_*{label}"), key=lambda p: p.stat().st_mtime, reverse=True)
    for session_dir in candidates:
        flat = session_dir / "results_flat.csv"
        if not flat.exists():
            continue
        n = sum(1 for _ in flat.open(encoding="utf-8")) - 1
        if n >= min_rows:
            return session_dir
    return None


def load_panel_frame(session_dir: Path, scenario: str, *, require_bind_ok: bool) -> pd.DataFrame:
    df = pd.read_csv(session_dir / "results_flat.csv")
    df = df[df["api_status"].eq(0)].copy()
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce")
    df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
    df["length_m"] = pd.to_numeric(df["length_m"], errors="coerce")
    df["road_class"] = pd.to_numeric(df["road_class"], errors="coerce")
    df = df[df["duration_sec"].gt(0) & df["length_m"].gt(0)].copy()
    df["travel_time_min"] = df["duration_sec"] / 60.0
    if require_bind_ok:
        bind = df["distance_m"] / df["length_m"]
        df = df[bind.between(0.6, 1.8)].copy()
    df["road_class_label"] = df["road_class"].map(ROAD_CLASS_LABELS).fillna("other")
    df["scenario"] = scenario
    df["session_dir"] = str(session_dir)
    return df


def plot_weekday_panels(path: Path, tt_df: pd.DataFrame, *, session_meta: dict[str, str]) -> None:
    tt_df = tt_df.copy()
    x_cap = travel_time_display_cap(tt_df["travel_time_min"])
    bins = np.arange(0.0, x_cap + BIN_WIDTH_MIN, BIN_WIDTH_MIN)
    class_order = [c for c in ["expressway", "arterial", "secondary", "other"] if c in set(tt_df["road_class_label"])]

    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.2), dpi=220, sharex=True, sharey=False)
    for ax, (scenario, _) in zip(axes, WEEKDAY_PANEL_SPECS):
        sub = tt_df[(tt_df["scenario"] == scenario) & tt_df["travel_time_min"].le(x_cap)].copy()
        if sub.empty:
            ax.set_title(scenario)
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            continue
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
        ax.grid(True, axis="y", alpha=0.25, linewidth=0.8)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=len(class_order), frameon=False, bbox_to_anchor=(0.5, 1.08))
    fig.supxlabel("Raw segment travel time, minutes")
    fig.supylabel("Density")
    fig.suptitle("Fine-bin histogram of raw segment travel time by weekday panel", y=1.14, fontsize=13)
    excluded = int((tt_df["travel_time_min"] > x_cap).sum())
    sessions_note = "; ".join(f"{k}: {Path(v).name}" for k, v in session_meta.items())
    note = (
        "One observation per baseline-kept raw segment. "
        "Travel time = Baidu API route duration. "
        f"Bins are {BIN_WIDTH_MIN:g} minutes wide and stacked by road class. "
        f"x-axis truncated at the 99.5th percentile ({x_cap:.2f} min; excludes {excluded:,} observations). "
        f"Sessions: {sessions_note}."
    )
    fig.text(0.5, -0.06, note, ha="center", va="top", fontsize=8.5, color="#4b5563", wrap=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config, args.version_id, args.output_dir)
    sessions_root = version_root / "api" / "crawl_sessions"
    figures_dir = version_root / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    session_meta: dict[str, str] = {}
    for scenario, panel in WEEKDAY_PANEL_SPECS:
        session_dir = pick_latest_session(sessions_root, panel, args.session_label, min_rows=args.min_rows)
        if session_dir is None:
            print(f"[plot_panel_travel_time] skip {scenario}: no usable {panel} session for label={args.session_label!r}")
            continue
        df = load_panel_frame(session_dir, scenario, require_bind_ok=args.require_bind_ok)
        frames.append(df)
        session_meta[panel] = str(session_dir)
        print(f"[plot_panel_travel_time] {scenario}: n={len(df):,} from {session_dir.name}")

    if not frames:
        raise RuntimeError("No panel data loaded.")

    tt_df = pd.concat(frames, ignore_index=True)
    out_path = figures_dir / "fig_raw_travel_time_weekday_panels.png"
    plot_weekday_panels(out_path, tt_df, session_meta=session_meta)

    summary = (
        tt_df.groupby("scenario", as_index=False)
        .agg(
            n=("travel_time_min", "size"),
            median_travel_time_min=("travel_time_min", "median"),
            p95_travel_time_min=("travel_time_min", lambda s: float(np.quantile(s, 0.95))),
        )
    )
    summary_path = version_root / "metrics" / "raw_travel_time_weekday_panel_summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False)

    meta = {"figure": str(out_path), "summary": str(summary_path), "sessions": session_meta, "n_total": int(len(tt_df))}
    meta_path = figures_dir / "fig_raw_travel_time_weekday_panels.meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[plot_panel_travel_time] saved {out_path}")
    print(f"[plot_panel_travel_time] saved {summary_path}")


if __name__ == "__main__":
    main()
