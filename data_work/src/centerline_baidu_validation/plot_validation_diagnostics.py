"""
Day-level validation diagnostics for centerline Baidu crawl results.

Produces descriptive statistics, comparison tables, and figures:
  - crawl QC (distance/length bind)
  - Baidu travel time vs centerline-inferred travel time
  - cross-crawl comparison (e.g. d01 vs prior full_round)
  - spatial fit: Baidu route geometry vs baseline centerline
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import LineString
from shapely import wkt

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baidu_validation_common.plot_style import (
    PANEL_COLORS,
    PANEL_ORDER,
    PANEL_SCENARIO,
    QC_HI,
    QC_LO,
    cap_axis,
    plot_bland_altman,
    plot_dist_ratio_hist,
    plot_reference_vs_api_scatter,
    plot_spatial_hists,
    plot_spatial_scatter,
    plot_speed_scatter,
    plot_time_ratio_hist,
)
from src.baidu_validation_common.spatial import (
    line_gcj_to_wgs,
    make_utm_transformer,
    spatial_metrics,
    spatial_pass,
)
from src.centerline_baidu_validation.build_crawl_manifest import load_baseline_centerlines
from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.diagnostics.plot_speed_diagnostics import (
    build_centerline_road_class,
    build_scenario_centerline_travel_times,
    load_inputs,
)
from src.national_tidal_commuting.common import ensure_version_dirs, get_version_root, load_config, resolve_path

SH_TZ = ZoneInfo("Asia/Shanghai")


def parse_args():
    p = argparse.ArgumentParser(description="Plot centerline Baidu validation diagnostics.")
    p.add_argument("--config", default="config/centerline_baidu_validation_beijing_v1.json")
    p.add_argument("--label", default="formal_week_20260527_d01", help="Crawl session label suffix.")
    p.add_argument(
        "--formal-week",
        default=None,
        help="If set, pool all complete day×panel sessions for this week label (e.g. formal_week_20260527).",
    )
    p.add_argument("--formal-days", type=int, default=5, help="Number of days to include with --formal-week.")
    p.add_argument(
        "--min-api-ok-frac",
        type=float,
        default=0.99,
        help="Minimum api_ok fraction to treat a day×panel session as complete.",
    )
    p.add_argument(
        "--reference-label",
        default="full_round_20260525",
        help="Prior crawl label for cross-date comparison (empty to skip).",
    )
    p.add_argument("--spatial-sample", type=int, default=2500, help="Rows per panel for spatial fit metrics.")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


EXPECTED_ROWS = 11890


def session_api_ok(session_dir: Path) -> tuple[int, int]:
    df = pd.read_csv(session_dir / "results_flat.csv", usecols=["api_status"])
    st = pd.to_numeric(df["api_status"], errors="coerce")
    return int(st.eq(0).sum()), len(df)


def find_sessions(version_root: Path, label: str) -> dict[str, Path]:
    day_match = re.search(r"_d(\d{2})$", label)
    if day_match:
        week_label = label[: day_match.start()]
        day = int(day_match.group(1))
        return find_best_sessions_for_day(version_root, week_label, day, min_ok_frac=0.0)

    sessions: dict[str, Path] = {}
    for sd in sorted((version_root / "api" / "crawl_sessions").glob(f"*_{label}")):
        panel = sd.name.split("_")[0]
        if panel in PANEL_ORDER:
            sessions[panel] = sd
    missing = [p for p in PANEL_ORDER if p not in sessions]
    if missing:
        raise FileNotFoundError(f"Missing panels {missing} for label={label!r} under {version_root}")
    return sessions


def find_best_sessions_for_day(
    version_root: Path,
    week_label: str,
    day: int,
    *,
    min_ok_frac: float = 0.99,
) -> dict[str, Path]:
    day_tag = f"d{day:02d}"
    candidates: dict[str, list[tuple[int, int, Path]]] = defaultdict(list)
    root = version_root / "api" / "crawl_sessions"
    for sd in root.iterdir():
        if not sd.is_dir():
            continue
        if week_label not in sd.name or f"_{day_tag}" not in sd.name:
            continue
        if not sd.name.endswith(day_tag) and f"_{day_tag}_" not in sd.name:
            continue
        panel = sd.name.split("_")[0]
        if panel not in PANEL_ORDER:
            continue
        flat = sd / "results_flat.csv"
        if not flat.exists():
            continue
        ok, n = session_api_ok(sd)
        candidates[panel].append((ok, n, sd))

    sessions: dict[str, Path] = {}
    for panel in PANEL_ORDER:
        if panel not in candidates:
            continue
        ok, n, sd = max(candidates[panel], key=lambda x: (x[0], x[1]))
        if min_ok_frac <= 0 or ok >= EXPECTED_ROWS * min_ok_frac:
            sessions[panel] = sd
    missing = [p for p in PANEL_ORDER if p not in sessions]
    if missing:
        raise FileNotFoundError(
            f"Missing complete panels {missing} for {week_label}_{day_tag} (min_ok_frac={min_ok_frac})"
        )
    return sessions


def find_formal_week_sessions(
    version_root: Path,
    week_label: str,
    *,
    days: int,
    min_ok_frac: float,
) -> dict[tuple[int, str], Path]:
    out: dict[tuple[int, str], Path] = {}
    for day in range(1, days + 1):
        day_sessions = find_best_sessions_for_day(version_root, week_label, day, min_ok_frac=min_ok_frac)
        for panel, sd in day_sessions.items():
            out[(day, panel)] = sd
    return out


def load_formal_week_frames(
    version_root: Path,
    week_label: str,
    *,
    days: int,
    min_ok_frac: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    session_map = find_formal_week_sessions(version_root, week_label, days=days, min_ok_frac=min_ok_frac)
    frames = []
    manifest_rows = []
    for (day, panel), sd in sorted(session_map.items()):
        df = enrich_crawl(load_panel_flat(sd))
        df["crawl_day"] = day
        df["crawl_label"] = f"{week_label}_d{day:02d}"
        ok, n = session_api_ok(sd)
        manifest_rows.append(
            {
                "crawl_day": day,
                "panel": panel,
                "session_dir": sd.name,
                "n_rows": n,
                "n_api_ok": ok,
                "api_ok_frac": ok / n if n else np.nan,
            }
        )
        frames.append(df)
    return pd.concat(frames, ignore_index=True), pd.DataFrame(manifest_rows)


def load_panel_flat(session_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(session_dir / "results_flat.csv")
    panel = session_dir.name.split("_")[0]
    if "panel" not in df.columns:
        df["panel"] = panel
    else:
        df["panel"] = df["panel"].fillna(panel)
    df["session_dir"] = str(session_dir)
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
    out["baidu_speed_kmh"] = np.where(
        out["duration_sec"].gt(0),
        (out["distance_m"] / 1000.0) / (out["duration_sec"] / 3600.0),
        np.nan,
    )
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
    out = out.merge(road_class, on=keys, how="left")
    out["road_class_label"] = out["road_class_label"].fillna("other")
    out["time_ratio_baidu_over_inferred"] = out["baidu_travel_time_min"] / out["inferred_travel_time_min"]
    out["speed_ratio_baidu_over_inferred"] = out["baidu_speed_kmh"] / out["cl_speed_kmh"]
    out["time_diff_min"] = out["baidu_travel_time_min"] - out["inferred_travel_time_min"]
    return out


def panel_summary(df: pd.DataFrame, panel: str) -> dict:
    ok = df["api_status"].eq(0)
    sub = df[ok]
    ratio = sub["distance_over_length"]
    bind = sub["bind_ok"]
    inf = sub["inferred_travel_time_min"].notna()
    bind_inf = bind & inf
    tr = sub.loc[bind_inf, "time_ratio_baidu_over_inferred"]
    return {
        "panel": panel,
        "n_rows": int(len(df)),
        "n_api_ok": int(ok.sum()),
        "api_success_rate": float(ok.mean()),
        "n_has_route_path": int(sub.get("has_route_path", pd.Series(False)).fillna(False).sum()),
        "median_duration_min": float(sub["baidu_travel_time_min"].median()) if len(sub) else np.nan,
        "median_distance_m": float(sub["distance_m"].median()) if len(sub) else np.nan,
        "median_ratio_dist_len": float(ratio.median()) if len(sub) else np.nan,
        "p10_ratio": float(ratio.quantile(0.1)) if len(sub) else np.nan,
        "p90_ratio": float(ratio.quantile(0.9)) if len(sub) else np.nan,
        "share_bind_ok": float(bind.mean()) if len(sub) else np.nan,
        "share_bind_ok_among_ok": float(bind.mean()) if len(sub) else np.nan,
        "n_bind_ok_inferred": int(bind_inf.sum()),
        "median_time_ratio_bind_inferred": float(tr.median()) if len(tr) else np.nan,
        "share_time_ratio_0p8_1p25": float(tr.between(0.8, 1.25).mean()) if len(tr) else np.nan,
        "mae_time_min_bind_inferred": float(sub.loc[bind_inf, "time_diff_min"].abs().median()) if bind_inf.any() else np.nan,
        "corr_time_bind_inferred": float(sub.loc[bind_inf, ["baidu_travel_time_min", "inferred_travel_time_min"]].corr().iloc[0, 1])
        if bind_inf.sum() >= 30
        else np.nan,
    }


def compute_spatial_sample(
    df: pd.DataFrame,
    *,
    baseline_root: Path,
    sample_n: int,
    seed: int,
    buffer_m: float = 50.0,
) -> pd.DataFrame:
    cl = load_baseline_centerlines(baseline_root).set_index(["skel_dir", "cline_id", "dir"])
    to_utm = make_utm_transformer()

    pool = df[df["api_status"].eq(0) & df.get("has_route_path", False).fillna(False)].copy()
    if pool.empty:
        return pd.DataFrame()
    # Stratify: half from bind_ok (analysis-ready), half from full pool.
    bind = pool[pool["bind_ok"]]
    not_bind = pool[~pool["bind_ok"]]
    n_bind = min(sample_n // 2, len(bind))
    n_rest = min(sample_n - n_bind, len(not_bind))
    parts = []
    if n_bind:
        parts.append(bind.sample(n_bind, random_state=seed))
    if n_rest:
        parts.append(not_bind.sample(n_rest, random_state=seed + 1))
    sample = pd.concat(parts, ignore_index=True) if parts else pool.sample(min(sample_n, len(pool)), random_state=seed)
    rows = []
    for rec in sample.to_dict("records"):
        wkt_text = rec.get("route_path_wkt")
        if not isinstance(wkt_text, str) or not wkt_text.strip():
            continue
        try:
            route_gcj = wkt.loads(wkt_text)
        except Exception:
            continue
        if not isinstance(route_gcj, LineString) or route_gcj.is_empty:
            continue
        key = (int(rec["skel_dir"]), int(rec["cline_id"]), str(rec["dir"]))
        if key not in cl.index:
            continue
        centerline = cl.loc[key].geometry
        if not isinstance(centerline, LineString):
            parts = [g for g in getattr(centerline, "geoms", []) if isinstance(g, LineString)]
            centerline = max(parts, key=lambda g: g.length) if parts else centerline
        sm = spatial_metrics(centerline, route_gcj, buffer_m=buffer_m, to_utm=to_utm)
        row = {
            "request_id": rec["request_id"],
            "panel": rec["panel"],
            "length_m": rec["length_m"],
            "distance_over_length": rec["distance_over_length"],
            "bind_ok": rec["bind_ok"],
            **sm,
        }
        row["spatial_ok"] = spatial_pass(pd.Series(row))
        rows.append(row)
    return pd.DataFrame(rows)


def cross_crawl_compare(current: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    keys = ["skel_dir", "cline_id", "dir", "panel"]
    cur = current[keys + ["baidu_travel_time_min", "baidu_speed_kmh", "distance_over_length", "bind_ok"]].rename(
        columns={
            "baidu_travel_time_min": "time_current_min",
            "baidu_speed_kmh": "speed_current_kmh",
            "distance_over_length": "ratio_current",
            "bind_ok": "bind_ok_current",
        }
    )
    ref = reference[keys + ["baidu_travel_time_min", "baidu_speed_kmh", "distance_over_length", "bind_ok"]].rename(
        columns={
            "baidu_travel_time_min": "time_reference_min",
            "baidu_speed_kmh": "speed_reference_kmh",
            "distance_over_length": "ratio_reference",
            "bind_ok": "bind_ok_reference",
        }
    )
    merged = cur.merge(ref, on=keys, how="inner")
    merged["time_ratio_current_over_ref"] = merged["time_current_min"] / merged["time_reference_min"]
    merged["speed_ratio_current_over_ref"] = merged["speed_current_kmh"] / merged["speed_reference_kmh"]
    merged["ratio_diff"] = merged["ratio_current"] - merged["ratio_reference"]
    return merged


def cross_crawl_summary(merged: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for panel, g in merged.groupby("panel"):
        ok = g["bind_ok_current"] & g["bind_ok_reference"]
        sub = g[ok]
        rows.append(
            {
                "panel": panel,
                "n_overlap": int(len(g)),
                "n_both_bind_ok": int(len(sub)),
                "corr_time_both_bind_ok": float(sub[["time_current_min", "time_reference_min"]].corr().iloc[0, 1])
                if len(sub) >= 30
                else np.nan,
                "median_time_ratio": float(sub["time_ratio_current_over_ref"].median()) if len(sub) else np.nan,
                "share_time_ratio_0p9_1p1": float(sub["time_ratio_current_over_ref"].between(0.9, 1.1).mean())
                if len(sub)
                else np.nan,
                "median_ratio_diff": float(sub["ratio_diff"].median()) if len(sub) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def plot_travel_time_hist(df: pd.DataFrame, path: Path):
    from src.baidu_validation_common.plot_style import FIG_DPI, PANEL_FIGSIZE_HIST, apply_validation_rc, save_fig

    apply_validation_rc()
    fig, axes = plt.subplots(1, 3, figsize=PANEL_FIGSIZE_HIST, dpi=FIG_DPI, sharey=True)
    cap = cap_axis(df.loc[df["bind_ok"], "baidu_travel_time_min"])
    bins = np.linspace(0, cap, 50)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = df[(df["panel"] == panel) & df["bind_ok"]]
        ax.hist(sub["baidu_travel_time_min"].dropna(), bins=bins, color=PANEL_COLORS[panel], alpha=0.75, edgecolor="white")
        ax.set_title(f"{panel} bind_ok (n={len(sub):,})")
        ax.set_xlabel("Baidu travel time (min)")
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_ylabel("Count")
    fig.suptitle("Centerline: Baidu travel time distribution (bind_ok)", y=1.02)
    save_fig(fig, path)


def plot_cross_crawl_scatter(merged: pd.DataFrame, path: Path, *, current_label: str, reference_label: str):
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), dpi=220)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = merged[(merged["panel"] == panel) & merged["bind_ok_current"] & merged["bind_ok_reference"]]
        x = sub["time_reference_min"]
        y = sub["time_current_min"]
        cap = cap_axis(pd.concat([x, y]))
        ax.scatter(x, y, s=6, alpha=0.25, c=PANEL_COLORS[panel], edgecolors="none")
        ax.plot([0, cap], [0, cap], "--", color="#636363", linewidth=1)
        corr = x.corr(y) if len(sub) >= 30 else np.nan
        ax.set_title(f"{panel} (n={len(sub):,}, r={corr:.2f})")
        ax.set_xlim(0, cap)
        ax.set_ylim(0, cap)
        ax.set_xlabel(f"Reference ({reference_label}) min")
        ax.set_ylabel(f"Current ({current_label}) min")
        ax.grid(True, alpha=0.25)
    fig.suptitle("Cross-crawl Baidu travel time (both bind_ok)", y=1.02)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def cross_day_stability(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["skel_dir", "cline_id", "dir", "panel"]
    ok = df["bind_ok"] & df["inferred_travel_time_min"].notna() & df["baidu_travel_time_min"].notna()
    sub = df[ok].copy()
    rows = []
    for panel, g in sub.groupby("panel"):
        wide = g.pivot_table(
            index=["skel_dir", "cline_id", "dir"],
            columns="crawl_day",
            values="baidu_travel_time_min",
            aggfunc="first",
        )
        n_days = wide.notna().sum(axis=1)
        multi = wide[n_days >= 2]
        if len(multi) == 0:
            continue
        cv = multi.std(axis=1, ddof=0) / multi.mean(axis=1)
        rows.append(
            {
                "panel": panel,
                "n_centerlines_ge2_days": int(len(multi)),
                "median_cv_time": float(cv.median()),
                "share_cv_le_0p15": float((cv <= 0.15).mean()),
                "median_time_ratio_max_over_min": float((multi.max(axis=1) / multi.min(axis=1)).median()),
            }
        )
    return pd.DataFrame(rows)


def day_panel_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (day, panel), g in df.groupby(["crawl_day", "panel"]):
        row = panel_summary(g, panel)
        row["crawl_day"] = day
        rows.append(row)
    return pd.DataFrame(rows)


def plot_day_panel_bind_ok(day_panel_df: pd.DataFrame, path: Path):
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), dpi=220, sharey=True)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = day_panel_df[day_panel_df["panel"] == panel].sort_values("crawl_day")
        ax.plot(sub["crawl_day"], 100 * sub["share_bind_ok"], marker="o", color=PANEL_COLORS[panel])
        ax.set_title(panel)
        ax.set_xlabel("Crawl day")
        ax.set_ylim(0, 100)
        ax.grid(True, alpha=0.25)
    axes[0].set_ylabel("Bind OK (%)")
    fig.suptitle("Centerline: bind_ok share by crawl day", y=1.02)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def write_report(
    path: Path,
    *,
    label: str,
    reference_label: str | None,
    panel_df: pd.DataFrame,
    cross_df: pd.DataFrame | None,
    spatial_df: pd.DataFrame,
    fig_dir: Path,
    metrics_dir: Path,
    day_panel_df: pd.DataFrame | None = None,
    stability_df: pd.DataFrame | None = None,
    session_manifest: pd.DataFrame | None = None,
):
    title = "Formal week 数据验证报告" if day_panel_df is not None else "Day1 数据验证报告"
    lines = [
        f"# {title}",
        "",
        f"- 生成时间: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        f"- 当前 crawl 标签: `{label}`",
        f"- 对照 crawl: `{reference_label or '无'}`",
        "",
    ]
    if session_manifest is not None and len(session_manifest):
        lines.extend(["## 0. 纳入 session", ""])
        lines.append("| Day | Panel | Session | n | api_ok |")
        lines.append("|-----|-------|---------|---|--------|")
        for _, r in session_manifest.sort_values(["crawl_day", "panel"]).iterrows():
            lines.append(
                f"| d{int(r['crawl_day']):02d} | {r['panel']} | `{r['session_dir']}` | "
                f"{int(r['n_rows']):,} | {100*r['api_ok_frac']:.1f}% |"
            )
        lines.append("")

    lines.extend([
        "## 1. 爬取完成度与 bind QC",
        "",
        "| Panel | 行数 | API 成功率 | median ratio | bind_ok 占比 | bind∩inferred n | time ratio median | r(time) |",
        "|-------|------|------------|--------------|--------------|-----------------|-------------------|---------|",
    ])
    for _, r in panel_df.iterrows():
        lines.append(
            f"| {r['panel']} | {r['n_rows']} | {100*r['api_success_rate']:.2f}% | "
            f"{r['median_ratio_dist_len']:.2f} | {100*r['share_bind_ok']:.1f}% | "
            f"{int(r['n_bind_ok_inferred'])} | {r['median_time_ratio_bind_inferred']:.2f} | "
            f"{r['corr_time_bind_inferred']:.2f} |"
        )

    if day_panel_df is not None and len(day_panel_df):
        lines.extend(["", "## 1b. 按 crawl day 分解", ""])
        lines.append("| Day | Panel | bind_ok | time ratio med | r(time) |")
        lines.append("|-----|-------|---------|----------------|---------|")
        for _, r in day_panel_df.sort_values(["crawl_day", "panel"]).iterrows():
            lines.append(
                f"| d{int(r['crawl_day']):02d} | {r['panel']} | {100*r['share_bind_ok']:.1f}% | "
                f"{r['median_time_ratio_bind_inferred']:.2f} | {r['corr_time_bind_inferred']:.2f} |"
            )

    if stability_df is not None and len(stability_df):
        lines.extend(["", "## 1c. 跨日稳定性（bind_ok∩inferred）", ""])
        lines.append("| Panel | n (≥2 days) | median CV | share CV≤0.15 | median max/min |")
        lines.append("|-------|-------------|-----------|---------------|----------------|")
        for _, r in stability_df.iterrows():
            lines.append(
                f"| {r['panel']} | {int(r['n_centerlines_ge2_days']):,} | {r['median_cv_time']:.3f} | "
                f"{100*r['share_cv_le_0p15']:.1f}% | {r['median_time_ratio_max_over_min']:.2f} |"
            )

    lines.extend(["", "## 2. 与推断 travel time 对比", ""])
    lines.append("Panel 对齐: FREE→FREE_WORK, AM→AMP_WORK, PM→PMP_WORK（weekday 聚合推断）。")
    lines.append("")
    for _, r in panel_df.iterrows():
        lines.append(
            f"- **{r['panel']}**: bind_ok∩inferred 中 ratio∈[0.8,1.25] 占 "
            f"**{100*r['share_time_ratio_0p8_1p25']:.1f}%**；MAE **{r['mae_time_min_bind_inferred']:.2f} min**"
        )

    if cross_df is not None and len(cross_df):
        lines.extend(["", "## 3. 与先前 crawl 交叉对照", ""])
        lines.append("| Panel | overlap n | both bind_ok | r(time) | median time ratio | share∈[0.9,1.1] |")
        lines.append("|-------|-----------|--------------|---------|-------------------|-----------------|")
        for _, r in cross_df.iterrows():
            lines.append(
                f"| {r['panel']} | {int(r['n_overlap'])} | {int(r['n_both_bind_ok'])} | "
                f"{r['corr_time_both_bind_ok']:.2f} | {r['median_time_ratio']:.2f} | "
                f"{100*r['share_time_ratio_0p9_1p1']:.1f}% |"
            )

    if len(spatial_df):
        lines.extend(["", "## 4. 路线几何 vs centerline", ""])
        lines.append(
            f"- 空间抽样 n={len(spatial_df)}（GCJ→WGS 校正后，50m buffer）"
        )
        lines.append(
            f"- spatial PASS（mean_dist≤80m & CL overlap≥50%）: **{100*spatial_df.spatial_ok.mean():.1f}%**"
        )
        if "bind_ok" in spatial_df.columns:
            bind_sp = spatial_df[spatial_df["bind_ok"]]
            if len(bind_sp):
                lines.append(
                    f"- bind_ok 子集 spatial PASS: **{100*bind_sp.spatial_ok.mean():.1f}%** (n={len(bind_sp)})"
                )
        lines.append(f"- CL overlap median: **{spatial_df.overlap_geom_in_route_buffer.median():.2f}**")
        lines.append(f"- sym_overlap median: **{spatial_df.sym_overlap.median():.2f}**")
        lines.append(f"- mean_dist median: **{spatial_df.mean_dist_geom_to_route_m.median():.1f} m**")

    lines.extend(
        [
            "",
            "## 5. 图表",
            "",
            f"- 指标 CSV: `{metrics_dir}`",
            f"- 图表目录: `{fig_dir}`",
            "",
            "| 文件 | 说明 |",
            "|------|------|",
            "| fig_dist_ratio_by_panel.png | distance/length 分布 |",
            "| fig_baidu_travel_time_hist_by_panel.png | Baidu 行程时间分布 |",
            "| fig_baidu_vs_inferred_scatter.png | Baidu vs 推断 scatter |",
            "| fig_bland_altman_by_panel.png | Bland–Altman |",
            "| fig_time_ratio_hist.png | time ratio 分布 |",
            "| fig_speed_vs_inferred_scatter.png | 速度 scatter |",
            "| fig_cross_crawl_scatter.png | 与先前 crawl 对照 |",
            "| fig_spatial_overlap_hist.png | 空间 overlap 分布 |",
            "| fig_spatial_mean_dist_hist.png | CL→route 平均距离 |",
            "| fig_spatial_scatter_overlap_vs_ratio.png | overlap vs bind ratio |",
            "| fig_day_panel_bind_ok.png | 正式周 bind_ok 按日变化 |",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


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

    formal_week = args.formal_week
    session_manifest = None
    day_panel_df = None
    stability_df = None

    if formal_week:
        all_df, session_manifest = load_formal_week_frames(
            version_root,
            formal_week,
            days=args.formal_days,
            min_ok_frac=args.min_api_ok_frac,
        )
        all_df = merge_inferred(all_df, inferred, road_class)
        label = formal_week
        tag_prefix = f"formal_week_validation_{formal_week}"
        spatial_source = all_df[all_df["crawl_day"].eq(1)].copy()
    else:
        sessions = find_sessions(version_root, args.label)
        frames = []
        for panel, sd in sessions.items():
            df = enrich_crawl(load_panel_flat(sd))
            df = merge_inferred(df, inferred, road_class)
            frames.append(df)
        all_df = pd.concat(frames, ignore_index=True)
        label = args.label
        tag_prefix = f"day01_validation_{args.label}"
        spatial_source = all_df

    stamp = datetime.now(SH_TZ).strftime("%Y%m%dT%H%M%S")
    tag = f"{tag_prefix}_{stamp}"
    metrics_dir = dirs["metrics"] / tag
    fig_dir = dirs["figures"] / tag
    metrics_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    panel_df = pd.DataFrame([panel_summary(all_df[all_df.panel == p], p) for p in PANEL_ORDER])
    panel_df.to_csv(metrics_dir / "panel_summary.csv", index=False)

    if formal_week:
        day_panel_df = day_panel_summary(all_df)
        day_panel_df.to_csv(metrics_dir / "day_panel_summary.csv", index=False)
        stability_df = cross_day_stability(all_df)
        if len(stability_df):
            stability_df.to_csv(metrics_dir / "cross_day_stability.csv", index=False)
        session_manifest.to_csv(metrics_dir / "session_manifest.csv", index=False)

    compare_cols = [
        "request_id", "skel_dir", "cline_id", "dir", "panel", "length_m", "distance_m", "duration_sec",
        "distance_over_length", "bind_ok", "baidu_travel_time_min", "baidu_speed_kmh",
        "inferred_travel_time_min", "cl_speed_kmh", "time_ratio_baidu_over_inferred", "time_diff_min",
        "road_class_label", "has_route_path", "query_time_local",
    ]
    if formal_week:
        compare_cols = ["crawl_day", "crawl_label"] + compare_cols
    all_df[compare_cols].to_parquet(metrics_dir / "comparison_table.parquet", index=False)
    all_df[compare_cols].to_csv(metrics_dir / "comparison_table.csv", index=False)

    cross_df = None
    cross_merged = None
    if args.reference_label:
        ref_sessions = find_sessions(version_root, args.reference_label)
        ref_frames = []
        for panel, sd in ref_sessions.items():
            ref_frames.append(enrich_crawl(load_panel_flat(sd)))
        ref_df = pd.concat(ref_frames, ignore_index=True)
        cross_merged = cross_crawl_compare(all_df, ref_df)
        cross_df = cross_crawl_summary(cross_merged)
        cross_df.to_csv(metrics_dir / "cross_crawl_summary.csv", index=False)
        cross_merged.to_parquet(metrics_dir / "cross_crawl_detail.parquet", index=False)

    spatial_df = compute_spatial_sample(
        spatial_source,
        baseline_root=baseline_root,
        sample_n=args.spatial_sample,
        seed=args.seed,
    )
    if len(spatial_df):
        spatial_df.to_csv(metrics_dir / "spatial_sample.csv", index=False)
        by_panel = (
            spatial_df.groupby("panel")
            .agg(
                n=("request_id", "count"),
                share_spatial_ok=("spatial_ok", "mean"),
                median_overlap_cl=("overlap_geom_in_route_buffer", "median"),
                median_sym_overlap=("sym_overlap", "median"),
                median_mean_dist_m=("mean_dist_geom_to_route_m", "median"),
                median_hausdorff_m=("hausdorff_m", "median"),
            )
            .reset_index()
        )
        bind_spatial = spatial_df[spatial_df["bind_ok"]]
        if len(bind_spatial):
            bind_summary = (
                bind_spatial.groupby("panel")
                .agg(n_bind=("request_id", "count"), share_spatial_ok_bind=("spatial_ok", "mean"))
                .reset_index()
            )
            by_panel = by_panel.merge(bind_summary, on="panel", how="left")
        by_panel.to_csv(metrics_dir / "spatial_summary_by_panel.csv", index=False)

    plot_dist_ratio_hist(
        all_df,
        fig_dir / "fig_dist_ratio_by_panel.png",
        length_label="API distance / centerline length",
        title="Centerline: route bind ratio by panel",
    )
    plot_travel_time_hist(all_df, fig_dir / "fig_baidu_travel_time_hist_by_panel.png")
    plot_reference_vs_api_scatter(
        all_df,
        fig_dir / "fig_baidu_vs_inferred_scatter.png",
        ref_col="inferred_travel_time_min",
        api_col="baidu_travel_time_min",
        ref_label="Inferred travel time (min)",
        api_label="Baidu API travel time (min)",
        title="Centerline: Baidu vs inferred travel time (bind_ok)",
        panel_titles=PANEL_SCENARIO,
    )
    plot_bland_altman(
        all_df,
        fig_dir / "fig_bland_altman_by_panel.png",
        ref_col="inferred_travel_time_min",
        api_col="baidu_travel_time_min",
        diff_col="time_diff_min",
        title="Centerline: Bland–Altman (Baidu vs inferred travel time)",
        ylabel="Baidu − inferred (min)",
        panel_titles=PANEL_SCENARIO,
    )
    plot_time_ratio_hist(
        all_df,
        fig_dir / "fig_time_ratio_hist.png",
        ratio_col="time_ratio_baidu_over_inferred",
        title="Centerline: travel time ratio distribution (bind_ok)",
        xlabel="Baidu / inferred travel time",
        panel_titles=PANEL_SCENARIO,
    )
    plot_speed_scatter(
        all_df,
        fig_dir / "fig_speed_vs_inferred_scatter.png",
        ref_speed_col="cl_speed_kmh",
        api_speed_col="baidu_speed_kmh",
        ref_label="Inferred centerline speed (km/h)",
        api_label="Baidu implied speed (km/h)",
        title="Centerline: Baidu implied speed vs inferred speed (bind_ok)",
        panel_titles=PANEL_SCENARIO,
    )
    if cross_merged is not None and len(cross_merged):
        plot_cross_crawl_scatter(
            cross_merged,
            fig_dir / "fig_cross_crawl_scatter.png",
            current_label=label,
            reference_label=args.reference_label,
        )
    if day_panel_df is not None and len(day_panel_df):
        plot_day_panel_bind_ok(day_panel_df, fig_dir / "fig_day_panel_bind_ok.png")
    if len(spatial_df):
        plot_spatial_hists(
            spatial_df,
            fig_dir / "fig_spatial_overlap_hist.png",
            fig_dir / "fig_spatial_mean_dist_hist.png",
            geom_label="Centerline",
        )
        plot_spatial_scatter(
            spatial_df,
            fig_dir / "fig_spatial_scatter_overlap_vs_ratio.png",
            geom_label="Centerline",
            length_label="API distance / centerline length",
        )

    write_report(
        metrics_dir / "DAY01_VALIDATION_REPORT.md",
        label=label,
        reference_label=args.reference_label or None,
        panel_df=panel_df,
        cross_df=cross_df,
        spatial_df=spatial_df,
        fig_dir=fig_dir,
        metrics_dir=metrics_dir,
        day_panel_df=day_panel_df,
        stability_df=stability_df,
        session_manifest=session_manifest,
    )

    print(f"[plot_validation_diagnostics] metrics -> {metrics_dir}")
    print(f"[plot_validation_diagnostics] figures -> {fig_dir}")
    print(panel_df.to_string(index=False))
    if cross_df is not None:
        print("\nCross-crawl summary:")
        print(cross_df.to_string(index=False))
    if len(spatial_df):
        print(f"\nSpatial sample n={len(spatial_df)}, pass rate={spatial_df.spatial_ok.mean():.3f}")


if __name__ == "__main__":
    main()
