"""
Validation diagnostics: Baidu API crawl vs direct speed-CSV travel time (raw segments).
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import LineString

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baidu_validation_common.plot_style import (
    PANEL_ORDER,
    PANEL_SCENARIO,
    plot_bland_altman,
    plot_dist_ratio_hist,
    plot_reference_vs_api_scatter,
    plot_spatial_hists,
    plot_spatial_scatter,
    plot_speed_scatter,
    plot_time_ratio_hist,
)
from src.baidu_validation_common.spatial import make_utm_transformer, spatial_metrics, spatial_pass
from src.diagnostics.mpl_setup import ensure_mpl_cache
from src.diagnostics.plot_speed_diagnostics import ROAD_CLASS_LABELS
from src.national_tidal_commuting.common import get_version_root, load_config, resolve_path
from src.raw_baidu_validation.build_crawl_manifest import load_baseline_raw_segments, resolve_raw_speed_path
from src.raw_baidu_validation.plot_panel_travel_time_histogram import pick_latest_session

SH_TZ = ZoneInfo("Asia/Shanghai")
EXPECTED_ROWS = 23799
WEEKDAY_SCENARIO_SPECS = [
    ("FREE_WORK", "weekday", "is_freeflow_2205"),
    ("AMP_WORK", "weekday", "is_am_peak"),
    ("PMP_WORK", "weekday", "is_pm_peak"),
]


def parse_args():
    p = argparse.ArgumentParser(description="Raw Baidu validation diagnostics (API vs direct speed).")
    p.add_argument("--config", default="config/raw_baidu_validation_beijing_v1.json")
    p.add_argument("--session-label", default="formal_week_20260528_d01")
    p.add_argument(
        "--formal-week",
        default=None,
        help="Pool all day×panel sessions for this week label (e.g. formal_week_20260528).",
    )
    p.add_argument("--formal-days", type=int, default=5)
    p.add_argument(
        "--min-api-ok-frac",
        type=float,
        default=0.0,
        help="Minimum api_ok fraction to include a day×panel (0 = keep best partial).",
    )
    p.add_argument("--panels", default=None, help="Comma-separated panels to include, e.g. FREE,AM")
    p.add_argument("--min-rows", type=int, default=1000)
    p.add_argument("--spatial-sample", type=int, default=2500)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def parse_panels(text: str | None) -> list[str]:
    if not text:
        return list(PANEL_ORDER)
    panels = [x.strip() for x in text.split(",") if x.strip()]
    bad = [p for p in panels if p not in PANEL_ORDER]
    if bad:
        raise ValueError(f"Unknown panels: {bad}")
    return panels


def load_speed_obs(speed_path: Path) -> pd.DataFrame:
    df = pd.read_csv(speed_path, usecols=["roadseg_id", "speed", "day", "hour"])
    df["roadseg_id"] = df["roadseg_id"].astype("string")
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df["hour_int"] = df["hour"].round().astype("Int64")
    df["dt"] = pd.to_datetime(df["hour_int"].astype(str), format="%Y%m%d%H%M", errors="coerce")
    df["hour_of_day"] = df["dt"].dt.hour
    df["is_weekday"] = df["dt"].dt.weekday < 5
    df["weekday_label"] = np.where(df["is_weekday"], "weekday", "weekend")
    df["is_am_peak"] = (df["hour_of_day"] >= 7) & (df["hour_of_day"] < 9)
    df["is_pm_peak"] = (df["hour_of_day"] >= 17) & (df["hour_of_day"] < 19)
    df["is_freeflow_2205"] = (df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 5)
    return df[df["roadseg_id"].notna() & df["speed"].gt(0)].copy()


def build_direct_travel_time_reference(manifest: pd.DataFrame, speed_path: Path) -> pd.DataFrame:
    speed = load_speed_obs(speed_path)
    seg = manifest[["split_id", "raw_edge_id", "roadseg_id", "length_m", "road_class"]].copy()
    seg["roadseg_id"] = seg["roadseg_id"].astype("string")
    seg["length_m"] = pd.to_numeric(seg["length_m"], errors="coerce")
    rows = []
    for scenario, day_label, peak_col in WEEKDAY_SCENARIO_SPECS:
        sub = speed[(speed["weekday_label"] == day_label) & speed[peak_col].fillna(False)].copy()
        if sub.empty:
            continue
        spd = sub.groupby("roadseg_id", as_index=False).agg(
            direct_speed_kmh=("speed", "mean"),
            n_speed_obs=("speed", "size"),
        )
        merged = seg.merge(spd, on="roadseg_id", how="left")
        merged = merged[merged["direct_speed_kmh"].gt(0) & merged["length_m"].gt(0)].copy()
        merged["direct_travel_time_min"] = (merged["length_m"] / 1000.0) / merged["direct_speed_kmh"] * 60.0
        merged["scenario"] = scenario
        rows.append(merged)
    if not rows:
        raise RuntimeError("No direct-speed reference rows built.")
    return pd.concat(rows, ignore_index=True)


def session_api_ok(session_dir: Path) -> tuple[int, int]:
    df = pd.read_csv(session_dir / "results_flat.csv", usecols=["api_status"])
    st = pd.to_numeric(df["api_status"], errors="coerce")
    return int(st.eq(0).sum()), len(df)


def find_best_sessions_for_day(
    version_root: Path,
    week_label: str,
    day: int,
    *,
    min_ok_frac: float = 0.0,
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
            f"Missing panels {missing} for {week_label}_{day_tag} (min_ok_frac={min_ok_frac})"
        )
    return sessions


def load_formal_week_frames(
    version_root: Path,
    week_label: str,
    *,
    days: int,
    min_ok_frac: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = []
    manifest_rows = []
    for day in range(1, days + 1):
        day_sessions = find_best_sessions_for_day(version_root, week_label, day, min_ok_frac=min_ok_frac)
        for panel, sd in day_sessions.items():
            df = pd.read_csv(sd / "results_flat.csv")
            df["panel"] = panel
            df["session_dir"] = str(sd)
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
                    "complete": ok >= EXPECTED_ROWS * 0.99,
                }
            )
            frames.append(df)
    return pd.concat(frames, ignore_index=True), pd.DataFrame(manifest_rows)


def cross_day_stability(df: pd.DataFrame) -> pd.DataFrame:
    ok = df["bind_ok"] & df["direct_travel_time_min"].notna() & df["api_travel_time_min"].notna()
    sub = df[ok].copy()
    rows = []
    for panel, g in sub.groupby("panel"):
        wide = g.pivot_table(
            index=["split_id", "raw_edge_id"],
            columns="crawl_day",
            values="api_travel_time_min",
            aggfunc="first",
        )
        multi = wide[wide.notna().sum(axis=1) >= 2]
        if len(multi) == 0:
            continue
        cv = multi.std(axis=1, ddof=0) / multi.mean(axis=1)
        rows.append(
            {
                "panel": panel,
                "n_segments_ge2_days": int(len(multi)),
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
    import matplotlib.pyplot as plt

    from src.baidu_validation_common.plot_style import PANEL_COLORS

    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), dpi=220, sharey=True)
    for ax, panel in zip(axes, PANEL_ORDER):
        sub = day_panel_df[day_panel_df["panel"] == panel].sort_values("crawl_day")
        ax.plot(sub["crawl_day"], 100 * sub["share_bind_ok"], marker="o", color=PANEL_COLORS[panel])
        ax.set_title(panel)
        ax.set_xlabel("Crawl day")
        ax.set_ylim(0, 100)
        ax.grid(True, alpha=0.25)
    axes[0].set_ylabel("Bind OK (%)")
    fig.suptitle("Raw segment: bind_ok share by crawl day", y=1.02)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def load_panel_sessions(version_root: Path, label: str, *, min_rows: int) -> dict[str, Path]:
    day_match = re.search(r"_d(\d{2})$", label)
    if day_match:
        week_label = label[: day_match.start()]
        day = int(day_match.group(1))
        return find_best_sessions_for_day(version_root, week_label, day, min_ok_frac=0.0)
    sessions = {}
    for panel in PANEL_ORDER:
        sd = pick_latest_session(version_root / "api" / "crawl_sessions", panel, label, min_rows=min_rows)
        if sd is not None:
            sessions[panel] = sd
    if not sessions:
        raise FileNotFoundError(f"No panel sessions found for label={label!r}")
    return sessions


def enrich_api(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["length_m"] = pd.to_numeric(out["length_m"], errors="coerce")
    out["distance_m"] = pd.to_numeric(out["distance_m"], errors="coerce")
    out["duration_sec"] = pd.to_numeric(out["duration_sec"], errors="coerce")
    out["api_status"] = pd.to_numeric(out["api_status"], errors="coerce")
    ok = out["api_status"].eq(0) & out["distance_m"].notna() & out["length_m"].gt(0)
    out["distance_over_length"] = np.where(ok, out["distance_m"] / out["length_m"], np.nan)
    out["bind_ok"] = ok & out["distance_over_length"].between(0.6, 1.8)
    out["api_travel_time_min"] = out["duration_sec"] / 60.0
    out["api_speed_kmh"] = np.where(
        out["duration_sec"].gt(0),
        (out["distance_m"] / 1000.0) / (out["duration_sec"] / 3600.0),
        np.nan,
    )
    out["road_class"] = pd.to_numeric(out.get("road_class"), errors="coerce")
    out["road_class_label"] = out["road_class"].map(ROAD_CLASS_LABELS).fillna("other")
    return out


def merge_direct(api_df: pd.DataFrame, direct_ref: pd.DataFrame) -> pd.DataFrame:
    out = api_df.copy()
    out["scenario"] = out["panel"].map(PANEL_SCENARIO)
    keys = ["split_id", "raw_edge_id", "scenario"]
    ref = direct_ref[keys + ["direct_travel_time_min", "direct_speed_kmh", "n_speed_obs"]].drop_duplicates(keys)
    out = out.merge(ref, on=keys, how="left")
    out["time_ratio_api_over_direct"] = out["api_travel_time_min"] / out["direct_travel_time_min"]
    out["speed_ratio_api_over_direct"] = out["api_speed_kmh"] / out["direct_speed_kmh"]
    out["time_diff_min"] = out["api_travel_time_min"] - out["direct_travel_time_min"]
    return out


def panel_summary(df: pd.DataFrame, panel: str) -> dict:
    sub = df[df["panel"] == panel]
    ok = sub["api_status"].eq(0)
    bind = sub["bind_ok"]
    has_direct = sub["direct_travel_time_min"].notna()
    bind_direct = bind & has_direct
    tr = sub.loc[bind_direct, "time_ratio_api_over_direct"]
    td = sub.loc[bind_direct, "time_diff_min"]
    return {
        "panel": panel,
        "scenario": PANEL_SCENARIO[panel],
        "n_rows": int(len(sub)),
        "n_api_ok": int(ok.sum()),
        "n_bind_ok": int(bind.sum()),
        "n_bind_ok_direct": int(bind_direct.sum()),
        "share_bind_ok": float(bind.mean()) if len(sub) else np.nan,
        "median_dist_len_ratio": float(sub.loc[ok, "distance_over_length"].median()) if ok.any() else np.nan,
        "median_time_ratio": float(tr.median()) if len(tr) else np.nan,
        "share_time_ratio_0p8_1p25": float(tr.between(0.8, 1.25).mean()) if len(tr) else np.nan,
        "mae_time_min": float(td.abs().median()) if len(td) else np.nan,
        "corr_time": float(sub.loc[bind_direct, ["api_travel_time_min", "direct_travel_time_min"]].corr().iloc[0, 1])
        if bind_direct.sum() >= 30
        else np.nan,
        "median_api_time_min": float(sub.loc[ok, "api_travel_time_min"].median()) if ok.any() else np.nan,
        "median_direct_time_min": float(sub.loc[has_direct, "direct_travel_time_min"].median()) if has_direct.any() else np.nan,
    }


def compute_spatial_sample(
    df: pd.DataFrame,
    *,
    baseline_root: Path,
    baseline_flag: str,
    sample_n: int,
    seed: int,
) -> pd.DataFrame:
    raw = load_baseline_raw_segments(baseline_root, baseline_flag).set_index(["split_id", "raw_edge_id"])
    to_utm = make_utm_transformer()
    pool = df[df["api_status"].eq(0) & df.get("has_route_path", False).fillna(False)].copy()
    if pool.empty:
        return pd.DataFrame()
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
        key = (int(rec["split_id"]), int(rec["raw_edge_id"]))
        if key not in raw.index:
            continue
        geom = raw.loc[key].geometry
        if not isinstance(geom, LineString):
            parts = [g for g in getattr(geom, "geoms", []) if isinstance(g, LineString)]
            geom = max(parts, key=lambda g: g.length) if parts else geom
        sm = spatial_metrics(geom, route_gcj, buffer_m=50.0, to_utm=to_utm)
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


def plot_ratio_by_road_class(df: pd.DataFrame, path: Path):
    import matplotlib.pyplot as plt

    from src.baidu_validation_common.plot_style import FIG_DPI, QC_BAND_COLOR, apply_validation_rc

    use = df[df["bind_ok"] & df["direct_travel_time_min"].notna()].copy()
    if use.empty:
        return
    classes = [c for c in ["expressway", "arterial", "secondary"] if c in set(use["road_class_label"])]
    apply_validation_rc()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=FIG_DPI)
    data = [use.loc[use["road_class_label"] == c, "time_ratio_api_over_direct"].clip(0, 4) for c in classes]
    ax.boxplot(data, labels=classes, showfliers=False)
    ax.axhspan(0.8, 1.25, color=QC_BAND_COLOR, alpha=0.35)
    ax.set_ylabel("API / direct travel time")
    ax.set_title("Raw segment: travel time ratio by road class (bind_ok, all panels)")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def write_report(
    path: Path,
    summaries: pd.DataFrame,
    label: str,
    sessions: dict[str, str],
    spatial_df: pd.DataFrame,
    *,
    day_panel_df: pd.DataFrame | None = None,
    stability_df: pd.DataFrame | None = None,
    session_manifest: pd.DataFrame | None = None,
) -> None:
    title = "Formal week 数据验证报告" if day_panel_df is not None else f"Raw validation report — {label}"
    lines = [
        f"# {title}",
        "",
        f"Generated: {datetime.now(SH_TZ).isoformat(timespec='seconds')}",
        "",
    ]
    if session_manifest is not None and len(session_manifest):
        lines.extend(["## Sessions (best per day×panel)", ""])
        lines.append("| Day | Panel | Session | n | api_ok | complete |")
        lines.append("|-----|-------|---------|---|--------|----------|")
        for _, r in session_manifest.sort_values(["crawl_day", "panel"]).iterrows():
            lines.append(
                f"| d{int(r['crawl_day']):02d} | {r['panel']} | `{r['session_dir']}` | "
                f"{int(r['n_rows']):,} | {100*r['api_ok_frac']:.1f}% | {bool(r['complete'])} |"
            )
        lines.append("")
    else:
        lines.extend(["## Sessions", ""])
        for panel, sd in sessions.items():
            lines.append(f"- **{panel}**: `{Path(sd).name}`")
    lines.extend(["", "## Panel summary (bind_ok ∩ direct speed)", ""])
    lines.append("| Panel | n bind∩direct | median ratio | share∈[0.8,1.25] | MAE (min) | r |")
    lines.append("|-------|---------------|--------------|----------------|-----------|---|")
    for _, r in summaries.iterrows():
        lines.append(
            f"| {r['panel']} | {int(r['n_bind_ok_direct'])} | {r['median_time_ratio']:.2f} | "
            f"{100*r['share_time_ratio_0p8_1p25']:.1f}% | {r['mae_time_min']:.2f} | {r['corr_time']:.2f} |"
        )
    if day_panel_df is not None and len(day_panel_df):
        lines.extend(["", "## By crawl day", ""])
        lines.append("| Day | Panel | api_ok | bind_ok | median ratio | r |")
        lines.append("|-----|-------|--------|---------|--------------|---|")
        for _, r in day_panel_df.sort_values(["crawl_day", "panel"]).iterrows():
            lines.append(
                f"| d{int(r['crawl_day']):02d} | {r['panel']} | {100*r['n_api_ok']/r['n_rows']:.0f}% | "
                f"{100*r['share_bind_ok']:.1f}% | {r['median_time_ratio']:.2f} | {r['corr_time']:.2f} |"
            )
    if stability_df is not None and len(stability_df):
        lines.extend(["", "## Cross-day stability", ""])
        for _, r in stability_df.iterrows():
            lines.append(
                f"- **{r['panel']}**: n={int(r['n_segments_ge2_days']):,}, "
                f"median CV={r['median_cv_time']:.3f}, share CV≤0.15={100*r['share_cv_le_0p15']:.1f}%"
            )
    if len(spatial_df):
        lines.extend(
            [
                "",
                "## Spatial fit (raw segment geometry)",
                "",
                f"- sample n={len(spatial_df)}",
                f"- spatial PASS: **{100*spatial_df.spatial_ok.mean():.1f}%**",
                f"- overlap median: **{spatial_df.overlap_geom_in_route_buffer.median():.2f}**",
                f"- mean_dist median: **{spatial_df.mean_dist_geom_to_route_m.median():.1f} m**",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    config = load_config(args.config)
    version_root = get_version_root(config)
    ensure_mpl_cache(str(version_root.parent))
    baseline_root = resolve_path(config["output_dir"]) / config["baseline_network_version_id"]
    baseline_flag = config.get("baseline_flag", "keep_baseline")
    metrics_dir = version_root / "metrics"
    fig_dir = version_root / "figures"
    formal_week = args.formal_week
    day_panel_df = None
    stability_df = None
    session_manifest = None

    if formal_week:
        tag = f"formal_week_validation_{formal_week}_{datetime.now(SH_TZ).strftime('%Y%m%dT%H%M%S')}"
        metrics_dir = metrics_dir / tag
        fig_dir = fig_dir / tag
        label = formal_week
    else:
        label = args.session_label

    metrics_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_parquet(version_root / "data" / "crawl_manifest.parquet")
    speed_path = resolve_raw_speed_path(config, baseline_root)
    direct_ref = build_direct_travel_time_reference(manifest, speed_path)
    direct_ref.to_parquet(metrics_dir / "direct_speed_reference.parquet", index=False)

    panels = parse_panels(args.panels)
    if formal_week:
        api_df_raw, session_manifest = load_formal_week_frames(
            version_root,
            formal_week,
            days=args.formal_days,
            min_ok_frac=args.min_api_ok_frac,
        )
        session_meta = {f"d{int(r.crawl_day):02d}_{r.panel}": r.session_dir for _, r in session_manifest.iterrows()}
    else:
        sessions = load_panel_sessions(version_root, args.session_label, min_rows=args.min_rows)
        sessions = {p: sd for p, sd in sessions.items() if p in panels}
        if not sessions:
            raise RuntimeError(f"No sessions left after panel filter: {panels}")
        frames = []
        session_meta = {}
        for panel, sd in sessions.items():
            df = pd.read_csv(sd / "results_flat.csv")
            df["panel"] = panel
            df["session_dir"] = str(sd)
            frames.append(df)
            session_meta[panel] = str(sd)
            print(f"[validation] loaded {panel}: n={len(df):,} from {sd.name}")
        api_df_raw = pd.concat(frames, ignore_index=True)

    api_df = enrich_api(api_df_raw)
    api_df = api_df[api_df["panel"].isin(panels)].copy()
    all_df = merge_direct(api_df, direct_ref)

    if formal_week:
        day_panel_df = day_panel_summary(all_df)
        day_panel_df.to_csv(metrics_dir / "day_panel_summary.csv", index=False)
        stability_df = cross_day_stability(all_df)
        if len(stability_df):
            stability_df.to_csv(metrics_dir / "cross_day_stability.csv", index=False)
        session_manifest.to_csv(metrics_dir / "session_manifest.csv", index=False)
        spatial_source = all_df[all_df["crawl_day"].eq(1)].copy()
    else:
        spatial_source = all_df

    compare_cols = [
        "request_id", "split_id", "raw_edge_id", "roadseg_id", "panel", "scenario",
        "length_m", "distance_m", "duration_sec", "api_status", "bind_ok",
        "distance_over_length", "api_travel_time_min", "api_speed_kmh",
        "direct_travel_time_min", "direct_speed_kmh", "n_speed_obs",
        "time_ratio_api_over_direct", "speed_ratio_api_over_direct", "time_diff_min",
        "road_class", "road_class_label", "session_dir", "has_route_path",
    ]
    if formal_week:
        compare_cols = ["crawl_day", "crawl_label"] + compare_cols
    all_df[compare_cols].to_parquet(metrics_dir / "comparison_table.parquet", index=False)
    all_df[compare_cols].to_csv(metrics_dir / "comparison_table.csv", index=False)

    summaries = pd.DataFrame([panel_summary(all_df, p) for p in panels])
    summaries.to_csv(metrics_dir / "validation_summary.csv", index=False)

    panel_titles = PANEL_SCENARIO
    plot_reference_vs_api_scatter(
        all_df,
        fig_dir / "fig_api_vs_direct_scatter.png",
        ref_col="direct_travel_time_min",
        api_col="api_travel_time_min",
        ref_label="Direct speed CSV travel time (min)",
        api_label="API route travel time (min)",
        title="Raw segment: API vs direct-speed travel time (bind_ok)",
        panel_titles=panel_titles,
    )
    plot_bland_altman(
        all_df,
        fig_dir / "fig_bland_altman_by_panel.png",
        ref_col="direct_travel_time_min",
        api_col="api_travel_time_min",
        diff_col="time_diff_min",
        title="Raw segment: Bland–Altman (API vs direct-speed travel time)",
        ylabel="API − direct (min)",
        panel_titles=panel_titles,
    )
    plot_time_ratio_hist(
        all_df,
        fig_dir / "fig_time_ratio_hist.png",
        ratio_col="time_ratio_api_over_direct",
        title="Raw segment: travel time ratio distribution (bind_ok)",
        xlabel="API / direct travel time",
        panel_titles=panel_titles,
    )
    plot_dist_ratio_hist(
        all_df,
        fig_dir / "fig_dist_ratio_by_panel.png",
        length_label="API distance / raw segment length",
        title="Raw segment: route bind ratio by panel",
    )
    plot_speed_scatter(
        all_df,
        fig_dir / "fig_speed_api_vs_direct_scatter.png",
        ref_speed_col="direct_speed_kmh",
        api_speed_col="api_speed_kmh",
        ref_label="Direct speed CSV (km/h)",
        api_label="API implied speed (km/h)",
        title="Raw segment: API vs direct-speed (bind_ok)",
        panel_titles=panel_titles,
    )
    plot_ratio_by_road_class(all_df, fig_dir / "fig_time_ratio_by_road_class.png")

    if day_panel_df is not None and len(day_panel_df):
        plot_day_panel_bind_ok(day_panel_df, fig_dir / "fig_day_panel_bind_ok.png")

    spatial_df = compute_spatial_sample(
        spatial_source,
        baseline_root=baseline_root,
        baseline_flag=baseline_flag,
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
                median_overlap_geom=("overlap_geom_in_route_buffer", "median"),
                median_mean_dist_m=("mean_dist_geom_to_route_m", "median"),
            )
            .reset_index()
        )
        by_panel.to_csv(metrics_dir / "spatial_summary_by_panel.csv", index=False)
        plot_spatial_hists(
            spatial_df,
            fig_dir / "fig_spatial_overlap_hist.png",
            fig_dir / "fig_spatial_mean_dist_hist.png",
            geom_label="Raw segment",
        )
        plot_spatial_scatter(
            spatial_df,
            fig_dir / "fig_spatial_scatter_overlap_vs_ratio.png",
            geom_label="Raw segment",
            length_label="API distance / raw segment length",
        )

    write_report(
        metrics_dir / "VALIDATION_REPORT.md",
        summaries,
        label,
        session_meta,
        spatial_df,
        day_panel_df=day_panel_df,
        stability_df=stability_df,
        session_manifest=session_manifest,
    )

    print(f"[validation] saved comparison_table -> {metrics_dir / 'comparison_table.parquet'}")
    print(f"[validation] saved figures -> {fig_dir}")
    print(summaries.to_string(index=False))
    if len(spatial_df):
        print(f"[validation] spatial pass rate={spatial_df.spatial_ok.mean():.3f}")


if __name__ == "__main__":
    main()
