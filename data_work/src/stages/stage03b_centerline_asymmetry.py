"""
Stage 03b: Centerline Asymmetry

Purpose:
- Compute AM/PM directional asymmetry on centerlines
- Export centerline-level asymmetry tables and tidal-lane candidates
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point


TIANANMEN_LONLAT = (116.397389, 39.908722)
RADIUS_M = 150_000
MIN_LEN_M = 0.0
COUNT_MISSING_AS_ASYM = False


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 03b: Centerline asymmetry")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    parser.add_argument("--version-id", required=True, help="Version identifier for outputs.")
    parser.add_argument("--output-dir", default="outputs", help="Base output directory for versioned results.")
    return parser.parse_args()


def save_config_snapshot(version_root: Path, config_path: str | None):
    payload = {
        "stage": "stage03b_centerline_asymmetry",
        "config_path": config_path,
        "radius_m": RADIUS_M,
        "min_len_m": MIN_LEN_M,
        "count_missing_as_asym": COUNT_MISSING_AS_ASYM,
    }
    (version_root / "config_snapshot.stage03b.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def within_radius(gdf: gpd.GeoDataFrame, center_lonlat: tuple[float, float], radius_m: float):
    center_pt = gpd.GeoSeries([Point(center_lonlat)], crs="EPSG:4326").to_crs(gdf.crs).iloc[0]
    midpts = gdf.geometry.interpolate(0.5, normalized=True)
    return gdf[midpts.distance(center_pt) <= radius_m].copy()


def load_inputs(version_root: Path):
    data_dir = version_root / "data"
    speed = pd.read_parquet(data_dir / "centerline_speed_master.parquet")
    cl_dir = gpd.read_parquet(data_dir / "centerline_dir_master.parquet")
    return speed, cl_dir


def build_peak_speed(speed: pd.DataFrame):
    weekday = speed[speed["is_weekday"] == True].copy()
    am = (
        weekday[weekday["is_am_peak"] == True]
        .groupby(["skel_dir", "cline_id", "dir"], as_index=False)
        .agg(speed_am=("cl_speed_kmh", "mean"))
    )
    pm = (
        weekday[weekday["is_pm_peak"] == True]
        .groupby(["skel_dir", "cline_id", "dir"], as_index=False)
        .agg(speed_pm=("cl_speed_kmh", "mean"))
    )
    return am.merge(pm, on=["skel_dir", "cline_id", "dir"], how="outer")


def build_pairs(gdf: gpd.GeoDataFrame, speed_col: str) -> pd.DataFrame:
    tmp = gdf[["skel_dir", "cline_id", "dir", "geometry", speed_col, "len_m"]].copy()
    tmp[speed_col] = pd.to_numeric(tmp[speed_col], errors="coerce")

    sp = tmp.pivot_table(index="cline_id", columns="dir", values=speed_col, aggfunc="mean")
    sp = sp.rename(columns={"AB": "speed_AB", "BA": "speed_BA"}).reset_index()

    ge = tmp.pivot_table(index="cline_id", columns="dir", values="geometry", aggfunc="first").reset_index()
    ge = ge.rename(columns={"AB": "geom_AB", "BA": "geom_BA"})

    le = tmp.pivot_table(index="cline_id", columns="dir", values="len_m", aggfunc="mean").reset_index()
    le["len_m_mean"] = le[[c for c in ["AB", "BA"] if c in le.columns]].mean(axis=1, skipna=True)

    skel = tmp.groupby("cline_id", as_index=False).agg(skel_dir=("skel_dir", "first"))

    out = sp.merge(ge, on="cline_id", how="left")
    out = out.merge(le[["cline_id", "len_m_mean"]], on="cline_id", how="left")
    out = out.merge(skel, on="cline_id", how="left")
    out["cl_key"] = out["cline_id"].astype(str)
    out["miss_AB"] = out["speed_AB"].isna()
    out["miss_BA"] = out["speed_BA"].isna()
    out["miss_one_dir"] = out["miss_AB"] ^ out["miss_BA"]
    out["miss_both"] = out["miss_AB"] & out["miss_BA"]
    return out


def add_asym_measures(df: pd.DataFrame):
    out = df.copy()
    ok = (
        out["speed_AB"].notna()
        & out["speed_BA"].notna()
        & (out["speed_AB"] > 0)
        & (out["speed_BA"] > 0)
    )
    r1 = out.loc[ok, "speed_AB"] / out.loc[ok, "speed_BA"]
    r2 = out.loc[ok, "speed_BA"] / out.loc[ok, "speed_AB"]
    out.loc[ok, "ratio"] = np.minimum(r1, r2)
    out.loc[ok, "ratio2"] = (out.loc[ok, "speed_AB"] - out.loc[ok, "speed_BA"]).abs() / (
        out.loc[ok, "speed_AB"] + out.loc[ok, "speed_BA"]
    )
    out["unsym1"] = np.where(ok, out["ratio"] < 0.5, np.nan)
    out["unsym2"] = np.where(ok, out["ratio"] < (1 / 3), np.nan)
    out["unsym3"] = np.where(ok, out["ratio2"] > 0.6, np.nan)
    if COUNT_MISSING_AS_ASYM:
        for col in ["unsym1", "unsym2", "unsym3"]:
            out[col] = np.where(out["miss_one_dir"], True, out[col])
    faster_dir = pd.Series(pd.NA, index=out.index, dtype="string")
    faster_dir.loc[ok & (out["speed_AB"] > out["speed_BA"])] = "AB"
    faster_dir.loc[ok & (out["speed_AB"] <= out["speed_BA"])] = "BA"
    out["faster_dir"] = faster_dir
    return out


def build_asymmetry_outputs(speed: pd.DataFrame, cl_dir: gpd.GeoDataFrame):
    peak = build_peak_speed(speed)
    cl_speed = cl_dir[["skel_dir", "cline_id", "dir", "geometry", "length_m"]].merge(
        peak, on=["skel_dir", "cline_id", "dir"], how="left"
    )
    cl_speed = gpd.GeoDataFrame(cl_speed, geometry="geometry", crs=cl_dir.crs)
    cl_speed = cl_speed.rename(columns={"length_m": "len_m"})

    cl_use = within_radius(cl_speed, TIANANMEN_LONLAT, RADIUS_M)
    cl_use["too_short"] = pd.to_numeric(cl_use["len_m"], errors="coerce") < MIN_LEN_M
    cl_use = cl_use.loc[~cl_use["too_short"]].copy()

    results = []
    for peak_name, speed_col in [("AM", "speed_am"), ("PM", "speed_pm")]:
        pairs = build_pairs(cl_use, speed_col)
        pairs = add_asym_measures(pairs)
        pairs["peak"] = peak_name
        results.append(pairs)

    asym_table = pd.concat(results, ignore_index=True)
    return cl_use, asym_table


def build_summary(asym_table: pd.DataFrame):
    rows = []
    for peak_name in ["AM", "PM"]:
        sub = asym_table[asym_table["peak"] == peak_name]
        n_total = len(sub)
        n_both = (~sub["miss_AB"] & ~sub["miss_BA"]).sum()
        for metric in ["unsym1", "unsym2", "unsym3"]:
            n_asym = int((sub[metric] == True).sum())
            rows.append(
                {
                    "peak": peak_name,
                    "metric": metric,
                    "n_centerlines": n_total,
                    "n_both_dir": int(n_both),
                    "n_asymmetric": n_asym,
                    "pct_asymmetric": (n_asym / n_both * 100.0) if n_both else np.nan,
                }
            )
    return pd.DataFrame(rows)


def build_am_pm_comparison(asym_table: pd.DataFrame):
    am_set = set(asym_table[(asym_table["peak"] == "AM") & (asym_table["unsym1"] == True)]["cl_key"].tolist())
    pm_set = set(asym_table[(asym_table["peak"] == "PM") & (asym_table["unsym1"] == True)]["cl_key"].tolist())
    return pd.DataFrame(
        {
            "category": ["AM only", "PM only", "Both AM & PM", "Total unique"],
            "n_centerlines": [len(am_set - pm_set), len(pm_set - am_set), len(am_set & pm_set), len(am_set | pm_set)],
        }
    )


def build_tidal_candidates(asym_table: pd.DataFrame):
    am = asym_table[asym_table["peak"] == "AM"][["cl_key", "faster_dir", "ratio", "speed_AB", "speed_BA"]].rename(
        columns={"faster_dir": "faster_am", "ratio": "ratio_am", "speed_AB": "speed_AB_am", "speed_BA": "speed_BA_am"}
    )
    pm = asym_table[asym_table["peak"] == "PM"][["cl_key", "faster_dir", "ratio", "speed_AB", "speed_BA"]].rename(
        columns={"faster_dir": "faster_pm", "ratio": "ratio_pm", "speed_AB": "speed_AB_pm", "speed_BA": "speed_BA_pm"}
    )
    tidal = am.merge(pm, on="cl_key", how="inner")
    tidal["reversed"] = (
        (tidal["faster_am"] != tidal["faster_pm"]) & tidal["faster_am"].notna() & tidal["faster_pm"].notna()
    )
    tidal["asym_am"] = tidal["ratio_am"] < 0.5
    tidal["asym_pm"] = tidal["ratio_pm"] < 0.5
    tidal["tidal_candidate"] = tidal["reversed"] & tidal["asym_am"] & tidal["asym_pm"]
    return tidal[tidal["tidal_candidate"]].copy(), tidal


def save_ratio_hist(asym_table: pd.DataFrame, figures_dir: Path):
    figures_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for i, peak_name in enumerate(["AM", "PM"]):
        ax = axes[i]
        sub = asym_table[(asym_table["peak"] == peak_name) & asym_table["ratio"].notna()].copy()
        ax.hist(sub["ratio"], bins=50, color="#2171b5", alpha=0.75, edgecolor="white")
        ax.axvline(x=0.5, color="#d73027", linestyle="--", linewidth=2)
        ax.axvline(x=1 / 3, color="#fc8d59", linestyle="--", linewidth=2)
        ax.set_title(f"{peak_name} Peak")
        ax.set_xlabel("Speed ratio")
        ax.set_ylabel("Count")
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = figures_dir / "hist_speed_ratio_am_pm.png"
    plt.savefig(out, dpi=160)
    plt.close(fig)


def save_metrics(asym_table: pd.DataFrame, summary: pd.DataFrame, tidal_all: pd.DataFrame, metrics_dir: Path):
    metrics_dir.mkdir(parents=True, exist_ok=True)
    rows = {
        "asym_rows": len(asym_table),
        "am_unsym1": int(((asym_table["peak"] == "AM") & (asym_table["unsym1"] == True)).sum()),
        "pm_unsym1": int(((asym_table["peak"] == "PM") & (asym_table["unsym1"] == True)).sum()),
        "reversed_count": int(tidal_all["reversed"].sum()),
        "tidal_candidate_count": int(tidal_all["tidal_candidate"].sum()),
    }
    if not summary.empty:
        am = summary[(summary["peak"] == "AM") & (summary["metric"] == "unsym1")]
        pm = summary[(summary["peak"] == "PM") & (summary["metric"] == "unsym1")]
        if len(am):
            rows["am_unsym1_pct"] = float(am.iloc[0]["pct_asymmetric"])
        if len(pm):
            rows["pm_unsym1_pct"] = float(pm.iloc[0]["pct_asymmetric"])
    pd.DataFrame([rows]).to_csv(metrics_dir / "stage03b_asymmetry_summary.csv", index=False)


def run(config_path: str | None, version_id: str, output_dir: str):
    version_root = Path(output_dir) / version_id
    data_dir = version_root / "data"
    metrics_dir = version_root / "metrics"
    figures_dir = version_root / "figures"
    data_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    save_config_snapshot(version_root, config_path)
    speed, cl_dir = load_inputs(version_root)
    _, asym_table = build_asymmetry_outputs(speed, cl_dir)
    summary = build_summary(asym_table)
    am_pm_comparison = build_am_pm_comparison(asym_table)
    tidal_candidates, tidal_all = build_tidal_candidates(asym_table)

    export = asym_table.drop(columns=["geom_AB", "geom_BA"], errors="ignore")
    export.to_parquet(data_dir / "centerline_asymmetry_table.parquet", index=False)
    export.to_csv(data_dir / "centerline_asymmetry_table.csv", index=False)
    summary.to_csv(data_dir / "centerline_asymmetry_summary.csv", index=False)
    am_pm_comparison.to_csv(data_dir / "centerline_asymmetry_am_pm_comparison.csv", index=False)
    tidal_candidates.to_csv(data_dir / "centerline_tidal_lane_candidates.csv", index=False)
    save_ratio_hist(asym_table, figures_dir)
    save_metrics(asym_table, summary, tidal_all, metrics_dir)

    print(f"[stage03b] saved centerline asymmetry outputs for version={version_id}")


def main():
    args = parse_args()
    run(args.config, args.version_id, args.output_dir)


if __name__ == "__main__":
    main()
