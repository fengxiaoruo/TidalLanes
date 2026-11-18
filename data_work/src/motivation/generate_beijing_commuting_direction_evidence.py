"""Generate Beijing OD evidence for directional commuting demand.

The script uses the full square-grid OD output from stage07, not the
network-LCC model sample. It is meant as a descriptive motivation asset:
morning home-to-work demand is compared with the spatial distribution of
jobs and residence, and the PM flow is the reverse of the same OD structure.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import TwoSlopeNorm
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Circle, Wedge
from sklearn.cluster import KMeans

try:
    from scipy.ndimage import gaussian_filter
except ImportError:  # pragma: no cover - fallback for minimal environments
    gaussian_filter = None


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diagnostics.mpl_setup import ensure_mpl_cache


SOURCE_VERSION = "manual_centerline_rules_v11"
DEFAULT_VERSION_ID = "beijing_commuting_direction_evidence_v1"
TARGET_CRS = "EPSG:32650"
ANGLE_MIN_DIST_KM = 1.0
CLUSTER_K = 6
PRIMARY_WEIGHT = "car_bus_commuters"
STORY_MIN_DISTANCE_KM = 8.0
STORY_TOP_CORRIDORS = 12
OD_NETWORK_MIN_DISTANCE_KM = 8.0
OD_NETWORK_TOP_N = 250
INBOUND_WORK_INNER_KM = 10.0
INBOUND_HOME_OUTER_KM = 15.0
INBOUND_SECTORS = 16

WEIGHT_SPECS = {
    "total_commuters": "All commuters",
    "car_commuters": "Car commuters",
    "car_bus_commuters": "Car + bus commuters",
    "identified_commuters": "Mode-identified commuters",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Beijing commuting-direction evidence.")
    parser.add_argument("--source-version", default=SOURCE_VERSION)
    parser.add_argument("--version-id", default=DEFAULT_VERSION_ID)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--cluster-k", type=int, default=CLUSTER_K)
    parser.add_argument("--primary-weight", default=PRIMARY_WEIGHT, choices=sorted(WEIGHT_SPECS))
    parser.add_argument("--top-flow-count", type=int, default=250)
    return parser.parse_args()


def configure_matplotlib(cache_root: Path) -> None:
    ensure_mpl_cache(str(cache_root))
    mpl.rcParams.update(
        {
            "font.sans-serif": [
                "Arial",
                "Helvetica",
                "Liberation Sans",
                "DejaVu Sans",
                "PingFang SC",
                "Noto Sans CJK SC",
            ],
            "font.family": "sans-serif",
            "axes.unicode_minus": False,
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "figure.dpi": 180,
            "savefig.dpi": 600,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "xtick.major.size": 3.0,
            "ytick.major.size": 3.0,
            "axes.labelsize": 8.5,
            "axes.titlesize": 9.0,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "legend.fontsize": 7.5,
        }
    )


def ensure_dirs(root: Path) -> dict[str, Path]:
    dirs = {
        "root": root,
        "data": root / "data",
        "figures": root / "figures",
        "metrics": root / "metrics",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def save_figure(fig: mpl.figure.Figure, stem: Path) -> None:
    for suffix in [".png", ".pdf", ".svg"]:
        fig.savefig(stem.with_suffix(suffix), bbox_inches="tight")
    plt.close(fig)


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = v.notna() & np.isfinite(v) & w.gt(0)
    if not mask.any():
        return float("nan")
    return float(np.average(v.loc[mask], weights=w.loc[mask]))


def weighted_median(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = v.notna() & np.isfinite(v) & w.gt(0)
    if not mask.any():
        return float("nan")
    vv = v.loc[mask].to_numpy()
    ww = w.loc[mask].to_numpy()
    order = np.argsort(vv)
    vv = vv[order]
    ww = ww[order]
    csum = np.cumsum(ww)
    return float(vv[np.searchsorted(csum, 0.5 * csum[-1], side="left")])


def weighted_share(condition: pd.Series, weights: pd.Series) -> float:
    c = condition.fillna(False).astype(float)
    return weighted_mean(c, weights)


def weighted_quantile(values: pd.Series, weights: pd.Series, quantile: float) -> float:
    v = pd.to_numeric(values, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = v.notna() & np.isfinite(v) & w.gt(0)
    if not mask.any():
        return float("nan")
    vv = v.loc[mask].to_numpy()
    ww = w.loc[mask].to_numpy()
    order = np.argsort(vv)
    vv = vv[order]
    ww = ww[order]
    csum = np.cumsum(ww)
    return float(vv[np.searchsorted(csum, quantile * csum[-1], side="left")])


def resultant_length(angle_rad: pd.Series, weights: pd.Series) -> tuple[float, float]:
    a = pd.to_numeric(angle_rad, errors="coerce").astype(float)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).astype(float)
    mask = a.notna() & np.isfinite(a) & w.gt(0)
    if not mask.any():
        return float("nan"), float("nan")
    ww = w.loc[mask].to_numpy()
    aa = a.loc[mask].to_numpy()
    x = float(np.sum(ww * np.cos(aa)))
    y = float(np.sum(ww * np.sin(aa)))
    total = float(np.sum(ww))
    angle = float((np.degrees(np.arctan2(y, x)) + 360.0) % 360.0)
    return float(np.hypot(x, y) / total), angle


def load_inputs(source_version: str) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    data_dir = ROOT / "outputs" / source_version / "data"
    grid = gpd.read_parquet(data_dir / "grid_square_master.parquet")
    pop = pd.read_csv(data_dir / "grid_population_summary_square.csv")
    od = pd.read_csv(data_dir / "OD_square.csv")

    grid = grid[["grid_id", "geometry", "area_km2"]].copy()
    grid["grid_id"] = grid["grid_id"].astype(str)
    grid_utm = grid.to_crs(TARGET_CRS)
    grid_utm["centroid"] = grid_utm.geometry.centroid
    grid_utm["x_m"] = grid_utm["centroid"].x
    grid_utm["y_m"] = grid_utm["centroid"].y
    grid_utm = grid_utm.drop(columns="centroid")

    pop["grid_id"] = pop["grid_id"].astype(str)
    grid_pop = grid_utm.merge(pop, on="grid_id", how="left")
    for col in ["residents", "jobs"]:
        grid_pop[col] = pd.to_numeric(grid_pop[col], errors="coerce").fillna(0.0)
    denom = grid_pop["residents"] + grid_pop["jobs"]
    grid_pop["job_resident_balance"] = np.where(
        denom > 0,
        (grid_pop["jobs"] - grid_pop["residents"]) / denom,
        np.nan,
    )

    od["home_grid"] = od["home_grid"].astype(str)
    od["work_grid"] = od["work_grid"].astype(str)
    for col in ["pop", "type_car", "type_bus", "type_walk", "type_bike", "type_sub", "identified"]:
        od[col] = pd.to_numeric(od[col], errors="coerce").fillna(0.0)
    od["total_commuters"] = od["pop"]
    od["car_commuters"] = od["type_car"]
    od["car_bus_commuters"] = od["type_car"] + od["type_bus"]
    od["identified_commuters"] = od["identified"]
    return grid, grid_pop, od


def add_spatial_metrics(
    od: pd.DataFrame,
    grid_pop: gpd.GeoDataFrame,
    cluster_k: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    centroids = grid_pop[["grid_id", "x_m", "y_m", "residents", "jobs"]].copy()
    home = centroids.rename(
        columns={"grid_id": "home_grid", "x_m": "home_x_m", "y_m": "home_y_m"}
    )[["home_grid", "home_x_m", "home_y_m"]]
    work = centroids.rename(
        columns={"grid_id": "work_grid", "x_m": "work_x_m", "y_m": "work_y_m"}
    )[["work_grid", "work_x_m", "work_y_m"]]
    use = od.merge(home, on="home_grid", how="inner").merge(work, on="work_grid", how="inner")

    dx = use["work_x_m"] - use["home_x_m"]
    dy = use["work_y_m"] - use["home_y_m"]
    dist_m = np.hypot(dx, dy)
    use["centroid_distance_km"] = dist_m / 1000.0
    use["same_grid"] = use["home_grid"].eq(use["work_grid"])
    use["angle_rad"] = np.arctan2(dy, dx)
    use["angle_deg"] = (np.degrees(use["angle_rad"]) + 360.0) % 360.0

    job_center = weighted_center(grid_pop, "jobs")
    res_center = weighted_center(grid_pop, "residents")
    use["home_dist_to_job_center_km"] = distance_to_point(use["home_x_m"], use["home_y_m"], job_center) / 1000.0
    use["work_dist_to_job_center_km"] = distance_to_point(use["work_x_m"], use["work_y_m"], job_center) / 1000.0
    use["delta_dist_to_job_center_km"] = (
        use["work_dist_to_job_center_km"] - use["home_dist_to_job_center_km"]
    )
    use["cos_toward_job_center"] = cosine_toward_point(
        use["home_x_m"],
        use["home_y_m"],
        use["work_x_m"],
        use["work_y_m"],
        job_center,
    )

    res_clusters = fit_weighted_clusters(grid_pop, "residents", cluster_k, "R")
    job_clusters = fit_weighted_clusters(grid_pop, "jobs", cluster_k, "J")
    use, grid_pop = assign_clusters(use, grid_pop, res_clusters, job_clusters)

    jx = use["job_cluster_x_m"]
    jy = use["job_cluster_y_m"]
    use["home_dist_to_work_job_cluster_km"] = np.hypot(use["home_x_m"] - jx, use["home_y_m"] - jy) / 1000.0
    use["work_dist_to_work_job_cluster_km"] = np.hypot(use["work_x_m"] - jx, use["work_y_m"] - jy) / 1000.0
    use["delta_dist_to_work_job_cluster_km"] = (
        use["work_dist_to_work_job_cluster_km"] - use["home_dist_to_work_job_cluster_km"]
    )
    use["cos_toward_work_job_cluster"] = cosine_toward_arrays(
        use["home_x_m"],
        use["home_y_m"],
        use["work_x_m"],
        use["work_y_m"],
        jx,
        jy,
    )

    centers = pd.concat(
        [
            cluster_centers_frame(res_clusters, "residents", res_center),
            cluster_centers_frame(job_clusters, "jobs", job_center),
        ],
        ignore_index=True,
    )
    return use, grid_pop, centers


def weighted_center(grid_pop: gpd.GeoDataFrame, weight_col: str) -> tuple[float, float]:
    w = pd.to_numeric(grid_pop[weight_col], errors="coerce").fillna(0.0)
    mask = w.gt(0) & grid_pop["x_m"].notna() & grid_pop["y_m"].notna()
    if not mask.any():
        return float(grid_pop["x_m"].mean()), float(grid_pop["y_m"].mean())
    return (
        float(np.average(grid_pop.loc[mask, "x_m"], weights=w.loc[mask])),
        float(np.average(grid_pop.loc[mask, "y_m"], weights=w.loc[mask])),
    )


def distance_to_point(x: pd.Series, y: pd.Series, point: tuple[float, float]) -> np.ndarray:
    return np.hypot(pd.to_numeric(x, errors="coerce") - point[0], pd.to_numeric(y, errors="coerce") - point[1])


def cosine_toward_point(
    home_x: pd.Series,
    home_y: pd.Series,
    work_x: pd.Series,
    work_y: pd.Series,
    point: tuple[float, float],
) -> np.ndarray:
    return cosine_toward_arrays(home_x, home_y, work_x, work_y, point[0], point[1])


def cosine_toward_arrays(
    home_x: pd.Series,
    home_y: pd.Series,
    work_x: pd.Series,
    work_y: pd.Series,
    target_x: pd.Series | float,
    target_y: pd.Series | float,
) -> np.ndarray:
    od_x = pd.to_numeric(work_x, errors="coerce") - pd.to_numeric(home_x, errors="coerce")
    od_y = pd.to_numeric(work_y, errors="coerce") - pd.to_numeric(home_y, errors="coerce")
    target_vec_x = target_x - pd.to_numeric(home_x, errors="coerce")
    target_vec_y = target_y - pd.to_numeric(home_y, errors="coerce")
    denom = np.hypot(od_x, od_y) * np.hypot(target_vec_x, target_vec_y)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = (od_x * target_vec_x + od_y * target_vec_y) / denom
    return np.where(denom > 0, out, np.nan)


def fit_weighted_clusters(
    grid_pop: gpd.GeoDataFrame,
    weight_col: str,
    cluster_k: int,
    prefix: str,
) -> dict[str, object]:
    cols = ["grid_id", "x_m", "y_m", weight_col]
    sample = grid_pop.loc[grid_pop[weight_col] > 0, cols].dropna().copy()
    if len(sample) < cluster_k:
        raise ValueError(f"Need at least {cluster_k} positive-weight grids for {weight_col}.")
    xy = sample[["x_m", "y_m"]].to_numpy(float)
    w = sample[weight_col].to_numpy(float)
    model = KMeans(n_clusters=cluster_k, random_state=20260629, n_init=30)
    model.fit(xy, sample_weight=w)
    raw_labels = model.predict(xy)
    sample["raw_cluster"] = raw_labels
    totals = sample.groupby("raw_cluster", as_index=False)[weight_col].sum()
    totals = totals.sort_values(weight_col, ascending=False).reset_index(drop=True)
    raw_to_rank = {int(row.raw_cluster): idx for idx, row in totals.iterrows()}
    label_map = {raw: f"{prefix}{rank + 1}" for raw, rank in raw_to_rank.items()}
    centers = model.cluster_centers_
    ranked_centers = {
        f"{prefix}{rank + 1}": (float(centers[raw, 0]), float(centers[raw, 1]))
        for raw, rank in raw_to_rank.items()
    }
    sample["cluster"] = sample["raw_cluster"].map(label_map)
    cluster_weight = sample.groupby("cluster", as_index=False)[weight_col].sum().rename(
        columns={weight_col: "cluster_weight"}
    )
    return {
        "model": model,
        "raw_to_rank": raw_to_rank,
        "label_map": label_map,
        "centers": ranked_centers,
        "cluster_weight": cluster_weight,
        "prefix": prefix,
        "weight_col": weight_col,
    }


def predict_cluster_labels(cluster_info: dict[str, object], xy: np.ndarray) -> list[str]:
    model: KMeans = cluster_info["model"]  # type: ignore[assignment]
    label_map: dict[int, str] = cluster_info["label_map"]  # type: ignore[assignment]
    raw = model.predict(xy)
    return [label_map[int(v)] for v in raw]


def assign_clusters(
    use: pd.DataFrame,
    grid_pop: gpd.GeoDataFrame,
    res_clusters: dict[str, object],
    job_clusters: dict[str, object],
) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    grid_xy = grid_pop[["x_m", "y_m"]].to_numpy(float)
    grid_pop = grid_pop.copy()
    grid_pop["res_cluster"] = predict_cluster_labels(res_clusters, grid_xy)
    grid_pop["job_cluster"] = predict_cluster_labels(job_clusters, grid_xy)

    res_lookup = grid_pop[["grid_id", "res_cluster"]].rename(columns={"grid_id": "home_grid"})
    job_lookup = grid_pop[["grid_id", "job_cluster"]].rename(columns={"grid_id": "work_grid"})
    use = use.merge(res_lookup, on="home_grid", how="left").merge(job_lookup, on="work_grid", how="left")

    job_centers = pd.DataFrame(
        [
            {"job_cluster": label, "job_cluster_x_m": xy[0], "job_cluster_y_m": xy[1]}
            for label, xy in job_clusters["centers"].items()  # type: ignore[union-attr]
        ]
    )
    use = use.merge(job_centers, on="job_cluster", how="left")
    return use, grid_pop


def cluster_centers_frame(
    cluster_info: dict[str, object],
    weight_name: str,
    global_center: tuple[float, float],
) -> pd.DataFrame:
    centers: dict[str, tuple[float, float]] = cluster_info["centers"]  # type: ignore[assignment]
    weights: pd.DataFrame = cluster_info["cluster_weight"]  # type: ignore[assignment]
    rows = []
    for label, (x, y) in centers.items():
        rows.append({"cluster": label, "cluster_type": weight_name, "x_m": x, "y_m": y})
    out = pd.DataFrame(rows).merge(weights, left_on="cluster", right_on="cluster", how="left")
    out["global_center_x_m"] = global_center[0]
    out["global_center_y_m"] = global_center[1]
    return out


def build_summary_metrics(odm: pd.DataFrame, matched_distance: pd.DataFrame | None = None) -> pd.DataFrame:
    rows = []
    angle_sample = odm["centroid_distance_km"] >= ANGLE_MIN_DIST_KM
    for weight_col, label in WEIGHT_SPECS.items():
        w = odm[weight_col]
        positive = w > 0
        wa = odm.loc[angle_sample, weight_col]
        r_len, r_angle = resultant_length(odm.loc[angle_sample, "angle_rad"], wa)
        total = float(w.sum())
        row = {
            "weight_col": weight_col,
            "weight_label": label,
            "total_weight": total,
            "positive_od_pairs": int(positive.sum()),
            "same_grid_weight_share": weighted_share(odm["same_grid"], w),
            "short_lt_1km_weight_share": weighted_share(odm["centroid_distance_km"] < ANGLE_MIN_DIST_KM, w),
            "mean_centroid_distance_km": weighted_mean(odm["centroid_distance_km"], w),
            "median_centroid_distance_km": weighted_median(odm["centroid_distance_km"], w),
            "p75_centroid_distance_km": weighted_quantile(odm["centroid_distance_km"], w, 0.75),
            "mean_delta_to_job_center_km": weighted_mean(odm["delta_dist_to_job_center_km"], w),
            "median_delta_to_job_center_km": weighted_median(odm["delta_dist_to_job_center_km"], w),
            "share_work_closer_to_job_center": weighted_share(odm["delta_dist_to_job_center_km"] < 0, w),
            "mean_cos_toward_job_center": weighted_mean(odm["cos_toward_job_center"], w),
            "share_cos_toward_job_center_positive": weighted_share(odm["cos_toward_job_center"] > 0, w),
            "mean_delta_to_work_job_cluster_km": weighted_mean(odm["delta_dist_to_work_job_cluster_km"], w),
            "median_delta_to_work_job_cluster_km": weighted_median(odm["delta_dist_to_work_job_cluster_km"], w),
            "share_work_closer_to_work_job_cluster": weighted_share(odm["delta_dist_to_work_job_cluster_km"] < 0, w),
            "mean_cos_toward_work_job_cluster": weighted_mean(odm["cos_toward_work_job_cluster"], w),
            "share_cos_toward_work_job_cluster_positive": weighted_share(odm["cos_toward_work_job_cluster"] > 0, w),
            "angle_sample_weight": float(wa.sum()),
            "angle_resultant_length": r_len,
            "dominant_angle_deg": r_angle,
        }
        if matched_distance is not None and weight_col in matched_distance.columns:
            mw = matched_distance[weight_col]
            row["point_level_mean_dis_km"] = weighted_mean(matched_distance["dis_km"], mw)
            row["point_level_mean_straight_r_km"] = weighted_mean(matched_distance["r_km"], mw)
        rows.append(row)
    return pd.DataFrame(rows)


def load_point_distance_summary(source_version: str) -> pd.DataFrame | None:
    path = ROOT / "outputs" / source_version / "data" / "commute_square_matched.csv"
    if not path.exists():
        return None
    mode_cols = ["type_walk", "type_bike", "type_sub", "type_bus", "type_car"]
    cols = ["pop", "dis", "r"] + mode_cols
    df = pd.read_csv(path, usecols=cols)
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    identified = df[mode_cols].sum(axis=1)
    out = pd.DataFrame(
        {
            "dis_km": df["dis"] / 1000.0,
            "r_km": df["r"] / 1000.0,
            "total_commuters": df["pop"],
            "car_commuters": df["type_car"],
            "car_bus_commuters": df["type_car"] + df["type_bus"],
            "identified_commuters": identified,
        }
    )
    return out


def build_origin_summary(odm: pd.DataFrame, primary_weight: str) -> pd.DataFrame:
    rows = []
    for home_grid, grp in odm.groupby("home_grid", sort=False):
        w = grp[primary_weight]
        total = float(w.sum())
        if total <= 0:
            continue
        rows.append(
            {
                "home_grid": home_grid,
                f"{primary_weight}_outflow": total,
                "mean_centroid_distance_km": weighted_mean(grp["centroid_distance_km"], w),
                "mean_delta_to_job_center_km": weighted_mean(grp["delta_dist_to_job_center_km"], w),
                "share_work_closer_to_job_center": weighted_share(grp["delta_dist_to_job_center_km"] < 0, w),
                "mean_delta_to_work_job_cluster_km": weighted_mean(grp["delta_dist_to_work_job_cluster_km"], w),
                "share_work_closer_to_work_job_cluster": weighted_share(
                    grp["delta_dist_to_work_job_cluster_km"] < 0,
                    w,
                ),
                "mean_cos_toward_work_job_cluster": weighted_mean(grp["cos_toward_work_job_cluster"], w),
            }
        )
    return pd.DataFrame(rows)


def build_cluster_flow(odm: pd.DataFrame, primary_weight: str) -> pd.DataFrame:
    flow = (
        odm.groupby(["res_cluster", "job_cluster"], as_index=False)[primary_weight]
        .sum()
        .rename(columns={primary_weight: "flow_weight"})
    )
    total = float(flow["flow_weight"].sum())
    flow["flow_share"] = np.where(total > 0, flow["flow_weight"] / total, np.nan)
    return flow.sort_values("flow_weight", ascending=False).reset_index(drop=True)


def load_major_roads() -> gpd.GeoDataFrame:
    path = ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"
    roads = gpd.read_file(path)
    roads["roadtype_num"] = pd.to_numeric(roads["roadtype"], errors="coerce")
    roads = roads.loc[roads["roadtype_num"].le(3)].copy()
    return roads.to_crs(TARGET_CRS)


def prep_boundary() -> gpd.GeoDataFrame:
    boundary = gpd.read_file(ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp")
    return boundary.to_crs(TARGET_CRS)


def weighted_bounds(grid_pop: gpd.GeoDataFrame, weight_col: str, qlo: float = 0.015, qhi: float = 0.985) -> tuple[float, float, float, float]:
    w = pd.to_numeric(grid_pop[weight_col], errors="coerce").fillna(0.0)
    mask = w.gt(0) & grid_pop["x_m"].notna() & grid_pop["y_m"].notna()
    x = grid_pop.loc[mask, "x_m"]
    y = grid_pop.loc[mask, "y_m"]
    ww = w.loc[mask]
    xmin = weighted_quantile(x, ww, qlo)
    xmax = weighted_quantile(x, ww, qhi)
    ymin = weighted_quantile(y, ww, qlo)
    ymax = weighted_quantile(y, ww, qhi)
    pad = max(xmax - xmin, ymax - ymin) * 0.08
    return xmin - pad, xmax + pad, ymin - pad, ymax + pad


def set_axis_extent(ax: mpl.axes.Axes, extent: tuple[float, float, float, float]) -> None:
    xmin, xmax, ymin, ymax = extent
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect("equal", adjustable="box")
    ax.set_axis_off()


def plot_base_map(
    ax: mpl.axes.Axes,
    boundary: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame | None,
    extent: tuple[float, float, float, float],
    *,
    road_alpha: float = 0.25,
    road_color: str = "#7f8c8d",
) -> None:
    boundary.boundary.plot(ax=ax, color="#303030", linewidth=0.55, zorder=8)
    if roads is not None and not roads.empty:
        roads.plot(ax=ax, color=road_color, linewidth=0.18, alpha=road_alpha, zorder=3)
    set_axis_extent(ax, extent)


def density_grid(
    grid_pop: gpd.GeoDataFrame,
    weight_col: str,
    extent: tuple[float, float, float, float],
    bins: int = 320,
) -> tuple[np.ndarray, tuple[float, float, float, float]]:
    xmin, xmax, ymin, ymax = extent
    sample = grid_pop.loc[grid_pop[weight_col] > 0, ["x_m", "y_m", weight_col]].dropna()
    hist, y_edges, x_edges = np.histogram2d(
        sample["y_m"].to_numpy(float),
        sample["x_m"].to_numpy(float),
        bins=[bins, bins],
        range=[[ymin, ymax], [xmin, xmax]],
        weights=sample[weight_col].to_numpy(float),
    )
    if gaussian_filter is not None:
        hist = gaussian_filter(hist, sigma=3.5)
    return hist, (xmin, xmax, ymin, ymax)


def draw_density_surface(
    ax: mpl.axes.Axes,
    grid_pop: gpd.GeoDataFrame,
    weight_col: str,
    extent: tuple[float, float, float, float],
    cmap: str,
    *,
    alpha: float = 0.78,
) -> None:
    surface, image_extent = density_grid(grid_pop, weight_col, extent)
    positive = surface[surface > 0]
    vmax = float(np.quantile(positive, 0.995)) if positive.size else 1.0
    ax.imshow(
        surface,
        origin="lower",
        extent=image_extent,
        cmap=cmap,
        vmin=0,
        vmax=vmax,
        alpha=np.where(surface > 0, alpha, 0),
        zorder=2,
    )


def build_balance_classes(grid_pop: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = grid_pop.copy()
    residents = out["residents"].replace(0, np.nan)
    ratio = out["jobs"] / residents
    out["jobs_per_resident"] = ratio
    out["balance_class"] = np.select(
        [
            ratio.lt(0.8),
            ratio.between(0.8, 1.2, inclusive="both"),
            ratio.gt(1.2),
        ],
        ["Resident-heavy", "Balanced", "Job-heavy"],
        default="No residents",
    )
    return out


def build_story_cluster_corridors(
    odm: pd.DataFrame,
    centers: pd.DataFrame,
    primary_weight: str,
    *,
    min_distance_km: float = STORY_MIN_DISTANCE_KM,
    top_n: int = STORY_TOP_CORRIDORS,
) -> pd.DataFrame:
    sample = odm.loc[
        (odm[primary_weight] > 0)
        & (odm["centroid_distance_km"] >= min_distance_km)
        & (odm["delta_dist_to_work_job_cluster_km"] < 0)
    ].copy()
    rows = []
    for (res_cluster, job_cluster), g in sample.groupby(["res_cluster", "job_cluster"], sort=False):
        rows.append(
            {
                "res_cluster": res_cluster,
                "job_cluster": job_cluster,
                "flow_weight": float(g[primary_weight].sum()),
                "mean_distance_km": weighted_mean(g["centroid_distance_km"], g[primary_weight]),
                "median_distance_km": weighted_median(g["centroid_distance_km"], g[primary_weight]),
                "mean_delta_to_job_cluster_km": weighted_mean(g["delta_dist_to_work_job_cluster_km"], g[primary_weight]),
                "od_pair_count": int(len(g)),
            }
        )
    grouped = pd.DataFrame(rows)
    total = float(odm[primary_weight].sum())
    long_total = float(sample[primary_weight].sum())
    grouped["flow_share_total"] = grouped["flow_weight"] / total if total > 0 else np.nan
    grouped["flow_share_long_inward"] = grouped["flow_weight"] / long_total if long_total > 0 else np.nan

    center_lookup = centers.set_index("cluster")[["x_m", "y_m", "cluster_weight"]]
    grouped["res_x_m"] = grouped["res_cluster"].map(center_lookup["x_m"])
    grouped["res_y_m"] = grouped["res_cluster"].map(center_lookup["y_m"])
    grouped["job_x_m"] = grouped["job_cluster"].map(center_lookup["x_m"])
    grouped["job_y_m"] = grouped["job_cluster"].map(center_lookup["y_m"])
    grouped["cluster_center_distance_km"] = (
        np.hypot(grouped["job_x_m"] - grouped["res_x_m"], grouped["job_y_m"] - grouped["res_y_m"]) / 1000.0
    )
    grouped = grouped.loc[grouped["cluster_center_distance_km"] >= 4.0].copy()
    return grouped.sort_values("flow_weight", ascending=False).head(top_n).reset_index(drop=True)


def build_dominant_od_network(
    odm: pd.DataFrame,
    primary_weight: str,
    *,
    min_distance_km: float = OD_NETWORK_MIN_DISTANCE_KM,
    top_n: int = OD_NETWORK_TOP_N,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sample = odm.loc[
        (odm[primary_weight] > 0)
        & (odm["centroid_distance_km"] >= min_distance_km)
        & odm["home_grid"].ne(odm["work_grid"])
    ].copy()
    reverse = sample[["home_grid", "work_grid", primary_weight]].rename(
        columns={
            "home_grid": "work_grid",
            "work_grid": "home_grid",
            primary_weight: "reverse_flow",
        }
    )
    sample = sample.merge(reverse, on=["home_grid", "work_grid"], how="left")
    sample["reverse_flow"] = sample["reverse_flow"].fillna(0.0)
    home_id = sample["home_grid"].astype(str)
    work_id = sample["work_grid"].astype(str)
    sample["pair_key"] = np.where(
        home_id < work_id,
        home_id + "|" + work_id,
        work_id + "|" + home_id,
    )
    sample = sample.loc[sample[primary_weight] >= sample["reverse_flow"]].copy()
    sample = sample.sort_values(primary_weight, ascending=False).drop_duplicates("pair_key")
    sample["two_way_flow"] = sample[primary_weight] + sample["reverse_flow"]
    sample["directional_share"] = np.where(
        sample["two_way_flow"] > 0,
        sample[primary_weight] / sample["two_way_flow"],
        np.nan,
    )
    sample["directional_ratio"] = np.where(
        sample["reverse_flow"] > 0,
        sample[primary_weight] / sample["reverse_flow"],
        np.inf,
    )
    network = sample.sort_values(primary_weight, ascending=False).head(top_n).reset_index(drop=True)
    network["rank"] = np.arange(1, len(network) + 1)

    positive = odm.loc[odm[primary_weight] > 0].copy()
    long = positive.loc[positive["centroid_distance_km"] >= min_distance_km].copy()
    stats = pd.DataFrame(
        [
            {
                "metric": "all_positive_flow",
                "value": float(positive[primary_weight].sum()),
            },
            {
                "metric": "long_distance_flow",
                "value": float(long[primary_weight].sum()),
            },
            {
                "metric": "long_distance_share",
                "value": float(long[primary_weight].sum() / positive[primary_weight].sum()) if positive[primary_weight].sum() > 0 else np.nan,
            },
            {
                "metric": "top_n",
                "value": float(len(network)),
            },
            {
                "metric": "top_n_flow",
                "value": float(network[primary_weight].sum()),
            },
            {
                "metric": "top_n_share_all",
                "value": float(network[primary_weight].sum() / positive[primary_weight].sum()) if positive[primary_weight].sum() > 0 else np.nan,
            },
            {
                "metric": "top_n_share_long",
                "value": float(network[primary_weight].sum() / long[primary_weight].sum()) if long[primary_weight].sum() > 0 else np.nan,
            },
            {
                "metric": "top20_share_all",
                "value": float(network.head(20)[primary_weight].sum() / positive[primary_weight].sum()) if positive[primary_weight].sum() > 0 else np.nan,
            },
            {
                "metric": "flow_weighted_mean_distance_km",
                "value": weighted_mean(positive["centroid_distance_km"], positive[primary_weight]),
            },
            {
                "metric": "flow_weighted_median_distance_km",
                "value": weighted_median(positive["centroid_distance_km"], positive[primary_weight]),
            },
            {
                "metric": "share_commute_ge_30km",
                "value": weighted_share(positive["centroid_distance_km"] >= 30, positive[primary_weight]),
            },
            {
                "metric": "share_commute_ge_50km",
                "value": weighted_share(positive["centroid_distance_km"] >= 50, positive[primary_weight]),
            },
            {
                "metric": "top_n_mean_directional_share",
                "value": weighted_mean(network["directional_share"], network[primary_weight]),
            },
        ]
    )
    return network, stats


def build_inbound_sector_summary(
    odm: pd.DataFrame,
    centers: pd.DataFrame,
    primary_weight: str,
    *,
    inner_km: float = INBOUND_WORK_INNER_KM,
    outer_km: float = INBOUND_HOME_OUTER_KM,
    n_sectors: int = INBOUND_SECTORS,
) -> pd.DataFrame:
    job_centers = centers.loc[centers["cluster_type"].eq("jobs")]
    center_x = float(job_centers["global_center_x_m"].iloc[0])
    center_y = float(job_centers["global_center_y_m"].iloc[0])
    bins = np.linspace(-np.pi, np.pi, n_sectors + 1)
    rows = []
    for idx in range(n_sectors):
        lo, hi = bins[idx], bins[idx + 1]
        theta_mid = (lo + hi) / 2
        inbound = odm.loc[
            (odm[primary_weight] > 0)
            & (odm["work_dist_to_job_center_km"] <= inner_km)
            & (odm["home_dist_to_job_center_km"] >= outer_km)
        ].copy()
        reverse = odm.loc[
            (odm[primary_weight] > 0)
            & (odm["home_dist_to_job_center_km"] <= inner_km)
            & (odm["work_dist_to_job_center_km"] >= outer_km)
        ].copy()
        inbound_angle = np.arctan2(inbound["home_y_m"] - center_y, inbound["home_x_m"] - center_x)
        reverse_angle = np.arctan2(reverse["work_y_m"] - center_y, reverse["work_x_m"] - center_x)
        inbound_mask = (inbound_angle >= lo) & (inbound_angle < hi)
        reverse_mask = (reverse_angle >= lo) & (reverse_angle < hi)
        inbound_sector = inbound.loc[inbound_mask]
        reverse_sector = reverse.loc[reverse_mask]
        in_flow = float(inbound_sector[primary_weight].sum())
        out_flow = float(reverse_sector[primary_weight].sum())
        rows.append(
            {
                "sector": idx + 1,
                "theta_lo_rad": lo,
                "theta_hi_rad": hi,
                "theta_mid_rad": theta_mid,
                "theta_mid_deg": (np.degrees(theta_mid) + 360.0) % 360.0,
                "inbound_flow": in_flow,
                "reverse_am_flow": out_flow,
                "inbound_minus_reverse": in_flow - out_flow,
                "inbound_share_of_two_way": in_flow / (in_flow + out_flow) if (in_flow + out_flow) > 0 else np.nan,
                "mean_inbound_distance_km": weighted_mean(inbound_sector["centroid_distance_km"], inbound_sector[primary_weight]),
                "center_x_m": center_x,
                "center_y_m": center_y,
                "inner_km": inner_km,
                "outer_km": outer_km,
            }
        )
    out = pd.DataFrame(rows)
    total_primary = float(odm[primary_weight].sum())
    out["inbound_share_total_commute"] = out["inbound_flow"] / total_primary if total_primary > 0 else np.nan
    out["inbound_reverse_ratio"] = np.where(
        out["reverse_am_flow"] > 0,
        out["inbound_flow"] / out["reverse_am_flow"],
        np.inf,
    )
    mean_inbound = float(out["inbound_flow"].mean()) if len(out) else np.nan
    out["inbound_over_sector_mean"] = out["inbound_flow"] / mean_inbound if mean_inbound > 0 else np.nan
    return out


def add_panel_label(ax: mpl.axes.Axes, label: str) -> None:
    ax.text(
        -0.03,
        1.03,
        label,
        transform=ax.transAxes,
        fontsize=11,
        fontweight="bold",
        va="bottom",
        ha="right",
    )


def plot_main_figure(
    grid_pop: gpd.GeoDataFrame,
    odm: pd.DataFrame,
    centers: pd.DataFrame,
    cluster_flow: pd.DataFrame,
    boundary: gpd.GeoDataFrame,
    out_stem: Path,
    primary_weight: str,
    top_flow_count: int,
) -> None:
    fig = plt.figure(figsize=(7.1, 6.5))
    gs = GridSpec(2, 2, figure=fig, width_ratios=[1.15, 0.85], height_ratios=[1.0, 1.0], wspace=0.26, hspace=0.28)
    ax_map = fig.add_subplot(gs[0, 0])
    ax_polar = fig.add_subplot(gs[0, 1], projection="polar")
    ax_hist = fig.add_subplot(gs[1, 0])
    ax_cluster = fig.add_subplot(gs[1, 1])

    plot_flow_map(ax_map, grid_pop, odm, boundary, primary_weight, top_flow_count)
    plot_direction_rose(ax_polar, odm, primary_weight)
    plot_inwardness_hist(ax_hist, odm, primary_weight)
    plot_cluster_map(ax_cluster, grid_pop, centers, cluster_flow, boundary)

    add_panel_label(ax_map, "a")
    ax_polar.text(-0.18, 1.08, "b", transform=ax_polar.transAxes, fontsize=11, fontweight="bold")
    add_panel_label(ax_hist, "c")
    add_panel_label(ax_cluster, "d")
    save_figure(fig, out_stem)


def plot_flow_map(
    ax: mpl.axes.Axes,
    grid_pop: gpd.GeoDataFrame,
    odm: pd.DataFrame,
    boundary: gpd.GeoDataFrame,
    primary_weight: str,
    top_flow_count: int,
) -> None:
    boundary.boundary.plot(ax=ax, color="#333333", linewidth=0.5)
    plot_grid = grid_pop.loc[(grid_pop["residents"] + grid_pop["jobs"]) > 0].copy()
    norm = TwoSlopeNorm(vmin=-1, vcenter=0, vmax=1)
    plot_grid.plot(
        ax=ax,
        column="job_resident_balance",
        cmap="RdBu_r",
        norm=norm,
        linewidth=0,
        alpha=0.78,
        missing_kwds={"color": "#f4f4f4", "alpha": 0.1},
    )
    flows = odm.loc[
        (odm[primary_weight] > 0)
        & (odm["centroid_distance_km"] >= ANGLE_MIN_DIST_KM)
    ].nlargest(top_flow_count, primary_weight)
    max_w = max(float(flows[primary_weight].max()), 1.0) if len(flows) else 1.0
    for row in flows.itertuples(index=False):
        weight = getattr(row, primary_weight)
        lw = 0.15 + 1.15 * np.sqrt(float(weight) / max_w)
        ax.annotate(
            "",
            xy=(row.work_x_m, row.work_y_m),
            xytext=(row.home_x_m, row.home_y_m),
            arrowprops={
                "arrowstyle": "-|>",
                "color": "#222222",
                "lw": lw,
                "alpha": 0.28,
                "mutation_scale": 5.0,
                "shrinkA": 0,
                "shrinkB": 0,
            },
        )
    ax.set_title("Home-to-work flows over job-residence balance")
    ax.set_axis_off()
    handles = [
        mpl.patches.Patch(color=plt.get_cmap("RdBu_r")(0.08), label="Resident-heavy grid"),
        mpl.patches.Patch(color=plt.get_cmap("RdBu_r")(0.92), label="Job-heavy grid"),
        mpl.lines.Line2D([0], [0], color="#222222", lw=1.2, alpha=0.5, label=f"Top {len(flows):,} OD flows"),
    ]
    ax.legend(handles=handles, loc="lower left", frameon=False, handlelength=1.2, borderpad=0.2, fontsize=6.8)


def plot_direction_rose(ax: mpl.axes.Axes, odm: pd.DataFrame, primary_weight: str) -> None:
    sample = odm.loc[
        (odm[primary_weight] > 0)
        & (odm["centroid_distance_km"] >= ANGLE_MIN_DIST_KM)
        & odm["angle_rad"].notna()
    ].copy()
    bins = np.linspace(-np.pi, np.pi, 25)
    hist, edges = np.histogram(sample["angle_rad"], bins=bins, weights=sample[primary_weight])
    share = hist / hist.sum() if hist.sum() > 0 else hist
    widths = np.diff(edges)
    centers = edges[:-1] + widths / 2
    ax.bar(centers, share, width=widths * 0.92, bottom=0, color="#4C78A8", alpha=0.72, edgecolor="white", linewidth=0.35)
    ax.set_theta_zero_location("E")
    ax.set_theta_direction(1)
    ax.set_xticks([0, np.pi / 2, np.pi, 3 * np.pi / 2])
    ax.set_xticklabels(["E", "N", "W", "S"])
    ax.set_yticklabels([])
    ax.grid(color="#d0d0d0", linewidth=0.5)
    ax.spines["polar"].set_color("#888888")
    ax.spines["polar"].set_linewidth(0.6)
    ax.set_title("Weighted direction angle", pad=12)
    max_share = float(np.max(share)) if len(share) else 0.0
    ax.text(0.5, -0.12, f"largest bin: {100 * max_share:.1f}%", ha="center", va="top", transform=ax.transAxes, fontsize=7.5)


def plot_inwardness_hist(ax: mpl.axes.Axes, odm: pd.DataFrame, primary_weight: str) -> None:
    sample = odm.loc[(odm[primary_weight] > 0) & odm["delta_dist_to_work_job_cluster_km"].notna()].copy()
    vals = sample["delta_dist_to_work_job_cluster_km"].clip(-80, 80)
    weights = sample[primary_weight]
    ax.hist(vals, bins=np.linspace(-60, 60, 49), weights=weights, density=True, color="#59A14F", alpha=0.74, edgecolor="white", linewidth=0.35)
    ax.axvline(0, color="#222222", lw=0.9, ls="--")
    share = weighted_share(sample["delta_dist_to_work_job_cluster_km"] < 0, weights)
    mean_delta = weighted_mean(sample["delta_dist_to_work_job_cluster_km"], weights)
    ax.text(
        0.02,
        0.95,
        f"closer to job cluster: {100 * share:.1f}%\nmean change: {mean_delta:.1f} km",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=7.5,
    )
    ax.set_title("Distance change toward destination job cluster")
    ax.set_xlabel("Work distance minus home distance (km)")
    ax.set_ylabel("Weighted density")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.22, linewidth=0.5)


def plot_cluster_map(
    ax: mpl.axes.Axes,
    grid_pop: gpd.GeoDataFrame,
    centers: pd.DataFrame,
    cluster_flow: pd.DataFrame,
    boundary: gpd.GeoDataFrame,
) -> None:
    boundary.boundary.plot(ax=ax, color="#333333", linewidth=0.5)
    grid_pop.loc[(grid_pop["residents"] + grid_pop["jobs"]) > 0].boundary.plot(ax=ax, color="#eeeeee", linewidth=0.08, alpha=0.35)
    res_centers = centers.loc[centers["cluster_type"].eq("residents")].copy()
    job_centers = centers.loc[centers["cluster_type"].eq("jobs")].copy()
    center_lookup = pd.concat(
        [
            res_centers[["cluster", "x_m", "y_m", "cluster_weight"]],
            job_centers[["cluster", "x_m", "y_m", "cluster_weight"]],
        ],
        ignore_index=True,
    ).set_index("cluster")
    top = cluster_flow.head(14).copy()
    max_flow = max(float(top["flow_weight"].max()), 1.0) if len(top) else 1.0
    for row in top.itertuples(index=False):
        if row.res_cluster not in center_lookup.index or row.job_cluster not in center_lookup.index:
            continue
        rx, ry = center_lookup.loc[row.res_cluster, ["x_m", "y_m"]]
        jx, jy = center_lookup.loc[row.job_cluster, ["x_m", "y_m"]]
        lw = 0.25 + 2.5 * np.sqrt(float(row.flow_weight) / max_flow)
        ax.annotate(
            "",
            xy=(jx, jy),
            xytext=(rx, ry),
            arrowprops={
                "arrowstyle": "-|>",
                "color": "#222222",
                "lw": lw,
                "alpha": 0.35,
                "mutation_scale": 7.0,
                "shrinkA": 5,
                "shrinkB": 5,
            },
        )
    size_scale = 260.0
    if len(res_centers):
        res_size = size_scale * np.sqrt(res_centers["cluster_weight"] / res_centers["cluster_weight"].max())
        ax.scatter(res_centers["x_m"], res_centers["y_m"], s=res_size, color="#4C78A8", alpha=0.88, edgecolor="white", linewidth=0.5)
    if len(job_centers):
        job_size = size_scale * np.sqrt(job_centers["cluster_weight"] / job_centers["cluster_weight"].max())
        ax.scatter(job_centers["x_m"], job_centers["y_m"], s=job_size, marker="s", color="#E15759", alpha=0.88, edgecolor="white", linewidth=0.5)
    for row in centers.itertuples(index=False):
        ax.text(row.x_m, row.y_m, row.cluster, ha="center", va="center", fontsize=6.5, color="white", fontweight="bold")
    ax.set_title("Residence clusters to job clusters")
    ax.set_axis_off()


def plot_origin_inwardness_map(
    grid_pop: gpd.GeoDataFrame,
    origin_summary: pd.DataFrame,
    boundary: gpd.GeoDataFrame,
    out_stem: Path,
    primary_weight: str,
) -> None:
    gdf = grid_pop.merge(origin_summary, left_on="grid_id", right_on="home_grid", how="left")
    fig, ax = plt.subplots(figsize=(5.2, 4.8))
    boundary.boundary.plot(ax=ax, color="#333333", linewidth=0.55)
    norm = TwoSlopeNorm(vmin=-25, vcenter=0, vmax=25)
    gdf.plot(
        ax=ax,
        column="mean_delta_to_work_job_cluster_km",
        cmap="RdBu",
        norm=norm,
        linewidth=0,
        alpha=0.82,
        missing_kwds={"color": "#f1f1f1", "alpha": 0.25},
    )
    sm = mpl.cm.ScalarMappable(cmap="RdBu", norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.036, pad=0.02)
    cbar.set_label("Mean work-home distance to job cluster (km)")
    ax.set_title(f"Origin-level inwardness ({WEIGHT_SPECS[primary_weight]})")
    ax.set_axis_off()
    save_figure(fig, out_stem)


def plot_cluster_flow_matrix(
    cluster_flow: pd.DataFrame,
    out_stem: Path,
) -> None:
    mat = cluster_flow.pivot(index="res_cluster", columns="job_cluster", values="flow_share").fillna(0.0)
    mat = mat.reindex(index=sorted(mat.index), columns=sorted(mat.columns))
    fig, ax = plt.subplots(figsize=(4.4, 3.9))
    values = mat.to_numpy() * 100.0
    cmap = plt.get_cmap("magma")
    im = ax.imshow(values, cmap=cmap, aspect="auto")
    ax.set_xticks(np.arange(mat.shape[1]), mat.columns)
    ax.set_yticks(np.arange(mat.shape[0]), mat.index)
    ax.set_xlabel("Job cluster")
    ax.set_ylabel("Residence cluster")
    ax.set_title("Cluster-to-cluster flow share")
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            val = mat.iloc[i, j] * 100.0
            if val >= 1.0:
                rgba = cmap((val - values.min()) / max(values.max() - values.min(), 1e-12))
                luminance = 0.299 * rgba[0] + 0.587 * rgba[1] + 0.114 * rgba[2]
                color = "black" if luminance > 0.55 else "white"
                ax.text(j, i, f"{val:.1f}", ha="center", va="center", fontsize=6.4, color=color)
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.03)
    cbar.set_label("Flow share (%)")
    ax.tick_params(axis="both", length=0)
    save_figure(fig, out_stem)


def plot_sleep_work_balance_story(
    grid_pop: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    out_stem: Path,
) -> None:
    res_extent = weighted_bounds(grid_pop, "residents")
    job_extent = weighted_bounds(grid_pop, "jobs")
    extent = (
        min(res_extent[0], job_extent[0]),
        max(res_extent[1], job_extent[1]),
        min(res_extent[2], job_extent[2]),
        max(res_extent[3], job_extent[3]),
    )
    fig, axes = plt.subplots(1, 2, figsize=(6.7, 3.35), constrained_layout=True)
    titles = ["Resident density", "Job density"]
    for ax, title in zip(axes, titles):
        plot_base_map(ax, boundary, roads, extent, road_alpha=0.18)
        ax.set_title(title, pad=3)

    draw_density_surface(axes[0], grid_pop, "residents", extent, "Blues", alpha=0.82)
    draw_density_surface(axes[1], grid_pop, "jobs", extent, "Reds", alpha=0.82)
    for label, ax in zip(["a", "b"], axes):
        add_panel_label(ax, label)
    save_figure(fig, out_stem)


def plot_story_corridor_network(
    grid_pop: gpd.GeoDataFrame,
    centers: pd.DataFrame,
    network: pd.DataFrame,
    network_stats: pd.DataFrame,
    boundary: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    out_stem: Path,
    primary_weight: str,
) -> None:
    res_extent = weighted_bounds(grid_pop, "residents", qlo=0.01, qhi=0.99)
    job_extent = weighted_bounds(grid_pop, "jobs", qlo=0.01, qhi=0.99)
    extent = (
        min(res_extent[0], job_extent[0]),
        max(res_extent[1], job_extent[1]),
        min(res_extent[2], job_extent[2]),
        max(res_extent[3], job_extent[3]),
    )
    fig = plt.figure(figsize=(7.4, 5.45), constrained_layout=True)
    gs = GridSpec(1, 2, figure=fig, width_ratios=[1.0, 0.27], wspace=0.015)
    ax = fig.add_subplot(gs[0, 0])
    ax_table = fig.add_subplot(gs[0, 1])
    plot_base_map(ax, boundary, roads, extent, road_alpha=0.14)
    draw_density_surface(ax, grid_pop, "jobs", extent, "Reds", alpha=0.22)

    res_centers = centers.loc[centers["cluster_type"].eq("residents")].copy()
    job_centers = centers.loc[centers["cluster_type"].eq("jobs")].copy()
    ax.scatter(res_centers["x_m"], res_centers["y_m"], s=44, color="#4C78A8", alpha=0.88, edgecolor="white", linewidth=0.45, zorder=20)
    ax.scatter(job_centers["x_m"], job_centers["y_m"], s=48, marker="s", color="#E15759", alpha=0.9, edgecolor="white", linewidth=0.45, zorder=21)

    cmap = plt.get_cmap("plasma")
    norm = mpl.colors.Normalize(vmin=0.5, vmax=1.0)
    max_flow = max(float(network[primary_weight].max()), 1.0) if len(network) else 1.0
    for row in network.sort_values(primary_weight, ascending=True).itertuples(index=False):
        flow = float(getattr(row, primary_weight))
        rank = int(row.rank)
        color = cmap(norm(float(row.directional_share)))
        alpha = 0.18 + 0.58 * np.sqrt(flow / max_flow)
        lw = 0.18 + 1.75 * np.sqrt(flow / max_flow)
        rad = 0.04 if rank % 2 == 0 else -0.04
        arrowstyle = "-|>" if rank <= 80 else "-"
        ax.annotate(
            "",
            xy=(row.work_x_m, row.work_y_m),
            xytext=(row.home_x_m, row.home_y_m),
            arrowprops={
                "arrowstyle": arrowstyle,
                "color": color,
                "lw": lw,
                "alpha": alpha,
                "mutation_scale": 6.5,
                "shrinkA": 0,
                "shrinkB": 0,
                "connectionstyle": f"arc3,rad={rad}",
            },
            zorder=8 + min(rank, 80) / 1000,
        )

    sm = mpl.cm.ScalarMappable(norm=norm, cmap=cmap)
    cbar = fig.colorbar(sm, ax=ax, fraction=0.030, pad=0.01)
    cbar.set_label("Main-direction share")
    cbar.ax.tick_params(labelsize=6.4, length=2)
    ax.set_title(f"Dominant OD network, top {len(network)} long pairs")

    ax_table.axis("off")
    stats = dict(zip(network_stats["metric"], network_stats["value"]))
    ax_table.text(0.0, 0.98, "Network summary", fontsize=7.2, fontweight="bold", ha="left", va="top")
    ax_table.text(
        0.0,
        0.90,
        (
            f"Lines drawn: {int(stats['top_n']):,}\n"
            f"Flow drawn: {stats['top_n_flow']/1000:.1f}k\n"
            f"Share of all: {100*stats['top_n_share_all']:.1f}%\n"
            f"Share of >=8 km: {100*stats['top_n_share_long']:.1f}%\n\n"
            f"Mean distance: {stats['flow_weighted_mean_distance_km']:.1f} km\n"
            f"Median distance: {stats['flow_weighted_median_distance_km']:.1f} km\n"
            f">=30 km share: {100*stats['share_commute_ge_30km']:.1f}%\n"
            f">=50 km share: {100*stats['share_commute_ge_50km']:.1f}%\n\n"
            f"Top 20 concentration:\n{100*stats['top20_share_all']:.2f}% of all flow"
        ),
        fontsize=6.9,
        ha="left",
        va="top",
        color="#222222",
        linespacing=1.35,
    )
    ax_table.text(
        0.0,
        0.24,
        "Selection",
        fontsize=6.9,
        fontweight="bold",
        ha="left",
        va="top",
    )
    ax_table.text(
        0.0,
        0.20,
        (
            f"OD distance >= {OD_NETWORK_MIN_DISTANCE_KM:.0f} km\n"
            "One line per unordered grid pair\n"
            "Arrow points along the stronger AM\n"
            "home-to-work direction\n\n"
            "Line width = commuters\n"
            "Color = stronger-direction share\n"
            "Blue dots = residence clusters\n"
            "Red squares = job clusters"
        ),
        fontsize=6.7,
        ha="left",
        va="top",
        color="#333333",
        linespacing=1.34,
    )
    save_figure(fig, out_stem)


def plot_inbound_sector_story(
    sector: pd.DataFrame,
    grid_pop: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    out_stem: Path,
) -> None:
    center_x = float(sector["center_x_m"].iloc[0])
    center_y = float(sector["center_y_m"].iloc[0])
    inner_m = float(sector["inner_km"].iloc[0]) * 1000.0
    outer_m = float(sector["outer_km"].iloc[0]) * 1000.0
    extent = (center_x - 2.25 * outer_m, center_x + 2.25 * outer_m, center_y - 2.0 * outer_m, center_y + 2.0 * outer_m)

    fig, ax = plt.subplots(figsize=(5.8, 5.4))
    plot_base_map(ax, boundary, roads, extent, road_alpha=0.2)
    draw_density_surface(ax, grid_pop, "jobs", extent, "Reds", alpha=0.35)

    flows = sector["inbound_flow"].to_numpy(float)
    max_flow = max(float(np.nanmax(flows)), 1.0) if len(flows) else 1.0
    ratios = pd.to_numeric(sector["inbound_reverse_ratio"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    finite_ratios = ratios[np.isfinite(ratios)]
    ratio_cap = max(3.0, float(finite_ratios.max())) if len(finite_ratios) else 3.0
    ratio_cap = min(max(ratio_cap, 3.0), 8.0)
    cmap = plt.get_cmap("magma")
    norm = mpl.colors.Normalize(vmin=0.0, vmax=ratio_cap)
    for row in sector.itertuples(index=False):
        theta1 = np.degrees(row.theta_lo_rad)
        theta2 = np.degrees(row.theta_hi_rad)
        value = float(row.inbound_flow)
        ratio_value = float(row.inbound_reverse_ratio)
        if not np.isfinite(ratio_value):
            ratio_value = ratio_cap
        color = cmap(norm(np.clip(ratio_value, 0.0, ratio_cap)))
        wedge = Wedge(
            (center_x, center_y),
            outer_m,
            theta1,
            theta2,
            width=outer_m - inner_m,
            facecolor=color,
            edgecolor="white",
            linewidth=0.45,
            alpha=0.74,
            zorder=6,
        )
        ax.add_patch(wedge)
        theta = row.theta_mid_rad
        start = (center_x + outer_m * np.cos(theta), center_y + outer_m * np.sin(theta))
        end = (center_x + inner_m * 0.35 * np.cos(theta), center_y + inner_m * 0.35 * np.sin(theta))
        lw = 0.5 + 3.3 * np.sqrt(value / max_flow)
        ax.annotate(
            "",
            xy=end,
            xytext=start,
            arrowprops={
                "arrowstyle": "-|>",
                "color": "#111111",
                "lw": lw,
                "alpha": 0.62,
                "mutation_scale": 8.5,
                "shrinkA": 0,
                "shrinkB": 0,
            },
            zorder=10,
        )

    ax.add_patch(Circle((center_x, center_y), inner_m, facecolor="none", edgecolor="#222222", linewidth=0.8, linestyle="--", zorder=11))
    ax.add_patch(Circle((center_x, center_y), outer_m, facecolor="none", edgecolor="#222222", linewidth=0.5, alpha=0.55, zorder=11))
    ax.scatter([center_x], [center_y], s=36, marker="s", color="#E15759", edgecolor="white", linewidth=0.6, zorder=14)

    total_inbound = float(sector["inbound_flow"].sum())
    total_reverse = float(sector["reverse_am_flow"].sum())
    total_ratio = total_inbound / total_reverse if total_reverse > 0 else np.inf
    peak_ratio = float(finite_ratios.max()) if len(finite_ratios) else np.inf
    peak_over_mean = float(sector["inbound_over_sector_mean"].max())
    top = sector.sort_values("inbound_flow", ascending=False).head(3)
    ax.text(
        0.02,
        0.98,
        (
            f"Outer homes -> core jobs\n"
            f"inbound: {total_inbound/1000:.1f}k commuters\n"
            f"reverse AM: {total_reverse/1000:.1f}k\n"
            f"inbound/reverse: {total_ratio:.1f}x\n"
            f"peak sector: {peak_ratio:.1f}x reverse, {peak_over_mean:.1f}x mean\n"
            f"core <= {INBOUND_WORK_INNER_KM:.0f} km; home >= {INBOUND_HOME_OUTER_KM:.0f} km"
        ),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=7.0,
        bbox={"boxstyle": "round,pad=0.22", "fc": "white", "ec": "none", "alpha": 0.85},
        zorder=30,
    )
    for idx, row in enumerate(top.itertuples(index=False), start=1):
        theta = row.theta_mid_rad
        label_xy = (center_x + outer_m * 1.13 * np.cos(theta), center_y + outer_m * 1.13 * np.sin(theta))
        ax.text(
            label_xy[0],
            label_xy[1],
            f"{idx}: {row.inbound_flow/1000:.1f}k",
            fontsize=6.4,
            ha="center",
            va="center",
            color="#222222",
            bbox={"boxstyle": "round,pad=0.12", "fc": "white", "ec": "none", "alpha": 0.82},
            zorder=31,
        )
    sm = mpl.cm.ScalarMappable(norm=norm, cmap=cmap)
    cbar = fig.colorbar(sm, ax=ax, fraction=0.034, pad=0.012)
    cbar.set_label("Inbound / reverse AM flow")
    cbar.ax.tick_params(labelsize=6.4, length=2)
    ax.set_title("AM inbound demand toward the employment core")
    save_figure(fig, out_stem)


def write_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float), encoding="utf-8")


def main() -> None:
    args = parse_args()
    out_root = ROOT / args.output_dir / args.version_id
    dirs = ensure_dirs(out_root)
    configure_matplotlib(out_root.parent)

    grid_wgs, grid_pop, od = load_inputs(args.source_version)
    odm, grid_pop, centers = add_spatial_metrics(od, grid_pop, args.cluster_k)
    matched_distance = load_point_distance_summary(args.source_version)

    summary = build_summary_metrics(odm, matched_distance)
    origin_summary = build_origin_summary(odm, args.primary_weight)
    cluster_flow = build_cluster_flow(odm, args.primary_weight)
    story_corridors = build_story_cluster_corridors(odm, centers, args.primary_weight)
    od_network, od_network_stats = build_dominant_od_network(odm, args.primary_weight)
    inbound_sector = build_inbound_sector_summary(odm, centers, args.primary_weight)
    boundary = prep_boundary()
    roads = load_major_roads()

    summary.to_csv(dirs["metrics"] / "commuting_direction_summary.csv", index=False)
    origin_summary.to_csv(dirs["metrics"] / "origin_direction_summary.csv", index=False)
    centers.to_csv(dirs["metrics"] / "residence_job_cluster_centers.csv", index=False)
    cluster_flow.to_csv(dirs["metrics"] / "cluster_to_cluster_flow.csv", index=False)
    story_corridors.to_csv(dirs["metrics"] / "story_top_cluster_corridors.csv", index=False)
    od_network.to_csv(dirs["metrics"] / "dominant_od_network_top_flows.csv", index=False)
    od_network_stats.to_csv(dirs["metrics"] / "dominant_od_network_summary.csv", index=False)
    inbound_sector.to_csv(dirs["metrics"] / "inbound_sector_summary.csv", index=False)
    odm.to_parquet(dirs["data"] / "od_direction_metrics_square.parquet", index=False)
    grid_pop.drop(columns="geometry").to_csv(dirs["data"] / "grid_population_clusters_square.csv", index=False)
    grid_wgs.to_file(dirs["data"] / "grid_square_source.geojson", driver="GeoJSON")

    plot_main_figure(
        grid_pop=grid_pop,
        odm=odm,
        centers=centers,
        cluster_flow=cluster_flow,
        boundary=boundary,
        out_stem=dirs["figures"] / "figure_beijing_commuting_direction_evidence",
        primary_weight=args.primary_weight,
        top_flow_count=args.top_flow_count,
    )
    plot_origin_inwardness_map(
        grid_pop,
        origin_summary,
        boundary,
        dirs["figures"] / "figure_beijing_origin_inwardness_map",
        args.primary_weight,
    )
    plot_cluster_flow_matrix(
        cluster_flow,
        dirs["figures"] / "figure_beijing_cluster_flow_matrix",
    )
    plot_sleep_work_balance_story(
        grid_pop,
        boundary,
        roads,
        dirs["figures"] / "figure_beijing_story_sleep_work_balance",
    )
    plot_story_corridor_network(
        grid_pop,
        centers,
        od_network,
        od_network_stats,
        boundary,
        roads,
        dirs["figures"] / "figure_beijing_story_top_corridors",
        args.primary_weight,
    )
    plot_inbound_sector_story(
        inbound_sector,
        grid_pop,
        boundary,
        roads,
        dirs["figures"] / "figure_beijing_story_inbound_sector",
    )

    manifest = {
        "version_id": args.version_id,
        "source_version": args.source_version,
        "primary_weight": args.primary_weight,
        "primary_weight_label": WEIGHT_SPECS[args.primary_weight],
        "cluster_k": args.cluster_k,
        "angle_min_distance_km": ANGLE_MIN_DIST_KM,
        "target_crs": TARGET_CRS,
        "inputs": {
            "od": str(ROOT / "outputs" / args.source_version / "data" / "OD_square.csv"),
            "grid": str(ROOT / "outputs" / args.source_version / "data" / "grid_square_master.parquet"),
            "population": str(ROOT / "outputs" / args.source_version / "data" / "grid_population_summary_square.csv"),
            "matched_commute": str(ROOT / "outputs" / args.source_version / "data" / "commute_square_matched.csv"),
            "boundary": str(ROOT / "raw_data" / "gis" / "map" / "北京市边界.shp"),
            "roads": str(ROOT / "raw_data" / "gis" / "roads_baidu" / "beijing_roads.shp"),
        },
        "outputs": {
            "summary": str(dirs["metrics"] / "commuting_direction_summary.csv"),
            "dominant_od_network": str(dirs["metrics"] / "dominant_od_network_top_flows.csv"),
            "dominant_od_network_summary": str(dirs["metrics"] / "dominant_od_network_summary.csv"),
            "main_figure": str(dirs["figures"] / "figure_beijing_commuting_direction_evidence.png"),
            "sleep_work_balance": str(dirs["figures"] / "figure_beijing_story_sleep_work_balance.png"),
            "top_corridors": str(dirs["figures"] / "figure_beijing_story_top_corridors.png"),
            "inbound_sector": str(dirs["figures"] / "figure_beijing_story_inbound_sector.png"),
        },
        "notes": [
            "Direction angles exclude OD pairs below 1 km when computing rose-plot concentration.",
            f"Dominant OD network map keeps actual grid OD pairs at least {OD_NETWORK_MIN_DISTANCE_KM:.0f} km, retains the stronger direction within each unordered pair, and plots the top {OD_NETWORK_TOP_N} flows.",
            f"The legacy cluster-corridor table keeps OD pairs at least {STORY_MIN_DISTANCE_KM:.0f} km and moving closer to the destination job cluster, then reports the top {STORY_TOP_CORRIDORS} cluster-to-cluster flows.",
            f"Inbound sector map keeps OD with work grid within {INBOUND_WORK_INNER_KM:.0f} km of the job-weighted city center and home grid at least {INBOUND_HOME_OUTER_KM:.0f} km away.",
            "The main sample is full stage07 Beijing square-grid OD, not the model LCC/gate5 sample.",
            "PM peak demand is interpreted as the reverse of the same observed home-to-work OD structure.",
        ],
    }
    write_json(dirs["metrics"] / "run_manifest.json", manifest)


if __name__ == "__main__":
    main()
