"""Compare geometric and arithmetic symmetric-input re-estimations.

The comparison keeps the directed topology, observed OD mass, parameters, and
solver fixed. Reciprocal AM and free-flow link times are symmetrized in physical
minutes before iceberg costs, routes, fundamentals, and equilibrium are rebuilt.
"""
from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra


DATA_WORK = Path(__file__).resolve().parents[3]
OUTPUT = DATA_WORK / "outputs" / "symmetric_input_mean_method_comparison_20260716_v1"
DIRECTED_INPUT = "model_input_edgeproj_v2_lcc_gate5_v1"
THETA = 6.83
EPS = 1e-12

METHODS = {
    "geometric": {
        "suite": "symmetric_input_reestimated_gate5_bpr_pos05_v1",
        "input": "model_input_edgeproj_v2_lcc_gate5_symmetric_geometric_v1",
    },
    "arithmetic": {
        "suite": "symmetric_input_reestimated_gate5_bpr_pos05_arithmetic_v1",
        "input": "model_input_edgeproj_v2_lcc_gate5_symmetric_arithmetic_v1",
    },
}


def weighted_quantile(values: np.ndarray, weights: np.ndarray, p: float) -> float:
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    cdf = np.cumsum(weights) / np.sum(weights)
    return float(values[np.searchsorted(cdf, p, side="left")])


def weighted_gradient(distance_km: np.ndarray, values: np.ndarray, weights: np.ndarray) -> tuple[float, float]:
    X = np.column_stack([np.ones(len(values)), distance_km])
    sw = np.sqrt(weights / np.mean(weights))
    coef = np.linalg.lstsq(X * sw[:, None], values * sw, rcond=None)[0]
    fitted = X @ coef
    mean = np.average(values, weights=weights)
    sse = np.sum(weights * (values - fitted) ** 2)
    sst = np.sum(weights * (values - mean) ** 2)
    return float(10.0 * coef[1]), float(1.0 - sse / max(sst, EPS))


def load_geography() -> tuple[gpd.GeoDataFrame, float, float]:
    grid = gpd.read_file(DATA_WORK / "raw_data" / "gis" / "map" / "grid_square_3km.shp").to_crs(32650)
    district = gpd.read_file(DATA_WORK / "raw_data" / "gis" / "map" / "北京市_区县边界.shp").to_crs(32650)
    grid["geometry"] = grid.geometry.centroid
    points = gpd.sjoin(
        grid[["grid_id", "geometry"]],
        district[["NAME", "geometry"]],
        predicate="within",
        how="left",
    ).drop(columns=["index_right"])
    missing = points["NAME"].isna()
    if missing.any():
        nearest = gpd.sjoin_nearest(
            points.loc[missing, ["grid_id", "geometry"]],
            district[["NAME", "geometry"]],
            how="left",
        ).drop_duplicates("grid_id")
        points.loc[missing, "NAME"] = points.loc[missing, "grid_id"].map(
            nearest.set_index("grid_id")["NAME"]
        )

    nodes = pd.read_parquet(
        DATA_WORK / "outputs" / DIRECTED_INPUT / "data" / "qsm_input_nodes_square.parquet"
    )
    geo = nodes[["grid_id", "node_i", "residents", "jobs"]].merge(
        points[["grid_id", "NAME", "geometry"]], on="grid_id", validate="one_to_one"
    )
    geo = gpd.GeoDataFrame(geo, geometry="geometry", crs=32650).sort_values("node_i")
    center_x = float(np.average(geo.geometry.x, weights=geo["jobs"]))
    center_y = float(np.average(geo.geometry.y, weights=geo["jobs"]))
    geo["distance_to_employment_center_km"] = np.hypot(
        geo.geometry.x - center_x, geo.geometry.y - center_y
    ) / 1000.0
    return geo, center_x, center_y


def physical_time_rows(method: str, suite: str) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    root = DATA_WORK / "outputs" / suite
    directed = pd.read_csv(root / "baseline_distribution" / "directed_main" / "edges.csv")
    symmetric = pd.read_csv(root / "baseline_distribution" / "symmetric_reestimated" / "edges.csv")
    od = pd.read_parquet(
        DATA_WORK / "outputs" / DIRECTED_INPUT / "data" / "qsm_input_od_square.parquet"
    )
    commuter_mass = float(od["commuters_road"].sum())

    base_minutes = float(np.dot(directed["Xi_kl"], directed["tau_min_equilibrium"]) / commuter_mass)
    fixed_passage_minutes = float(
        np.dot(directed["Xi_kl"], symmetric["tau_min_equilibrium"]) / commuter_mass
    )
    symmetric_minutes = float(
        np.dot(symmetric["Xi_kl"], symmetric["tau_min_equilibrium"]) / commuter_mass
    )
    comparison = json.loads((root / "baseline_comparison.json").read_text())
    summary = {
        "method": method,
        "commuter_mass": commuter_mass,
        "directed_mean_physical_minutes": base_minutes,
        "symmetric_mean_physical_minutes": symmetric_minutes,
        "total_physical_time_bias_seconds": 60.0 * (symmetric_minutes - base_minutes),
        "fixed_passage_time_bias_seconds": 60.0 * (fixed_passage_minutes - base_minutes),
        "passage_reallocation_seconds": 60.0 * (symmetric_minutes - fixed_passage_minutes),
        "directed_unweighted_mean_link_input_minutes": float(directed["tau_obs_input"].mean()),
        "symmetric_unweighted_mean_link_input_minutes": float(symmetric["tau_obs_input"].mean()),
        "welfare_pct_change": comparison["equilibrium_Wbar_pct_change"],
        "residence_distribution_dissimilarity_pct": comparison["equilibrium_residence_dissimilarity_pct"],
        "workplace_distribution_dissimilarity_pct": comparison["equilibrium_workplace_dissimilarity_pct"],
        "od_dissimilarity_pct": comparison["equilibrium_od_dissimilarity_pct"],
        "bilateral_composite_cost_pct_change": comparison["equilibrium_od_weighted_bilateral_cost_pct_change"],
    }
    return summary, directed, symmetric


def exposure_covariance(directed_eq: pd.DataFrame, commuter_mass: float) -> dict:
    edges = pd.read_parquet(
        DATA_WORK / "outputs" / DIRECTED_INPUT / "data" / "qsm_input_edges_square.parquet"
    ).reset_index(names="edge_idx")
    if not (
        np.array_equal(edges["i"].to_numpy(), directed_eq["edge_i"].to_numpy())
        and np.array_equal(edges["j"].to_numpy(), directed_eq["edge_j"].to_numpy())
    ):
        raise ValueError("directed edge order does not match equilibrium edge order")
    edges["Xi_kl"] = directed_eq["Xi_kl"].to_numpy()
    reverse = {(str(row.grid_o), str(row.grid_d)): idx for idx, row in edges.iterrows()}
    seen: set[tuple[str, str]] = set()
    rows = []
    for _, row in edges.iterrows():
        key = tuple(sorted((str(row.grid_o), str(row.grid_d))))
        if key in seen:
            continue
        seen.add(key)
        rev_idx = reverse.get((str(row.grid_d), str(row.grid_o)))
        if rev_idx is None:
            continue
        rev = edges.loc[rev_idx]
        slow, fast = (row, rev) if row.tau_obs_min >= rev.tau_obs_min else (rev, row)
        rows.append(
            {
                "slow_grid_o": slow.grid_o,
                "slow_grid_d": slow.grid_d,
                "slow_time_minutes": slow.tau_obs_min,
                "fast_time_minutes": fast.tau_obs_min,
                "directional_gap_minutes": slow.tau_obs_min - fast.tau_obs_min,
                "slow_passages": slow.Xi_kl,
                "fast_passages": fast.Xi_kl,
                "pair_passages": slow.Xi_kl + fast.Xi_kl,
                "arithmetic_fixed_passage_person_minutes": (
                    -0.5 * (slow.Xi_kl - fast.Xi_kl) * (slow.tau_obs_min - fast.tau_obs_min)
                ),
            }
        )
    pair = pd.DataFrame(rows)
    total_passages = float(pair["pair_passages"].sum())
    contribution = pair["arithmetic_fixed_passage_person_minutes"]
    ranked = pair.assign(abs_contribution=contribution.abs()).sort_values("abs_contribution", ascending=False)
    total_contribution = float(contribution.sum())
    return {
        "n_reciprocal_pairs": int(len(pair)),
        "share_passages_on_slower_direction": float(pair["slow_passages"].sum() / total_passages),
        "slow_to_fast_passage_ratio": float(pair["slow_passages"].sum() / pair["fast_passages"].sum()),
        "passage_weighted_share_pairs_slow_direction_busier": float(
            np.average(pair["slow_passages"] > pair["fast_passages"], weights=pair["pair_passages"])
        ),
        "arithmetic_fixed_passage_bias_seconds": float(60.0 * total_contribution / commuter_mass),
        "top_10_signed_contribution_share": float(ranked.head(10)["arithmetic_fixed_passage_person_minutes"].sum() / total_contribution),
        "top_50_signed_contribution_share": float(ranked.head(50)["arithmetic_fixed_passage_person_minutes"].sum() / total_contribution),
        "top_100_signed_contribution_share": float(ranked.head(100)["arithmetic_fixed_passage_person_minutes"].sum() / total_contribution),
    }, pair.sort_values("arithmetic_fixed_passage_person_minutes")


def fundamental_rows(method: str, suite: str, geo: gpd.GeoDataFrame) -> tuple[list[dict], pd.DataFrame]:
    root = DATA_WORK / "outputs" / suite / "baseline_distribution"
    directed = pd.read_csv(root / "directed_main" / "nodes.csv")
    symmetric = pd.read_csv(root / "symmetric_reestimated" / "nodes.csv")
    nodes = directed.merge(
        symmetric, on=["node_idx", "grid_id"], suffixes=("_directed", "_symmetric"), validate="one_to_one"
    ).merge(
        geo[["grid_id", "NAME", "residents", "jobs", "distance_to_employment_center_km"]],
        on="grid_id",
        validate="one_to_one",
    )
    nodes["delta_log_residence_fundamental"] = (
        np.log(np.clip(nodes["ubar_theta_symmetric"], EPS, None))
        - np.log(np.clip(nodes["ubar_theta_directed"], EPS, None))
    ) / THETA
    nodes["delta_log_workplace_fundamental"] = (
        np.log(np.clip(nodes["abar_theta_symmetric"], EPS, None))
        - np.log(np.clip(nodes["abar_theta_directed"], EPS, None))
    ) / THETA

    rows = []
    for metric, weight_col in (
        ("delta_log_residence_fundamental", "residents"),
        ("delta_log_workplace_fundamental", "jobs"),
    ):
        values = nodes[metric].to_numpy()
        weights = nodes[weight_col].to_numpy()
        slope, r2 = weighted_gradient(
            nodes["distance_to_employment_center_km"].to_numpy(), values, weights
        )
        rows.append(
            {
                "method": method,
                "metric": metric,
                "weight": weight_col,
                "weighted_mean": float(np.average(values, weights=weights)),
                "weighted_l2": float(np.sqrt(np.average(values**2, weights=weights))),
                "weighted_p10": weighted_quantile(values, weights, 0.10),
                "weighted_p90": weighted_quantile(values, weights, 0.90),
                "weighted_share_abs_ge_005": float(weights[np.abs(values) >= 0.05].sum() / weights.sum()),
                "weighted_share_abs_ge_010": float(weights[np.abs(values) >= 0.10].sum() / weights.sum()),
                "slope_per_10km_actual_geometry": slope,
                "gradient_r2": r2,
            }
        )
    return rows, nodes


def district_rows(method: str, nodes: pd.DataFrame) -> list[dict]:
    rows = []
    for district, group in nodes.groupby("NAME"):
        for metric, weight_col in (
            ("delta_log_residence_fundamental", "residents"),
            ("delta_log_workplace_fundamental", "jobs"),
        ):
            weights = group[weight_col].to_numpy()
            rows.append(
                {
                    "method": method,
                    "district": district,
                    "metric": metric,
                    "weighted_mean": float(np.average(group[metric], weights=weights)),
                    "district_weight_share": float(weights.sum() / nodes[weight_col].sum()),
                    "weighted_mean_absolute": float(np.average(np.abs(group[metric]), weights=weights)),
                }
            )
    return rows


def od_spatial_rows(method: str, suite: str, geo: gpd.GeoDataFrame) -> list[dict]:
    ordered = geo.sort_values("node_i")
    x = ordered.geometry.x.to_numpy()
    y = ordered.geometry.y.to_numpy()
    distance = np.hypot(x[:, None] - x[None, :], y[:, None] - y[None, :]) / 1000.0
    radial = ordered["distance_to_employment_center_km"].to_numpy()
    radial_gap = np.abs(radial[:, None] - radial[None, :])
    names = ordered["NAME"].fillna("missing").to_numpy()
    cross_district = names[:, None] != names[None, :]

    rows = []
    root = DATA_WORK / "outputs" / suite / "baseline_distribution"
    for world in ("directed_main", "symmetric_reestimated"):
        shares = np.load(root / world / "od_shares.npz")["od_shares"]
        shares = shares / shares.sum()
        rows.append(
            {
                "method": method,
                "world": world,
                "mean_euclidean_home_work_distance_km": float(np.sum(shares * distance)),
                "mean_absolute_radial_gap_km": float(np.sum(shares * radial_gap)),
                "share_od_distance_ge_20km": float(shares[distance >= 20.0].sum()),
                "share_cross_district": float(shares[cross_district].sum()),
            }
        )
    return rows


def shortest_path_matrix(input_version: str, n_nodes: int) -> np.ndarray:
    edges = pd.read_parquet(
        DATA_WORK / "outputs" / input_version / "data" / "qsm_input_edges_square.parquet"
    )
    edges = edges.groupby(["i", "j"], as_index=False)["tau_obs_min"].min()
    graph = csr_matrix(
        (edges["tau_obs_min"], (edges["i"], edges["j"])),
        shape=(n_nodes, n_nodes),
    )
    return dijkstra(graph, directed=True)


def observed_od_shortest_path_rows() -> list[dict]:
    nodes = pd.read_parquet(
        DATA_WORK / "outputs" / DIRECTED_INPUT / "data" / "qsm_input_nodes_square.parquet"
    )
    od = pd.read_parquet(
        DATA_WORK / "outputs" / DIRECTED_INPUT / "data" / "qsm_input_od_square.parquet"
    )
    positive = od["commuters_road"].to_numpy() > 0
    origin = od.loc[positive, "origin_node"].to_numpy(dtype=int)
    destination = od.loc[positive, "destination_node"].to_numpy(dtype=int)
    weights = od.loc[positive, "commuters_road"].to_numpy(dtype=float)
    directed_matrix = shortest_path_matrix(DIRECTED_INPUT, len(nodes))
    directed_values = directed_matrix[origin, destination]

    rows = []
    for method, spec in METHODS.items():
        symmetric_matrix = shortest_path_matrix(spec["input"], len(nodes))
        symmetric_values = symmetric_matrix[origin, destination]
        finite = np.isfinite(directed_values) & np.isfinite(symmetric_values)
        w = weights[finite]
        base = directed_values[finite]
        symmetric = symmetric_values[finite]
        delta = symmetric - base
        rows.append(
            {
                "method": method,
                "reachable_observed_road_commuter_share": float(w.sum() / weights.sum()),
                "directed_observed_od_shortest_path_minutes": float(np.average(base, weights=w)),
                "symmetric_observed_od_shortest_path_minutes": float(np.average(symmetric, weights=w)),
                "difference_seconds": float(60.0 * np.average(delta, weights=w)),
                "difference_pct": float(100.0 * (np.average(symmetric, weights=w) / np.average(base, weights=w) - 1.0)),
                "weighted_delta_p10_minutes": weighted_quantile(delta, w, 0.10),
                "weighted_delta_p50_minutes": weighted_quantile(delta, w, 0.50),
                "weighted_delta_p90_minutes": weighted_quantile(delta, w, 0.90),
                "observed_flow_share_faster_under_symmetry": float(w[delta < 0].sum() / w.sum()),
                "observed_flow_share_slower_under_symmetry": float(w[delta > 0].sum() / w.sum()),
            }
        )
    return rows


def main() -> None:
    metrics = OUTPUT / "metrics"
    metrics.mkdir(parents=True, exist_ok=True)
    geo, center_x, center_y = load_geography()

    time_rows = []
    fundamental_summary = []
    districts = []
    od_spatial = []
    directed_eq = None
    for method, spec in METHODS.items():
        time_row, directed, _ = physical_time_rows(method, spec["suite"])
        time_rows.append(time_row)
        if directed_eq is None:
            directed_eq = directed
        summary_rows, nodes = fundamental_rows(method, spec["suite"], geo)
        fundamental_summary.extend(summary_rows)
        districts.extend(district_rows(method, nodes))
        od_spatial.extend(od_spatial_rows(method, spec["suite"], geo))
        nodes.to_csv(metrics / f"node_fundamental_deltas_{method}.csv", index=False)

    pd.DataFrame(time_rows).to_csv(metrics / "mean_method_baseline_summary.csv", index=False)
    pd.DataFrame(fundamental_summary).to_csv(metrics / "fundamental_summary.csv", index=False)
    pd.DataFrame(districts).to_csv(metrics / "fundamental_district_summary.csv", index=False)
    pd.DataFrame(od_spatial).to_csv(metrics / "od_spatial_summary.csv", index=False)
    pd.DataFrame(observed_od_shortest_path_rows()).to_csv(
        metrics / "observed_od_shortest_path_summary.csv", index=False
    )
    exposure, pairs = exposure_covariance(directed_eq, time_rows[0]["commuter_mass"])
    pairs.to_csv(metrics / "reciprocal_pair_exposure_covariance.csv", index=False)
    (metrics / "reciprocal_pair_exposure_summary.json").write_text(json.dumps(exposure, indent=2))
    (OUTPUT / "run_metadata.json").write_text(
        json.dumps(
            {
                "directed_input": DIRECTED_INPUT,
                "methods": METHODS,
                "theta": THETA,
                "symmetrization_layer": "reciprocal physical AM and free-flow link times in minutes",
                "arithmetic_formula": "0.5 * (m_kl + m_lk)",
                "geometric_formula": "sqrt(m_kl * m_lk)",
                "employment_center_epsg32650": [center_x, center_y],
                "distance_note": "Gradients use projected grid-polygon centroids, not the legacy 3 km grid-index proxy.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
