"""Candidate metro-line experiment under directed and symmetric inputs.

The experiment interprets a targeted bilateral commuting-cost reduction as a
reduced-form metro investment.  Each candidate connects two endpoint
catchments and lowers generalized bilateral commuting costs between the two
catchments in both directions.  The treatment is held fixed across the fully
re-estimated directed and symmetric calibration worlds.

The runner reports policy-evaluation differences for each common candidate,
candidate-ranking overlap, and decision regret when the symmetric model is
used to select a line but welfare is evaluated in the directed benchmark.
It does not use tidal lanes, road-lane selection, or link-level reallocation.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
from scipy.stats import spearmanr


DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    employment_centroid,
    grid_xy,
)
from src.model.full_estimation.run_symmetric_input_place_cost_suite_gate5 import (  # noqa: E402
    OdTreatment,
    _bilateral_asymmetry_gap,
    run_od_treatment,
)
from src.model.full_estimation.run_symmetric_input_reestimated_suite_gate5 import (  # noqa: E402
    DEFAULT_DIRECTED_VERSION,
    DEFAULT_SYMMETRIC_VERSION,
    _build_world,
    _load_params,
    build_symmetric_cost_bundle,
)


LOG = logging.getLogger("symmetric_input_metro_candidates")
EPS = 1e-12
GRID_KM = 3.0
DEFAULT_OUTPUT = "symmetric_input_metro_candidates_20260716_v1"

DIRECTED_COLOR = "#4C78A8"
SYMMETRIC_COLOR = "#B279A2"
COMMON_COLOR = "#5B4B8A"
RESIDENCE_COLOR = "#4C78A8"
WORKPLACE_COLOR = "#E15759"
NEUTRAL_COLOR = "#BAB0AC"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    node_a: int
    node_b: int
    source_families: str
    primary_family: str
    combined_observed_flow: float
    bilateral_asymmetry_gap: float
    line_length_km: float
    radius_a_km: float
    radius_b_km: float
    angular_separation_deg: float


def _setup_style() -> None:
    mpl.rcParams.update(
        {
            "figure.dpi": 180,
            "savefig.dpi": 600,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
            "axes.labelsize": 9.0,
            "axes.titlesize": 9.5,
            "xtick.labelsize": 8.0,
            "ytick.labelsize": 8.0,
            "legend.fontsize": 8.0,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "xtick.major.size": 3.0,
            "ytick.major.size": 3.0,
            "xtick.direction": "out",
            "ytick.direction": "out",
            "legend.frameon": False,
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def _axis_style(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for spine in ax.spines.values():
        spine.set_linewidth(0.8)
        spine.set_color("black")
    ax.tick_params(direction="out", width=0.8, length=3.0, colors="black")


def _save_figure(fig: plt.Figure, base: Path) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    for suffix in (".svg", ".pdf", ".png"):
        kwargs = {"bbox_inches": "tight"}
        if suffix == ".png":
            kwargs["dpi"] = 600
        fig.savefig(base.with_suffix(suffix), **kwargs)
    plt.close(fig)


def _angle_difference_deg(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    diff = np.abs(a - b)
    return np.degrees(np.minimum(diff, 2.0 * np.pi - diff))


def _pair_universe(directed) -> pd.DataFrame:
    prim = directed.prim
    x, y = grid_xy(prim)
    cx, cy = employment_centroid(prim)
    dx = x - cx
    dy = y - cy
    radius = np.hypot(dx, dy) * GRID_KM
    angle = np.mod(np.arctan2(dy, dx), 2.0 * np.pi)

    upper_i, upper_j = np.triu_indices(prim.n_nodes, k=1)
    flow = np.asarray(prim.L_ij_obs, dtype=float)
    combined_flow = flow[upper_i, upper_j] + flow[upper_j, upper_i]
    asym = _bilateral_asymmetry_gap(directed.baseline, directed.params.theta)
    asym_gap = asym[upper_i, upper_j]
    length = np.hypot(x[upper_i] - x[upper_j], y[upper_i] - y[upper_j]) * GRID_KM
    angular_sep = _angle_difference_deg(angle[upper_i], angle[upper_j])

    out = pd.DataFrame(
        {
            "node_a": upper_i,
            "node_b": upper_j,
            "combined_observed_flow": combined_flow,
            "bilateral_asymmetry_gap": asym_gap,
            "line_length_km": length,
            "radius_a_km": radius[upper_i],
            "radius_b_km": radius[upper_j],
            "angular_separation_deg": angular_sep,
        }
    )
    out["radius_inner_km"] = out[["radius_a_km", "radius_b_km"]].min(axis=1)
    out["radius_outer_km"] = out[["radius_a_km", "radius_b_km"]].max(axis=1)
    out["radius_gap_km"] = out["radius_outer_km"] - out["radius_inner_km"]
    return out[
        (out["combined_observed_flow"] > EPS)
        & out["bilateral_asymmetry_gap"].notna()
        & out["line_length_km"].between(9.0, 70.0)
    ].copy()


def _top_family(
    pairs: pd.DataFrame,
    *,
    family: str,
    valid: pd.Series,
    score: pd.Series,
    n: int,
) -> pd.DataFrame:
    d = pairs.loc[valid].copy()
    d["selection_score"] = np.asarray(score.loc[valid], dtype=float)
    d = d[np.isfinite(d["selection_score"])].nlargest(n, "selection_score")
    d["source_family"] = family
    return d


def build_candidates(directed, *, n_per_family: int) -> tuple[list[Candidate], pd.DataFrame]:
    pairs = _pair_universe(directed)

    radial_valid = (
        (pairs["radius_inner_km"] <= 30.0)
        & (pairs["radius_outer_km"] >= 35.0)
        & (pairs["angular_separation_deg"] <= 35.0)
        & pairs["line_length_km"].between(12.0, 70.0)
    )
    radial_score = pairs["combined_observed_flow"] * np.clip(pairs["radius_gap_km"], 1.0, None)

    orbital_valid = (
        (pairs["radius_inner_km"] >= 25.0)
        & (pairs["radius_gap_km"] <= 12.0)
        & pairs["angular_separation_deg"].between(20.0, 110.0)
        & pairs["line_length_km"].between(12.0, 70.0)
    )
    orbital_score = (
        pairs["combined_observed_flow"]
        * pairs["line_length_km"]
        / (1.0 + pairs["radius_gap_km"])
    )

    high_flow_valid = pairs["line_length_km"].between(9.0, 60.0)
    high_flow_score = pairs["combined_observed_flow"]

    high_asym_valid = pairs["line_length_km"].between(9.0, 60.0)
    high_asym_score = pairs["combined_observed_flow"] * pairs["bilateral_asymmetry_gap"]

    selected = pd.concat(
        [
            _top_family(
                pairs,
                family="radial",
                valid=radial_valid,
                score=radial_score,
                n=n_per_family,
            ),
            _top_family(
                pairs,
                family="orbital",
                valid=orbital_valid,
                score=orbital_score,
                n=n_per_family,
            ),
            _top_family(
                pairs,
                family="high_flow",
                valid=high_flow_valid,
                score=high_flow_score,
                n=n_per_family,
            ),
            _top_family(
                pairs,
                family="high_asymmetry",
                valid=high_asym_valid,
                score=high_asym_score,
                n=n_per_family,
            ),
        ],
        ignore_index=True,
    )

    family_priority = {"radial": 0, "orbital": 1, "high_asymmetry": 2, "high_flow": 3}
    selected["family_priority"] = selected["source_family"].map(family_priority)
    family_map = (
        selected.groupby(["node_a", "node_b"], sort=False)["source_family"]
        .agg(lambda s: "+".join(sorted(set(s), key=lambda z: family_priority[z])))
        .rename("source_families")
    )
    primary = (
        selected.sort_values(["node_a", "node_b", "family_priority", "selection_score"], ascending=[True, True, True, False])
        .drop_duplicates(["node_a", "node_b"])
        .set_index(["node_a", "node_b"])
    )
    unique = primary.join(family_map).reset_index()
    unique = unique.sort_values(
        ["family_priority", "selection_score", "combined_observed_flow"],
        ascending=[True, False, False],
        kind="stable",
    ).reset_index(drop=True)
    unique["candidate_id"] = [f"metro_{i:04d}" for i in range(1, len(unique) + 1)]
    unique["primary_family"] = unique["source_family"]

    candidates = [
        Candidate(
            candidate_id=str(r.candidate_id),
            node_a=int(r.node_a),
            node_b=int(r.node_b),
            source_families=str(r.source_families),
            primary_family=str(r.primary_family),
            combined_observed_flow=float(r.combined_observed_flow),
            bilateral_asymmetry_gap=float(r.bilateral_asymmetry_gap),
            line_length_km=float(r.line_length_km),
            radius_a_km=float(r.radius_a_km),
            radius_b_km=float(r.radius_b_km),
            angular_separation_deg=float(r.angular_separation_deg),
        )
        for r in unique.itertuples(index=False)
    ]
    return candidates, unique


def _endpoint_catchments(x: np.ndarray, y: np.ndarray, candidate: Candidate, catchment_km: float) -> tuple[np.ndarray, np.ndarray]:
    radius_grid = catchment_km / GRID_KM
    a = np.hypot(x - x[candidate.node_a], y - y[candidate.node_a]) <= radius_grid + 1e-9
    b = np.hypot(x - x[candidate.node_b], y - y[candidate.node_b]) <= radius_grid + 1e-9
    return a, b


def _metro_mask(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    mask = (a[:, None] & b[None, :]) | (b[:, None] & a[None, :])
    np.fill_diagonal(mask, False)
    return mask


def _weighted_mean(values: np.ndarray, weights: np.ndarray, mask: np.ndarray) -> float:
    use = np.asarray(mask, dtype=bool) & np.isfinite(values) & np.isfinite(weights) & (weights > 0)
    if not use.any():
        return np.nan
    return float(np.average(values[use], weights=weights[use]))


def augment_candidate_definitions(
    definitions: pd.DataFrame,
    candidates: list[Candidate],
    directed,
    symmetric,
    *,
    catchment_km: float,
) -> pd.DataFrame:
    prim = directed.prim
    x, y = grid_xy(prim)
    flow = np.asarray(prim.L_ij_obs, dtype=float)
    total_flow = float(flow.sum())
    theta = directed.params.theta
    delta_ubar = np.log(np.clip(symmetric.fund.ubar_theta, EPS, None)) / theta - np.log(
        np.clip(directed.fund.ubar_theta, EPS, None)
    ) / theta
    delta_abar = np.log(np.clip(symmetric.fund.abar_theta, EPS, None)) / theta - np.log(
        np.clip(directed.fund.abar_theta, EPS, None)
    ) / theta
    asym = _bilateral_asymmetry_gap(directed.baseline, theta)

    rows = []
    for candidate in candidates:
        a, b = _endpoint_catchments(x, y, candidate, catchment_km)
        mask = _metro_mask(a, b)
        flow_ab = float(flow[np.ix_(a, b)].sum())
        flow_ba = float(flow[np.ix_(b, a)].sum())
        if flow_ab >= flow_ba:
            origin, destination = a, b
            dominant_direction = "a_to_b"
        else:
            origin, destination = b, a
            dominant_direction = "b_to_a"

        positive = mask & (flow > EPS)
        asym_exposure = float(
            np.average(asym[positive], weights=flow[positive])
        ) if positive.any() else 0.0
        endpoint_union = a | b
        row = {
            "candidate_id": candidate.candidate_id,
            "grid_a": str(prim.node_grid_ids[candidate.node_a]),
            "grid_b": str(prim.node_grid_ids[candidate.node_b]),
            "x_a": float(x[candidate.node_a]),
            "y_a": float(y[candidate.node_a]),
            "x_b": float(x[candidate.node_b]),
            "y_b": float(y[candidate.node_b]),
            "n_catchment_a": int(a.sum()),
            "n_catchment_b": int(b.sum()),
            "n_treated_od_cells": int(mask.sum()),
            "treated_observed_flow": float(flow[mask].sum()),
            "treated_flow_share_pct": 100.0 * float(flow[mask].sum()) / max(total_flow, EPS),
            "dominant_direction": dominant_direction,
            "dominant_flow_share": max(flow_ab, flow_ba) / max(flow_ab + flow_ba, EPS),
            "flow_weighted_asymmetry": asym_exposure,
            "delta_log_ubar_origin": _weighted_mean(delta_ubar, prim.L_R_obs, origin),
            "delta_log_abar_destination": _weighted_mean(delta_abar, prim.L_F_obs, destination),
            "abs_delta_log_ubar_endpoints": _weighted_mean(np.abs(delta_ubar), prim.L_R_obs, endpoint_union),
            "abs_delta_log_abar_endpoints": _weighted_mean(np.abs(delta_abar), prim.L_F_obs, endpoint_union),
        }
        row["fundamental_alignment"] = row["delta_log_ubar_origin"] + row["delta_log_abar_destination"]
        rows.append(row)

    extra = pd.DataFrame(rows)
    return definitions.merge(extra, on="candidate_id", how="left", validate="one_to_one")


def run_candidates(
    candidates: list[Candidate],
    definitions: pd.DataFrame,
    worlds: list,
    *,
    catchment_km: float,
    cost_scale: float,
) -> pd.DataFrame:
    x, y = grid_xy(worlds[0].prim)
    by_id = definitions.set_index("candidate_id")
    rows: list[dict] = []
    for world in worlds:
        LOG.info("Evaluating %d metro candidates in %s", len(candidates), world.name)
        for idx, candidate in enumerate(candidates, start=1):
            a, b = _endpoint_catchments(x, y, candidate, catchment_km)
            mask = _metro_mask(a, b)
            spec = OdTreatment(
                scenario=candidate.candidate_id,
                family="reduced_form_metro_line",
                mask=mask,
                cost_scale=cost_scale,
                selection_rule="bidirectional cost reduction between endpoint catchments",
            )
            row = run_od_treatment(world, spec)
            row.update(
                {
                    "candidate_id": candidate.candidate_id,
                    "primary_family": candidate.primary_family,
                    "source_families": candidate.source_families,
                    "catchment_km": catchment_km,
                    "line_length_km": candidate.line_length_km,
                    "treated_flow_share_pct_common": float(by_id.loc[candidate.candidate_id, "treated_flow_share_pct"]),
                }
            )
            rows.append(row)
            if idx % 50 == 0 or idx == len(candidates):
                LOG.info("%s: completed %d/%d", world.name, idx, len(candidates))
    return pd.DataFrame(rows)


def compare_candidates(effects: pd.DataFrame, definitions: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "Wbar_pct_change",
        "od_weighted_bilateral_cost_pct_change",
        "od_cost_change_sec_per_commuter",
        "od_cost_direct_sec_baseline_od",
        "od_cost_sorting_sec",
        "residence_dissimilarity_pct",
        "workplace_dissimilarity_pct",
        "od_dissimilarity_pct",
    ]
    out = definitions.copy()
    for metric in metrics:
        wide = effects.pivot(index="candidate_id", columns="world", values=metric)
        out = out.merge(
            wide.rename(
                columns={
                    "directed_main": f"directed_{metric}",
                    "symmetric_reestimated": f"symmetric_{metric}",
                }
            ).reset_index(),
            on="candidate_id",
            how="left",
            validate="one_to_one",
        )
        out[f"bias_{metric}"] = out[f"symmetric_{metric}"] - out[f"directed_{metric}"]

    out["directed_rank"] = out["directed_Wbar_pct_change"].rank(method="min", ascending=False).astype(int)
    out["symmetric_rank"] = out["symmetric_Wbar_pct_change"].rank(method="min", ascending=False).astype(int)
    out["rank_difference"] = out["symmetric_rank"] - out["directed_rank"]
    out["directed_Wbar_pct_change_per_km"] = out["directed_Wbar_pct_change"] / np.clip(
        out["line_length_km"], EPS, None
    )
    out["symmetric_Wbar_pct_change_per_km"] = out["symmetric_Wbar_pct_change"] / np.clip(
        out["line_length_km"], EPS, None
    )
    out["bias_Wbar_pct_change_per_km"] = (
        out["symmetric_Wbar_pct_change_per_km"] - out["directed_Wbar_pct_change_per_km"]
    )
    out["directed_rank_per_km"] = out["directed_Wbar_pct_change_per_km"].rank(
        method="min", ascending=False
    ).astype(int)
    out["symmetric_rank_per_km"] = out["symmetric_Wbar_pct_change_per_km"].rank(
        method="min", ascending=False
    ).astype(int)
    for k in (10, 25, 50, 100):
        out[f"directed_top{k}"] = out["directed_rank"] <= k
        out[f"symmetric_top{k}"] = out["symmetric_rank"] <= k
    out["top100_map_category"] = "not_top100"
    common = out["directed_top100"] & out["symmetric_top100"]
    directed_only = out["directed_top100"] & ~out["symmetric_top100"]
    symmetric_only = out["symmetric_top100"] & ~out["directed_top100"]
    out.loc[common, "top100_map_category"] = "common"
    out.loc[directed_only, "top100_map_category"] = "directed_only"
    out.loc[symmetric_only, "top100_map_category"] = "symmetric_only"
    return out.sort_values("directed_rank", kind="stable").reset_index(drop=True)


def ranking_summary(comparison: pd.DataFrame) -> dict:
    rho = spearmanr(
        comparison["directed_Wbar_pct_change"],
        comparison["symmetric_Wbar_pct_change"],
    )
    d_best = comparison.loc[comparison["directed_Wbar_pct_change"].idxmax()]
    s_best = comparison.loc[comparison["symmetric_Wbar_pct_change"].idxmax()]
    directed_value_of_symmetric_choice = float(s_best["directed_Wbar_pct_change"])
    directed_optimal = float(d_best["directed_Wbar_pct_change"])
    regret = directed_optimal - directed_value_of_symmetric_choice
    summary = {
        "n_candidates": int(len(comparison)),
        "spearman_rank_correlation": float(rho.statistic),
        "spearman_pvalue_descriptive": float(rho.pvalue),
        "directed_best_candidate": str(d_best["candidate_id"]),
        "symmetric_best_candidate": str(s_best["candidate_id"]),
        "same_best_candidate": bool(d_best["candidate_id"] == s_best["candidate_id"]),
        "directed_optimal_Wbar_pct_change": directed_optimal,
        "directed_Wbar_pct_change_of_symmetric_choice": directed_value_of_symmetric_choice,
        "directed_policy_regret_pct_point": float(regret),
        "directed_policy_regret_share_pct": 100.0 * float(regret) / max(abs(directed_optimal), EPS),
        "directed_rank_of_symmetric_best": int(s_best["directed_rank"]),
        "max_abs_welfare_bias_pct_point": float(comparison["bias_Wbar_pct_change"].abs().max()),
        "median_abs_welfare_bias_pct_point": float(comparison["bias_Wbar_pct_change"].abs().median()),
    }
    for k in (10, 25, 50, 100):
        d = set(comparison.loc[comparison[f"directed_top{k}"], "candidate_id"])
        s = set(comparison.loc[comparison[f"symmetric_top{k}"], "candidate_id"])
        summary[f"top{k}_overlap_n"] = int(len(d & s))
        summary[f"top{k}_overlap_share_pct"] = 100.0 * len(d & s) / max(len(d), 1)
        summary[f"top{k}_jaccard"] = len(d & s) / max(len(d | s), 1)

    rho_per_km = spearmanr(
        comparison["directed_Wbar_pct_change_per_km"],
        comparison["symmetric_Wbar_pct_change_per_km"],
    )
    d_best_per_km = comparison.loc[comparison["directed_Wbar_pct_change_per_km"].idxmax()]
    s_best_per_km = comparison.loc[comparison["symmetric_Wbar_pct_change_per_km"].idxmax()]
    regret_per_km = float(
        d_best_per_km["directed_Wbar_pct_change_per_km"]
        - s_best_per_km["directed_Wbar_pct_change_per_km"]
    )
    summary.update(
        {
            "per_km_spearman_rank_correlation": float(rho_per_km.statistic),
            "per_km_directed_best_candidate": str(d_best_per_km["candidate_id"]),
            "per_km_symmetric_best_candidate": str(s_best_per_km["candidate_id"]),
            "per_km_same_best_candidate": bool(
                d_best_per_km["candidate_id"] == s_best_per_km["candidate_id"]
            ),
            "per_km_directed_policy_regret": regret_per_km,
            "per_km_directed_policy_regret_share_pct": 100.0
            * regret_per_km
            / max(abs(float(d_best_per_km["directed_Wbar_pct_change_per_km"])), EPS),
        }
    )
    for k in (10, 25, 50, 100):
        d = set(comparison.nsmallest(k, "directed_rank_per_km")["candidate_id"])
        s = set(comparison.nsmallest(k, "symmetric_rank_per_km")["candidate_id"])
        summary[f"per_km_top{k}_overlap_n"] = int(len(d & s))
        summary[f"per_km_top{k}_overlap_share_pct"] = 100.0 * len(d & s) / max(len(d), 1)
    return summary


def association_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    outcome = comparison["bias_Wbar_pct_change"].to_numpy(dtype=float)
    predictors = [
        "fundamental_alignment",
        "abs_delta_log_ubar_endpoints",
        "abs_delta_log_abar_endpoints",
        "flow_weighted_asymmetry",
        "treated_flow_share_pct",
        "line_length_km",
    ]
    rows = []
    for predictor in predictors:
        x = comparison[predictor].to_numpy(dtype=float)
        valid = np.isfinite(x) & np.isfinite(outcome)
        rho = spearmanr(x[valid], outcome[valid])
        sx = float(np.std(x[valid]))
        sy = float(np.std(outcome[valid]))
        corr = float(np.corrcoef(x[valid], outcome[valid])[0, 1]) if sx > 0 and sy > 0 else np.nan
        slope = float(np.cov(x[valid], outcome[valid], ddof=0)[0, 1] / max(np.var(x[valid]), EPS))
        rows.append(
            {
                "predictor": predictor,
                "n": int(valid.sum()),
                "pearson_correlation": corr,
                "spearman_correlation": float(rho.statistic),
                "spearman_pvalue_descriptive": float(rho.pvalue),
                "univariate_slope_welfare_bias_per_unit": slope,
                "predictor_sd": sx,
                "welfare_bias_sd_pct_point": sy,
            }
        )

    valid = np.isfinite(outcome)
    for predictor in predictors:
        valid &= np.isfinite(comparison[predictor].to_numpy(dtype=float))
    Xraw = comparison.loc[valid, predictors].to_numpy(dtype=float)
    y = outcome[valid]
    means = Xraw.mean(axis=0)
    sds = Xraw.std(axis=0)
    Xstd = (Xraw - means) / np.where(sds > EPS, sds, 1.0)
    X = np.column_stack([np.ones(len(Xstd)), Xstd])
    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    yhat = X @ beta
    ssr = float(np.sum((y - yhat) ** 2))
    sst = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ssr / max(sst, EPS)
    for predictor, coefficient in zip(predictors, beta[1:]):
        rows.append(
            {
                "predictor": predictor,
                "n": int(len(y)),
                "standardized_multivariate_coefficient_pct_point_per_sd": float(coefficient),
                "multivariate_r2": float(r2),
                "model": "joint_standardized_ols_descriptive",
            }
        )
    return pd.DataFrame(rows)


def _radial_profile(directed, symmetric, *, n_bins: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
    prim = directed.prim
    x, y = grid_xy(prim)
    cx, cy = employment_centroid(prim)
    distance = np.hypot(x - cx, y - cy) * GRID_KM
    theta = directed.params.theta
    delta_ubar = np.log(np.clip(symmetric.fund.ubar_theta, EPS, None)) / theta - np.log(
        np.clip(directed.fund.ubar_theta, EPS, None)
    ) / theta
    delta_abar = np.log(np.clip(symmetric.fund.abar_theta, EPS, None)) / theta - np.log(
        np.clip(directed.fund.abar_theta, EPS, None)
    ) / theta
    nodes = pd.DataFrame(
        {
            "node_idx": np.arange(prim.n_nodes),
            "distance_km": distance,
            "delta_log_ubar": delta_ubar,
            "delta_log_abar": delta_abar,
            "L_R_obs": prim.L_R_obs,
            "L_F_obs": prim.L_F_obs,
            "x": x,
            "y": y,
        }
    )
    nodes["distance_bin"] = pd.qcut(nodes["distance_km"], n_bins, duplicates="drop")
    rows = []
    for _, group in nodes.groupby("distance_bin", observed=True):
        rows.append(
            {
                "distance_km": float(group["distance_km"].mean()),
                "n_nodes": int(len(group)),
                "delta_log_ubar": float(
                    np.average(group["delta_log_ubar"], weights=np.clip(group["L_R_obs"], EPS, None))
                ),
                "delta_log_abar": float(
                    np.average(group["delta_log_abar"], weights=np.clip(group["L_F_obs"], EPS, None))
                ),
            }
        )
    profile = pd.DataFrame(rows)

    gradients = []
    for metric, weight in (("delta_log_ubar", "L_R_obs"), ("delta_log_abar", "L_F_obs")):
        X = np.column_stack([np.ones(len(nodes)), nodes["distance_km"].to_numpy(dtype=float)])
        w = np.clip(nodes[weight].to_numpy(dtype=float), EPS, None)
        sw = np.sqrt(w / np.mean(w))
        beta = np.linalg.lstsq(X * sw[:, None], nodes[metric].to_numpy(dtype=float) * sw, rcond=None)[0]
        yhat = X @ beta
        y = nodes[metric].to_numpy(dtype=float)
        ybar = np.average(y, weights=w)
        r2 = 1.0 - np.sum(w * (y - yhat) ** 2) / max(np.sum(w * (y - ybar) ** 2), EPS)
        gradients.append(
            {
                "metric": metric,
                "intercept": float(beta[0]),
                "slope_per_10km": float(beta[1] * 10.0),
                "r2": float(r2),
            }
        )
    return profile, pd.DataFrame(gradients)


def _draw_radial_panel(ax: plt.Axes, profile: pd.DataFrame, gradients: pd.DataFrame) -> None:
    ax.axhline(0.0, color="#555555", lw=0.8, zorder=0)
    ax.plot(
        profile["distance_km"],
        profile["delta_log_ubar"],
        color=RESIDENCE_COLOR,
        marker="o",
        ms=3.8,
        lw=1.6,
        label="Residence fundamental",
    )
    ax.plot(
        profile["distance_km"],
        profile["delta_log_abar"],
        color=WORKPLACE_COLOR,
        marker="s",
        ms=3.4,
        lw=1.6,
        label="Workplace fundamental",
    )
    for metric, color in (("delta_log_ubar", RESIDENCE_COLOR), ("delta_log_abar", WORKPLACE_COLOR)):
        row = gradients.loc[gradients["metric"] == metric].iloc[0]
        xx = np.array([0.0, float(profile["distance_km"].max())])
        yy = float(row["intercept"]) + float(row["slope_per_10km"]) * xx / 10.0
        ax.plot(xx, yy, color=color, lw=0.9, ls="--", alpha=0.6)
    r = gradients.set_index("metric")
    note = (
        f"Residence: {r.loc['delta_log_ubar', 'slope_per_10km']:+.4f} per 10 km "
        f"(R²={r.loc['delta_log_ubar', 'r2']:.3f})\n"
        f"Workplace: {r.loc['delta_log_abar', 'slope_per_10km']:+.4f} per 10 km "
        f"(R²={r.loc['delta_log_abar', 'r2']:.3f})"
    )
    ax.text(
        0.03,
        0.05,
        note,
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=7.5,
        color="#333333",
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.82, "pad": 2.0},
    )
    ax.set_xlabel("Distance to employment-weighted center (km)")
    ax.set_ylabel("Symmetric minus directed fundamental (log point)")
    ax.set_title("Recovered fundamentals shift in opposite radial directions", loc="left")
    ax.legend(loc="upper left", handlelength=2.2)
    _axis_style(ax)


def _draw_corridor_panel(ax: plt.Axes, comparison: pd.DataFrame, directed, *, top_k: int = 100) -> None:
    prim = directed.prim
    x, y = grid_xy(prim)
    cx, cy = employment_centroid(prim)

    edge_segments = np.stack(
        [
            np.column_stack([x[prim.edge_i], y[prim.edge_i]]),
            np.column_stack([x[prim.edge_j], y[prim.edge_j]]),
        ],
        axis=1,
    )
    ax.add_collection(LineCollection(edge_segments, colors="#E4E4E4", linewidths=0.25, alpha=0.45, zorder=0))
    ax.scatter(x, y, s=2.0, color="#D4D4D4", alpha=0.45, linewidths=0, zorder=1)

    comparison = comparison.copy()
    comparison["map_category"] = "not_selected"
    directed_selected = comparison["directed_rank"] <= top_k
    symmetric_selected = comparison["symmetric_rank"] <= top_k
    comparison.loc[directed_selected & symmetric_selected, "map_category"] = "common"
    comparison.loc[directed_selected & ~symmetric_selected, "map_category"] = "directed_only"
    comparison.loc[~directed_selected & symmetric_selected, "map_category"] = "symmetric_only"

    order = [
        ("directed_only", DIRECTED_COLOR, "Directed only"),
        ("symmetric_only", SYMMETRIC_COLOR, "Symmetric only"),
        ("common", COMMON_COLOR, "Common"),
    ]
    for category, color, label in order:
        subset = comparison[comparison["map_category"] == category]
        segments = [
            [(float(r.x_a), float(r.y_a)), (float(r.x_b), float(r.y_b))]
            for r in subset.itertuples(index=False)
        ]
        if not segments:
            continue
        ax.add_collection(
            LineCollection(
                segments,
                colors=color,
                linewidths=0.9 if category == "common" else 0.75,
                alpha=0.50 if category == "common" else 0.62,
                zorder=3 if category == "common" else 2,
            )
        )
    ax.scatter([cx], [cy], marker="*", s=50, color="black", edgecolor="white", linewidth=0.6, zorder=5)
    scale_x = float(x.min()) + 3.0
    scale_y = float(y.min()) + 2.0
    scale_grid = 30.0 / GRID_KM
    ax.plot([scale_x, scale_x + scale_grid], [scale_y, scale_y], color="black", lw=1.4, zorder=5)
    ax.text(scale_x + scale_grid / 2.0, scale_y + 1.0, "30 km", ha="center", va="bottom", fontsize=7.5)
    ax.set_aspect("equal")
    ax.autoscale_view()
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title(f"Top-{top_k} reduced-form metro corridors", loc="left")
    legend_handles = []
    for category, color, label in order:
        count = int((comparison["map_category"] == category).sum())
        legend_handles.append(Line2D([0], [0], color=color, lw=1.4, label=f"{label} (n={count})"))
    ax.legend(handles=legend_handles, loc="upper right", handlelength=2.4)
    ax.text(
        0.01,
        0.01,
        "Endpoint links; 10% bilateral cost reduction",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=7.2,
        color="#444444",
    )


def make_figures(
    out: Path,
    profile: pd.DataFrame,
    gradients: pd.DataFrame,
    comparison: pd.DataFrame,
    directed,
    ranking: dict,
) -> None:
    figures = out / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(5.2, 3.6))
    _draw_radial_panel(ax, profile, gradients)
    fig.tight_layout()
    _save_figure(fig, figures / "figure_fundamental_radial_profile")

    fig, ax = plt.subplots(figsize=(5.2, 4.5))
    _draw_corridor_panel(ax, comparison, directed, top_k=100)
    fig.tight_layout()
    _save_figure(fig, figures / "figure_metro_top100_selection_map")

    fig, ax = plt.subplots(figsize=(5.2, 4.5))
    _draw_corridor_panel(ax, comparison, directed, top_k=25)
    fig.tight_layout()
    _save_figure(fig, figures / "figure_metro_top25_selection_map")

    fig = plt.figure(figsize=(11.2, 4.5))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.05, 1.15], wspace=0.22)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    _draw_radial_panel(ax1, profile, gradients)
    _draw_corridor_panel(ax2, comparison, directed, top_k=100)
    ax1.text(-0.12, 1.03, "a", transform=ax1.transAxes, fontsize=12, fontweight="bold")
    ax2.text(-0.08, 1.03, "b", transform=ax2.transAxes, fontsize=12, fontweight="bold")
    _save_figure(fig, figures / "figure_fundamentals_and_metro_selection")

    fig, ax = plt.subplots(figsize=(5.0, 4.2))
    cats = comparison["top100_map_category"].map(
        {
            "common": COMMON_COLOR,
            "directed_only": DIRECTED_COLOR,
            "symmetric_only": SYMMETRIC_COLOR,
            "not_top100": "#CFCFCF",
        }
    )
    ax.scatter(
        comparison["directed_Wbar_pct_change"],
        comparison["symmetric_Wbar_pct_change"],
        s=13,
        c=cats,
        alpha=0.75,
        linewidths=0,
    )
    lo = float(min(comparison["directed_Wbar_pct_change"].min(), comparison["symmetric_Wbar_pct_change"].min()))
    hi = float(max(comparison["directed_Wbar_pct_change"].max(), comparison["symmetric_Wbar_pct_change"].max()))
    ax.plot([lo, hi], [lo, hi], color="black", ls="--", lw=0.8)
    ax.set_xlabel("Directed-model welfare effect (%)")
    ax.set_ylabel("Symmetric-model welfare effect (%)")
    ax.set_title("Candidate metro-line welfare rankings", loc="left")
    ax.text(
        0.04,
        0.95,
        f"Spearman ρ = {ranking['spearman_rank_correlation']:.3f}\n"
        f"Top-100 overlap = {ranking['top100_overlap_share_pct']:.1f}%\n"
        f"Policy regret = {ranking['directed_policy_regret_share_pct']:.2f}%",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8.0,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.85, "pad": 2.0},
    )
    _axis_style(ax)
    fig.tight_layout()
    _save_figure(fig, figures / "figure_metro_candidate_welfare_ranking")


def write_documentation(out: Path, meta: dict, ranking: dict) -> None:
    readme = f"""# Symmetric-input metro candidate experiment

## Purpose

This package evaluates whether reciprocal-symmetric commuting inputs change the welfare ranking of reduced-form metro investments. It does not use tidal lanes or road-lane reallocation.

## Policy experiment

Each candidate connects two endpoint catchments with radius `{meta['catchment_km']:.1f}` km. The counterfactual lowers generalized bilateral commuting costs by `{100.0 * (1.0 - meta['cost_scale']):.1f}%` for origin--destination markets linking the two catchments. The reduction is applied in both directions. All candidate locations and treatment masks are identical across the directed and symmetric calibration worlds.

The line representation is reduced form. Straight endpoint links are not engineering alignments, and the experiment does not model stations, construction costs, capacity, fares, or an explicit transit network.

## Main outputs

- `metrics/metro_candidate_definitions.csv`: candidate endpoints, catchments, observed flows, directional exposure, and recovered-fundamental differences.
- `metrics/metro_candidate_effects_by_world.csv`: counterfactual outcomes for every candidate in both calibration worlds.
- `metrics/metro_candidate_comparison.csv`: welfare biases, ranks, and top-K membership.
- `metrics/metro_ranking_summary.json`: rank correlation, overlap, and policy regret.
- `metrics/metro_bias_associations.csv`: descriptive associations between welfare bias and candidate characteristics.
- `figures/figure_fundamentals_and_metro_selection.*`: slide-ready two-panel figure.
- `figures/figure_metro_candidate_welfare_ranking.*`: welfare-rank diagnostic.

## Retained interpretation

The experiment separates evaluation from selection. Candidate-specific symmetric-minus-directed welfare differences measure policy-evaluation sensitivity for the same physical line. Rank overlap and regret measure policy-design sensitivity.

Current run: `{ranking['n_candidates']}` candidates, Spearman rank correlation `{ranking['spearman_rank_correlation']:.4f}`, top-100 overlap `{ranking['top100_overlap_share_pct']:.1f}%`, and directed-benchmark regret `{ranking['directed_policy_regret_share_pct']:.3f}%` of the directed-optimal welfare effect.

The line-length-normalized ranking has Spearman correlation `{ranking['per_km_spearman_rank_correlation']:.4f}`, top-100 overlap `{ranking['per_km_top100_overlap_share_pct']:.1f}%`, and directed-benchmark regret `{ranking['per_km_directed_policy_regret_share_pct']:.3f}%`.
"""
    (out / "README.md").write_text(readme)

    docs = {
        "analysis-plan.md": """# Analysis plan

Estimate a common set of reduced-form metro counterfactuals in the fully re-estimated directed and symmetric calibration worlds. Compare candidate-specific welfare effects, candidate rankings, top-K overlap, and directed-benchmark policy regret. Relate welfare differences to directional exposure and recovered-fundamental differences at line endpoints.
""",
        "variable-dictionary.md": """# Variable dictionary

- `candidate_id`: stable identifier for a candidate endpoint link.
- `treated_flow_share_pct`: observed commuting flow between the two endpoint catchments as a share of total flow.
- `flow_weighted_asymmetry`: observed-flow-weighted reciprocal bilateral-cost gap within the treated OD set.
- `delta_log_ubar_origin`: symmetric-minus-directed residential fundamental at the dominant origin catchment.
- `delta_log_abar_destination`: symmetric-minus-directed workplace fundamental at the dominant destination catchment.
- `fundamental_alignment`: sum of the two signed fundamental differences.
- `Wbar_pct_change`: welfare change relative to the baseline of the same calibration world.
- `bias_Wbar_pct_change`: symmetric minus directed welfare effect for a common candidate.
- `directed_rank`, `symmetric_rank`: descending welfare ranks within each world.
""",
        "sample-lineage.md": f"""# Sample lineage

The node and OD universe is loaded from `{meta['directed_version_id']}`. Positive-flow unordered node pairs between 9 and 70 km enter candidate screening. Up to `{meta['n_per_family']}` candidates are retained from each radial, orbital, high-flow, and high-asymmetry screen, followed by endpoint-pair deduplication. The same retained candidates are evaluated in both calibration worlds.
""",
        "geospatial-data-notes.md": """# Geospatial data notes

Node coordinates are square-grid indices. One grid step is treated as 3 km. The center is the employment-weighted centroid of observed workplace employment. Candidate lines are straight links between endpoint nodes, and catchments are Euclidean buffers in grid space. The geometry is a diagnostic proxy rather than a representation of actual metro alignments or administrative ring roads.
""",
        "exposure-definition.md": f"""# Exposure definition

A candidate metro line treats all off-diagonal OD markets connecting its two endpoint catchments. Both travel directions are included. Generalized bilateral commuting costs are multiplied by `{meta['cost_scale']:.2f}`. Fundamentals remain fixed within each candidate counterfactual, but they differ across the separately re-estimated directed and symmetric baselines.
""",
        "regression-spec-matrix.md": """# Regression specification matrix

The association table is descriptive. Each row is a candidate line. The outcome is the symmetric-minus-directed welfare effect in percentage points. Predictors include signed fundamental alignment, absolute residential and workplace fundamental differences, flow-weighted directional asymmetry, treated-flow share, and line length. Univariate Pearson and Spearman correlations are reported together with a joint standardized OLS summary. No causal interpretation or conventional inference is assigned to these associations.
""",
        "table-shells.md": """# Table shells

## Candidate ranking

Report the number of candidates, Spearman rank correlation, top-10, top-25, top-50, and top-100 overlap, directed-optimal welfare, welfare of the symmetric choice in the directed benchmark, and policy regret.

## Candidate heterogeneity

Report candidate welfare effects in both worlds, symmetric-minus-directed welfare differences, sorting responses, directional exposure, and recovered-fundamental differences.
""",
        "replication-checklist.md": """# Replication checklist

- Run the Python entry point recorded in the package metadata.
- Confirm that both baseline inversions and equilibria converge.
- Confirm that each candidate has one row per calibration world.
- Confirm that treatment masks are common across worlds.
- Confirm that welfare ranks have no missing values.
- Confirm that the directed-benchmark regret is nonnegative up to numerical tolerance.
- Inspect SVG, PDF, and PNG exports for label overlap and color separation.
""",
    }
    for name, text in docs.items():
        (out / name).write_text(text)


def run_experiment(
    data_work: Path,
    *,
    directed_version_id: str,
    symmetric_version_id: str,
    output_name: str,
    sym_method: str,
    n_per_family: int,
    catchment_km: float,
    cost_scale: float,
    force_bundle: bool,
) -> None:
    out = data_work / "outputs" / output_name
    metrics = out / "metrics"
    metrics.mkdir(parents=True, exist_ok=True)
    _setup_style()

    build_symmetric_cost_bundle(
        data_work,
        directed_version_id=directed_version_id,
        symmetric_version_id=symmetric_version_id,
        sym_method=sym_method,
        force=force_bundle,
    )
    params = _load_params()
    directed = _build_world(data_work, name="directed_main", version_id=directed_version_id, params=params)
    symmetric = _build_world(data_work, name="symmetric_reestimated", version_id=symmetric_version_id, params=params)
    if not directed.fund.converged or not symmetric.fund.converged:
        raise RuntimeError("baseline fundamental inversion did not converge")
    if not directed.baseline.converged or not symmetric.baseline.converged:
        raise RuntimeError("baseline equilibrium did not converge")

    candidates, definitions = build_candidates(directed, n_per_family=n_per_family)
    if len(candidates) < 100:
        raise RuntimeError(f"candidate universe too small for top-100 comparison: {len(candidates)}")
    definitions = augment_candidate_definitions(
        definitions,
        candidates,
        directed,
        symmetric,
        catchment_km=catchment_km,
    )
    definitions.to_csv(metrics / "metro_candidate_definitions.csv", index=False)

    effects = run_candidates(
        candidates,
        definitions,
        [directed, symmetric],
        catchment_km=catchment_km,
        cost_scale=cost_scale,
    )
    effects.to_csv(metrics / "metro_candidate_effects_by_world.csv", index=False)

    comparison = compare_candidates(effects, definitions)
    comparison.to_csv(metrics / "metro_candidate_comparison.csv", index=False)
    ranking = ranking_summary(comparison)
    (metrics / "metro_ranking_summary.json").write_text(json.dumps(ranking, indent=2, default=float))

    associations = association_summary(comparison)
    associations.to_csv(metrics / "metro_bias_associations.csv", index=False)

    profile, gradients = _radial_profile(directed, symmetric)
    profile.to_csv(metrics / "fundamental_radial_profile.csv", index=False)
    gradients.to_csv(metrics / "fundamental_distance_gradients.csv", index=False)

    meta = {
        "entry_point": str(Path(__file__).resolve()),
        "output_name": output_name,
        "directed_version_id": directed_version_id,
        "symmetric_version_id": symmetric_version_id,
        "sym_method": sym_method,
        "n_per_family": n_per_family,
        "n_candidates": len(candidates),
        "catchment_km": catchment_km,
        "cost_scale": cost_scale,
        "grid_km": GRID_KM,
        "policy_interpretation": "reduced-form bidirectional metro access shock",
        "no_tidal_lanes": True,
        "no_lane_reallocation": True,
        "common_physical_candidates": True,
        "baseline_inversion_converged": {
            "directed": bool(directed.fund.converged),
            "symmetric": bool(symmetric.fund.converged),
        },
        "baseline_equilibrium_converged": {
            "directed": bool(directed.baseline.converged),
            "symmetric": bool(symmetric.baseline.converged),
        },
    }
    (out / "experiment_meta.json").write_text(json.dumps(meta, indent=2, default=float))
    write_documentation(out, meta, ranking)
    make_figures(out, profile, gradients, comparison, directed, ranking)
    LOG.info("Wrote metro candidate experiment to %s", out)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=str(DATA_WORK))
    parser.add_argument("--directed-version-id", default=DEFAULT_DIRECTED_VERSION)
    parser.add_argument("--symmetric-version-id", default=DEFAULT_SYMMETRIC_VERSION)
    parser.add_argument("--output-name", default=DEFAULT_OUTPUT)
    parser.add_argument("--sym-method", choices=("geometric", "arithmetic"), default="geometric")
    parser.add_argument("--n-per-family", type=int, default=100)
    parser.add_argument("--catchment-km", type=float, default=6.0)
    parser.add_argument("--cost-scale", type=float, default=0.90)
    parser.add_argument("--force-bundle", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    logging.getLogger("fontTools").setLevel(logging.WARNING)
    run_experiment(
        Path(args.repo_root),
        directed_version_id=args.directed_version_id,
        symmetric_version_id=args.symmetric_version_id,
        output_name=args.output_name,
        sym_method=args.sym_method,
        n_per_family=args.n_per_family,
        catchment_km=args.catchment_km,
        cost_scale=args.cost_scale,
        force_bundle=args.force_bundle,
    )

    out = Path(args.repo_root) / "outputs" / args.output_name / "metrics"
    print(json.dumps(json.loads((out / "metro_ranking_summary.json").read_text()), indent=2))


if __name__ == "__main__":
    main()
