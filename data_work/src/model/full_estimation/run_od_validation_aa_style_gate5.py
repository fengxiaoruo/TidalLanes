"""AA-style predicted-versus-observed commuting OD validation.

The script reproduces the fixed-effect residualization used for Figure 2(b)
in Allen and Arkolakis (2022), verifies that implementation against the
bundled Seattle replication files, and applies it to the locked Beijing
gate5 mainline.

Two prediction objects receive primary attention:

1. The current level-form model's baseline endogenous OD prediction.
2. Equation (35)'s Leontief inversion using baseline model-implied link
   traffic Xi_kl.

The second object is an internal consistency check because Beijing link
traffic is model-implied rather than directly observed AADT. Speed-implied
traffic and a lanes-only placebo are retained as diagnostics.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import json
from pathlib import Path
import sys
from typing import Iterable

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.linalg import lsmr


DATA_WORK = Path(__file__).resolve().parents[3]
REPO_ROOT = DATA_WORK.parent
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    _build_A_matrix,
    _compute_tau_inv_theta,
)


DEFAULT_VERSION = "model_input_edgeproj_v2_lcc_gate5_v1"
DEFAULT_STAGE2 = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_OUTPUT = "od_validation_aa_style_gate5_bpr_pos05_v1"
AA_REFERENCE_CORRELATION = 0.4335
AA_DELTA1 = 0.488
EPS = 1e-12


@dataclass
class ProjectionResult:
    residual: np.ndarray
    mask: np.ndarray
    n_obs: int
    istop: int
    n_iter: int
    residual_norm: float
    max_abs_fe_orthogonality: float


@dataclass
class PredictionBundle:
    prediction: np.ndarray
    diagnostics: dict


def _setup_publication_style() -> None:
    mpl.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans"],
            "axes.labelsize": 8.5,
            "axes.titlesize": 8.5,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "xtick.major.size": 3.0,
            "ytick.major.size": 3.0,
            "xtick.direction": "out",
            "ytick.direction": "out",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )


def _style_axis(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("black")
        ax.spines[spine].set_linewidth(0.8)
    ax.tick_params(colors="black", direction="out", width=0.8, length=3.0)


def _save_figure(fig: plt.Figure, base: Path) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
    fig.savefig(base.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(base.with_suffix(".png"), dpi=600, bbox_inches="tight")


def _dense_index_map(version_root: Path, grid_type: str) -> tuple[np.ndarray, dict[int, int]]:
    nodes = pd.read_parquet(version_root / "data" / f"qsm_input_nodes_{grid_type}.parquet")
    nodes = nodes.copy()
    nodes["node_i"] = pd.to_numeric(nodes["node_i"], errors="coerce")
    nodes = nodes.dropna(subset=["node_i"])
    nodes["node_i"] = nodes["node_i"].astype(int)
    if "keep_for_model" in nodes.columns:
        nodes = nodes[nodes["keep_for_model"] == 1]
    nodes = nodes.sort_values("node_i").reset_index(drop=True)
    raw_to_dense = {int(raw): int(idx) for idx, raw in enumerate(nodes["node_i"])}
    return nodes["grid_id"].astype(str).to_numpy(), raw_to_dense


def load_od_universe(version_root: Path, grid_type: str, n_nodes: int) -> tuple[np.ndarray, dict]:
    """Recover the exported OD sample universe before absent cells become zeros."""
    _, raw_to_dense = _dense_index_map(version_root, grid_type)
    od = pd.read_parquet(version_root / "data" / f"qsm_input_od_{grid_type}.parquet")
    origin = pd.to_numeric(od["origin_node"], errors="coerce")
    destination = pd.to_numeric(od["destination_node"], errors="coerce")
    keep = origin.notna() & destination.notna()
    origin = origin[keep].astype(int)
    destination = destination[keep].astype(int)
    in_model = origin.isin(raw_to_dense) & destination.isin(raw_to_dense)
    origin = origin[in_model].map(raw_to_dense).to_numpy(dtype=int)
    destination = destination[in_model].map(raw_to_dense).to_numpy(dtype=int)

    universe = np.zeros((n_nodes, n_nodes), dtype=bool)
    universe[origin, destination] = True
    meta = {
        "raw_od_rows": int(len(od)),
        "mapped_od_rows": int(len(origin)),
        "unique_od_cells": int(universe.sum()),
        "duplicate_mapped_rows": int(len(origin) - universe.sum()),
        "sample_definition_values": sorted(od.get("sample_definition", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        "road_mode_rule_values": sorted(od.get("road_mode_rule", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
    }
    return universe, meta


def _load_parameters(stage2_root: Path) -> pm.Parameters:
    obj = json.loads((stage2_root / "parameters.json").read_text())
    return replace(
        pm.Parameters(),
        theta=float(obj["theta"]),
        alpha=float(obj["alpha"]),
        beta=float(obj["beta"]),
    )


def _load_equilibrium_prediction(
    prim: bp.Primitives,
    stage2_root: Path,
    label: str,
    abar_theta: np.ndarray,
    params: pm.Parameters,
) -> tuple[np.ndarray, np.ndarray, dict]:
    root = stage2_root / label
    with np.load(root / "equilibrium_arrays.npz") as arrays:
        values = {name: arrays[name].copy() for name in arrays.files}
    meta = json.loads((root / "equilibrium_meta.json").read_text())
    kappa = float(meta["kappa"])
    adjacency = _build_A_matrix(
        prim.edge_i,
        prim.edge_j,
        values["tau_min_kl"],
        prim.n_nodes,
        kappa=kappa,
    )
    tau_inv_theta = _compute_tau_inv_theta(adjacency)
    result = ft.StaticEquilibriumResult(
        label=str(meta["label"]),
        tau_min_kl=values["tau_min_kl"],
        tau_ij_inv_theta=tau_inv_theta,
        l_R=values["l_R"],
        l_F=values["l_F"],
        Pi_i=values["Pi_i"],
        P_j=values["P_j"],
        Wbar=float(meta["Wbar"]),
        n_iter=int(meta["n_outer"]),
        converged=bool(meta["converged"]),
        max_log_change=float(meta["max_log_change"]),
        kappa=kappa,
    )
    prediction = ft.predicted_od_shares(result, abar_theta, params) * prim.Lbar
    return prediction, values["Xi_kl"], meta


def aa_equation35_prediction(
    prim: bp.Primitives,
    edge_traffic: np.ndarray,
    label: str,
) -> PredictionBundle:
    """Recover commuting OD flows from link traffic using AA equation (35)."""
    n = prim.n_nodes
    traffic = np.asarray(edge_traffic, dtype=float)
    if traffic.shape != (prim.n_edges,):
        raise ValueError(f"{label}: edge traffic shape {traffic.shape} != {(prim.n_edges,)}")
    if not np.all(np.isfinite(traffic)) or np.any(traffic < 0):
        raise ValueError(f"{label}: edge traffic must be finite and nonnegative")

    xi = np.zeros((n, n), dtype=float)
    np.add.at(xi, (prim.edge_i, prim.edge_j), traffic)

    l_r = np.asarray(prim.L_R_obs, dtype=float).copy()
    l_f = np.asarray(prim.L_F_obs, dtype=float).copy()
    target = 0.5 * (float(l_r.sum()) + float(l_f.sum()))
    l_r *= target / max(float(l_r.sum()), EPS)
    l_f *= target / max(float(l_f.sum()), EPS)

    avg_pop = 0.5 * (l_r + l_f)
    avg_flow = 0.5 * (xi.sum(axis=1) + xi.sum(axis=0))
    system = np.diag(avg_pop + avg_flow) - xi
    inverse = np.linalg.solve(system, np.eye(n))
    prediction = inverse * l_r[:, None] * l_f[None, :]

    negative = prediction < 0
    material_negative = prediction < -1e-10
    prediction = np.where(negative & ~material_negative, 0.0, prediction)
    diagnostics = {
        "label": label,
        "edge_traffic_min": float(traffic.min()),
        "edge_traffic_median": float(np.median(traffic)),
        "edge_traffic_mean": float(traffic.mean()),
        "edge_traffic_max": float(traffic.max()),
        "edge_traffic_sum": float(traffic.sum()),
        "system_condition_number": float(np.linalg.cond(system)),
        "inverse_min": float(inverse.min()),
        "prediction_min_before_roundoff_clip": float(np.min(inverse * l_r[:, None] * l_f[None, :])),
        "n_negative_predictions": int(negative.sum()),
        "n_material_negative_predictions": int(material_negative.sum()),
        "n_positive_predictions": int((prediction > 0).sum()),
        "prediction_sum": float(prediction.sum()),
    }
    return PredictionBundle(prediction=prediction, diagnostics=diagnostics)


def _two_way_projection(
    values: np.ndarray,
    sample_mask: np.ndarray,
    *,
    atol: float = 1e-12,
    btol: float = 1e-12,
) -> ProjectionResult:
    """Project log values on origin and destination fixed effects."""
    values = np.asarray(values, dtype=float)
    valid = sample_mask & np.isfinite(values) & (values > 0)
    origin, destination = np.where(valid)
    y = np.log(values[valid])
    n_obs = len(y)
    n_nodes = values.shape[0]
    if n_obs < 2:
        raise ValueError("two-way projection requires at least two observations")

    rows = np.repeat(np.arange(n_obs), 2)
    columns = np.empty(2 * n_obs, dtype=int)
    columns[0::2] = origin
    columns[1::2] = n_nodes + destination
    design = sparse.coo_matrix(
        (np.ones(2 * n_obs), (rows, columns)),
        shape=(n_obs, 2 * n_nodes),
    ).tocsr()
    solution = lsmr(
        design,
        y,
        atol=atol,
        btol=btol,
        conlim=1e12,
        maxiter=10000,
    )
    residual_values = y - design @ solution[0]
    residual = np.full(values.shape, np.nan, dtype=float)
    residual[valid] = residual_values
    orthogonality = float(np.max(np.abs(design.T @ residual_values)))
    return ProjectionResult(
        residual=residual,
        mask=valid,
        n_obs=n_obs,
        istop=int(solution[1]),
        n_iter=int(solution[2]),
        residual_norm=float(solution[3]),
        max_abs_fe_orthogonality=orthogonality,
    )


def _pair_statistics(observed_resid: np.ndarray, predicted_resid: np.ndarray) -> dict:
    mask = np.isfinite(observed_resid) & np.isfinite(predicted_resid)
    x = predicted_resid[mask]
    y = observed_resid[mask]
    if len(x) < 2:
        raise ValueError("pair statistics require at least two matched residuals")
    correlation = float(np.corrcoef(x, y)[0, 1])
    design = np.column_stack([np.ones(len(x)), x])
    coefficients, *_ = np.linalg.lstsq(design, y, rcond=None)
    fitted = design @ coefficients
    residual = y - fitted
    return {
        "n_pairwise": int(len(x)),
        "correlation": correlation,
        "r_squared": correlation**2,
        "lfit_intercept": float(coefficients[0]),
        "lfit_slope": float(coefficients[1]),
        "lfit_rmse": float(np.sqrt(np.mean(residual**2))),
        "pred_residual_mean": float(np.mean(x)),
        "pred_residual_sd": float(np.std(x)),
        "obs_residual_mean": float(np.mean(y)),
        "obs_residual_sd": float(np.std(y)),
    }


def _raw_log_correlation(observed: np.ndarray, predicted: np.ndarray, mask: np.ndarray) -> float:
    valid = mask & np.isfinite(observed) & np.isfinite(predicted) & (observed > 0) & (predicted > 0)
    if int(valid.sum()) < 2:
        return float("nan")
    return float(np.corrcoef(np.log(observed[valid]), np.log(predicted[valid]))[0, 1])


def evaluate_prediction(
    label: str,
    observed: np.ndarray,
    predicted: np.ndarray,
    universe: np.ndarray,
    observed_projection_cache: dict[str, ProjectionResult],
) -> tuple[list[dict], ProjectionResult, ProjectionResult]:
    """Run the exact AA procedure and retained sample robustness checks."""
    n = observed.shape[0]
    off_diagonal = ~np.eye(n, dtype=bool)
    variants = [
        ("aa_exact_all_positive", universe & (observed > 0), universe & (predicted > 0), "separate"),
        ("aa_exact_off_diagonal", universe & off_diagonal & (observed > 0), universe & off_diagonal & (predicted > 0), "separate"),
        ("aa_exact_observed_ge_1", universe & (observed >= 1.0), universe & (predicted > 0), "separate"),
    ]

    rows: list[dict] = []
    main_observed: ProjectionResult | None = None
    main_predicted: ProjectionResult | None = None
    for variant, observed_mask, predicted_mask, residualization in variants:
        if variant not in observed_projection_cache:
            observed_projection_cache[variant] = _two_way_projection(observed, observed_mask)
        obs_projection = observed_projection_cache[variant]
        pred_projection = _two_way_projection(predicted, predicted_mask)
        pair = _pair_statistics(obs_projection.residual, pred_projection.residual)
        matched = np.isfinite(obs_projection.residual) & np.isfinite(pred_projection.residual)
        row = {
            "prediction": label,
            "sample_variant": variant,
            "residualization": residualization,
            "n_od_universe": int(universe.sum()),
            "n_observed_projection": int(obs_projection.n_obs),
            "n_predicted_projection": int(pred_projection.n_obs),
            "n_pairwise": pair["n_pairwise"],
            "observed_positive_coverage_pct": 100.0 * pair["n_pairwise"] / max(obs_projection.n_obs, 1),
            "raw_log_correlation_pairwise": _raw_log_correlation(observed, predicted, matched),
            "obs_projection_iterations": int(obs_projection.n_iter),
            "pred_projection_iterations": int(pred_projection.n_iter),
            "obs_fe_orthogonality_max_abs": obs_projection.max_abs_fe_orthogonality,
            "pred_fe_orthogonality_max_abs": pred_projection.max_abs_fe_orthogonality,
            **pair,
        }
        rows.append(row)

        common_mask = observed_mask & predicted_mask
        obs_common = _two_way_projection(observed, common_mask)
        pred_common = _two_way_projection(predicted, common_mask)
        common_pair = _pair_statistics(obs_common.residual, pred_common.residual)
        rows.append(
            {
                "prediction": label,
                "sample_variant": variant.replace("aa_exact", "common_sample"),
                "residualization": "common",
                "n_od_universe": int(universe.sum()),
                "n_observed_projection": int(obs_common.n_obs),
                "n_predicted_projection": int(pred_common.n_obs),
                "n_pairwise": common_pair["n_pairwise"],
                "observed_positive_coverage_pct": 100.0 * common_pair["n_pairwise"] / max(int(observed_mask.sum()), 1),
                "raw_log_correlation_pairwise": _raw_log_correlation(observed, predicted, common_mask),
                "obs_projection_iterations": int(obs_common.n_iter),
                "pred_projection_iterations": int(pred_common.n_iter),
                "obs_fe_orthogonality_max_abs": obs_common.max_abs_fe_orthogonality,
                "pred_fe_orthogonality_max_abs": pred_common.max_abs_fe_orthogonality,
                **common_pair,
            }
        )

        if variant == "aa_exact_all_positive":
            main_observed = obs_projection
            main_predicted = pred_projection

    if main_observed is None or main_predicted is None:
        raise RuntimeError("main AA projection was not constructed")
    return rows, main_observed, main_predicted


def reproduce_aa_seattle_reference(repo_root: Path) -> dict:
    """Replicate the paper's 0.4335 Seattle correlation from local source files."""
    root = repo_root / "Literatures" / "AA_ReplicationFinal"
    derived = root / "data" / "seattle" / "derived"
    required = [
        derived / "adjacency_mat_gridcity.dta",
        derived / "index_id_xwalk.dta",
        derived / "predicted_lij.csv",
    ]
    if not all(path.exists() for path in required):
        return {"available": False, "required_files": [str(path) for path in required]}

    frame = pd.read_stata(required[0])
    crosswalk = pd.read_stata(required[1])
    predicted = pd.read_csv(required[2], header=None, names=["h_index", "w_index", "predicted"])
    frame = frame.merge(crosswalk, on=["h_id", "w_id"], validate="1:1")
    frame = frame.merge(predicted, on=["h_index", "w_index"], validate="1:1")

    h_codes, h_levels = pd.factorize(frame["h_id"], sort=True)
    w_codes, w_levels = pd.factorize(frame["w_id"], sort=True)
    n_h = len(h_levels)
    n_w = len(w_levels)
    observed_matrix = np.zeros((n_h, n_w), dtype=float)
    predicted_matrix = np.zeros((n_h, n_w), dtype=float)
    universe = np.zeros((n_h, n_w), dtype=bool)
    observed_matrix[h_codes, w_codes] = frame["grid_commuters"].to_numpy(dtype=float)
    predicted_matrix[h_codes, w_codes] = frame["predicted"].to_numpy(dtype=float)
    universe[h_codes, w_codes] = True

    observed_projection = _two_way_projection(observed_matrix, universe & (observed_matrix > 0))
    predicted_projection = _two_way_projection(predicted_matrix, universe & (predicted_matrix > 0))
    pair = _pair_statistics(observed_projection.residual, predicted_projection.residual)
    return {
        "available": True,
        "correlation": pair["correlation"],
        "reported_correlation": AA_REFERENCE_CORRELATION,
        "absolute_difference": abs(pair["correlation"] - AA_REFERENCE_CORRELATION),
        "n_universe": int(universe.sum()),
        "n_observed_positive": int((universe & (observed_matrix > 0)).sum()),
        "n_predicted_positive": int((universe & (predicted_matrix > 0)).sum()),
        "n_pairwise": pair["n_pairwise"],
        "observed_projection_iterations": observed_projection.n_iter,
        "predicted_projection_iterations": predicted_projection.n_iter,
    }


def _plot_panel(
    ax: plt.Axes,
    observed_resid: np.ndarray,
    predicted_resid: np.ndarray,
    title: str,
    panel: str,
    correlation: float,
    *,
    central: bool,
) -> None:
    mask = np.isfinite(observed_resid) & np.isfinite(predicted_resid)
    x_full = predicted_resid[mask]
    y_full = observed_resid[mask]
    if central:
        x_lo, x_hi = np.quantile(x_full, [0.005, 0.995])
        y_lo, y_hi = np.quantile(y_full, [0.005, 0.995])
        plot_mask = (x_full >= x_lo) & (x_full <= x_hi) & (y_full >= y_lo) & (y_full <= y_hi)
        x = x_full[plot_mask]
        y = y_full[plot_mask]
    else:
        x, y = x_full, y_full

    ax.scatter(
        x,
        y,
        s=2.0,
        alpha=0.09,
        color="#4C78A8",
        linewidths=0,
        rasterized=True,
    )
    fit_x = np.linspace(float(np.min(x)), float(np.max(x)), 200)
    coefficients, *_ = np.linalg.lstsq(
        np.column_stack([np.ones(len(x_full)), x_full]),
        y_full,
        rcond=None,
    )
    ax.plot(fit_x, coefficients[0] + coefficients[1] * fit_x, color="#202020", linewidth=1.1)
    ax.axhline(0.0, color="#BAB0AC", linewidth=0.6, zorder=0)
    ax.axvline(0.0, color="#BAB0AC", linewidth=0.6, zorder=0)
    ax.set_title(title, loc="left", pad=5)
    ax.set_xlabel("Predicted log-flow residual")
    ax.set_ylabel("Observed road-commuting log-flow residual")
    ax.text(
        0.98,
        0.03,
        f"Correlation = {correlation:.3f}\nN = {len(x_full):,}",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=7.5,
        color="black",
    )
    if panel:
        ax.text(
            0.02,
            0.97,
            panel,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=11,
            fontweight="bold",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.82, "pad": 1.0},
        )
    _style_axis(ax)


def plot_validation(
    out: Path,
    residuals: dict[str, tuple[np.ndarray, np.ndarray]],
    correlations: dict[str, float],
) -> None:
    _setup_publication_style()
    specs = [
        ("structural_endogenous", "Current structural OD prediction", "a"),
        ("aa_eq35_endogenous_xi", "AA equation (35), baseline model-implied traffic", "b"),
    ]
    for central, suffix in ((False, "full"), (True, "central99")):
        fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.25))
        for ax, (spec, title, panel) in zip(axes, specs):
            observed_resid, predicted_resid = residuals[spec]
            _plot_panel(
                ax,
                observed_resid,
                predicted_resid,
                title,
                panel,
                correlations[spec],
                central=central,
            )
        if central:
            fig.text(
                0.5,
                -0.02,
                "Axes omit the outer 0.5% of each residual distribution; correlations use the full pairwise sample.",
                ha="center",
                va="top",
                fontsize=7.0,
            )
        fig.subplots_adjust(left=0.09, right=0.99, bottom=0.18, top=0.88, wspace=0.32)
        _save_figure(fig, out / "figures" / f"figure_od_validation_aa_style_{suffix}")
        plt.close(fig)

    observed_resid, predicted_resid = residuals["structural_endogenous"]
    fig, ax = plt.subplots(figsize=(4.8, 3.15))
    _plot_panel(
        ax,
        observed_resid,
        predicted_resid,
        "Current structural OD prediction",
        "",
        correlations["structural_endogenous"],
        central=True,
    )
    fig.subplots_adjust(left=0.14, right=0.99, bottom=0.18, top=0.88)
    _save_figure(fig, out / "figures" / "figure_od_validation_structural_central99")
    plt.close(fig)


def _write_pair_data(
    out: Path,
    prim: bp.Primitives,
    universe: np.ndarray,
    observed: np.ndarray,
    predictions: dict[str, np.ndarray],
    main_residuals: dict[str, tuple[ProjectionResult, ProjectionResult]],
) -> None:
    origin, destination = np.where(universe)
    frame = pd.DataFrame(
        {
            "origin_idx": origin,
            "destination_idx": destination,
            "origin_grid_id": prim.node_grid_ids[origin],
            "destination_grid_id": prim.node_grid_ids[destination],
            "is_intrazonal": origin == destination,
            "observed_commuters_road": observed[origin, destination],
        }
    )
    for label, matrix in predictions.items():
        frame[f"pred_{label}"] = matrix[origin, destination]
    observed_projection = next(iter(main_residuals.values()))[0]
    frame["resid_observed_aa_exact"] = observed_projection.residual[origin, destination]
    for label, (_, predicted_projection) in main_residuals.items():
        frame[f"resid_pred_{label}_aa_exact"] = predicted_projection.residual[origin, destination]
        frame[f"in_pairwise_{label}"] = (
            np.isfinite(observed_projection.residual[origin, destination])
            & np.isfinite(predicted_projection.residual[origin, destination])
        )
    frame.to_parquet(out / "metrics" / "od_validation_pairs.parquet", index=False)


def _write_documentation(out: Path, meta: dict, main: pd.DataFrame) -> None:
    structural = main.loc[main["prediction"] == "structural_endogenous"].iloc[0]
    equation35 = main.loc[main["prediction"] == "aa_eq35_endogenous_xi"].iloc[0]
    speed_proxy = main.loc[
        main["prediction"] == "aa_eq35_speed_proxy_delta0488"
    ].iloc[0]
    lanes_placebo = main.loc[
        main["prediction"] == "aa_eq35_lanes_only_placebo"
    ].iloc[0]
    readme = f"""# AA-style commuting OD validation

## Purpose

This package compares observed Beijing road-commuting origin--destination flows with two model predictions after absorbing origin and destination fixed effects. The procedure reproduces Allen and Arkolakis (2022), Figure 2(b), before it is applied to the locked gate5 mainline.

## Main results

The current endogenous baseline OD prediction has a fixed-effect residual correlation of `{structural['correlation']:.3f}` with observed road commuting (`N={int(structural['n_pairwise']):,}`). Applying equation (35) to baseline model-implied link traffic gives `{equation35['correlation']:.3f}` (`N={int(equation35['n_pairwise']):,}`). The bundled Seattle replication reproduces `{meta['aa_reference']['correlation']:.6f}`, compared with the paper's reported `0.4335`.

These correlations measure bilateral sorting after fixed effects, not one-for-one calibration. The residual-fit slopes are `{structural['lfit_slope']:.3f}` for the structural prediction and `{equation35['lfit_slope']:.3f}` for equation (35), so predicted pair residuals are substantially more dispersed than observed residuals.

## Scope

The structural OD comparison is the direct validation of the current model output. The equation (35) result is an internal consistency check because Beijing link traffic is model-implied rather than observed AADT. The speed-implied traffic proxy yields a residual correlation of `{speed_proxy['correlation']:.3f}`, compared with `{lanes_placebo['correlation']:.3f}` for the lanes-only placebo. The strict inversion therefore adds no detectable pair-level fit from the current congestion proxy beyond the network and lane structure.

## Main outputs

- `metrics/summary_main.csv`: exact AA residual correlations for all prediction objects.
- `metrics/summary_robustness.csv`: common-sample, off-diagonal, and observed-flow-threshold variants.
- `metrics/od_validation_pairs.parquet`: pair-level flows, predictions, and residuals.
- `metrics/qa_checks.csv`: method-replication and numerical checks.
- `figures/figure_od_validation_aa_style_full.*`: full-range validation scatter.
- `figures/figure_od_validation_aa_style_central99.*`: central-distribution version for display.
"""
    (out / "README.md").write_text(readme)

    docs = {
        "analysis-plan.md": """# Analysis plan

Recover the current baseline OD prediction and an equation (35) OD prediction from the locked gate5 run. Compare each object with observed road commuting after separate origin and destination fixed-effect projections. Replicate the published Seattle correlation with the bundled source files before interpreting the Beijing results.
""",
        "variable-dictionary.md": """# Variable dictionary

- `observed_commuters_road`: car-mode commuting flow in the exported reachable-AM OD sample.
- `pred_structural_endogenous`: workplace-choice flow implied by the current endogenous baseline equilibrium.
- `pred_aa_eq35_endogenous_xi`: equation (35) flow recovered from current baseline model-implied link traffic.
- `pred_aa_eq35_speed_proxy_delta0488`: equation (35) flow recovered from a speed-implied traffic proxy with delta1 equal to 0.488.
- `pred_aa_eq35_lanes_only_placebo`: equation (35) flow using directed lanes as a network placebo.
- `resid_observed_aa_exact`: log observed commuting after origin and destination fixed effects.
- `resid_pred_*_aa_exact`: log predicted commuting after origin and destination fixed effects.
""",
        "sample-lineage.md": f"""# Sample lineage

The locked `{meta['version_id']}` square-grid bundle supplies `{meta['n_nodes']:,}` nodes and `{meta['n_edges']:,}` directed links. The exported reachable-AM file contains `{meta['od_universe']['unique_od_cells']:,}` unique OD cells. Among them, `{meta['n_observed_positive']:,}` have positive car-mode commuting flow. Absent OD rows are excluded from the fixed-effect projection rather than recoded as observed zeros.
""",
        "geospatial-data-notes.md": """# Geospatial data notes

Origins and destinations are nodes in the locked gate5 square-grid network. Directed links retain AM travel time, late-night free-flow time, and directional lanes. The OD validation does not rematch routes or alter the gate5 detour restriction. Model-unreachable predicted cells remain outside the pairwise equation (35) correlation when their recovered flow is zero.
""",
        "regression-spec-matrix.md": """# Regression specification matrix

For each prediction object, log observed flow is projected on origin and destination fixed effects over positive observed cells. Log predicted flow is projected separately over positive predicted cells in the exported OD universe, matching the published code. The reported statistic is the unweighted Pearson correlation between pairwise non-missing residuals. No controls, weights, clustered standard errors, or causal interpretation enter this validation.
""",
        "table-shells.md": """# Table shells

## Main OD validation

Report prediction object, link-traffic source, observed projection sample, predicted projection sample, pairwise sample, residual correlation, fitted slope, and pairwise coverage.

## Robustness

Report the common-sample projection, off-diagonal sample, observed flow of at least one commuter, speed-implied traffic proxy, and lanes-only placebo.
""",
        "replication-checklist.md": """# Replication checklist

- Reproduce the Seattle fixed-effect residual correlation within 0.0005 of 0.4335.
- Confirm that the OD universe is read from exported rows before dense zero filling.
- Confirm that observed residence and workplace marginals add to the same commuter total.
- Confirm convergence of the retained Stage 2 baselines.
- Confirm fixed-effect orthogonality below the recorded numerical tolerance.
- Confirm that equation (35) has no materially negative predicted cells.
- Inspect PDF, SVG, and PNG exports for text overlap and residual-tail compression.
""",
    }
    for filename, text in docs.items():
        (out / filename).write_text(text)


def _qa_table(
    aa_reference: dict,
    prim: bp.Primitives,
    universe: np.ndarray,
    observed: np.ndarray,
    prediction_diagnostics: dict[str, dict],
    summary: pd.DataFrame,
    equilibrium_meta: dict[str, dict],
) -> pd.DataFrame:
    checks: list[dict] = []

    def add(check: str, passed: bool, value, tolerance: str) -> None:
        checks.append({"check": check, "passed": bool(passed), "value": value, "tolerance": tolerance})

    add(
        "aa_seattle_reference_correlation",
        bool(aa_reference.get("available")) and aa_reference.get("absolute_difference", np.inf) <= 5e-4,
        aa_reference.get("correlation"),
        "absolute difference from 0.4335 <= 0.0005",
    )
    add(
        "observed_marginals_add_up",
        abs(float(prim.L_R_obs.sum() - prim.L_F_obs.sum())) <= 1e-8,
        float(prim.L_R_obs.sum() - prim.L_F_obs.sum()),
        "absolute gap <= 1e-8",
    )
    add(
        "positive_observed_cells_within_universe",
        int(((observed > 0) & ~universe).sum()) == 0,
        int(((observed > 0) & ~universe).sum()),
        "zero",
    )
    for label, meta in equilibrium_meta.items():
        add(
            f"{label}_converged",
            bool(meta["converged"]),
            bool(meta["converged"]),
            "True",
        )
    for label, diag in prediction_diagnostics.items():
        if "n_material_negative_predictions" in diag:
            add(
                f"{label}_no_material_negative_predictions",
                int(diag["n_material_negative_predictions"]) == 0,
                int(diag["n_material_negative_predictions"]),
                "zero",
            )
    main = summary[summary["sample_variant"] == "aa_exact_all_positive"]
    add(
        "fixed_effect_projection_orthogonality",
        float(main[["obs_fe_orthogonality_max_abs", "pred_fe_orthogonality_max_abs"]].to_numpy().max()) <= 1e-5,
        float(main[["obs_fe_orthogonality_max_abs", "pred_fe_orthogonality_max_abs"]].to_numpy().max()),
        "max absolute FE cross-product <= 1e-5",
    )
    return pd.DataFrame(checks)


def run_validation(
    data_work: Path,
    *,
    version_id: str,
    stage2_name: str,
    output_name: str,
    grid_type: str,
) -> None:
    version_root = data_work / "outputs" / version_id
    stage2_root = data_work / "outputs" / stage2_name
    out = data_work / "outputs" / output_name
    (out / "metrics").mkdir(parents=True, exist_ok=True)
    (out / "figures").mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(version_root, grid_type=grid_type, period="AM")
    universe, universe_meta = load_od_universe(version_root, grid_type, prim.n_nodes)
    observed = np.asarray(prim.L_ij_obs, dtype=float)
    params = _load_parameters(stage2_root)
    with np.load(stage2_root / "fundamentals" / "fundamentals_arrays.npz") as arrays:
        abar_theta = arrays["abar_theta"].copy()

    structural_endogenous, xi_endogenous, meta_endogenous = _load_equilibrium_prediction(
        prim,
        stage2_root,
        "baseline_endogenous",
        abar_theta,
        params,
    )
    structural_exogenous, xi_exogenous, meta_exogenous = _load_equilibrium_prediction(
        prim,
        stage2_root,
        "baseline_exogenous_tau",
        abar_theta,
        params,
    )

    aa_endogenous = aa_equation35_prediction(prim, xi_endogenous, "aa_eq35_endogenous_xi")
    aa_exogenous = aa_equation35_prediction(prim, xi_exogenous, "aa_eq35_exogenous_xi")
    congestion_ratio = np.maximum(
        np.asarray(prim.tau_obs_min, dtype=float) / np.clip(prim.tau_ff_min, EPS, None),
        1.0,
    )
    speed_proxy = np.asarray(prim.n_kl, dtype=float) * congestion_ratio ** (1.0 / AA_DELTA1)
    aa_speed = aa_equation35_prediction(
        prim,
        speed_proxy,
        "aa_eq35_speed_proxy_delta0488",
    )
    aa_lanes = aa_equation35_prediction(
        prim,
        np.asarray(prim.n_kl, dtype=float),
        "aa_eq35_lanes_only_placebo",
    )

    predictions = {
        "structural_endogenous": structural_endogenous,
        "structural_exogenous_tau": structural_exogenous,
        "aa_eq35_endogenous_xi": aa_endogenous.prediction,
        "aa_eq35_exogenous_xi": aa_exogenous.prediction,
        "aa_eq35_speed_proxy_delta0488": aa_speed.prediction,
        "aa_eq35_lanes_only_placebo": aa_lanes.prediction,
    }
    prediction_diagnostics = {
        "structural_endogenous": {
            "prediction_sum": float(structural_endogenous.sum()),
            "n_positive_predictions": int((structural_endogenous > 0).sum()),
            "traffic_source": "current endogenous baseline equilibrium",
        },
        "structural_exogenous_tau": {
            "prediction_sum": float(structural_exogenous.sum()),
            "n_positive_predictions": int((structural_exogenous > 0).sum()),
            "traffic_source": "current fixed-observed-time baseline equilibrium",
        },
        "aa_eq35_endogenous_xi": aa_endogenous.diagnostics,
        "aa_eq35_exogenous_xi": aa_exogenous.diagnostics,
        "aa_eq35_speed_proxy_delta0488": aa_speed.diagnostics,
        "aa_eq35_lanes_only_placebo": aa_lanes.diagnostics,
    }

    observed_projection_cache: dict[str, ProjectionResult] = {}
    all_rows: list[dict] = []
    main_residuals: dict[str, tuple[ProjectionResult, ProjectionResult]] = {}
    for label, prediction in predictions.items():
        rows, observed_projection, predicted_projection = evaluate_prediction(
            label,
            observed,
            prediction,
            universe,
            observed_projection_cache,
        )
        all_rows.extend(rows)
        main_residuals[label] = (observed_projection, predicted_projection)

    summary = pd.DataFrame(all_rows)
    main = summary[summary["sample_variant"] == "aa_exact_all_positive"].copy()
    main["role"] = main["prediction"].map(
        {
            "structural_endogenous": "primary current-model validation",
            "structural_exogenous_tau": "baseline robustness",
            "aa_eq35_endogenous_xi": "strict AA equation (35), internal consistency",
            "aa_eq35_exogenous_xi": "strict AA equation (35), baseline robustness",
            "aa_eq35_speed_proxy_delta0488": "speed-implied traffic diagnostic",
            "aa_eq35_lanes_only_placebo": "network and lanes placebo",
        }
    )
    main.to_csv(out / "metrics" / "summary_main.csv", index=False)
    summary.to_csv(out / "metrics" / "summary_robustness.csv", index=False)
    pd.DataFrame.from_dict(prediction_diagnostics, orient="index").rename_axis("prediction").reset_index().to_csv(
        out / "metrics" / "prediction_diagnostics.csv",
        index=False,
    )

    aa_reference = reproduce_aa_seattle_reference(data_work.parent)
    equilibrium_meta = {
        "baseline_endogenous": meta_endogenous,
        "baseline_exogenous_tau": meta_exogenous,
    }
    qa = _qa_table(
        aa_reference,
        prim,
        universe,
        observed,
        prediction_diagnostics,
        summary,
        equilibrium_meta,
    )
    qa.to_csv(out / "metrics" / "qa_checks.csv", index=False)

    _write_pair_data(out, prim, universe, observed, predictions, main_residuals)

    correlation_map = dict(zip(main["prediction"], main["correlation"]))
    plot_residuals = {
        label: (pair[0].residual, pair[1].residual)
        for label, pair in main_residuals.items()
    }
    plot_validation(out, plot_residuals, correlation_map)

    meta = {
        "script": str(Path(__file__).resolve()),
        "version_id": version_id,
        "stage2_name": stage2_name,
        "output_name": output_name,
        "grid_type": grid_type,
        "period": "AM",
        "n_nodes": prim.n_nodes,
        "n_edges": prim.n_edges,
        "Lbar": prim.Lbar,
        "n_observed_positive": int((universe & (observed > 0)).sum()),
        "od_universe": universe_meta,
        "aa_reference": aa_reference,
        "parameters": {
            "theta": params.theta,
            "alpha": params.alpha,
            "beta": params.beta,
            "aa_delta1_for_speed_proxy": AA_DELTA1,
        },
        "method": {
            "observed_projection": "log positive observed flow on origin and destination fixed effects",
            "predicted_projection": "log positive predicted flow on origin and destination fixed effects",
            "projection_samples": "separate, matching AA analysis/results.do",
            "reported_statistic": "unweighted Pearson correlation on pairwise non-missing residuals",
            "od_universe_rule": "rows exported in qsm_input_od_square.parquet; absent cells excluded",
        },
        "prediction_diagnostics": prediction_diagnostics,
        "all_qa_passed": bool(qa["passed"].all()),
    }
    (out / "run_meta.json").write_text(json.dumps(meta, indent=2, default=float))
    _write_documentation(out, meta, main)

    print(main[["prediction", "n_pairwise", "correlation", "observed_positive_coverage_pct"]].to_string(index=False))
    print(f"aa_reference_correlation={aa_reference.get('correlation')}")
    print(f"all_qa_passed={bool(qa['passed'].all())}")
    print(f"saved={out}")


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-work", default=str(DATA_WORK))
    parser.add_argument("--version-id", default=DEFAULT_VERSION)
    parser.add_argument("--stage2-name", default=DEFAULT_STAGE2)
    parser.add_argument("--output-name", default=DEFAULT_OUTPUT)
    parser.add_argument("--grid-type", default="square")
    args = parser.parse_args(argv)
    run_validation(
        Path(args.data_work).resolve(),
        version_id=args.version_id,
        stage2_name=args.stage2_name,
        output_name=args.output_name,
        grid_type=args.grid_type,
    )


if __name__ == "__main__":
    main()
