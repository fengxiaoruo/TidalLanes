"""Welfare decomposition + annotated sorting maps (gate5, dual alpha tracks).

Outputs:
  - Node maps: Δlog l_R, Δlog l_F with treated-corridor endpoint overlays
  - Wbar_theta = Σ u_i Π_i^{-θ} decomposition (composition-fixed vs sorting)
  - Π^{-θ} sequential decomposition: τ | l^0  →  +Δl_F  →  +Δl_R
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import invert_fundamentals as inv  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation import stage2_counterfactuals as s2  # noqa: E402
from src.model.full_estimation.counterfactual_levels import select_tidal_lane_pairs_by_rank  # noqa: E402
from src.model.full_estimation.equilibrium_solve import (  # noqa: E402
    MAINLINE_PAIR_SELECTION,
    compute_observation_anchors,
)
from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    DEFAULT_TOP_K,
    DEFAULT_VERSION,
    data_work_root,
    grid_xy,
    node_change_frame,
    write_json,
)
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import (  # noqa: E402
    solver_overrides_for_params,
)

LOG = logging.getLogger("welfare_sorting_diag")
EPS = 1e-12
PRESLIDE_FIG_DIR = DATA_WORK.parent / "Documents" / "PreSlides" / "figs"

TRACKS = [
    {"key": "seattle", "alpha": -0.12, "beta": -0.10},
    {"key": "pos05", "alpha": 0.05, "beta": -0.10},
    {"key": "zero", "alpha": 0.0, "beta": 0.0},
]


def _params(track: dict) -> pm.Parameters:
    return replace(pm.Parameters(), alpha=float(track["alpha"]), beta=float(track["beta"]))


def _solve_track(prim: bp.Primitives, track: dict, *, top_k: int, pair_selection: str):
    params = _params(track)
    skw = solver_overrides_for_params(params)
    fund = inv.invert_fundamentals(prim, params, tol=1.5e-4, max_iter=1800, kappa=1.0)
    anchors = compute_observation_anchors(
        prim, fund.ubar_theta, fund.abar_theta, params, chi=fund.chi, kappa=fund.kappa,
    )
    kw = s2.build_congestion_solver_kwargs(
        prim, fund, params, congestion_form="anchored", congestion_lambda=1.1, **skw,
    )
    kw.update(
        xi_obs_kl=anchors["xi_obs_kl"],
        n_obs_kl=anchors["n_obs_kl"],
        tau_obs_anchor=anchors["tau_obs_anchor"],
    )
    base = s2.solve_baseline_endogenous(prim, fund, params, **kw)
    eq_st, _ = s2.run_tidal_endogenous(
        prim, fund, params, base, top_k=top_k, horizon="short_run",
        pair_selection=pair_selection, **kw,
    )
    eq_lt, sel = s2.run_tidal_endogenous(
        prim, fund, params, base, top_k=top_k, horizon="long_run",
        pair_selection=pair_selection, **kw,
    )
    return params, fund, base, eq_st, eq_lt, sel


def _u_theta(l_R: np.ndarray, fund: inv.InversionResult, params: pm.Parameters, lbar: float) -> np.ndarray:
    L_R = np.clip(l_R * lbar, EPS, None)
    return fund.ubar_theta * (L_R ** (params.beta * params.theta))


def _w_theta(l_F: np.ndarray, fund: inv.InversionResult, params: pm.Parameters, lbar: float) -> np.ndarray:
    L_F = np.clip(l_F * lbar, EPS, None)
    return fund.abar_theta * (L_F ** (params.alpha * params.theta))


def _primitive_welfare_theta(
    eq,
    fund: inv.InversionResult,
    params: pm.Parameters,
    prim: bp.Primitives,
) -> float:
    """Direct Frechet denominator: Σ_ij τ_ij^{-θ} u_i^θ w_j^θ."""
    u = _u_theta(eq.l_R, fund, params, prim.Lbar)
    w = _w_theta(eq.l_F, fund, params, prim.Lbar)
    return float(np.sum(eq.tau_ij_inv_theta * u[:, None] * w[None, :]))


def _primitive_od_shares(
    eq,
    fund: inv.InversionResult,
    params: pm.Parameters,
    prim: bp.Primitives,
) -> np.ndarray:
    u = _u_theta(eq.l_R, fund, params, prim.Lbar)
    w = _w_theta(eq.l_F, fund, params, prim.Lbar)
    mass = eq.tau_ij_inv_theta * u[:, None] * w[None, :]
    return mass / np.clip(float(np.sum(mass)), EPS, None)


def _bilateral_cost_min(eq, theta: float) -> np.ndarray:
    return np.clip(eq.tau_ij_inv_theta, EPS, None) ** (-1.0 / theta)


def primitive_welfare_summary(
    base,
    eq_st,
    eq_lt,
    fund: inv.InversionResult,
    params: pm.Parameters,
    prim: bp.Primitives,
) -> dict:
    """Primitive welfare and OD-weighted commute-cost changes."""
    th = params.theta
    wtheta0 = _primitive_welfare_theta(base, fund, params, prim)
    wtheta_st = _primitive_welfare_theta(eq_st, fund, params, prim)
    wtheta_lt = _primitive_welfare_theta(eq_lt, fund, params, prim)
    w0 = wtheta0 ** (1.0 / th)
    wst = wtheta_st ** (1.0 / th)
    wlt = wtheta_lt ** (1.0 / th)

    s0 = _primitive_od_shares(base, fund, params, prim)
    slt = _primitive_od_shares(eq_lt, fund, params, prim)
    t0 = _bilateral_cost_min(base, th)
    tst = _bilateral_cost_min(eq_st, th)
    tlt = _bilateral_cost_min(eq_lt, th)

    direct_st = float(np.sum(s0 * (tst - t0)))
    ge_wedge = float(np.sum(s0 * (tlt - tst)))
    sorting = float(np.sum((slt - s0) * tlt))
    total_lt = float(np.sum(slt * tlt - s0 * t0))

    return {
        "primitive_Wtheta_base": wtheta0,
        "primitive_Wtheta_st": wtheta_st,
        "primitive_Wtheta_lt": wtheta_lt,
        "primitive_W_base": w0,
        "primitive_W_st": wst,
        "primitive_W_lt": wlt,
        "primitive_W_pct_st": 100.0 * (wst / w0 - 1.0),
        "primitive_W_pct_lt": 100.0 * (wlt / w0 - 1.0),
        "primitive_avg_commute_cost_base_min": float(np.sum(s0 * t0)),
        "primitive_avg_commute_cost_lt_min": float(np.sum(slt * tlt)),
        "primitive_direct_st_cost_change_sec": 60.0 * direct_st,
        "primitive_ge_cost_wedge_sec": 60.0 * ge_wedge,
        "primitive_sorting_wedge_sec": 60.0 * sorting,
        "primitive_total_lt_cost_change_sec": 60.0 * total_lt,
    }


def decompose_wbar_theta(
    base,
    eq_st,
    eq_lt,
    fund: inv.InversionResult,
    params: pm.Parameters,
    prim: bp.Primitives,
) -> dict:
    """Gauge-free, fully-closed welfare decomposition on W̄^θ = Σ_i u_i^θ M_i,

    where M_i = (Σ_j τ_ij^{-θ} w_j^θ) is resident i's gauge-free market access.
    Sequential layers (each evaluated at the previous state, exact additive):

        ① ST τ          : Σ u0 (M_st     − M0)        [ST τ, base shares]
        ② GE τ beyond ST: Σ u0 (M_tauLT  − M_st)      [LT τ, base shares]
        ③ l_F sorting   : Σ u0 (M_lf     − M_tauLT)   [LT τ, LT l_F]
        ④ l_R crowding  : Σ (u_lt − u0) M_lf          [u^θ channel via β]

    ①+②+③+④ ≡ W̄^θ_lt − W̄^θ_base exactly (see check_layers_sum_minus_delta).
    No Π/P market-access scalars enter, so the result is gauge-invariant.
    """
    th = params.theta
    u0 = _u_theta(base.l_R, fund, params, prim.Lbar)
    u_lt = _u_theta(eq_lt.l_R, fund, params, prim.Lbar)
    w0 = _w_theta(base.l_F, fund, params, prim.Lbar)
    w_lt = _w_theta(eq_lt.l_F, fund, params, prim.Lbar)

    # M_i sequence (use each scenario's own gauge-free tau^{-θ} matrix)
    M0 = base.tau_ij_inv_theta @ w0          # base τ, base l_F
    M_st = eq_st.tau_ij_inv_theta @ w0       # ST τ,   base l_F
    M_tauLT = eq_lt.tau_ij_inv_theta @ w0    # LT τ,   base l_F
    M_lf = eq_lt.tau_ij_inv_theta @ w_lt     # LT τ,   LT l_F

    wtheta_base = float(np.sum(u0 * M0))
    wtheta_lt = float(np.sum(u_lt * M_lf))
    wtheta_st = float(np.sum(u0 * M_st))     # ST welfare = base shares, ST τ
    total = wtheta_lt - wtheta_base

    layer_st_tau = float(np.sum(u0 * (M_st - M0)))
    layer_ge_tau = float(np.sum(u0 * (M_tauLT - M_st)))
    layer_lF_sorting = float(np.sum(u0 * (M_lf - M_tauLT)))
    layer_lR_crowding = float(np.sum((u_lt - u0) * M_lf))
    layers_sum = layer_st_tau + layer_ge_tau + layer_lF_sorting + layer_lR_crowding

    return {
        "Wbar_theta_base": wtheta_base,
        "Wbar_theta_lt": wtheta_lt,
        "Wbar_theta_delta": total,
        "Wbar_theta_pct": 100.0 * total / max(wtheta_base, EPS),
        "Wbar_base": wtheta_base ** (1.0 / th),
        "Wbar_lt": wtheta_lt ** (1.0 / th),
        "Wbar_pct": 100.0 * ((wtheta_lt / wtheta_base) ** (1.0 / th) - 1.0),
        "Wbar_st_pct": 100.0 * ((wtheta_st / wtheta_base) ** (1.0 / th) - 1.0),
        "layer_st_tau": layer_st_tau,
        "layer_ge_tau": layer_ge_tau,
        "layer_lF_sorting": layer_lF_sorting,
        "layer_lR_crowding": layer_lR_crowding,
        "layers_sum": layers_sum,
        "check_layers_sum_minus_delta": layers_sum - total,
    }


def _resident_access_inv_theta(
    eq,
    fund: inv.InversionResult,
    prim: bp.Primitives,
    params: pm.Parameters,
) -> np.ndarray:
    """Gauge-free resident market access: Σ_j τ_ij^{-θ} w_j^θ."""
    w = _w_theta(eq.l_F, fund, params, prim.Lbar)
    return eq.tau_ij_inv_theta @ w


def decompose_resident_access_inv_theta(
    base,
    eq_st,
    eq_lt,
    fund: inv.InversionResult,
    prim: bp.Primitives,
    params: pm.Parameters,
) -> dict:
    """Sequential gauge-free resident-access decomposition: τ(ST) → τ(LT|l⁰) → +Δl_F."""
    th = params.theta
    u0 = _u_theta(base.l_R, fund, params, prim.Lbar)

    def access_inv(tau_inv, l_f):
        w = _w_theta(l_f, fund, params, prim.Lbar)
        return tau_inv @ w

    ma0 = access_inv(base.tau_ij_inv_theta, base.l_F)
    ma_st = access_inv(eq_st.tau_ij_inv_theta, base.l_F)
    ma_lt_tau_only = access_inv(eq_lt.tau_ij_inv_theta, base.l_F)
    ma_lt_full = access_inv(eq_lt.tau_ij_inv_theta, eq_lt.l_F)

    d_total = ma_lt_full - ma0
    d_st = ma_st - ma0
    d_tau_extra_lt = ma_lt_tau_only - ma_st
    d_lf = ma_lt_full - ma_lt_tau_only

    wR = base.l_R / base.l_R.sum()

    def agg(x, weights=None):
        if weights is None:
            return float(np.mean(x))
        return float(np.sum(weights * x))

    return {
        "mean_access_inv_delta_total": agg(d_total),
        "mean_access_inv_delta_st_tau": agg(d_st),
        "mean_access_inv_delta_tau_ge_beyond_st": agg(d_tau_extra_lt),
        "mean_access_inv_delta_lF_sorting": agg(d_lf),
        "pop_weighted_access_inv_delta_total": agg(d_total, wR),
        "pop_weighted_access_inv_delta_st_tau": agg(d_st, wR),
        "pop_weighted_access_inv_delta_tau_ge_beyond_st": agg(d_tau_extra_lt, wR),
        "pop_weighted_access_inv_delta_lF_sorting": agg(d_lf, wR),
        "check_sequential_sum_mean": float(np.mean(d_st + d_tau_extra_lt + d_lf)),
        "w_contrib_access_st_tau": float(np.sum(u0 * d_st)),
        "w_contrib_access_tau_ge": float(np.sum(u0 * d_tau_extra_lt)),
        "w_contrib_access_lF": float(np.sum(u0 * d_lf)),
    }


def _treated_endpoint_flags(prim: bp.Primitives, sel) -> tuple[np.ndarray, np.ndarray]:
    """Endpoint role: slow-only, any-treated-endpoint."""
    n = prim.n_nodes
    slow_nodes: set[int] = set()
    any_nodes: set[int] = set()
    for slow, fast in sel.treated_pairs:
        i, j = int(prim.edge_i[slow]), int(prim.edge_j[slow])
        slow_nodes.update([i, j])
        any_nodes.update([int(prim.edge_i[slow]), int(prim.edge_j[slow]),
                          int(prim.edge_i[fast]), int(prim.edge_j[fast])])
    is_slow = np.zeros(n, dtype=int)
    is_any = np.zeros(n, dtype=int)
    for nd in slow_nodes:
        is_slow[nd] = 1
    for nd in any_nodes:
        is_any[nd] = 1
    return is_slow, is_any


def _ensure_log_pi_inv_theta_change(df: pd.DataFrame, *, theta: float) -> pd.DataFrame:
    """Δlog resident-access object; kept under legacy column name for slide paths."""
    if "log_Pi_inv_theta_change" not in df.columns:
        out = df.copy()
        out["log_Pi_inv_theta_change"] = -float(theta) * out["log_Pi_change"]
        return out
    return df


def _plot_sorting_map(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    path: Path,
    *,
    vmax_pct: float = 0.98,
    demean: bool = False,
    cbar_label: str = "red = increase, blue = decrease",
) -> None:
    v = df[value_col].values.astype(float)
    if demean:
        v = v - np.mean(v)
    vmax = float(np.quantile(np.abs(v), vmax_pct))
    vmax = max(vmax, 1e-6)

    fig, ax = plt.subplots(figsize=(8, 7), dpi=220)
    # Force a symmetric diverging scale centered at 0: positive = red, negative = blue.
    sc = ax.scatter(
        df["sq_x"], df["sq_y"],
        c=np.clip(v, -vmax, vmax), s=10, cmap="RdBu_r",
        vmin=-vmax, vmax=vmax, linewidths=0, alpha=0.85,
    )
    slow = df[df["is_slow_endpoint"] == 1]
    anyep = df[(df["is_treated_endpoint"] == 1) & (df["is_slow_endpoint"] == 0)]
    ax.scatter(
        slow["sq_x"], slow["sq_y"], s=28, facecolors="none", edgecolors="#111111",
        linewidths=0.9, label=f"slow-direction endpoint (n={len(slow)})",
    )
    ax.scatter(
        anyep["sq_x"], anyep["sq_y"], s=14, c="#ffd700", edgecolors="#333333",
        linewidths=0.4, label=f"fast/other endpoint (n={len(anyep)})",
    )
    plt.colorbar(sc, ax=ax, label=cbar_label)
    ax.set_title(title)
    ax.set_xlabel("grid x")
    ax.set_ylabel("grid y")
    ax.set_aspect("equal")
    ax.legend(loc="upper right", fontsize=7, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def export_preslide_welfare_sorting_figures(
    track_out: Path,
    *,
    theta: float = pm.Parameters().theta,
) -> None:
    """Copy pos05 LT maps into Documents/PreSlides/figs/sort_*.png."""
    csv_path = track_out / "node_sorting_changes.csv"
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    df = _ensure_log_pi_inv_theta_change(pd.read_csv(csv_path), theta=theta)
    PRESLIDE_FIG_DIR.mkdir(parents=True, exist_ok=True)

    _plot_sorting_map(
        df, "log_Pi_inv_theta_change",
        r"$\Delta\log\sum_j \tau_{ij}^{-\theta}w_j^\theta$ — market access (LT, pos05)",
        PRESLIDE_FIG_DIR / "sort_Pi.png",
        cbar_label=r"gauge-free access: red = access $\uparrow$",
    )
    _plot_sorting_map(
        df, "log_l_R_change",
        r"$\Delta\log\ell^R - \overline{\Delta\log\ell^R}$ (LT, pos05)",
        PRESLIDE_FIG_DIR / "sort_lR.png",
        demean=True,
    )
    _plot_sorting_map(
        df, "log_l_F_change",
        r"$\Delta\log\ell^F - \overline{\Delta\log\ell^F}$ (LT, pos05)",
        PRESLIDE_FIG_DIR / "sort_lF.png",
        demean=True,
    )
    LOG.info("Exported preslide welfare/sorting maps to %s", PRESLIDE_FIG_DIR)


def run_track(
    prim: bp.Primitives,
    track: dict,
    out: Path,
    *,
    top_k: int,
    pair_selection: str,
) -> None:
    key = track["key"]
    track_out = out / key
    track_out.mkdir(parents=True, exist_ok=True)

    params, fund, base, eq_st, eq_lt, sel = _solve_track(
        prim, track, top_k=top_k, pair_selection=pair_selection,
    )
    is_slow, is_any = _treated_endpoint_flags(prim, sel)

    wdec = decompose_wbar_theta(base, eq_st, eq_lt, fund, params, prim)
    pdec = decompose_resident_access_inv_theta(base, eq_st, eq_lt, fund, prim, params)
    prim_w = primitive_welfare_summary(base, eq_st, eq_lt, fund, params, prim)

    write_json(track_out / "wbar_theta_decomposition.json", {**wdec, "track": track, "pair_selection": pair_selection})
    write_json(track_out / "resident_access_decomposition.json", {**pdec, "track": track})
    write_json(track_out / "primitive_welfare_summary.json", {**prim_w, "track": track, "pair_selection": pair_selection})

    df = node_change_frame(prim, base, eq_lt, label=f"{key}_lt")
    df["is_slow_endpoint"] = is_slow
    df["is_treated_endpoint"] = is_any
    df["delta_l_R"] = df["l_R_level_scenario"] - df["l_R_level_base"]
    df["delta_l_F"] = df["l_F_level_scenario"] - df["l_F_level_base"]
    th = params.theta
    raw_pi0 = np.clip(base.Pi_i, EPS, None)
    raw_pi1 = np.clip(eq_lt.Pi_i, EPS, None)
    ma0 = np.clip(_resident_access_inv_theta(base, fund, prim, params), EPS, None)
    ma1 = np.clip(_resident_access_inv_theta(eq_lt, fund, prim, params), EPS, None)
    df["raw_log_Pi_inv_theta_change"] = np.log(raw_pi1 ** (-th)) - np.log(raw_pi0 ** (-th))
    df["log_Pi_inv_theta_change"] = np.log(ma1) - np.log(ma0)
    df["log_resident_access_inv_theta_change"] = df["log_Pi_inv_theta_change"]
    df.to_csv(track_out / "node_sorting_changes.csv", index=False)

    _plot_sorting_map(
        df, "log_l_R_change",
        f"Δlog ℓ^R — {key} (α={track['alpha']}, β={track['beta']}) LT",
        track_out / "map_delta_log_lR_annotated.png",
    )
    _plot_sorting_map(
        df, "log_l_F_change",
        f"Δlog ℓ^H — {key} LT",
        track_out / "map_delta_log_lF_annotated.png",
    )
    _plot_sorting_map(
        df, "log_Pi_inv_theta_change",
        f"Δlog resident access — {key} LT",
        track_out / "map_delta_log_Pi_inv_theta_annotated.png",
        cbar_label="Δlog Σ_j τ_ij^{-θ}w_j^θ: red = access ↑",
    )

    # summary by endpoint type
    grp = df.groupby(["is_slow_endpoint", "is_treated_endpoint"]).agg(
        n=("node_idx", "count"),
        mean_log_l_R=("log_l_R_change", "mean"),
        mean_log_l_F=("log_l_F_change", "mean"),
        mean_log_Pi_inv_theta=("log_Pi_inv_theta_change", "mean"),
        sum_delta_l_R=("delta_l_R", "sum"),
        sum_delta_l_F=("delta_l_F", "sum"),
    ).reset_index()
    grp.to_csv(track_out / "endpoint_group_summary.csv", index=False)

    if key == "pos05":
        export_preslide_welfare_sorting_figures(track_out, theta=th)

    LOG.info(
        "%s | primitive W LT %+.3f%% | raw-Pi W LT %+.3f%% | access w-contrib tau_st %+.2f tau_ge %+.2f lF %+.2f",
        key, prim_w["primitive_W_pct_lt"], wdec["Wbar_pct"],
        pdec["w_contrib_access_st_tau"], pdec["w_contrib_access_tau_ge"],
        pdec["w_contrib_access_lF"],
    )


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="welfare_sorting_diagnostics_gate5_bpr_pos05_v1")
    p.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION, choices=("asym", "score_bpr_st"))
    p.add_argument("--tracks", default="seattle,pos05,zero")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / args.version_id, period="AM")
    keys = [k.strip() for k in args.tracks.split(",") if k.strip()]
    tracks = [t for t in TRACKS if t["key"] in keys]

    summary_rows = []
    for track in tracks:
        run_track(prim, track, out, top_k=args.top_k, pair_selection=args.pair_selection)
        w = json.loads((out / track["key"] / "wbar_theta_decomposition.json").read_text())
        pi = json.loads((out / track["key"] / "resident_access_decomposition.json").read_text())
        prim_w = json.loads((out / track["key"] / "primitive_welfare_summary.json").read_text())
        summary_rows.append({
            "track": track["key"], "alpha": track["alpha"], "beta": track["beta"],
            **{k: w[k] for k in (
                "Wbar_pct", "Wbar_st_pct", "Wbar_theta_delta",
                "layer_st_tau", "layer_ge_tau", "layer_lF_sorting", "layer_lR_crowding",
                "check_layers_sum_minus_delta",
            )},
            **{k: pi[k] for k in (
                "w_contrib_access_st_tau", "w_contrib_access_tau_ge", "w_contrib_access_lF",
            )},
            **{k: prim_w[k] for k in (
                "primitive_W_pct_st", "primitive_W_pct_lt",
                "primitive_total_lt_cost_change_sec", "primitive_direct_st_cost_change_sec",
                "primitive_ge_cost_wedge_sec", "primitive_sorting_wedge_sec",
            )},
        })

    pd.DataFrame(summary_rows).to_csv(out / "welfare_decomposition_summary.csv", index=False)
    LOG.info("Wrote %s", out)


if __name__ == "__main__":
    main()
