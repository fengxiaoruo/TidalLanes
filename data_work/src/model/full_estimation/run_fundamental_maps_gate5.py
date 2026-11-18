"""Fundamental intuition maps: ū, ā, Π, and W̄ contribution weights (gate5)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation.equilibrium_solve import EquilibriumResult  # noqa: E402
from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    data_work_root,
    grid_xy,
    load_equilibrium_npz,
    write_json,
)

LOG = logging.getLogger("fundamental_maps")
EPS = 1e-12
DEFAULT_STAGE2 = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_OUT = "fundamental_maps_gate5_bpr_pos05_v1"


def _load_baseline_endog(stage2_dir: Path) -> dict[str, np.ndarray]:
    path = stage2_dir / "baseline_endogenous" / "equilibrium_arrays.npz"
    if not path.exists():
        raise FileNotFoundError(f"baseline equilibrium missing: {path}")
    return load_equilibrium_npz(path)


def _load_fundamentals(stage2_dir: Path) -> dict[str, np.ndarray]:
    path = stage2_dir / "fundamentals" / "fundamentals_arrays.npz"
    if not path.exists():
        raise FileNotFoundError(f"fundamentals missing: {path}")
    return load_equilibrium_npz(path)


def _scatter_map(
    x: np.ndarray,
    y: np.ndarray,
    values: np.ndarray,
    title: str,
    path: Path,
    *,
    cmap: str = "viridis",
    log_scale: bool = False,
) -> None:
    v = np.asarray(values, dtype=float)
    if log_scale:
        v = np.log(np.clip(v, EPS, None))
        cbar_label = f"log({title})"
    else:
        cbar_label = title
    vmax = float(np.quantile(np.abs(v - np.nanmedian(v)), 0.98))
    vmax = max(vmax, 1e-6)

    fig, ax = plt.subplots(figsize=(8, 7), dpi=220)
    sc = ax.scatter(x, y, c=v, s=10, cmap=cmap, linewidths=0, alpha=0.85)
    plt.colorbar(sc, ax=ax, label=cbar_label)
    ax.set_title(title)
    ax.set_xlabel("grid x")
    ax.set_ylabel("grid y")
    ax.set_aspect("equal")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(stage2_dir: Path, out: Path, version_id: str, data_work: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    maps_dir = out / "maps"
    maps_dir.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(data_work / "outputs" / version_id, period="AM")
    params_path = stage2_dir / "parameters.json"
    params = pm.Parameters()
    if params_path.exists():
        meta = json.loads(params_path.read_text())
        from dataclasses import replace
        params = replace(
            params,
            alpha=float(meta.get("alpha", params.alpha)),
            beta=float(meta.get("beta", params.beta)),
            theta=float(meta.get("theta", params.theta)),
        )

    fund = _load_fundamentals(stage2_dir)
    eq = _load_baseline_endog(stage2_dir)
    x, y = grid_xy(prim)

    ubar = fund["ubar_theta"]
    abar = fund["abar_theta"]
    pi_i = eq["Pi_i"]
    p_j = eq["P_j"]
    l_r = eq["l_R"]
    l_f = eq["l_F"]
    th = params.theta
    lbar = prim.Lbar

    u_theta = ubar * np.clip(l_r * lbar, EPS, None) ** (params.beta * th)
    w_contrib = u_theta * np.clip(pi_i, EPS, None) ** (-th)

    df = pd.DataFrame({
        "node_idx": np.arange(prim.n_nodes),
        "grid_id": prim.node_grid_ids,
        "sq_x": x,
        "sq_y": y,
        "ubar_theta": ubar,
        "abar_theta": abar,
        "L_R_obs": prim.L_R_obs,
        "L_F_obs": prim.L_F_obs,
        "Pi_i": pi_i,
        "P_j": p_j,
        "u_theta": u_theta,
        "w_contrib_u_Pi_inv_theta": w_contrib,
        "w_contrib_share": w_contrib / max(float(np.sum(w_contrib)), EPS),
    })
    df.to_csv(out / "fundamental_node_table.csv", index=False)

    _scatter_map(x, y, ubar, "ū_θ (residential fundamental)", maps_dir / "map_ubar_theta.png", log_scale=True)
    _scatter_map(x, y, abar, "ā_θ (workplace fundamental)", maps_dir / "map_abar_theta.png", log_scale=True)
    _scatter_map(x, y, prim.L_R_obs, "Observed residence L_R", maps_dir / "map_L_R_obs.png", log_scale=True)
    _scatter_map(x, y, prim.L_F_obs, "Observed employment L_F", maps_dir / "map_L_F_obs.png", log_scale=True)
    _scatter_map(x, y, pi_i, "Market access Π_i", maps_dir / "map_Pi_baseline.png", log_scale=True)
    _scatter_map(x, y, p_j, "Workplace access P_j", maps_dir / "map_P_j_baseline.png", log_scale=True)
    _scatter_map(
        x, y, w_contrib,
        "W̄ weight: u_i Π_i^{-θ}",
        maps_dir / "map_wbar_contribution.png",
        cmap="YlOrRd",
    )

    summary = {
        "n_nodes": prim.n_nodes,
        "alpha": params.alpha,
        "beta": params.beta,
        "corr_ubar_L_R_obs": float(np.corrcoef(ubar, prim.L_R_obs)[0, 1]),
        "corr_abar_L_F_obs": float(np.corrcoef(abar, prim.L_F_obs)[0, 1]),
        "top5_w_contrib_share": float(np.sort(df["w_contrib_share"].values)[-5:].sum()),
        "stage2_dir": str(stage2_dir),
    }
    write_json(out / "fundamental_maps_summary.json", summary)
    LOG.info(
        "Maps written — corr(ū,L_R)=%.3f corr(ā,L_F)=%.3f",
        summary["corr_ubar_L_R_obs"], summary["corr_abar_L_F_obs"],
    )


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--stage2-dir", default=DEFAULT_STAGE2)
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-name", default=DEFAULT_OUT)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    stage2_dir = Path(args.stage2_dir)
    if not stage2_dir.is_absolute():
        stage2_dir = data_work / "outputs" / stage2_dir
    out = data_work / "outputs" / args.output_name
    run(stage2_dir, out, args.version_id, data_work)
    LOG.info("Done — %s", out)


if __name__ == "__main__":
    main()
