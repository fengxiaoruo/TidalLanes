"""Phase B — LT sorting spatial patterns (maps, radial profiles, top movers)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation.phase_metrics_common import (  # noqa: E402
    DEFAULT_TOP_K,
    DEFAULT_VERSION,
    data_work_root,
    load_fundamentals,
    node_change_frame,
    treated_edge_indices,
    write_json,
)

LOG = logging.getLogger("phase_b")


def _radial_profile(df: pd.DataFrame, value_col: str, n_bins: int = 10) -> pd.DataFrame:
    d = df.copy()
    d["dist_bin"] = pd.qcut(d["dist_to_hub"], n_bins, duplicates="drop")
    g = d.groupby("dist_bin", observed=True).agg(
        dist_mid=("dist_to_hub", "mean"),
        mean_log_l_R_change=(value_col, "mean"),
        mean_log_l_F_change=("log_l_F_change", "mean"),
        mean_L_R_obs=("L_R_obs", "mean"),
        n_nodes=("node_idx", "count"),
    ).reset_index(drop=True)
    return g


def _top_movers(df: pd.DataFrame, col: str, n: int = 20) -> pd.DataFrame:
    sub = df[["grid_id", "sq_x", "sq_y", "dist_to_hub", "is_hub", "is_outer_ring", col, "L_R_obs", "L_F_obs"]].copy()
    sub = sub.sort_values(col, ascending=False)
    top = pd.concat([sub.head(n).assign(rank_type="gainer"), sub.tail(n).assign(rank_type="loser")])
    top["metric"] = col
    return top


def _plot_scatter(df: pd.DataFrame, col: str, title: str, path: Path, vmax: float | None = None) -> None:
    v = df[col].values
    if vmax is None:
        vmax = float(np.quantile(np.abs(v), 0.98))
        vmax = max(vmax, 1e-6)
    fig, ax = plt.subplots(figsize=(7, 6), dpi=200)
    sc = ax.scatter(df["sq_x"], df["sq_y"], c=np.clip(v, -vmax, vmax), s=8, cmap="RdBu_r", linewidths=0)
    plt.colorbar(sc, ax=ax, label=col)
    ax.set_title(title)
    ax.set_xlabel("grid x")
    ax.set_ylabel("grid y")
    ax.set_aspect("equal")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _plot_radial(prof: pd.DataFrame, path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(6, 4), dpi=200)
    ax.plot(prof["dist_mid"], prof["mean_log_l_R_change"], "o-", label="Δlog l_R")
    ax.plot(prof["dist_mid"], prof["mean_log_l_F_change"], "s-", label="Δlog l_F")
    ax.axhline(0, color="gray", lw=0.8)
    ax.set_xlabel("Distance to employment centroid")
    ax.set_ylabel("Mean log share change")
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def run(
    data_work: Path,
    *,
    version_id: str = DEFAULT_VERSION,
    stage2_dir: str = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1",
    spec: str = "top100_d1.0",
    top_k: int = DEFAULT_TOP_K,
    out: Path,
) -> pd.DataFrame:
    prim, _, _ = load_fundamentals(data_work, version_id)
    base_path = data_work / "outputs" / stage2_dir / "baseline_endogenous" / "equilibrium_arrays.npz"
    scen_path = (
        data_work / "outputs" / stage2_dir
        / "counterfactuals" / f"tidal_endog_top{top_k}_d1.0_long_run" / "equilibrium_arrays.npz"
    )
    base = dict(np.load(base_path))
    scen = dict(np.load(scen_path))
    treated = treated_edge_indices(
        scen_path.parent / "selected_pairs.csv",
    )

    df = node_change_frame(prim, base, scen, label=f"tidal_{spec}_lt")
    df["near_treated_corridor"] = 0
    # nodes incident to treated edges
    for e in treated:
        i, j = int(prim.edge_i[e]), int(prim.edge_j[e])
        df.loc[df["node_idx"].isin([i, j]), "near_treated_corridor"] = 1

    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "node_sorting_changes.csv", index=False)

    prof = _radial_profile(df, "log_l_R_change")
    prof.to_csv(out / "radial_profile_log_lR.csv", index=False)

    movers = pd.concat([
        _top_movers(df, "log_l_R_change"),
        _top_movers(df, "log_l_F_change"),
        _top_movers(df, "log_Pi_change"),
    ], ignore_index=True)
    movers.to_csv(out / "top_movers.csv", index=False)

    _plot_scatter(df, "log_l_R_change", f"Δlog residence share — {spec} LT", out / "map_log_lR_change.png")
    _plot_scatter(df, "log_l_F_change", f"Δlog workplace share — {spec} LT", out / "map_log_lF_change.png")
    _plot_scatter(df, "log_Pi_change", f"Δlog market access Π — {spec} LT", out / "map_log_Pi_change.png")
    _plot_radial(prof, out / "radial_profile_log_lR.png", f"Sorting vs distance to hub — {spec} LT")

    near = df[df["near_treated_corridor"] == 1]
    summary = {
        "spec": spec,
        "top_k": top_k,
        "max_abs_log_l_R_change": float(df["log_l_R_change"].abs().max()),
        "max_abs_log_l_F_change": float(df["log_l_F_change"].abs().max()),
        "mean_log_l_R_near_corridor": float(near["log_l_R_change"].mean()) if len(near) else np.nan,
        "mean_log_l_R_far_corridor": float(df[df["near_treated_corridor"] == 0]["log_l_R_change"].mean()),
        "n_nodes_near_treated": int(len(near)),
    }
    write_json(out / "summary.json", summary)
    return df


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="phase_b_sorting_patterns_gate5_bpr_pos05_v1")
    p.add_argument("--stage2-dir", default="stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1")
    p.add_argument("--spec", default="top100_d1.0")
    p.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    run(
        data_work,
        version_id=args.version_id,
        stage2_dir=args.stage2_dir,
        spec=args.spec,
        top_k=args.top_k,
        out=out,
    )
    LOG.info("Wrote %s", out)


if __name__ == "__main__":
    main()
