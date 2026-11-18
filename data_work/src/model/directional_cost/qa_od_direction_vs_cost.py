"""QA: compare model soft OD cost T_ij^{-1/theta} vs directed SP on AM edge times."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.model.directional_cost import fixed_tau_model as ft  # noqa: E402
from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402

EPS = 1e-12


def _sp_times(prim, tau_edge: np.ndarray) -> np.ndarray:
    n = prim.n_nodes
    data = tau_edge.astype(float)
    graph = csr_matrix((data, (prim.edge_i, prim.edge_j)), shape=(n, n))
    dist = dijkstra(graph, directed=True, indices=np.arange(n))
    return np.asarray(dist, dtype=float)


def _corr(x: np.ndarray, y: np.ndarray, w: np.ndarray | None = None) -> dict:
    mask = np.isfinite(x) & np.isfinite(y) & (x > 0) & (y > 0)
    if mask.sum() < 3:
        return {"n": int(mask.sum())}
    xv, yv = np.log(x[mask]), np.log(y[mask])
    if w is None:
        return {
            "n": int(mask.sum()),
            "pearson_log": float(np.corrcoef(xv, yv)[0, 1]),
            "spearman_level": float(__import__("scipy.stats", fromlist=["spearmanr"]).spearmanr(
                x[mask], y[mask]
            ).correlation),
        }
    wv = w[mask]
    mx = np.average(xv, weights=wv)
    my = np.average(yv, weights=wv)
    cov = np.average((xv - mx) * (yv - my), weights=wv)
    vx = np.average((xv - mx) ** 2, weights=wv)
    vy = np.average((yv - my) ** 2, weights=wv)
    return {
        "n": int(mask.sum()),
        "weighted_pearson_log": float(cov / np.sqrt(vx * vy)),
    }


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--output-root", default="data_work/outputs/directional_cost_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--sample-size", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    out_root = repo / args.output_root
    diag = out_root / "diagnostics"
    diag.mkdir(parents=True, exist_ok=True)

    prim = bp.load_primitives(repo / "data_work" / "outputs" / args.version_id, period="AM")
    params = pm.Parameters()
    theta = params.theta
    kappa = ft.pick_kappa(prim.edge_i, prim.tau_obs_min, prim.n_nodes, 0.9)
    T = ft.build_tau_inv_theta(prim, prim.tau_obs_min, kappa=kappa)
    tau_soft = np.clip(T, EPS, None) ** (-1.0 / theta)
    sp = _sp_times(prim, prim.tau_obs_min)

    n = prim.n_nodes
    ii, jj = np.indices((n, n))
    off = ii != jj
    pos = prim.L_ij_obs > 0
    pos_off = pos & off

    rng = np.random.default_rng(args.seed)
    idx = np.flatnonzero(pos_off)
    if len(idx) > args.sample_size:
        w = prim.L_ij_obs.ravel()[idx]
        p = w / w.sum()
        idx = rng.choice(idx, size=args.sample_size, replace=False, p=p)

    def pack(mask_flat):
        i = ii.ravel()[mask_flat]
        j = jj.ravel()[mask_flat]
        return {
            "soft": tau_soft[i, j],
            "sp": sp[i, j],
            "obs_flow": prim.L_ij_obs[i, j],
            "origin": prim.node_grid_ids[i],
            "dest": prim.node_grid_ids[j],
        }

    sample_i = ii.ravel()[idx]
    sample_j = jj.ravel()[idx]
    soft_s = tau_soft[sample_i, sample_j]
    sp_s = sp[sample_i, sample_j]

    # Direction sanity: home→work SP vs work→home SP on observed OD pairs
    rev_sp = sp[sample_j, sample_i]
    both = np.isfinite(sp_s) & np.isfinite(rev_sp) & (sp_s > 0) & (rev_sp > 0)
    asym = np.log(sp_s[both]) - np.log(rev_sp[both])

    pos_off_finite_sp = pos_off & np.isfinite(sp) & (sp > 0)

    top_k = 20
    top_idx = np.argsort(-prim.L_ij_obs.ravel())
    top_rows = []
    for flat in top_idx:
        if len(top_rows) >= top_k:
            break
        i, j = divmod(int(flat), n)
        if i == j:
            continue
        if not (np.isfinite(sp[i, j]) and sp[i, j] > 0):
            continue
        top_rows.append({
            "origin": str(prim.node_grid_ids[i]),
            "dest": str(prim.node_grid_ids[j]),
            "obs_flow": float(prim.L_ij_obs[i, j]),
            "sp_home_to_work_min": float(sp[i, j]),
            "sp_work_to_home_min": float(sp[j, i]),
            "soft_cost_min": float(tau_soft[i, j]),
            "ratio_sp_over_soft": float(sp[i, j] / max(tau_soft[i, j], EPS)),
        })

    unreachable_sp = int(np.sum(pos_off & ~np.isfinite(sp)))
    unreachable_soft = int(np.sum(pos_off & ~np.isfinite(tau_soft)))

    report = {
        "version_id": args.version_id,
        "period": "AM",
        "interpretation": "OD i->j is home->work; edge tau_kl is AM travel k->l; compare SP(i,j) vs T_ij^{-1/theta}",
        "n_nodes": n,
        "n_edges": prim.n_edges,
        "kappa": kappa,
        "theta": theta,
        "positive_od_off_diagonal": int(pos_off.sum()),
        "unreachable_sp_on_pos_off": unreachable_sp,
        "unreachable_soft_on_pos_off": unreachable_soft,
        "all_pos_off_diagonal": _corr(tau_soft[pos_off_finite_sp], sp[pos_off_finite_sp], prim.L_ij_obs[pos_off_finite_sp]),
        "flow_weighted_sample": _corr(soft_s, sp_s, prim.L_ij_obs[sample_i, sample_j]),
        "sample_both_directions_finite": int(both.sum()),
        "sample_log_sp_minus_log_sp_reverse_mean": float(np.mean(asym)) if both.any() else None,
        "sample_log_sp_minus_log_sp_reverse_p50": float(np.median(asym)) if both.any() else None,
        "sample_share_sp_home_to_work_gt_reverse": float(np.mean(sp_s[both] > rev_sp[both])) if both.any() else None,
        "top_od_pairs": top_rows,
    }
    out_path = diag / "od_direction_cost_qa.json"
    out_path.write_text(json.dumps(report, indent=2, default=float))
    print(json.dumps(report, indent=2, default=float))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
