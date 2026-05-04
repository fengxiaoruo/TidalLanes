from __future__ import annotations

import pandas as pd
import numpy as np

from counterfactual import solve_counterfactual_lr_lf
from data_io import COUNTERFACTUALS_SEATTLE, load_seattle_commuting_data


def run_edge(edge_i: int, edge_j: int, data, ref: pd.DataFrame) -> dict[str, float]:
    theta = 6.83
    delta1 = 0.488
    lambd = (1.0 / theta) * delta1
    alpha = -0.12
    beta = -0.1

    l_r = data.l_r_raw / data.l_r_raw.sum()
    l_f = data.l_f_raw / data.l_f_raw.sum()
    l_bar = np.mean([data.l_r_raw.sum(), data.l_f_raw.sum()])
    l_bar_hat = 1.0
    t_bar_prod_hat = np.ones(data.n)
    u_bar_hat = np.ones(data.n)

    t_bar_hat = np.ones((data.n, data.n))
    t_bar_hat[edge_i - 1, edge_j - 1] = 0.99

    result = solve_counterfactual_lr_lf(
        t_bar_hat=t_bar_hat,
        t_bar_prod_hat=t_bar_prod_hat,
        u_bar_hat=u_bar_hat,
        l_bar_hat=l_bar_hat,
        l_r=l_r,
        l_f=l_f,
        l_bar=l_bar,
        xi_ij=data.xi_ij,
        theta=theta,
        lambd=lambd,
        alpha=alpha,
        beta=beta,
        tol=1e-8,
        slack=1.0,
        maxiter=200000,
    )

    ref_edge = ref[(ref["i"] == edge_i) & (ref["j"] == edge_j)].sort_values("idx")
    ref_chi = float(ref_edge["chi"].iloc[0])
    ref_lr = ref_edge["lr"].to_numpy(dtype=float)
    ref_lf = ref_edge["lf"].to_numpy(dtype=float)

    return {
        "i": float(edge_i),
        "j": float(edge_j),
        "chi_hat_python": float(result.chi_hat),
        "chi_hat_ref": ref_chi,
        "chi_abs_diff": float(abs(result.chi_hat - ref_chi)),
        "max_abs_l_r_hat_diff": float(np.max(np.abs(result.l_r_hat - ref_lr))),
        "max_abs_l_f_hat_diff": float(np.max(np.abs(result.l_f_hat - ref_lf))),
        "err_eqm1": float(result.err_eqm1),
        "err_eqm2": float(result.err_eqm2),
        "outer_iterations": float(result.outer_iterations),
        "inner_iterations": float(result.inner_iterations),
    }


def main() -> None:
    data = load_seattle_commuting_data()
    ref = pd.read_csv(COUNTERFACTUALS_SEATTLE / "all_chi_lr_lf_ber.csv", header=None)
    ref.columns = ["i", "j", "idx", "chi", "lr", "lf"]

    # Spread across different parts of the reference file, while keeping runtime manageable.
    edges = [(1, 2), (36, 26), (97, 102), (100, 101), (217, 213)]

    rows = []
    for edge_i, edge_j in edges:
        rows.append(run_edge(edge_i, edge_j, data, ref))

    out = pd.DataFrame(rows)
    print(out.to_string(index=False))
    print("\nsummary")
    print(f"max chi abs diff: {out['chi_abs_diff'].max():.12g}")
    print(f"max l_r abs diff: {out['max_abs_l_r_hat_diff'].max():.12g}")
    print(f"max l_f abs diff: {out['max_abs_l_f_hat_diff'].max():.12g}")


if __name__ == "__main__":
    main()
