from __future__ import annotations

import pandas as pd
import numpy as np

from counterfactual import solve_counterfactual_lr_lf
from data_io import COUNTERFACTUALS_SEATTLE, load_seattle_commuting_data


def main() -> None:
    data = load_seattle_commuting_data()

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

    edge_i = 100
    edge_j = 101
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

    ref = pd.read_csv(COUNTERFACTUALS_SEATTLE / "all_chi_lr_lf_ber.csv", header=None)
    ref.columns = ["i", "j", "index", "chi_hat", "l_r_hat", "l_f_hat"]
    ref_edge = ref[(ref["i"] == edge_i) & (ref["j"] == edge_j)].sort_values("index")

    print("Single-edge Seattle counterfactual")
    print(f"edge=({edge_i},{edge_j})")
    print(f"chi_hat_python={result.chi_hat:.12g}")
    print(f"err_eqm1={result.err_eqm1:.12g}")
    print(f"err_eqm2={result.err_eqm2:.12g}")
    print(f"outer_iterations={result.outer_iterations}")
    print(f"inner_iterations={result.inner_iterations}")
    print(f"ref_chi_hat={ref_edge['chi_hat'].iloc[0]:.12g}")
    print(
        f"max_abs_l_r_hat_diff={np.max(np.abs(result.l_r_hat - ref_edge['l_r_hat'].to_numpy())):.12g}"
    )
    print(
        f"max_abs_l_f_hat_diff={np.max(np.abs(result.l_f_hat - ref_edge['l_f_hat'].to_numpy())):.12g}"
    )


if __name__ == "__main__":
    main()
