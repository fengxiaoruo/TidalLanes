from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from counterfactual import solve_counterfactual_lr_lf
from data_io import COUNTERFACTUALS_SEATTLE, load_seattle_commuting_data


def get_edge_list() -> np.ndarray:
    bilateral = pd.read_csv(COUNTERFACTUALS_SEATTLE / "sparse_adjmat_seattle.csv", header=None).to_numpy(dtype=float)
    return bilateral[:, :2].astype(int)


def run_one_edge(
    edge_i: int,
    edge_j: int,
    data,
    theta: float,
    lambd: float,
    alpha: float,
    beta: float,
    tol: float,
    slack: float,
    maxiter: int,
) -> tuple[np.ndarray, np.ndarray]:
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
        tol=tol,
        slack=slack,
        maxiter=maxiter,
    )

    chi_lr_lf_block = np.column_stack(
        [
            np.full(data.n, edge_i, dtype=float),
            np.full(data.n, edge_j, dtype=float),
            np.arange(1, data.n + 1, dtype=float),
            np.full(data.n, result.chi_hat, dtype=float),
            result.l_r_hat,
            result.l_f_hat,
        ]
    )
    err_row = np.array([[edge_i, edge_j, result.err_eqm1, result.err_eqm2]], dtype=float)
    return chi_lr_lf_block, err_row


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Seattle commuting counterfactuals over all directed edges.")
    parser.add_argument("--limit", type=int, default=None, help="Run only the first N directed edges for testing.")
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="python_berlin",
        help="Prefix for output CSV files written to counterfactuals/seattle.",
    )
    parser.add_argument("--tol", type=float, default=1e-8)
    parser.add_argument("--slack", type=float, default=1.0)
    parser.add_argument("--maxiter", type=int, default=200000)
    args = parser.parse_args()

    data = load_seattle_commuting_data()
    edges = get_edge_list()
    if args.limit is not None:
        edges = edges[: args.limit]

    theta = 6.83
    delta1 = 0.488
    lambd = (1.0 / theta) * delta1
    alpha = -0.12
    beta = -0.1

    chi_blocks: list[np.ndarray] = []
    err_rows: list[np.ndarray] = []

    total = len(edges)
    for idx, (edge_i, edge_j) in enumerate(edges, start=1):
        print(f"[{idx}/{total}] edge=({edge_i},{edge_j})")
        chi_block, err_row = run_one_edge(
            edge_i=edge_i,
            edge_j=edge_j,
            data=data,
            theta=theta,
            lambd=lambd,
            alpha=alpha,
            beta=beta,
            tol=args.tol,
            slack=args.slack,
            maxiter=args.maxiter,
        )
        chi_blocks.append(chi_block)
        err_rows.append(err_row)

    output_dir = Path(COUNTERFACTUALS_SEATTLE)
    chi_out = output_dir / f"{args.output_prefix}_all_chi_lr_lf.csv"
    err_out = output_dir / f"{args.output_prefix}_all_err.csv"

    pd.DataFrame(np.vstack(chi_blocks)).to_csv(chi_out, header=False, index=False)
    pd.DataFrame(np.vstack(err_rows)).to_csv(err_out, header=False, index=False)

    print(f"saved={chi_out}")
    print(f"saved={err_out}")


if __name__ == "__main__":
    main()
