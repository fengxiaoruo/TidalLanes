from __future__ import annotations

import numpy as np

from static_eqm import solve_static_eqm_lr_lf


def make_grid_adjacency(side: int, direct_cost: float) -> np.ndarray:
    n = side * side
    x, y = np.meshgrid(np.arange(side), np.arange(side))
    coords = np.column_stack([x.ravel(), y.ravel()])
    adjacency = np.zeros((n, n), dtype=float)
    for i in range(n):
        manhattan = np.abs(coords[i, 0] - coords[:, 0]) + np.abs(coords[i, 1] - coords[:, 1])
        adjacency[i, manhattan == 1] = 1.0
    t_bar = direct_cost * adjacency
    t_bar[t_bar == 0] = np.inf
    return t_bar


def main() -> None:
    alpha = 0.0
    beta = 0.0
    theta = 4.0
    lambd = 0.05
    l_bar = 100.0
    side = 5
    n = side * side

    t_bar = make_grid_adjacency(side=side, direct_cost=1.5)
    t_bar_prod = np.ones(n, dtype=float)
    u_bar = np.ones(n, dtype=float)

    result = solve_static_eqm_lr_lf(
        t_bar=t_bar,
        t_bar_prod=t_bar_prod,
        u_bar=u_bar,
        l_bar=l_bar,
        theta=theta,
        lambd=lambd,
        alpha=alpha,
        beta=beta,
        tol=1e-6,
        slack=0.0,
        maxiter=10000,
    )

    print("Static commuting equilibrium on 5x5 grid")
    print(f"chi={result.chi:.12g}")
    print(f"sum_l_r={result.l_r.sum():.12g}")
    print(f"sum_l_f={result.l_f.sum():.12g}")
    print(f"inner_iterations={result.inner_iterations}")
    print(f"outer_iterations={result.outer_iterations}")
    print(f"max_xi={np.nanmax(result.xi_ij):.12g}")


if __name__ == "__main__":
    main()
