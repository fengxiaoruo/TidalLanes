from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class StaticEqmResult:
    chi: float
    l_r: np.ndarray
    l_f: np.ndarray
    tau: np.ndarray
    l_ij: np.ndarray
    xi_ij: np.ndarray
    inner_iterations: int
    outer_iterations: int


def lr_lf_to_x_matrix(theta: float, lambd: float, alpha: float, beta: float) -> np.ndarray:
    return np.array(
        [
            [1 - beta * theta, theta * lambd * (1 - alpha * theta) / (1 + theta * lambd)],
            [theta * lambd * (1 - beta * theta) / (1 + theta * lambd), 1 - alpha * theta],
        ],
        dtype=float,
    )


def get_lr_lf(x1: np.ndarray, x2: np.ndarray, mat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    temp = np.exp(mat @ np.vstack([np.log(x1), np.log(x2)]))
    return temp[0, :].copy(), temp[1, :].copy()


def assert_scale(x1: np.ndarray, x2: np.ndarray, inv_mat: np.ndarray, mat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    l_r, l_f = get_lr_lf(x1, x2, inv_mat)
    l_r = l_r / l_r.sum()
    l_f = l_f / l_f.sum()
    return get_lr_lf(l_r, l_f, mat)


def calc_t1(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    chi: float,
    l_f: np.ndarray,
    theta: float,
    lambd: float,
    alpha: float,
) -> np.ndarray:
    del t_bar
    return chi * (t_bar_prod**theta) * (u_bar**theta) * (
        l_f ** (theta * alpha + (theta * lambd * (1 - alpha * theta) / (1 + theta * lambd)))
    )


def calc_t2(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    chi: float,
    l_r: np.ndarray,
    theta: float,
    lambd: float,
    beta: float,
) -> np.ndarray:
    del t_bar
    return chi * (t_bar_prod**theta) * (u_bar**theta) * (
        l_r ** (theta * beta + (theta * lambd * (1 - beta * theta) / (1 + theta * lambd)))
    )


def calc_sum1(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    chi: float,
    l_r: np.ndarray,
    l_bar: float,
    theta: float,
    lambd: float,
    beta: float,
) -> np.ndarray:
    term = (
        t_bar ** (-theta / (1 + theta * lambd))
        * (t_bar_prod ** ((theta**2) * lambd / (1 + theta * lambd)))
        * (u_bar**theta)[:, None]
        * (u_bar ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_r ** ((1 - beta * theta) / (1 + theta * lambd)))[None, :]
    )
    return (
        chi ** (theta * lambd / (1 + theta * lambd))
        * (l_bar ** (-theta * lambd / (1 + theta * lambd)))
        * term.sum(axis=1)
    )


def calc_sum2(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    chi: float,
    l_f: np.ndarray,
    l_bar: float,
    theta: float,
    lambd: float,
    alpha: float,
) -> np.ndarray:
    term = (
        (t_bar ** (-theta / (1 + theta * lambd))).T
        * (t_bar_prod**theta)[:, None]
        * (u_bar ** ((theta**2) * lambd / (1 + theta * lambd)))[:, None]
        * (t_bar_prod ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_f ** ((1 - alpha * theta) / (1 + theta * lambd)))[None, :]
    )
    return (
        chi ** (theta * lambd / (1 + theta * lambd))
        * (l_bar ** (-theta * lambd / (1 + theta * lambd)))
        * term.sum(axis=1)
    )


def calc_eqm_commuting_static(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    chi: float,
    l_r: np.ndarray,
    l_f: np.ndarray,
    l_bar: float,
    theta: float,
    lambd: float,
    alpha: float,
    beta: float,
) -> tuple[np.ndarray, np.ndarray]:
    eqm1 = calc_t1(t_bar, t_bar_prod, u_bar, chi, l_f, theta, lambd, alpha) + calc_sum1(
        t_bar, t_bar_prod, u_bar, chi, l_r, l_bar, theta, lambd, beta
    )
    eqm2 = calc_t2(t_bar, t_bar_prod, u_bar, chi, l_r, theta, lambd, beta) + calc_sum2(
        t_bar, t_bar_prod, u_bar, chi, l_f, l_bar, theta, lambd, alpha
    )
    return eqm1, eqm2


def solve_static_eqm_lr_lf(
    t_bar: np.ndarray,
    t_bar_prod: np.ndarray,
    u_bar: np.ndarray,
    l_bar: float,
    theta: float,
    lambd: float,
    alpha: float,
    beta: float,
    tol: float = 1e-6,
    slack: float = 0.0,
    maxiter: int = 10000,
) -> StaticEqmResult:
    del slack
    n = len(t_bar_prod)
    x_mat = lr_lf_to_x_matrix(theta, lambd, alpha, beta)
    inv_x_mat = np.linalg.inv(x_mat)

    chi_lo = 0.0
    chi_hi = 100.0
    x1 = np.ones(n, dtype=float)
    x2 = np.ones(n, dtype=float)
    outer_diff = 1.0
    outer_iter = 0
    inner_update = 0.1

    last_inner_iter = 0
    while outer_diff > tol and outer_iter < maxiter:
        outer_iter += 1
        chi = (chi_lo + chi_hi) / 2.0

        inner_diff = 1.0
        inner_iter = 0
        while inner_diff > tol and inner_iter < maxiter:
            inner_iter += 1
            x1, x2 = assert_scale(x1, x2, inv_x_mat, x_mat)
            l_r, l_f = get_lr_lf(x1, x2, inv_x_mat)
            x1_new, x2_new = calc_eqm_commuting_static(
                t_bar, t_bar_prod, u_bar, chi, l_r, l_f, l_bar, theta, lambd, alpha, beta
            )
            x1_new, x2_new = assert_scale(x1_new, x2_new, inv_x_mat, x_mat)

            inner_diff = np.linalg.norm(np.log(x1_new) - np.log(x1)) + np.linalg.norm(np.log(x2_new) - np.log(x2))
            x1 = x1_new * inner_update + x1 * (1 - inner_update)
            x2 = x2_new * inner_update + x2 * (1 - inner_update)

        last_inner_iter = inner_iter
        outer_diff = np.linalg.norm(np.log(np.array([chi_hi])) - np.log(np.array([chi_lo + 1e-300])))

        scale_l_r, scale_l_f = get_lr_lf(x1, x2, inv_x_mat)
        scale_x1, scale_x2 = calc_eqm_commuting_static(
            t_bar, t_bar_prod, u_bar, chi, scale_l_r, scale_l_f, l_bar, theta, lambd, alpha, beta
        )
        lambda1 = x1_new / scale_x1
        lambda2 = x2_new / scale_x2

        if np.allclose(lambda1, 1.0, atol=tol, rtol=0.0) and np.allclose(lambda2, 1.0, atol=tol, rtol=0.0):
            break
        if np.all(lambda1 > 1.0) and np.all(lambda2 > 1.0):
            chi_lo = chi
        elif np.all(lambda1 < 1.0) and np.all(lambda2 < 1.0):
            chi_hi = chi
        else:
            raise RuntimeError("Conflicting lambdas in static equilibrium solve.")

    l_r, l_f = get_lr_lf(x1, x2, inv_x_mat)
    t = (
        chi ** (-lambd / (1 + theta * lambd))
        * (l_bar ** (lambd / (1 + theta * lambd)))
        * (t_bar ** (1 / (1 + theta * lambd)))
        * (u_bar ** (-theta * lambd / (1 + theta * lambd)))[None, :]
        * (t_bar_prod ** (-theta * lambd / (1 + theta * lambd)))[:, None]
        * (l_r ** (lambd * (1 - beta * theta) / (1 + theta * lambd)))[None, :]
        * (l_f ** (lambd * (1 - alpha * theta) / (1 + theta * lambd)))[:, None]
    )
    tau = np.linalg.inv(np.eye(n) - (t ** (-theta))) ** (-1 / theta)
    l_ij = chi * (tau ** (-theta)) * (t_bar_prod**theta)[None, :] * (u_bar**theta)[:, None] * (
        l_r ** (beta * theta)
    )[:, None] * (l_f ** (alpha * theta))[None, :]
    xi_ij = (
        chi ** (-1 / (1 + theta * lambd))
        * (l_bar ** (1 / (1 + theta * lambd)))
        * (t_bar ** (-theta / (1 + theta * lambd)))
        * (t_bar_prod ** (-theta / (1 + theta * lambd)))[:, None]
        * (u_bar ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_r ** ((1 - beta * theta) / (1 + theta * lambd)))[None, :]
        * (l_f ** ((1 - alpha * theta) / (1 + theta * lambd)))[:, None]
    )

    return StaticEqmResult(
        chi=float(chi),
        l_r=l_r,
        l_f=l_f,
        tau=tau,
        l_ij=l_ij,
        xi_ij=xi_ij,
        inner_iterations=last_inner_iter,
        outer_iterations=outer_iter,
    )
