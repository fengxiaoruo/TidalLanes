from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.optimize import minimize

from static_eqm import get_lr_lf, lr_lf_to_x_matrix


@dataclass
class CounterfactualResult:
    chi_hat: float
    l_r_hat: np.ndarray
    l_f_hat: np.ndarray
    t_hat: np.ndarray
    xi_ij_hat: np.ndarray
    err_eqm1: float
    err_eqm2: float
    inner_iterations: int
    outer_iterations: int


def _matlab_norm(arr: np.ndarray) -> float:
    arr = np.asarray(arr, dtype=float)
    if arr.ndim == 1:
        return float(np.linalg.norm(arr, ord=2))
    return float(np.linalg.norm(arr, ord=2))


def _assert_scale(x1: np.ndarray, x2: np.ndarray, inv_mat: np.ndarray, mat: np.ndarray) -> np.ndarray:
    l_r, l_f = get_lr_lf(x1, x2, inv_mat)
    l_r = l_r / l_r.sum()
    l_f = l_f / l_f.sum()
    x1_adj, x2_adj = get_lr_lf(l_r, l_f, mat)
    return np.column_stack([x1_adj, x2_adj])


def _assert_cf_scale(
    x1hat: np.ndarray,
    x2hat: np.ndarray,
    x1: np.ndarray,
    x2: np.ndarray,
    inv_mat: np.ndarray,
    mat: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    x1_prime = x1hat * x1
    x2_prime = x2hat * x2
    xhat_adj = _assert_scale(x1_prime, x2_prime, inv_mat, mat) / np.column_stack([x1, x2])
    return xhat_adj[:, 0], xhat_adj[:, 1]


def calc_eqm_commuting_counterfactual(
    t_bar_hat: np.ndarray,
    t_bar_prod_hat: np.ndarray,
    u_bar_hat: np.ndarray,
    chi_hat: float,
    l_r_hat: np.ndarray,
    l_f_hat: np.ndarray,
    l_bar_hat: float,
    l_r: np.ndarray,
    l_f: np.ndarray,
    l_bar: float,
    xi_ij: np.ndarray,
    theta: float,
    lambd: float,
    alpha: float,
    beta: float,
) -> tuple[np.ndarray, np.ndarray]:
    term1_prefactor = (xi_ij / (l_bar * l_f[:, None])).sum(axis=1) + 1.0
    term2_prefactor = (xi_ij.T / (l_bar * l_r[:, None])).sum(axis=1) + 1.0

    t1 = (
        (term1_prefactor ** -1.0)
        * chi_hat
        * (t_bar_prod_hat**theta)
        * (u_bar_hat**theta)
        * (
            l_f_hat
            ** (theta * alpha + (theta * lambd * (1 - alpha * theta) / (1 + theta * lambd)))
        )
    )
    s1_term = (
        (term1_prefactor ** -1.0)[:, None]
        * (xi_ij / (l_bar * l_f[:, None]))
        * (t_bar_hat ** (-theta / (1 + theta * lambd)))
        * (t_bar_prod_hat ** ((theta**2) * lambd / (1 + theta * lambd)))[:, None]
        * (u_bar_hat**theta)[:, None]
        * (u_bar_hat ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_r_hat ** ((1 - beta * theta) / (1 + theta * lambd)))[None, :]
    )
    s1 = (
        chi_hat ** (theta * lambd / (1 + theta * lambd))
        * l_bar_hat ** (-theta * lambd / (1 + theta * lambd))
        * s1_term.sum(axis=1)
    )

    t2 = (
        (term2_prefactor ** -1.0)
        * chi_hat
        * (t_bar_prod_hat**theta)
        * (u_bar_hat**theta)
        * (
            l_r_hat
            ** (theta * beta + (theta * lambd * (1 - beta * theta) / (1 + theta * lambd)))
        )
    )
    s2_term = (
        (term2_prefactor ** -1.0)[:, None]
        * (xi_ij.T / (l_bar * l_r[:, None]))
        * (t_bar_hat ** (-theta / (1 + theta * lambd))).T
        * (t_bar_prod_hat**theta)[:, None]
        * (u_bar_hat ** ((theta**2) * lambd / (1 + theta * lambd)))[:, None]
        * (t_bar_prod_hat ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_f_hat ** ((1 - alpha * theta) / (1 + theta * lambd)))[None, :]
    )
    s2 = (
        chi_hat ** (theta * lambd / (1 + theta * lambd))
        * l_bar_hat ** (-theta * lambd / (1 + theta * lambd))
        * s2_term.sum(axis=1)
    )
    return t1 + s1, t2 + s2


def solve_counterfactual_lr_lf(
    t_bar_hat: np.ndarray,
    t_bar_prod_hat: np.ndarray,
    u_bar_hat: np.ndarray,
    l_bar_hat: float,
    l_r: np.ndarray,
    l_f: np.ndarray,
    l_bar: float,
    xi_ij: np.ndarray,
    theta: float,
    lambd: float,
    alpha: float,
    beta: float,
    tol: float = 1e-8,
    slack: float = 1.0,
    maxiter: int = 200000,
    inner_update: float = 0.1,
) -> CounterfactualResult:
    x_mat = lr_lf_to_x_matrix(theta, lambd, alpha, beta)
    inv_x_mat = np.linalg.inv(x_mat)

    x1, x2 = get_lr_lf(l_r, l_f, x_mat)
    chi_hat = 1.0
    x1hat = np.ones_like(l_r)
    x2hat = np.ones_like(l_f)
    outer_update = 0.1
    outer_diff = 1.0
    outer_iter = 0
    last_inner_iter = 0

    while outer_diff > tol and outer_iter < maxiter:
        outer_iter += 1
        inner_diff = 1.0
        inner_iter = 0

        while inner_diff > tol and inner_iter < maxiter:
            inner_iter += 1
            l_r_hat, l_f_hat = get_lr_lf(x1hat, x2hat, inv_x_mat)
            x1hat_new, x2hat_new = calc_eqm_commuting_counterfactual(
                t_bar_hat,
                t_bar_prod_hat,
                u_bar_hat,
                chi_hat,
                l_r_hat,
                l_f_hat,
                l_bar_hat,
                l_r,
                l_f,
                l_bar,
                xi_ij,
                theta,
                lambd,
                alpha,
                beta,
            )
            x1hat_new, x2hat_new = _assert_cf_scale(x1hat_new, x2hat_new, x1, x2, inv_x_mat, x_mat)
            inner_diff = np.linalg.norm(np.log(x1hat_new) - np.log(x1hat)) + np.linalg.norm(
                np.log(x2hat_new) - np.log(x2hat)
            )
            x1hat = x1hat_new * inner_update + x1hat * (1 - inner_update)
            x2hat = x2hat_new * inner_update + x2hat * (1 - inner_update)

        last_inner_iter = inner_iter
        l_r_hat, l_f_hat = get_lr_lf(x1hat, x2hat, inv_x_mat)

        def objective(ch: float) -> float:
            ch_scalar = float(np.atleast_1d(ch)[0])
            if not np.isfinite(ch_scalar) or ch_scalar <= 0.0:
                return float("inf")
            eqm1, eqm2 = calc_eqm_commuting_counterfactual(
                t_bar_hat,
                t_bar_prod_hat,
                u_bar_hat,
                ch_scalar,
                l_r_hat,
                l_f_hat,
                l_bar_hat,
                l_r,
                l_f,
                l_bar,
                xi_ij,
                theta,
                lambd,
                alpha,
                beta,
            )
            target = np.column_stack(get_lr_lf(l_r_hat, l_f_hat, x_mat))
            rhs = np.column_stack([eqm1, eqm2])
            if np.any(~np.isfinite(rhs)) or np.any(rhs <= 0.0) or np.any(~np.isfinite(target)) or np.any(target <= 0.0):
                return float("inf")
            return _matlab_norm(np.log(target) - np.log(rhs))

        opt = minimize(
            objective,
            x0=np.array([1.0]),
            method="SLSQP",
            constraints=[{"type": "ineq", "fun": lambda x: float(x[0])}],
            options={"disp": False, "ftol": tol, "maxiter": 1000},
        )
        chi_hat_new = float(opt.x[0])
        outer_diff = abs(np.log(chi_hat_new) - np.log(chi_hat))
        chi_hat = outer_update * chi_hat_new + (1 - outer_update) * chi_hat

        scale_l_r_hat, scale_l_f_hat = get_lr_lf(x1hat, x2hat, inv_x_mat)
        scale_xhat1, scale_xhat2 = calc_eqm_commuting_counterfactual(
            t_bar_hat,
            t_bar_prod_hat,
            u_bar_hat,
            chi_hat,
            scale_l_r_hat,
            scale_l_f_hat,
            l_bar_hat,
            l_r,
            l_f,
            l_bar,
            xi_ij,
            theta,
            lambd,
            alpha,
            beta,
        )
        lambda1 = scale_xhat1 / x1hat_new
        lambda2 = scale_xhat2 / x2hat_new
        if np.linalg.norm(np.log(lambda1)) < tol * (10**slack) and np.linalg.norm(np.log(lambda2)) < tol * (10**slack):
            break

    final_eqm1, final_eqm2 = calc_eqm_commuting_counterfactual(
        t_bar_hat,
        t_bar_prod_hat,
        u_bar_hat,
        chi_hat,
        l_r_hat,
        l_f_hat,
        l_bar_hat,
        l_r,
        l_f,
        l_bar,
        xi_ij,
        theta,
        lambd,
        alpha,
        beta,
    )
    eqm1_check, eqm2_check = get_lr_lf(l_r_hat, l_f_hat, x_mat)
    err_eqm1 = float(np.linalg.norm(np.log(final_eqm1) - np.log(eqm1_check)))
    err_eqm2 = float(np.linalg.norm(np.log(final_eqm2) - np.log(eqm2_check)))

    t_hat = (
        chi_hat ** (-lambd / (1 + theta * lambd))
        * l_bar_hat ** (lambd / (1 + theta * lambd))
        * (t_bar_hat ** (1 / (1 + theta * lambd)))
        * (u_bar_hat ** (-theta * lambd / (1 + theta * lambd)))[None, :]
        * (t_bar_prod_hat ** (-theta * lambd / (1 + theta * lambd)))[:, None]
        * (l_r_hat ** (lambd * (1 - beta * theta) / (1 + theta * lambd)))[None, :]
        * (l_f_hat ** (lambd * (1 - alpha * theta) / (1 + theta * lambd)))[:, None]
    )
    xi_ij_hat = (
        chi_hat ** (-1 / (1 + theta * lambd))
        * l_bar_hat ** (1 / (1 + theta * lambd))
        * (t_bar_hat ** (-theta / (1 + theta * lambd)))
        * (t_bar_prod_hat ** (-theta / (1 + theta * lambd)))[:, None]
        * (u_bar_hat ** (-theta / (1 + theta * lambd)))[None, :]
        * (l_r_hat ** ((1 - beta * theta) / (1 + theta * lambd)))[None, :]
        * (l_f_hat ** ((1 - alpha * theta) / (1 + theta * lambd)))[:, None]
    )

    return CounterfactualResult(
        chi_hat=chi_hat,
        l_r_hat=l_r_hat,
        l_f_hat=l_f_hat,
        t_hat=t_hat,
        xi_ij_hat=xi_ij_hat,
        err_eqm1=err_eqm1,
        err_eqm2=err_eqm2,
        inner_iterations=last_inner_iter,
        outer_iterations=outer_iter,
    )
