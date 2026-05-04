"""
compare_asym_vs_sym.py
======================
Run all-edge counterfactuals for two baseline traffic matrices:

  ASYM : xi_ij loaded directly from aa_input_square_v2   (directional, asymmetric)
  SYM  : xi_sym = (xi_ij + xi_ij.T) / 2                  (symmetrised version)

For each directed edge (i→j) we shock t_bar_hat[i-1, j-1] = 0.99 and solve the
exact-hat commuting counterfactual, recording chi_hat, l_r_hat, l_f_hat.

Outputs (written to Replication_apply/results/asym_vs_sym/):
  chi_lr_lf_asym.csv   – [edge_i, edge_j, node_k, chi_hat, l_r_hat, l_f_hat]
  chi_lr_lf_sym.csv    – same for symmetric baseline
  errors_asym.csv      – [edge_i, edge_j, err_eqm1, err_eqm2]
  errors_sym.csv       – same for symmetric baseline

Usage:
  python compare_asym_vs_sym.py [--limit N] [--workers W] [--tol 1e-8]
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths  (absolute so workers can import correctly)
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).resolve().parent
REPL_ROOT    = SCRIPT_DIR.parent
V2_INPUT     = REPL_ROOT / "aa_input_square_v2"
RESULTS_DIR  = REPL_ROOT / "results" / "asym_vs_sym"

sys.path.insert(0, str(SCRIPT_DIR))

from counterfactual import solve_counterfactual_lr_lf   # noqa: E402
from data_io import load_seattle_commuting_data         # noqa: E402


# ---------------------------------------------------------------------------
# Parameters (Seattle, from paper)
# ---------------------------------------------------------------------------
THETA  = 6.83
DELTA1 = 0.488
LAMBD  = (1.0 / THETA) * DELTA1
ALPHA  = -0.12
BETA   = -0.1


# ---------------------------------------------------------------------------
# Worker (called in a subprocess)
# ---------------------------------------------------------------------------
def _run_one(args_tuple):
    """Solve one edge counterfactual; returns (chi_block, err_row)."""
    (edge_i, edge_j, n, l_r, l_f, l_bar, xi_ij,
     theta, lambd, alpha, beta, tol, slack, maxiter, inner_update) = args_tuple

    t_bar_hat = np.ones((n, n), dtype=float)
    t_bar_hat[edge_i - 1, edge_j - 1] = 0.99

    result = solve_counterfactual_lr_lf(
        t_bar_hat       = t_bar_hat,
        t_bar_prod_hat  = np.ones(n),
        u_bar_hat       = np.ones(n),
        l_bar_hat       = 1.0,
        l_r             = l_r,
        l_f             = l_f,
        l_bar           = l_bar,
        xi_ij           = xi_ij,
        theta           = theta,
        lambd           = lambd,
        alpha           = alpha,
        beta            = beta,
        tol             = tol,
        slack           = slack,
        maxiter         = maxiter,
        inner_update    = inner_update,
    )

    chi_block = np.column_stack([
        np.full(n, edge_i,         dtype=float),
        np.full(n, edge_j,         dtype=float),
        np.arange(1, n + 1,        dtype=float),
        np.full(n, result.chi_hat, dtype=float),
        result.l_r_hat,
        result.l_f_hat,
    ])
    err_row = np.array([[edge_i, edge_j, result.err_eqm1, result.err_eqm2]])
    return chi_block, err_row


# ---------------------------------------------------------------------------
# Run all edges for one variant
# ---------------------------------------------------------------------------
def run_all_edges(
    variant_name: str,
    xi_ij: np.ndarray,
    l_r: np.ndarray,
    l_f: np.ndarray,
    l_bar: float,
    edges: np.ndarray,          # (M, 2) int array
    n: int,
    workers: int,
    tol: float,
    slack: float,
    maxiter: int,
    inner_update: float = 0.3,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (chi_lr_lf_array, errors_array) over all edges."""

    tasks = [
        (int(ei), int(ej), n, l_r, l_f, l_bar, xi_ij,
         THETA, LAMBD, ALPHA, BETA, tol, slack, maxiter, inner_update)
        for ei, ej in edges
    ]

    chi_blocks: list[np.ndarray] = []
    err_rows:   list[np.ndarray] = []
    total = len(tasks)

    if workers == 1:
        for idx, task in enumerate(tasks, 1):
            print(f"  [{variant_name}] {idx}/{total}  edge=({task[0]},{task[1]})", flush=True)
            cb, er = _run_one(task)
            chi_blocks.append(cb)
            err_rows.append(er)
    else:
        futures = {}
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for idx, task in enumerate(tasks, 1):
                fut = pool.submit(_run_one, task)
                futures[fut] = (idx, task[0], task[1])

            done = 0
            for fut in as_completed(futures):
                done += 1
                idx, ei, ej = futures[fut]
                try:
                    cb, er = fut.result()
                    chi_blocks.append(cb)
                    err_rows.append(er)
                    print(f"  [{variant_name}] done {done}/{total}  edge=({ei},{ej})", flush=True)
                except Exception as exc:
                    print(f"  [{variant_name}] FAILED edge=({ei},{ej}): {exc}", flush=True)

    return np.vstack(chi_blocks), np.vstack(err_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Compare asym vs sym baseline counterfactuals.")
    parser.add_argument("--limit",   type=int,   default=None, help="Run only first N edges (for testing).")
    parser.add_argument("--workers", type=int,   default=1,    help="Parallel worker processes.")
    parser.add_argument("--tol",          type=float, default=1e-4)
    parser.add_argument("--slack",        type=float, default=1.0)
    parser.add_argument("--maxiter",      type=int,   default=200000)
    parser.add_argument("--inner-update", type=float, default=0.3,
                        dest="inner_update",
                        help="Damping factor for inner fixed-point (0.1=safe/slow, 0.3=fast/stable)")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load v2 data ----
    print("Loading v2 data …")
    data = load_seattle_commuting_data(
        adjmat_path  = V2_INPUT / "sparse_adjmat_seattle.csv",
        node_path    = V2_INPUT / "node_lr_lf_seattle.csv",
        commute_path = V2_INPUT / "sparse_commute_seattle.csv",
    )

    n    = data.n

    # Floor at a small epsilon before normalisation.
    # Nodes with zero l_r or l_f cause log(0)=-inf inside get_lr_lf and
    # propagate NaN through the solver.  A 1e-4 floor (≈0.004% of the mean
    # share for N=2729) keeps them negligible but avoids numerical blow-up.
    EPS = 1e-4
    l_r_raw = np.maximum(data.l_r_raw, EPS)
    l_f_raw = np.maximum(data.l_f_raw, EPS)

    l_r  = l_r_raw / l_r_raw.sum()
    l_f  = l_f_raw / l_f_raw.sum()
    l_bar = float(np.mean([l_r_raw.sum(), l_f_raw.sum()]))

    xi_asym = data.xi_ij
    xi_sym  = (data.xi_ij + data.xi_ij.T) / 2.0

    print(f"  N={n} nodes")
    print(f"  xi_ij asymmetric check: "
          f"max |xi[i,j]-xi[j,i]| = {np.abs(xi_asym - xi_asym.T).max():.4f}")
    print(f"  xi_sym max off-diag diff = {np.abs(xi_sym - xi_sym.T).max():.2e}  (should be ~0)")

    # ---- Edge list ----
    adjmat_raw = data.sparse_adjmat_raw
    edges = adjmat_raw[:, :2].astype(int)
    if args.limit is not None:
        edges = edges[: args.limit]
    print(f"  Running {len(edges)} directed edges")

    # ---- ASYMMETRIC run ----
    print("\n=== ASYMMETRIC baseline ===")
    chi_asym, err_asym = run_all_edges(
        variant_name = "ASYM",
        xi_ij        = xi_asym,
        l_r          = l_r, l_f = l_f, l_bar = l_bar,
        edges        = edges, n = n,
        workers      = args.workers,
        tol          = args.tol, slack = args.slack, maxiter = args.maxiter,
        inner_update = args.inner_update,
    )

    # ---- SYMMETRIC run ----
    print("\n=== SYMMETRIC baseline ===")
    chi_sym, err_sym = run_all_edges(
        variant_name = "SYM",
        xi_ij        = xi_sym,
        l_r          = l_r, l_f = l_f, l_bar = l_bar,
        edges        = edges, n = n,
        workers      = args.workers,
        tol          = args.tol, slack = args.slack, maxiter = args.maxiter,
        inner_update = args.inner_update,
    )

    # ---- Save raw outputs ----
    cols_chi  = ["edge_i", "edge_j", "node_k", "chi_hat", "l_r_hat", "l_f_hat"]
    cols_err  = ["edge_i", "edge_j", "err_eqm1", "err_eqm2"]

    pd.DataFrame(chi_asym, columns=cols_chi).to_csv(RESULTS_DIR / "chi_lr_lf_asym.csv", index=False)
    pd.DataFrame(chi_sym,  columns=cols_chi).to_csv(RESULTS_DIR / "chi_lr_lf_sym.csv",  index=False)
    pd.DataFrame(err_asym, columns=cols_err).to_csv(RESULTS_DIR / "errors_asym.csv",     index=False)
    pd.DataFrame(err_sym,  columns=cols_err).to_csv(RESULTS_DIR / "errors_sym.csv",      index=False)

    print(f"\nSaved to {RESULTS_DIR}")

    # ---- Quick per-edge summary ----
    _write_summary(chi_asym, chi_sym, n, RESULTS_DIR)


def _write_summary(
    chi_asym: np.ndarray,
    chi_sym:  np.ndarray,
    n: int,
    out_dir: Path,
) -> None:
    """
    Per-edge summary: chi_hat, mean/std of l_r_hat and l_f_hat, plus diffs.
    Columns: edge_i, edge_j,
             chi_hat_asym, chi_hat_sym, d_chi_hat (asym-sym),
             mean_lr_hat_asym, mean_lr_hat_sym, d_mean_lr_hat,
             std_lr_hat_asym,  std_lr_hat_sym,
             mean_lf_hat_asym, mean_lf_hat_sym, d_mean_lf_hat,
             std_lf_hat_asym,  std_lf_hat_sym
    """
    def _agg(arr: np.ndarray) -> pd.DataFrame:
        df = pd.DataFrame(arr, columns=["edge_i","edge_j","node_k","chi_hat","l_r_hat","l_f_hat"])
        grp = df.groupby(["edge_i","edge_j"])
        return pd.DataFrame({
            "chi_hat"      : grp["chi_hat"].first(),
            "mean_lr_hat"  : grp["l_r_hat"].mean(),
            "std_lr_hat"   : grp["l_r_hat"].std(),
            "mean_lf_hat"  : grp["l_f_hat"].mean(),
            "std_lf_hat"   : grp["l_f_hat"].std(),
        }).reset_index()

    asym = _agg(chi_asym).rename(columns=lambda c: c + "_asym" if c not in ("edge_i","edge_j") else c)
    sym  = _agg(chi_sym ).rename(columns=lambda c: c + "_sym"  if c not in ("edge_i","edge_j") else c)

    summary = asym.merge(sym, on=["edge_i","edge_j"])
    summary["d_chi_hat"]     = summary["chi_hat_asym"]     - summary["chi_hat_sym"]
    summary["d_mean_lr_hat"] = summary["mean_lr_hat_asym"] - summary["mean_lr_hat_sym"]
    summary["d_mean_lf_hat"] = summary["mean_lf_hat_asym"] - summary["mean_lf_hat_sym"]

    summary.to_csv(out_dir / "compare_summary.csv", index=False)

    print("\n--- Per-edge summary statistics ---")
    print(f"  chi_hat_asym  : mean={summary['chi_hat_asym'].mean():.6f}  "
          f"std={summary['chi_hat_asym'].std():.6f}  "
          f"min={summary['chi_hat_asym'].min():.6f}  "
          f"max={summary['chi_hat_asym'].max():.6f}")
    print(f"  chi_hat_sym   : mean={summary['chi_hat_sym'].mean():.6f}  "
          f"std={summary['chi_hat_sym'].std():.6f}  "
          f"min={summary['chi_hat_sym'].min():.6f}  "
          f"max={summary['chi_hat_sym'].max():.6f}")
    print(f"  d_chi_hat     : mean={summary['d_chi_hat'].mean():.2e}  "
          f"std={summary['d_chi_hat'].std():.2e}  "
          f"max_abs={summary['d_chi_hat'].abs().max():.2e}")
    print(f"  d_mean_lr_hat : mean={summary['d_mean_lr_hat'].mean():.2e}  "
          f"max_abs={summary['d_mean_lr_hat'].abs().max():.2e}")
    print(f"  d_mean_lf_hat : mean={summary['d_mean_lf_hat'].mean():.2e}  "
          f"max_abs={summary['d_mean_lf_hat'].abs().max():.2e}")
    print(f"\n  Full summary saved: {out_dir / 'compare_summary.csv'}")


if __name__ == "__main__":
    main()
