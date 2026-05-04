"""
compare_batch.py
================
Checkpoint-resumable version of compare_asym_vs_sym.py.

Each call processes the next --batch-size edges (for both ASYM and SYM baselines)
and appends results to the output CSVs.  A progress.json file tracks which edges
are done, so repeated calls pick up where they left off.

Usage (run repeatedly until all edges are done):
    python compare_batch.py --batch-size 30 --workers 4 --tol 1e-4 --inner-update 0.3

Full run on Mac (single call, all edges):
    python compare_asym_vs_sym.py --workers 8 --tol 1e-4 --inner-update 0.3
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

SCRIPT_DIR  = Path(__file__).resolve().parent
REPL_ROOT   = SCRIPT_DIR.parent
V2_INPUT    = REPL_ROOT / "aa_input_square_v2"
RESULTS_DIR = REPL_ROOT / "results" / "asym_vs_sym"
PROGRESS_F  = RESULTS_DIR / "progress.json"

sys.path.insert(0, str(SCRIPT_DIR))

from counterfactual import solve_counterfactual_lr_lf  # noqa: E402
from data_io import load_seattle_commuting_data        # noqa: E402

THETA = 6.83
DELTA1 = 0.488
LAMBD = (1.0 / THETA) * DELTA1
ALPHA = -0.12
BETA  = -0.1
EPS   = 1e-4   # floor for zero-population nodes


# ---------------------------------------------------------------------------
# Worker (same as compare_asym_vs_sym.py)
# ---------------------------------------------------------------------------
def _run_one(args_tuple):
    import warnings, numpy as np
    warnings.filterwarnings("ignore")
    from counterfactual import solve_counterfactual_lr_lf

    (edge_i, edge_j, n, l_r, l_f, l_bar, xi_ij,
     theta, lambd, alpha, beta, tol, slack, maxiter, inner_update) = args_tuple

    t_bar_hat = np.ones((n, n), dtype=float)
    t_bar_hat[edge_i - 1, edge_j - 1] = 0.99

    result = solve_counterfactual_lr_lf(
        t_bar_hat=t_bar_hat, t_bar_prod_hat=np.ones(n),
        u_bar_hat=np.ones(n), l_bar_hat=1.0,
        l_r=l_r, l_f=l_f, l_bar=l_bar, xi_ij=xi_ij,
        theta=theta, lambd=lambd, alpha=alpha, beta=beta,
        tol=tol, slack=slack, maxiter=maxiter, inner_update=inner_update,
    )

    chi_block = np.column_stack([
        np.full(n, edge_i, dtype=float),
        np.full(n, edge_j, dtype=float),
        np.arange(1, n + 1, dtype=float),
        np.full(n, result.chi_hat, dtype=float),
        result.l_r_hat,
        result.l_f_hat,
    ])
    err_row = np.array([[edge_i, edge_j, result.err_eqm1, result.err_eqm2]])
    return chi_block, err_row


def _run_batch(
    edges: np.ndarray, n: int,
    l_r: np.ndarray, l_f: np.ndarray, l_bar: float,
    xi_ij: np.ndarray, workers: int,
    tol: float, slack: float, maxiter: int, inner_update: float,
) -> tuple[np.ndarray, np.ndarray]:
    tasks = [
        (int(ei), int(ej), n, l_r, l_f, l_bar, xi_ij,
         THETA, LAMBD, ALPHA, BETA, tol, slack, maxiter, inner_update)
        for ei, ej in edges
    ]
    chi_blocks, err_rows = [], []

    if workers == 1:
        for task in tasks:
            cb, er = _run_one(task)
            chi_blocks.append(cb); err_rows.append(er)
    else:
        futures = {}
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for task in tasks:
                fut = pool.submit(_run_one, task)
                futures[fut] = (task[0], task[1])
            for fut in as_completed(futures):
                cb, er = fut.result()
                chi_blocks.append(cb); err_rows.append(er)

    return np.vstack(chi_blocks), np.vstack(err_rows)


# ---------------------------------------------------------------------------
# Load progress
# ---------------------------------------------------------------------------
def load_progress() -> set[tuple[int, int]]:
    if PROGRESS_F.exists():
        data = json.loads(PROGRESS_F.read_text())
        return {tuple(e) for e in data["done_edges"]}
    return set()


def save_progress(done: set[tuple[int, int]]) -> None:
    PROGRESS_F.write_text(json.dumps({"done_edges": sorted(done)}, indent=2))


# ---------------------------------------------------------------------------
# Append to CSV (with header on first write)
# ---------------------------------------------------------------------------
CHI_COLS = ["edge_i", "edge_j", "node_k", "chi_hat", "l_r_hat", "l_f_hat"]
ERR_COLS = ["edge_i", "edge_j", "err_eqm1", "err_eqm2"]


def _append_csv(path: Path, arr: np.ndarray, cols: list[str]) -> None:
    df = pd.DataFrame(arr, columns=cols)
    write_header = not path.exists()
    df.to_csv(path, mode="a", index=False, header=write_header)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Checkpoint-resumable asym vs sym batch runner.")
    parser.add_argument("--batch-size",  type=int,   default=30)
    parser.add_argument("--workers",     type=int,   default=4)
    parser.add_argument("--tol",         type=float, default=1e-4)
    parser.add_argument("--slack",       type=float, default=1.0)
    parser.add_argument("--maxiter",     type=int,   default=200000)
    parser.add_argument("--inner-update",type=float, default=0.3, dest="inner_update")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load data ----
    data = load_seattle_commuting_data(
        adjmat_path  = V2_INPUT / "sparse_adjmat_seattle.csv",
        node_path    = V2_INPUT / "node_lr_lf_seattle.csv",
        commute_path = V2_INPUT / "sparse_commute_seattle.csv",
    )
    n       = data.n
    l_r_raw = np.maximum(data.l_r_raw, EPS)
    l_f_raw = np.maximum(data.l_f_raw, EPS)
    l_r     = l_r_raw / l_r_raw.sum()
    l_f     = l_f_raw / l_f_raw.sum()
    l_bar   = float(np.mean([l_r_raw.sum(), l_f_raw.sum()]))
    xi_asym = data.xi_ij
    xi_sym  = (data.xi_ij + data.xi_ij.T) / 2.0

    all_edges = data.sparse_adjmat_raw[:, :2].astype(int)
    total     = len(all_edges)

    # ---- Check progress ----
    done = load_progress()
    remaining = [(int(ei), int(ej)) for ei, ej in all_edges
                 if (int(ei), int(ej)) not in done]

    print(f"Total edges: {total}  |  Done: {len(done)}  |  Remaining: {len(remaining)}")

    if not remaining:
        print("All edges complete! Run analyze_comparison.py to see results.")
        _finalize_summary()
        return

    batch = remaining[: args.batch_size]
    batch_arr = np.array(batch, dtype=int)
    print(f"Processing batch of {len(batch)} edges …")

    # ---- ASYM ----
    print("  [ASYM]", flush=True)
    cb_a, er_a = _run_batch(batch_arr, n, l_r, l_f, l_bar, xi_asym,
                            args.workers, args.tol, args.slack,
                            args.maxiter, args.inner_update)
    _append_csv(RESULTS_DIR / "chi_lr_lf_asym.csv", cb_a, CHI_COLS)
    _append_csv(RESULTS_DIR / "errors_asym.csv",    er_a, ERR_COLS)

    # ---- SYM ----
    print("  [SYM]", flush=True)
    cb_s, er_s = _run_batch(batch_arr, n, l_r, l_f, l_bar, xi_sym,
                            args.workers, args.tol, args.slack,
                            args.maxiter, args.inner_update)
    _append_csv(RESULTS_DIR / "chi_lr_lf_sym.csv", cb_s, CHI_COLS)
    _append_csv(RESULTS_DIR / "errors_sym.csv",    er_s, ERR_COLS)

    # ---- Update progress ----
    for ei, ej in batch:
        done.add((ei, ej))
    save_progress(done)

    pct = 100 * len(done) / total
    print(f"Done: {len(done)}/{total} ({pct:.1f}%)")

    # ---- Per-edge summary (chi only, quick) ----
    chi_a = pd.DataFrame(cb_a, columns=CHI_COLS).groupby(["edge_i","edge_j"])
    chi_s = pd.DataFrame(cb_s, columns=CHI_COLS).groupby(["edge_i","edge_j"])

    batch_summary_a = chi_a.agg(chi_hat=("chi_hat","first"),
                                 mean_lr_hat=("l_r_hat","mean"),
                                 std_lr_hat=("l_r_hat","std"),
                                 mean_lf_hat=("l_f_hat","mean"),
                                 std_lf_hat=("l_f_hat","std")).reset_index()
    batch_summary_s = chi_s.agg(chi_hat=("chi_hat","first"),
                                 mean_lr_hat=("l_r_hat","mean"),
                                 std_lr_hat=("l_r_hat","std"),
                                 mean_lf_hat=("l_f_hat","mean"),
                                 std_lf_hat=("l_f_hat","std")).reset_index()
    merged = batch_summary_a.merge(batch_summary_s, on=["edge_i","edge_j"], suffixes=("_asym","_sym"))
    merged["d_chi_hat"]     = merged["chi_hat_asym"]     - merged["chi_hat_sym"]
    merged["d_mean_lr_hat"] = merged["mean_lr_hat_asym"] - merged["mean_lr_hat_sym"]
    merged["d_mean_lf_hat"] = merged["mean_lf_hat_asym"] - merged["mean_lf_hat_sym"]

    summ_path = RESULTS_DIR / "compare_summary.csv"
    write_header = not summ_path.exists()
    merged.to_csv(summ_path, mode="a", index=False, header=write_header)

    print(f"\n  Batch chi_hat_asym: mean={batch_summary_a['chi_hat'].mean():.8f}  "
          f"min={batch_summary_a['chi_hat'].min():.8f}")
    print(f"  Batch d_chi_hat:    mean={merged['d_chi_hat'].mean():.2e}  "
          f"max_abs={merged['d_chi_hat'].abs().max():.2e}")
    print(f"  Batch d_mean_lr:    mean={merged['d_mean_lr_hat'].mean():.2e}  "
          f"max_abs={merged['d_mean_lr_hat'].abs().max():.2e}")


def _finalize_summary() -> None:
    """Recompute compare_summary.csv from full chi files (call after all batches done)."""
    for variant, fname in [("asym", "chi_lr_lf_asym.csv"), ("sym", "chi_lr_lf_sym.csv")]:
        path = RESULTS_DIR / fname
        if not path.exists():
            print(f"Missing {fname}, skipping finalize.")
            return

    print("Recomputing full compare_summary.csv …")
    def _agg(fname, suffix):
        df = pd.read_csv(RESULTS_DIR / fname)
        g = df.groupby(["edge_i","edge_j"])
        out = g.agg(chi_hat=("chi_hat","first"),
                    mean_lr_hat=("l_r_hat","mean"),
                    std_lr_hat=("l_r_hat","std"),
                    mean_lf_hat=("l_f_hat","mean"),
                    std_lf_hat=("l_f_hat","std")).reset_index()
        return out.rename(columns={c: c+suffix for c in out.columns if c not in ("edge_i","edge_j")})

    a = _agg("chi_lr_lf_asym.csv", "_asym")
    s = _agg("chi_lr_lf_sym.csv",  "_sym")
    merged = a.merge(s, on=["edge_i","edge_j"])
    merged["d_chi_hat"]     = merged["chi_hat_asym"]     - merged["chi_hat_sym"]
    merged["d_mean_lr_hat"] = merged["mean_lr_hat_asym"] - merged["mean_lr_hat_sym"]
    merged["d_mean_lf_hat"] = merged["mean_lf_hat_asym"] - merged["mean_lf_hat_sym"]
    merged.to_csv(RESULTS_DIR / "compare_summary.csv", index=False)
    print(f"  Saved {len(merged)} edges to compare_summary.csv")
    print(f"  d_chi_hat: mean={merged['d_chi_hat'].mean():.3e}  "
          f"std={merged['d_chi_hat'].std():.3e}  "
          f"max_abs={merged['d_chi_hat'].abs().max():.3e}")


if __name__ == "__main__":
    main()
