from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
COUNTERFACTUALS_SEATTLE = ROOT / "counterfactuals" / "seattle"
DERIVED_SEATTLE = ROOT / "data" / "seattle" / "derived"


@dataclass(frozen=True)
class SeattleCommutingData:
    sparse_adjmat_raw: np.ndarray
    node_raw: np.ndarray
    commute_raw: np.ndarray
    xi_ij: np.ndarray
    l_ij: np.ndarray
    l_r_raw: np.ndarray
    l_f_raw: np.ndarray
    n: int


def _read_csv(path: Path) -> np.ndarray:
    return pd.read_csv(path, header=None).to_numpy(dtype=float)


def _sparse_triplets_to_dense(triplets: np.ndarray, n: int, value_col: int) -> np.ndarray:
    dense = np.zeros((n, n), dtype=float)
    row = triplets[:, 0].astype(int) - 1
    col = triplets[:, 1].astype(int) - 1
    dense[row, col] = triplets[:, value_col]
    return dense


def load_seattle_commuting_data(
    adjmat_path: Path | None = None,
    node_path: Path | None = None,
    commute_path: Path | None = None,
) -> SeattleCommutingData:
    adjmat_path = adjmat_path or COUNTERFACTUALS_SEATTLE / "sparse_adjmat_seattle.csv"
    node_path = node_path or COUNTERFACTUALS_SEATTLE / "node_lr_lf_seattle.csv"
    commute_path = commute_path or COUNTERFACTUALS_SEATTLE / "sparse_commute_seattle.csv"

    sparse_adjmat_raw = _read_csv(adjmat_path)
    node_raw = _read_csv(node_path)
    commute_raw = _read_csv(commute_path)

    n = int(node_raw.shape[0])
    xi_ij = _sparse_triplets_to_dense(sparse_adjmat_raw, n=n, value_col=2)
    l_ij = _sparse_triplets_to_dense(commute_raw, n=n, value_col=2)
    l_r_raw = node_raw[:, 1].copy()
    l_f_raw = node_raw[:, 2].copy()

    return SeattleCommutingData(
        sparse_adjmat_raw=sparse_adjmat_raw,
        node_raw=node_raw,
        commute_raw=commute_raw,
        xi_ij=xi_ij,
        l_ij=l_ij,
        l_r_raw=l_r_raw,
        l_f_raw=l_f_raw,
        n=n,
    )
