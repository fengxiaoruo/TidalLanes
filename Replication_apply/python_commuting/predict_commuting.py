from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from data_io import DERIVED_SEATTLE, load_seattle_commuting_data


def matlab_population_consistency(l_r_raw: np.ndarray, l_f_raw: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    l_r = l_r_raw.astype(float).copy()
    l_f = l_f_raw.astype(float).copy()
    l_r = np.mean([l_r.sum(), l_f.sum()]) * (l_r / l_r.sum())
    l_f = np.mean([l_r.sum(), l_f.sum()]) * (l_f / l_f.sum())
    return l_r, l_f


def predict_commuting_matrix(xi_ij: np.ndarray, l_r_raw: np.ndarray, l_f_raw: np.ndarray) -> np.ndarray:
    l_r, l_f = matlab_population_consistency(l_r_raw, l_f_raw)
    avg_pop = (l_r + l_f) / 2.0
    avg_flow = (xi_ij.sum(axis=1) + xi_ij.T.sum(axis=1)) / 2.0
    b = np.linalg.inv(np.diag(avg_pop + avg_flow) - xi_ij)
    return b * l_r[:, None] * l_f[None, :]


def matrix_to_triplets(mat: np.ndarray) -> np.ndarray:
    row, col = np.nonzero(mat)
    order = np.lexsort((row, col))
    row = row[order]
    col = col[order]
    val = mat[row, col]
    return np.column_stack([row + 1, col + 1, val])


def compare_with_reference(predicted_triplets: np.ndarray, reference_path: Path | None = None) -> dict[str, float]:
    reference_path = reference_path or DERIVED_SEATTLE / "predicted_lij.csv"
    ref = pd.read_csv(reference_path, header=None).to_numpy(dtype=float)
    same_shape = predicted_triplets.shape == ref.shape
    if not same_shape:
        raise ValueError(
            f"Predicted shape {predicted_triplets.shape} does not match reference shape {ref.shape}."
        )
    return {
        "max_abs_diff": float(np.max(np.abs(predicted_triplets - ref))),
        "mean_abs_diff": float(np.mean(np.abs(predicted_triplets - ref))),
        "rmse": float(np.sqrt(np.mean((predicted_triplets - ref) ** 2))),
    }


def main(save: bool = False) -> None:
    data = load_seattle_commuting_data()
    predicted = predict_commuting_matrix(data.xi_ij, data.l_r_raw, data.l_f_raw)
    triplets = matrix_to_triplets(predicted)

    print("Seattle commuting prediction")
    print(f"n={data.n}")
    print(f"triplets={triplets.shape[0]}")
    reference_path = DERIVED_SEATTLE / "predicted_lij.csv"
    if reference_path.exists():
        stats = compare_with_reference(triplets, reference_path=reference_path)
        for key, value in stats.items():
            print(f"{key}={value:.12g}")
    else:
        print(f"reference_missing={reference_path}")

    if save:
        out_path = DERIVED_SEATTLE / "predicted_lij_python.csv"
        pd.DataFrame(triplets).to_csv(out_path, header=False, index=False)
        print(f"saved={out_path}")


if __name__ == "__main__":
    main(save=True)
