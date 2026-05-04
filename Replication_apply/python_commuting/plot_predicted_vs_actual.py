from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DERIVED = ROOT / "data" / "seattle" / "derived"
COUNTER = ROOT / "counterfactuals" / "seattle"
FIG_DIR = ROOT / "data" / "seattle" / "figures"


def load_compare_frame() -> pd.DataFrame:
    pred = pd.read_csv(
        DERIVED / "predicted_lij_python.csv",
        header=None,
        names=["o", "d", "pred"],
    )
    actual = pd.read_csv(
        COUNTER / "sparse_commute_seattle.csv",
        header=None,
        names=["o", "d", "actual"],
    )
    merged = pred.merge(actual, on=["o", "d"], how="left")
    merged["actual"] = merged["actual"].fillna(0.0)
    return merged


def summarize(df: pd.DataFrame) -> dict[str, float]:
    use = df[(df["pred"] > 0) | (df["actual"] > 0)].copy()
    corr_levels = float(np.corrcoef(use["pred"], use["actual"])[0, 1])
    corr_logs = float(np.corrcoef(np.log1p(use["pred"]), np.log1p(use["actual"]))[0, 1])
    return {
        "n_rows": int(len(df)),
        "n_actual_positive": int((df["actual"] > 0).sum()),
        "n_pred_positive": int((df["pred"] > 0).sum()),
        "corr_levels": corr_levels,
        "corr_logs": corr_logs,
    }


def plot(df: pd.DataFrame) -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    use_all = df[(df["pred"] > 0) | (df["actual"] > 0)].copy()
    use_pos = df[df["actual"] > 0].copy()

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    x_all = np.log1p(use_all["actual"].to_numpy())
    y_all = np.log1p(use_all["pred"].to_numpy())
    axes[0].scatter(x_all, y_all, s=2, alpha=0.08, color="#1f77b4", linewidths=0)
    lim0 = max(float(x_all.max()), float(y_all.max()))
    axes[0].plot([0, lim0], [0, lim0], color="#555555", linestyle="--", linewidth=1)
    axes[0].set_title("All OD pairs")
    axes[0].set_xlabel("log(1 + actual commuters_road)")
    axes[0].set_ylabel("log(1 + predicted commuting)")

    x_pos = np.log1p(use_pos["actual"].to_numpy())
    y_pos = np.log1p(use_pos["pred"].to_numpy())
    axes[1].scatter(x_pos, y_pos, s=3, alpha=0.12, color="#d62728", linewidths=0)
    lim1 = max(float(x_pos.max()), float(y_pos.max()))
    axes[1].plot([0, lim1], [0, lim1], color="#555555", linestyle="--", linewidth=1)
    axes[1].set_title("OD pairs with positive actual flow")
    axes[1].set_xlabel("log(1 + actual commuters_road)")
    axes[1].set_ylabel("log(1 + predicted commuting)")

    fig.suptitle("AA apply version: predicted vs actual OD matrix")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "predicted_vs_actual_od_scatter_python.png", dpi=240)
    fig.savefig(FIG_DIR / "predicted_vs_actual_od_scatter_python.pdf")


def main() -> None:
    df = load_compare_frame()
    stats = summarize(df)
    for key, value in stats.items():
        print(f"{key}={value}")
    plot(df)
    print(f"saved={FIG_DIR / 'predicted_vs_actual_od_scatter_python.png'}")
    print(f"saved={FIG_DIR / 'predicted_vs_actual_od_scatter_python.pdf'}")


if __name__ == "__main__":
    main()
