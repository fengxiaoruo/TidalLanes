from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.model.full_estimation.run_stage2_gate5 import main as run_stage2  # noqa: E402


def main() -> None:
    out = ROOT / "outputs/smoke_model"
    args = [
        "--repo-root", str(ROOT),
        "--output-name", "smoke_model",
        "--quick",
        "--skip-tidal-grid",
        "--max-outer", "30",
        "--tol", "1e-4",
    ]
    run_stage2(args)
    summary = out / "baseline_summary.json"
    if summary.exists():
        data = json.loads(summary.read_text())
        b = data.get("baseline_endogenous", {})
        print(
            "smoke model ok: "
            f"Wbar={b.get('Wbar'):.6f}, "
            f"median_tau_obs_ratio={b.get('median_tau_ratio_vs_obs'):.4f}, "
            f"converged={b.get('converged')}"
        )
    else:
        raise SystemExit("smoke model did not write baseline_summary.json")


if __name__ == "__main__":
    main()
