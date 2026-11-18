"""α,β sensitivity grid with score_bpr_st pair selection (mainline bpr+pos05)."""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation import build_primitives as bp  # noqa: E402
from src.model.full_estimation import parameters as pm  # noqa: E402
from src.model.full_estimation.equilibrium_solve import MAINLINE_PAIR_SELECTION  # noqa: E402
from src.model.full_estimation.phase_metrics_common import DEFAULT_VERSION, data_work_root  # noqa: E402
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import run_spec  # noqa: E402

LOG = logging.getLogger("alpha_beta_sensitivity_bpr")

ALPHA_GRID = [-0.12, 0.0, 0.02, 0.03, 0.05, 0.10]
BETA_GRID = [-0.10, 0.0, 0.05, 0.10]
# Full cross-product (legacy; use --sweep mini instead).
EXPLORE_ALPHA = [
    -0.12, -0.10, -0.08, -0.06, -0.05, -0.04, -0.03, -0.02,
    0.0, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.06, 0.07, 0.08,
]
EXPLORE_BETA = [-0.15, -0.12, -0.10, -0.08, -0.06, -0.05, -0.03, 0.0, 0.02, 0.05, 0.10]
# Targeted add-on (~10 specs) toward Wbar_lt ≈ 0; skips overlap with 9-grid via --skip-existing.
MINI_SPECS: list[tuple[float, float]] = [
    (-0.10, -0.10), (-0.08, -0.10), (-0.06, -0.10), (-0.04, -0.10),
    (0.04, -0.10), (0.045, -0.10),
    (-0.12, -0.12), (-0.12, -0.08),
    (0.05, -0.08), (0.05, -0.05),
]


def _spec_label(alpha: float, beta: float) -> str:
    return f"alpha_{alpha:+.2f}_beta_{beta:+.2f}"


def _already_converged(out: Path, label: str) -> bool:
    meta = out / label / "convergence_meta.json"
    if not meta.exists():
        return False
    import json
    try:
        d = json.loads(meta.read_text())
        return bool(d.get("baseline", {}).get("converged")) and bool(d.get("tidal_lt", {}).get("converged"))
    except (json.JSONDecodeError, OSError):
        return False


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--version-id", default=DEFAULT_VERSION)
    p.add_argument("--output-name", default="alpha_beta_sensitivity_bpr_pos05_v1")
    p.add_argument("--pair-selection", default=MAINLINE_PAIR_SELECTION)
    p.add_argument("--top-k", type=int, default=100)
    p.add_argument("--fix-beta", type=float, default=-0.10, help="Beta held for alpha sweep")
    p.add_argument("--fix-alpha", type=float, default=0.05, help="Alpha held for beta sweep")
    p.add_argument("--sweep", choices=("alpha", "beta", "both", "explore", "mini"), default="both")
    p.add_argument("--skip-existing", action="store_true", help="Skip specs with converged meta on disk")
    args = p.parse_args(argv)

    data_work = data_work_root(args.repo_root)
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(out / "run.log"), logging.StreamHandler()],
    )

    prim = bp.load_primitives(data_work / "outputs" / args.version_id, period="AM")
    rows = []

    def _run_one(alpha: float, beta: float) -> dict | None:
        label = _spec_label(alpha, beta)
        if args.skip_existing and _already_converged(out, label):
            LOG.info("Skip converged: %s", label)
            return None
        params = replace(pm.Parameters(), alpha=alpha, beta=beta)
        LOG.info("Running: %s", label)
        return run_spec(
            prim, params, label=label, top_k=args.top_k,
            max_outer=500, out_dir=out,
            pair_selection=args.pair_selection,
        )

    if args.sweep == "explore":
        for alpha in EXPLORE_ALPHA:
            for beta in EXPLORE_BETA:
                row = _run_one(alpha, beta)
                if row is not None:
                    rows.append(row)
    elif args.sweep == "mini":
        for alpha, beta in MINI_SPECS:
            row = _run_one(alpha, beta)
            if row is not None:
                rows.append(row)
    else:
        if args.sweep in ("alpha", "both"):
            for alpha in ALPHA_GRID:
                row = _run_one(alpha, args.fix_beta)
                if row is not None:
                    rows.append(row)

        if args.sweep in ("beta", "both"):
            for beta in BETA_GRID:
                if args.sweep == "both" and beta == args.fix_beta:
                    continue
                row = _run_one(args.fix_alpha, beta)
                if row is not None:
                    rows.append(row)

    # Merge with any prior rows on disk (explore mode accumulates).
    grid_name = {
        "explore": "explore_grid.csv",
        "mini": "mini_grid.csv",
    }.get(args.sweep, "sensitivity_grid.csv")
    grid_path = out / grid_name
    if grid_path.exists() and rows:
        prev = pd.read_csv(grid_path)
        df = pd.concat([prev, pd.DataFrame(rows)], ignore_index=True)
        df = df.drop_duplicates(subset=["spec_label"], keep="last")
    elif grid_path.exists():
        df = pd.read_csv(grid_path)
    else:
        df = pd.DataFrame(rows)

    df["all_converged"] = df["baseline_converged"] & df["tidal_st_converged"] & df["tidal_lt_converged"]
    df.to_csv(grid_path, index=False)
    conv = df[df["all_converged"]].copy()
    conv_name = {
        "explore": "explore_grid_converged.csv",
        "mini": "mini_grid_converged.csv",
    }.get(args.sweep, "sensitivity_grid_converged.csv")
    conv.to_csv(out / conv_name, index=False)
    pos = conv[conv["Wbar_pct_change_lt"] > 0]
    if len(pos):
        pos.to_csv(out / "wbar_lt_positive_candidates.csv", index=False)
        LOG.info("Found %d converged specs with Wbar_lt > 0", len(pos))
    else:
        LOG.info("No converged specs with Wbar_lt > 0 in current grid")

    LOG.info("Wrote %s (%d rows, %d converged)", grid_path, len(df), len(conv))
    show = conv if len(conv) else df
    cols = ["spec_label", "alpha", "beta", "sorting_wedge_sec", "person_min_day_lt_total", "Wbar_pct_change_lt"]
    print(show[cols].sort_values(["Wbar_pct_change_lt"], ascending=False).to_string(index=False))


if __name__ == "__main__":
    main()
