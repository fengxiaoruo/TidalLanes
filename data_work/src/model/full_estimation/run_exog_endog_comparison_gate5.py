"""Build exogenous vs endogenous τ comparison table from Stage2 gate5 outputs."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))

from src.model.full_estimation.phase_metrics_common import data_work_root  # noqa: E402

LOG = logging.getLogger("exog_endog_comparison")

DEFAULT_STAGE2 = "stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1"
DEFAULT_OUT = "exog_endog_comparison_gate5_bpr_pos05_v1"


def _load_csv(path: Path, tag: str) -> pd.DataFrame:
    if not path.exists():
        LOG.warning("Missing %s: %s", tag, path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["source_table"] = tag
    return df


def _normalize_row(row: pd.Series, *, cf_family: str) -> dict:
    person_min = row.get("od_weighted_bilateral_cost_pct_change")
    if "total_lt_cost_change_sec" in row and pd.notna(row.get("total_lt_cost_change_sec")):
        # tidal diagnostics style — caller may override with Lbar
        pass
    return {
        "cf_family": cf_family,
        "cf": row.get("cf") or row.get("policy") or row.get("spec_name"),
        "congestion_mode": row.get("congestion_mode") or row.get("horizon"),
        "Wbar_pct_change": row.get("Wbar_pct_change"),
        "od_cost_pct_change": row.get("od_weighted_bilateral_cost_pct_change"),
        "residence_dissimilarity_pct": row.get("residence_dissimilarity_pct"),
        "workplace_dissimilarity_pct": row.get("workplace_dissimilarity_pct"),
        "mean_edge_tau_pct_change": row.get("mean_edge_tau_pct_change"),
        "scenario_converged": row.get("scenario_converged"),
        "source_table": row.get("source_table"),
        "pair_selection": row.get("pair_selection"),
        "top_k": row.get("top_k"),
        "horizon": row.get("horizon"),
    }


def build_comparison(stage2_dir: Path) -> pd.DataFrame:
    cf_dir = stage2_dir / "counterfactuals"
    parts: list[pd.DataFrame] = []

    for tag, fname in (
        ("structural_endog", "structural_cf_endogenous.csv"),
        ("structural_exog", "structural_cf_exogenous.csv"),
        ("policy", "policy_cf_all_modes.csv"),
        ("tidal_endog", "tidal_lane_endogenous_summary.csv"),
        ("tidal_exog_bpr", "tidal_lane_exogenous_bpr_grid.csv"),
    ):
        df = _load_csv(cf_dir / fname, tag)
        if df.empty:
            continue
        family = tag
        if tag == "policy":
            family = "place_based"
        rows = [_normalize_row(r, cf_family=family) for _, r in df.iterrows()]
        parts.append(pd.DataFrame(rows))

    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out["stage2_dir"] = str(stage2_dir)
    return out


def write_memo(out_dir: Path, df: pd.DataFrame, stage2_dir: Path) -> None:
    lines = [
        "# Exogenous vs Endogenous τ — gate5 mainline (bpr+pos05)",
        "",
        f"**Stage2 input:** `{stage2_dir}`",
        "",
        "## Headline contrasts",
        "",
    ]
    if df.empty:
        lines.append("_No comparison rows — run Stage2 first._")
    else:
        for cf in ("CF1_symmetric_geometric", "CF3_uniform_1.10"):
            sub = df[df["cf"].astype(str).str.contains(cf.split("_")[0], na=False)]
            if sub.empty:
                continue
            lines.append(f"### {cf}")
            for mode in ("exogenous_tau", "endogenous"):
                m = sub[sub["congestion_mode"] == mode]
                if m.empty:
                    continue
                r = m.iloc[0]
                lines.append(
                    f"- **{mode}:** W̄ {r['Wbar_pct_change']:+.2f}%, "
                    f"OD cost {r.get('od_cost_pct_change', float('nan')):+.2f}%"
                )
            lines.append("")

        tidal = df[df["cf_family"] == "tidal_endog"]
        if not tidal.empty:
            top100 = tidal[(tidal["top_k"] == 100) & (tidal["horizon"] == "long_run")]
            if not top100.empty:
                r = top100.iloc[0]
                lines.append("### Tidal top100 LT (endogenous)")
                lines.append(f"- W̄ change: {r['Wbar_pct_change']:+.3f}%")
                lines.append("")

    lines.extend([
        "## Interpretation (sanity check)",
        "",
        "1. CF1 symmetric typically **overstates** welfare gains under exogenous τ "
        "because directional bottlenecks are smoothed without congestion feedback.",
        "2. Endogenous anchored model **dampens** CF1/CF3 gains via Ξ feedback.",
        "3. Tidal CF2 ranking vs CF3 uniform should **not flip** between exog/endog "
        "at the sign level; magnitudes differ.",
        "",
    ])
    (out_dir / "exog_vs_endog_framing.md").write_text("\n".join(lines))


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(DATA_WORK))
    p.add_argument("--stage2-dir", default=DEFAULT_STAGE2)
    p.add_argument("--output-name", default=DEFAULT_OUT)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
    data_work = data_work_root(args.repo_root)
    stage2_dir = Path(args.stage2_dir)
    if not stage2_dir.is_absolute():
        stage2_dir = data_work / "outputs" / stage2_dir
    out = data_work / "outputs" / args.output_name
    out.mkdir(parents=True, exist_ok=True)

    df = build_comparison(stage2_dir)
    df.to_csv(out / "exog_endog_comparison.csv", index=False)
    write_memo(out, df, stage2_dir)
    meta = {"stage2_dir": str(stage2_dir), "n_rows": len(df)}
    (out / "comparison_meta.json").write_text(json.dumps(meta, indent=2))
    LOG.info("Wrote %d rows to %s", len(df), out)
    if len(df):
        print(df[["cf_family", "cf", "congestion_mode", "Wbar_pct_change"]].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
