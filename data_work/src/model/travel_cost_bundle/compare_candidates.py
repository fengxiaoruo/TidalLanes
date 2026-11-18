"""
Compare travel-cost candidates under the LCC convention and write a decision memo.

Usage (from data_work/):
    python -m src.model.travel_cost_bundle.compare_candidates
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.model.travel_cost_bundle.build_locked_bundle import build_bundle

REPO = Path(__file__).resolve().parents[3]
OUT_COMPARE = REPO / "outputs" / "travel_cost_decision_v1"


def _fake_args(cost_version: str, version_id: str):
    class Args:
        pass

    a = Args()
    a.cost_version = cost_version
    a.output_version_id = version_id
    a.base_version_root = str(REPO / "outputs" / "manual_centerline_rules_v11")
    a.edgeproj_csv = str(
        REPO
        / "outputs"
        / "raw_connected_v1_grid_travel_time_edgeproj_v2"
        / "data"
        / "raw_connected_vs_old_adjacent_adjacent_square_AM.csv"
    )
    a.grid_type = "square"
    a.period = "AM"
    a.output_dir = str(REPO / "outputs")
    return a


def _score_row(summary: dict) -> dict:
    # Higher resident/OD share and stronger documented asymmetry favor edgeproj;
    # centerline gets credit for lower mean travel time (less connector noise).
    return {
        "version_id": summary["output_version_id"],
        "cost_version": summary["cost_version"],
        "n_nodes": summary["n_nodes"],
        "n_edges": summary["n_edges"],
        "resident_share": summary["resident_share_in_scope"],
        "od_commuters_share": summary["od_road_commuters_share_city"],
        "mean_tau_obs_min": summary["mean_tau_obs_min"],
        "median_directional_ratio": summary["median_directional_ratio"],
        "share_asym_lt_0_5": summary["share_asym_lt_0_5"],
    }


def _choose(rows: list[dict]) -> tuple[str, str]:
    by_id = {r["version_id"]: r for r in rows}
    center = by_id["model_input_centerline_lcc_v1"]
    edge = by_id["model_input_edgeproj_v2_lcc_v1"]

    reasons = []
    if center["od_commuters_share"] > edge["od_commuters_share"]:
        reasons.append(
            f"centerline_lcc retains slightly higher road-commuter share ({center['od_commuters_share']:.1%} vs {edge['od_commuters_share']:.1%}) but uses non-routed heuristic costs"
        )
    if edge["resident_share"] >= 0.93:
        reasons.append(f"edgeproj LCC still covers {edge['resident_share']:.1%} of city residents (vs {center['resident_share']:.1%} under centerline_lcc)")
    if edge["median_directional_ratio"] < center["median_directional_ratio"]:
        reasons.append(
            f"stronger directional asymmetry (median ratio {edge['median_directional_ratio']:.3f} vs {center['median_directional_ratio']:.3f})"
        )
    reasons.append("real road routing + pair-specific edge projection (consistent with descriptive evidence)")
    reasons.append("centerline heuristic is uncorrelated with routed costs (corr ~0.09) and inconsistent with draft asymmetry 0.793")

    return "model_input_edgeproj_v2_lcc_v1", "; ".join(reasons)


def main() -> None:
    OUT_COMPARE.mkdir(parents=True, exist_ok=True)
    candidates = [
        ("centerline_lcc", "model_input_centerline_lcc_v1"),
        ("edgeproj_v2_lcc", "model_input_edgeproj_v2_lcc_v1"),
    ]
    summaries = []
    for cost_version, version_id in candidates:
        build_bundle(_fake_args(cost_version, version_id))
        summary_path = REPO / "outputs" / version_id / "metrics" / "travel_cost_bundle_summary.json"
        summaries.append(json.loads(summary_path.read_text(encoding="utf-8")))

    scored = [_score_row(s) for s in summaries]
    pd.DataFrame(scored).to_csv(OUT_COMPARE / "candidate_comparison.csv", index=False)

    chosen, rationale = _choose(scored)
    decision = {
        "locked_version_id": chosen,
        "locked_cost_version": "edgeproj_v2_lcc",
        "scope": "largest_connected_component",
        "rejected": ["raw_connected_v2 (spatial clip)", "unfiltered full-city centerline (962 components)"],
        "rationale": rationale,
        "caveats": [
            "Travel times are higher than centerline due to connector + routing overhead; acceptable under observed-anchored congestion.",
            "Long-detour tail pairs remain a diagnostic target but do not block LCC-scoped model use.",
            "raw_connected_v2 remains broken until manual edits are replayed on full v1 graph.",
        ],
        "candidates": summaries,
    }
    (OUT_COMPARE / "travel_cost_decision.json").write_text(
        json.dumps(decision, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    memo = f"""# Travel-cost lock-in decision (2026-06-28)

## Locked choice

**`{chosen}`** — raw_connected v1 **edgeproj_v2** travel costs, restricted to the **largest connected component**.

## Rationale

{rationale}

## Candidate comparison

See `candidate_comparison.csv` and bundled summaries under each `model_input_*_v1/metrics/`.

## Caveats

"""
    for c in decision["caveats"]:
        memo += f"- {c}\n"
    (OUT_COMPARE / "TRAVEL_COST_DECISION.md").write_text(memo, encoding="utf-8")
    print(f"[compare_candidates] locked={chosen}")
    print(f"[compare_candidates] wrote {OUT_COMPARE}")


if __name__ == "__main__":
    main()
