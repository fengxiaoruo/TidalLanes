"""Build provisional, versioned cross-grid edge tiers for network diagnostics.

The exported edge tables are robustness candidates, not production inputs.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd

from diagnose_centerline_crossgrid_gaps import raw_pair_support
from bridge_centerline_endpoints_experiment import rook_pairs


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source-root", default="data_work/outputs/manual_centerline_rules_v11")
    p.add_argument(
        "--gap-root",
        default="data_work/outputs/centerline_crossgrid_gap_candidates_v1",
    )
    p.add_argument(
        "--output-root",
        default="data_work/outputs/centerline_crossgrid_robustness_tiers_v1",
    )
    p.add_argument("--speed-path", default="data_work/raw_data/speed_Beijing_all_wgs84.csv")
    return p.parse_args()


def speed_by_period(path):
    d = pd.read_csv(path, usecols=["roadseg_id", "speed", "hour"])
    d["speed"] = pd.to_numeric(d.speed, errors="coerce")
    hstamp = pd.to_numeric(d.hour, errors="coerce").round().astype("Int64")
    dt = pd.to_datetime(hstamp.astype(str), format="%Y%m%d%H%M", errors="coerce")
    d["weekday"] = dt.dt.weekday < 5
    d["hour_of_day"] = dt.dt.hour
    masks = {
        "am_speed_kmh": d.weekday & d.hour_of_day.ge(7) & d.hour_of_day.lt(9),
        "pm_speed_kmh": d.weekday & d.hour_of_day.ge(17) & d.hour_of_day.lt(19),
        "ff_speed_kmh": d.hour_of_day.ge(22) | d.hour_of_day.le(5),
    }
    pieces = []
    for col, mask in masks.items():
        pieces.append(d.loc[mask].groupby("roadseg_id", as_index=False).speed.median().rename(columns={"speed": col}))
    out = pieces[0]
    for piece in pieces[1:]:
        out = out.merge(piece, on="roadseg_id", how="outer")
    return out


def harmonic(group, speed_col):
    valid = group[speed_col].gt(0) & group[speed_col].notna()
    if not valid.any():
        return np.nan
    length = group.loc[valid, "support_len_m"]
    speed_ms = group.loc[valid, speed_col] / 3.6
    return float(length.sum() / (length / speed_ms).sum() * 3.6)


def network_metrics(edges, od):
    graph = nx.DiGraph()
    graph.add_weighted_edges_from(
        [(str(r.grid_o), str(r.grid_d), float(r.t_min)) for r in edges.itertuples(index=False)]
    )
    weak = list(nx.weakly_connected_components(graph))
    comp_map = {node: i for i, comp in enumerate(weak) for node in comp}
    lcc_n = max((len(x) for x in weak), default=0)
    same = od.origin_grid.map(comp_map).eq(od.destination_grid.map(comp_map)) & od.origin_grid.map(comp_map).notna()
    total = float(od.commuters_road.sum())
    reachable = float(od.loc[same, "commuters_road"].sum())
    return {
        "edge_count": int(len(edges)),
        "weak_components": int(len(weak)),
        "lcc_nodes": int(lcc_n),
        "road_commuter_same_component_share": reachable / total if total else np.nan,
    }


def main():
    args = parse_args()
    source, gap_root, output = Path(args.source_root), Path(args.gap_root), Path(args.output_root)
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing output: {output}")
    if output.resolve() in {source.resolve(), gap_root.resolve()}:
        raise SystemExit("Refusing to write into an input version.")
    for name in ["data", "metrics", "docs"]:
        (output / name).mkdir(parents=True, exist_ok=False)

    data = source / "data"
    grid = gpd.read_parquet(data / "grid_square_master.parquet")
    raw = gpd.read_parquet(data / "raw_split_centerline.parquet")
    raw = raw.loc[raw.keep_baseline & raw.is_valid_geometry & raw.is_linestring].copy()
    if grid.crs != raw.crs:
        grid = grid.to_crs(raw.crs)
    rook_u = rook_pairs(grid)
    rook_d = {(a, b) for a, b in rook_u} | {(b, a) for a, b in rook_u}
    support = raw_pair_support(raw, grid, rook_d)
    candidates = pd.read_csv(gap_root / "data" / "crossgrid_gap_candidates.csv")
    candidate_keys = pd.MultiIndex.from_frame(candidates[["grid_o", "grid_d"]])
    support = support.loc[
        pd.MultiIndex.from_frame(support[["grid_o", "grid_d"]]).isin(candidate_keys)
    ].copy()
    support = support.merge(speed_by_period(args.speed_path), on="roadseg_id", how="left")

    records = []
    centroid = grid.set_index(grid.grid_id.astype(str)).geometry.centroid
    for (go, gd), group in support.groupby(["grid_o", "grid_d"]):
        dist_m = float(centroid.loc[go].distance(centroid.loc[gd]))
        row = {"grid_o": go, "grid_d": gd, "grid_dist_m": dist_m}
        for period, col in [("am", "am_speed_kmh"), ("pm", "pm_speed_kmh"), ("ff", "ff_speed_kmh")]:
            v = harmonic(group, col)
            row[f"v_{period}_harm_kmh"] = v
            row[f"t_{period}_min"] = dist_m / (v / 3.6) / 60 if np.isfinite(v) and v > 0 else np.nan
        records.append(row)
    provisional = pd.DataFrame(records).merge(candidates, on=["grid_o", "grid_d"], how="left")
    provisional.to_csv(output / "data" / "provisional_candidate_edges.csv", index=False)

    base_am = pd.read_csv(data / "t_edges_square_AM.csv", dtype={"grid_o": str, "grid_d": str})
    base_pm = pd.read_csv(data / "t_edges_square_PM.csv", dtype={"grid_o": str, "grid_d": str})
    nodes = pd.read_csv(data / "t_nodes_square.csv", dtype={"grid_id": str})
    node_map = nodes.set_index("grid_id").node_i.to_dict()
    od = pd.read_parquet(data / "qsm_input_od_square.parquet")
    od["origin_grid"] = od.origin_grid.astype(str); od["destination_grid"] = od.destination_grid.astype(str)
    tiers = {
        "tier_A_speed_missing_matched": (
            provisional.gap_type.eq("speed_missing")
            & provisional.speed_support_share.ge(0.999)
            & provisional.matched_support_share.ge(0.999)
        ),
        "tier_B_plus_geometry_matched": (
            provisional.speed_support_share.ge(0.999)
            & provisional.matched_support_share.ge(0.999)
        ),
        "tier_C_all_full_speed": provisional.speed_support_share.ge(0.999),
    }
    metric_rows = []
    base_metrics = network_metrics(base_am, od)
    metric_rows.append({"scenario": "baseline", "candidate_count": 0, **base_metrics})
    for name, mask in tiers.items():
        add = provisional.loc[mask & provisional.t_am_min.notna() & provisional.t_pm_min.notna() & provisional.t_ff_min.notna()].copy()
        def add_edges(base, time_col):
            extra = pd.DataFrame({
                "grid_o": add.grid_o,
                "grid_d": add.grid_d,
                "i": add.grid_o.map(node_map),
                "j": add.grid_d.map(node_map),
                "t_min": add[time_col],
                "t_ff_min": add.t_ff_min,
                "candidate_id": add.candidate_id,
                "edge_source": name,
            })
            old = base.copy(); old["candidate_id"] = pd.NA; old["edge_source"] = "baseline"
            return pd.concat([old, extra], ignore_index=True)
        am_out = add_edges(base_am, "t_am_min")
        pm_out = add_edges(base_pm, "t_pm_min")
        am_out.to_csv(output / "data" / f"t_edges_square_AM_{name}.csv", index=False)
        pm_out.to_csv(output / "data" / f"t_edges_square_PM_{name}.csv", index=False)
        metrics = network_metrics(am_out, od)
        metric_rows.append({"scenario": name, "candidate_count": int(len(add)), **metrics})

    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(output / "metrics" / "network_impact_by_tier.csv", index=False)
    summary = {
        "version": output.name,
        "source_root": str(source),
        "gap_root": str(gap_root),
        "source_untouched": True,
        "production_ready": False,
        "candidate_edges": int(len(provisional)),
        "scenario_metrics": metrics.to_dict("records"),
    }
    (output / "metrics" / "run_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (output / "README.md").write_text(
        """# Provisional centerline cross-grid robustness tiers

These files are diagnostic edge tables, not production QSM inputs. Candidate travel times are constructed from raw-road length-weighted harmonic AM, PM, and free-flow speeds and square-grid centroid distance. The retained centerline version and its stage06 outputs remain unchanged.
""",
        encoding="utf-8",
    )
    (output / "docs" / "sample-lineage.md").write_text(
        f"""# Sample lineage

The immutable source is `{source}`. Cross-grid candidate definitions are read from `{gap_root}`. Raw segment crossings and raw speed observations are reconstructed before the three nested tiers are exported. The resulting edge tables are provisional robustness inputs and are not referenced by the active model configuration.
""",
        encoding="utf-8",
    )
    (output / "docs" / "replication-checklist.md").write_text(
        f"""# Replication checklist

- [x] Input versions are immutable.
- [x] The output directory refuses overwrite.
- [x] AM, PM, and free-flow speed support is required.
- [x] Tier definitions are nested and explicit.
- [x] Production configurations remain unchanged.

```bash
python data_work/src/diagnostics/build_centerline_crossgrid_robustness_tiers.py --source-root {source} --gap-root {gap_root} --output-root {output}
```
""",
        encoding="utf-8",
    )
    shutil.copy2(__file__, output / "docs" / Path(__file__).name)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
