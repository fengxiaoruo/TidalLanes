"""
Gate5 model input bundle with the 2025-05 platform OD
=====================================================

Copies nodes and edges of ``model_input_edgeproj_v2_lcc_gate5_v1`` unchanged and
replaces the OD with the 202505_v01 platform exports (Platform/runs/202505_v01/
query-results). The source bundle is only read; the new bundle goes to a new
version directory and the script refuses to overwrite it.

OD columns (all strict, joint home-work SIMs, >=5-SIM cells only):
    commuters_road   = car_am_trips_day (main; the column the runners read)
    car_am_trips_day = AM-peak HW mode1 trips, 19 ordinary weekdays pooled / 19
    car_am_people    = distinct SIMs with an AM-peak HW mode1 trip in the 19 days
    hw_people_month  = monthly joint home-work SIMs, all modes (0401 N export)
Counts are sample SIMs, not scaled to population. Cells outside the gate5 model
nodes (including OUT) are dropped and reported.

Node ``residents``/``jobs`` are replaced by the 2025 OD marginals (v2); the 2022
all-mode values are kept as ``residents_2022``/``jobs_2022``. Nodes and edges
themselves do not depend on the OD: the gate5 node set is the largest connected
component of the gated edge graph (build_locked_bundle), so it is not rebuilt.
"""
from __future__ import annotations

import argparse
import glob
import json
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]   # -> data_work/
N_WEEKDAYS = 19


# variant -> (export, rule). All strict joint HW, mode1, cells >=5 SIM, trips per weekday (/19).
OD_VARIANTS = {
    "win0709": ("0407 WIN_strict", "HW trips departing 07:00-09:00 (2026-11 mainline)"),
    "dep0709": ("0712 OB band dep_0709", "HW trips departing 07:00-09:00 (0701 rebuild, should equal win0709)"),
    "dep0610": ("0712 OB band dep_0610", "HW trips departing 06:00-10:00"),
    "onroad0610": ("0713 OB band onroad_0610", "HW trips on the road at some point in 06:00-10:00"),
    "onroad0709": ("0713 OB band onroad_0709", "HW trips on the road at some point in 07:00-09:00"),
    "allday": ("0711 OB band all_day", "HW trips at any time of day"),
    "chain0610": ("0712 OB dep_0610 + 0742 CHND dep_0610", "HW trips departing 06:00-10:00 plus home-to-work chains (transfer_only + activity_stop) with all legs mode1, first leg departing 06:00-10:00"),
    "chain0610mx": ("0712 OB dep_0610 + 0742 CHND dep_0610", "as chain0610, plus chains with some (not all) legs mode1"),
    "chain0709": ("0712 OB dep_0709 + 0743 CHND dep_0709", "HW trips departing 07:00-09:00 plus home-to-work chains (transfer_only + activity_stop) with all legs mode1, first leg departing 07:00-09:00"),
}


def load_variant(qr: Path, variant: str) -> pd.DataFrame:
    dt = {"grid_o": str, "grid_d": str}
    if variant == "win0709":
        win = pd.concat([pd.read_csv(f, dtype=dt) for f in sorted(glob.glob(str(qr / "TL25M05_V01_040[78]_WIN_*.csv")))])
        d = win[(win.variant == "strict") & (win.purpose == "HW") & (win.mode_group == "mode1")]
        n = d.n_trips
    elif variant.startswith("chain"):
        # direct HW trips from 0712 (identical to 0702 direct, checked) + non-direct chains from 0742 / 0743;
        # every cell is >=5 SIM on its own, so the sum of a pair's cells is the pair's commuters
        band = "dep_0709" if variant == "chain0709" else "dep_0610"
        modes = ["mode1", "mixed"] if variant == "chain0610mx" else ["mode1"]
        a = pd.read_csv(qr / "TL25M05_V01_0712_OB_HW_dep0610_dep0709.csv", dtype=dt)
        a = a[(a.purpose == "HW") & (a.band == band) & (a.mode_group == "mode1")][["grid_o", "grid_d", "n_trips", "n_people"]]
        f = {"dep_0610": "0742_CHND_HW_dep0610", "dep_0709": "0743_CHND_HW_dep0709"}[band]
        c = pd.read_csv(qr / f"TL25M05_V01_{f}.csv", dtype=dt)
        c = c[(c.purpose == "HW") & (c.band == band) & c.mode_group.isin(modes)]
        c = c.rename(columns={"n_chains": "n_trips"})[["grid_o", "grid_d", "n_trips", "n_people"]]
        d = pd.concat([a, c]).groupby(["grid_o", "grid_d"], as_index=False).agg(n_trips=("n_trips", "sum"), n_people=("n_people", "sum"))
        n = d.n_trips
    else:
        band = {"dep0709": "dep_0709", "dep0610": "dep_0610", "onroad0610": "onroad_0610",
                "onroad0709": "onroad_0709", "allday": "all_day"}[variant]
        f = {"dep_0709": "0712_OB_HW_dep0610_dep0709", "dep_0610": "0712_OB_HW_dep0610_dep0709",
             "onroad_0610": "0713_OB_HW_onroad0610_onroad0709", "onroad_0709": "0713_OB_HW_onroad0610_onroad0709",
             "all_day": "0711_OB_HW_allday"}[band]
        d = pd.read_csv(qr / f"TL25M05_V01_{f}.csv", dtype=dt)
        d = d[(d.purpose == "HW") & (d.band == band) & (d.mode_group == "mode1")]
        n = d.n_trips
    d = d.assign(car_am_trips_day=n / N_WEEKDAYS, car_am_people=d.n_people)
    d = d.rename(columns={"grid_o": "home_grid", "grid_d": "work_grid"})
    assert not d.duplicated(["home_grid", "work_grid"]).any()
    return d[["home_grid", "work_grid", "car_am_trips_day", "car_am_people"]]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(ROOT.parent))
    p.add_argument("--source-version", default="model_input_edgeproj_v2_lcc_gate5_v1")
    p.add_argument("--version-id", default="model_input_edgeproj_v2_lcc_gate5_od2025_v2")
    p.add_argument("--qr", default="Platform/runs/202505_v01/query-results")
    p.add_argument("--od-variant", choices=list(OD_VARIANTS), default="win0709",
                   help="which platform export feeds commuters_road (default: 0407 window, the 2026-11 mainline)")
    args = p.parse_args(argv)
    repo = Path(args.repo_root).resolve()
    src = repo / "data_work" / "outputs" / args.source_version / "data"
    out = repo / "data_work" / "outputs" / args.version_id
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --version-id")
    (out / "data").mkdir(parents=True)

    nodes = pd.read_parquet(src / "qsm_input_nodes_square.parquet")
    shutil.copy2(src / "qsm_input_edges_square.parquet", out / "data" / "qsm_input_edges_square.parquet")
    node_i = dict(zip(nodes.grid_id, nodes.node_i))

    qr = repo / args.qr
    car = load_variant(qr, args.od_variant)
    n = pd.read_csv(qr / "TL25M05_V01_0401_N.csv", dtype={"home_grid_id": str, "work_grid_id": str})
    n = n.rename(columns={"home_grid_id": "home_grid", "work_grid_id": "work_grid", "n_people": "hw_people_month"})[["home_grid", "work_grid", "hw_people_month"]]
    od = car.merge(n, on=["home_grid", "work_grid"], how="outer").fillna({"car_am_trips_day": 0.0, "car_am_people": 0, "hw_people_month": 0})

    totals = {c: float(od[c].sum()) for c in ["car_am_trips_day", "car_am_people", "hw_people_month"]}
    inside = od.home_grid.isin(node_i) & od.work_grid.isin(node_i)
    od = od[inside].copy()
    od["home_i"] = od.home_grid.map(node_i).astype(int)
    od["work_i"] = od.work_grid.map(node_i).astype(int)
    od["origin_grid"], od["destination_grid"] = od.home_grid, od.work_grid
    od["origin_node"], od["destination_node"] = od.home_i, od.work_i
    od["commuters_road"] = od.car_am_trips_day
    od["grid_type"] = "square"
    od["version_id"] = args.version_id
    od["sample_definition"] = "platform_202505_v01_strict_joint_hw_ge5sim"
    od["od_variant"] = args.od_variant
    od["road_mode_rule"] = "commuters_road=car_am_trips_day: " + OD_VARIANTS[args.od_variant][1]
    od.to_parquet(out / "data" / "qsm_input_od_square.parquet", index=False)

    lr = od.groupby("home_grid").commuters_road.sum()
    lf = od.groupby("work_grid").commuters_road.sum()
    nodes = nodes.rename(columns={"residents": "residents_2022", "jobs": "jobs_2022"})
    nodes = nodes.assign(residents=nodes.grid_id.map(lr).fillna(0.0), jobs=nodes.grid_id.map(lf).fillna(0.0),
                         version_id=args.version_id)
    nodes.to_parquet(out / "data" / "qsm_input_nodes_square.parquet", index=False)

    meta = {
        "source_version": args.source_version, "version_id": args.version_id,
        "od_source": f"Platform 202505_v01 export {OD_VARIANTS[args.od_variant][0]} and 0401 (N)",
        "od_variant": args.od_variant, "od_variant_rule": OD_VARIANTS[args.od_variant][1],
        "totals_all_cells": totals,
        "totals_in_model_nodes": {c: float(od[c].sum()) for c in totals},
        "share_in_model_nodes": {c: float(od[c].sum() / totals[c]) for c in totals},
        "od_rows": int(len(od)),
        "nodes_zero_residents": int((nodes.residents == 0).sum()),
        "nodes_zero_jobs": int((nodes.jobs == 0).sum()),
        "same_cell_share_commuters_road": float(od.loc[od.home_grid == od.work_grid, "commuters_road"].sum() / od.commuters_road.sum()),
    }
    (out / "BUNDLE_META.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
