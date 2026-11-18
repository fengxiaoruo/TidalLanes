"""
Road input for a centerline rebuild on raw_connected_v3 (nextstep D step 6)
==========================================================================
stage01 reads a Baidu-style road layer (cityname, roadseg_id, roadname, roadtype,
semantic, location, geometry). This writes the same layer from the v3 directed edges
(src/raw_connected_v3/Processed_Data/directed_edges.xlsx), so the v11 corridor rules
(keyed by roadseg_id or roadname) and the speed attachment (by roadseg_id) work unchanged:
    roadseg_id = v3 road_id (original Baidu id; split pieces keep their parent id)
    roadtype   = the original Baidu roadtype for that id
    geometry   = v3 geometry; cells truncated by Excel (32,767 chars) use the original layer
Manual connector edges (road_id MANUAL_*, no Baidu speed) are kept as roads. Their
roadtype is the most common roadtype (ties: the higher class, i.e. smaller code) among
Baidu edges sharing either end node, so stage04 gives them that class's lane count;
connectors touching no Baidu edge take the citywide modal roadtype. They carry no
speed: stage03 leaves them unobserved, the same treatment as any v11 road without data.
Inputs read only. Output: data_work/outputs/<output-name>/data/beijing_roads_v3.gpkg
(refuses to overwrite)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkt

ROOT = Path(__file__).resolve().parents[2]
V3 = ROOT / "src/raw_connected_v3/Processed_Data/directed_edges.xlsx"
SHP = ROOT / "raw_data/gis/roads_baidu/beijing_roads.shp"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-name", default="centerline_v3_input_v1")
    args = ap.parse_args(argv)
    out = ROOT / "outputs" / args.output_name
    if out.exists():
        raise SystemExit(f"{out} exists; choose a new --output-name")
    (out / "data").mkdir(parents=True)

    d = pd.read_excel(V3, usecols=["edge_id", "from_node_id", "to_node_id", "road_id", "roadname", "semantic", "location", "geometry"])
    shp = gpd.read_file(SHP).to_crs(4326)
    s = pd.DataFrame(shp[["roadseg_id", "roadtype", "cityname"]])
    d = d.merge(s, left_on="road_id", right_on="roadseg_id", how="left")
    manual = d.roadseg_id.isna()
    # roadtype for manual connectors from Baidu edges at their end nodes
    ends = pd.concat([d.loc[~manual, ["from_node_id", "roadtype"]].rename(columns={"from_node_id": "node"}),
                      d.loc[~manual, ["to_node_id", "roadtype"]].rename(columns={"to_node_id": "node"})])
    city_mode = int(d.loc[~manual, "roadtype"].mode().min())
    rt = []
    for f, t in zip(d.loc[manual, "from_node_id"], d.loc[manual, "to_node_id"]):
        v = ends.roadtype[ends.node.isin([f, t])]
        if len(v):
            c = v.value_counts()
            rt.append(int(c[c == c.max()].index.min()))
        else:
            rt.append(city_mode)
    d.loc[manual, "roadtype"] = rt
    d.loc[manual, "roadseg_id"] = d.loc[manual, "road_id"]
    d["cityname"] = d.cityname.fillna("北京市")
    d["is_manual_connector"] = manual
    # Excel truncates cells at 32,767 characters: those geometries come from the original layer
    trunc = d.geometry.str.len() >= 32767
    shp_geom = shp.set_index("roadseg_id").geometry
    geom = [shp_geom[r] if t else wkt.loads(w) for r, t, w in zip(d.road_id, trunc, d.geometry)]
    g = gpd.GeoDataFrame(d[["cityname", "roadseg_id", "roadname", "roadtype", "semantic", "location", "edge_id", "is_manual_connector"]],
                         geometry=geom, crs=4326)
    g["roadtype"] = g.roadtype.astype(int)
    g.to_file(out / "data/beijing_roads_v3.gpkg", driver="GPKG")
    g3857 = g.to_crs(3857)
    info = {"rows": int(len(g)), "geometry_from_original_layer_excel_truncated": int(trunc.sum()), "baidu_rows": int((~manual).sum()), "manual_connectors": int(manual.sum()),
            "manual_km": round(float(g3857.length[manual.to_numpy()].sum() / 1000 * 0.766), 1),
            "unique_roadseg_id": int(g.roadseg_id.nunique()),
            "manual_roadtype_counts": g.roadtype[manual.to_numpy()].value_counts().sort_index().to_dict(),
            "note": "manual_km uses cos(lat ~40) to undo the 3857 stretch"}
    (out / "summary.json").write_text(json.dumps(info, indent=2, ensure_ascii=False, default=int))
    print(json.dumps(info, ensure_ascii=False, default=int))


if __name__ == "__main__":
    main()
