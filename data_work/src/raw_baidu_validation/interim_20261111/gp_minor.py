"""1B + OSM minor roads (residential/unclassified/living_street[/service]) at a fixed speed; Baidu query points snap to nearest node."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work")
from src.network import build_osm_grid_travel_time as gt
from scipy.spatial import cKDTree
from scipy.sparse.csgraph import dijkstra
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"; V=f"{O}/grid_pair_baidu_validation_v3"
S="/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad"
ARCS="/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
a=gt.impute(gt.arc_table()); b=a[gt.gap_keep(a)].copy()
mn=pd.read_parquet(ARCS,columns=["edge_idx","road_class","from_node_id","to_node_id","length_m"])
for s in ["from","to"]:
    p=mn[f"{s}_node_id"].str.extract(r"^([0-9.]+),([0-9.]+)\|"); mn[f"{s}_xy"]=p[0]+","+p[1]
def run(classes,v,tag):
    x=mn[mn.road_class.isin(classes)].copy()
    for p in ["AM","PM","FF"]: x[f"v_{p}"]=float(v)
    x["obs"]=False
    G=gt.Graph(pd.concat([b[["from_xy","to_xy","length_m","obs","v_AM","v_PM","v_FF"]],x[["from_xy","to_xy","length_m","obs","v_AM","v_PM","v_FF"]]],ignore_index=True))
    lat_o,lon_o=np.array([s.split(",") for s in m.origin_text],float).T; lat_d,lon_d=np.array([s.split(",") for s in m.destination_text],float).T
    tr=cKDTree(np.c_[G.x,G.y]); so,io=tr.query(np.c_[gt.T3857.transform(lon_o,lat_o)]); sd,id_=tr.query(np.c_[gt.T3857.transform(lon_d,lat_d)])
    res=pd.DataFrame({"request_id":m.request_id,"tag":tag,"snap_o_m":so*np.cos(np.radians(lat_o)),"snap_d_m":sd*np.cos(np.radians(lat_d))})
    for p in ["AM","PM","FF"]:
        W=G.arc_of[p][1]; src=np.unique(io); D=dijkstra(W,directed=True,indices=src,limit=240.0); row={v_:i for i,v_ in enumerate(src)}
        res[f"t_{p}"]=[D[row[o],d] for o,d in zip(io,id_)]
    print(tag,"nodes",G.N,"lcc km share",round(G.km_share,3),"snap median",round(float(res[['snap_o_m','snap_d_m']].max(axis=1).median())),flush=True)
    return res
R=[]
base=["residential","unclassified","living_street"]
for v in [10,15,20]: R.append(run(base,v,f"minor_v{v}"))
R.append(run(base+["service"],15,"minor_srv_v15"))
pd.concat(R).to_parquet(f"{S}/gp_minor.parquet",index=False)
