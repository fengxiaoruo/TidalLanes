import sys, time, numpy as np, pandas as pd
sys.path.insert(0, "/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work")
from src.network import build_osm_grid_travel_time as gt
from src.network import minor_access as ma
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"; V=f"{O}/grid_pair_baidu_validation_v3"
S="/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
pts=pd.Index(pd.concat([m.origin_text,m.destination_text]).unique()); lat,lon=np.array([s.split(",") for s in pts],float).T
oi,di=pts.get_indexer(m.origin_text),pts.get_indexer(m.destination_text)
t0=time.time(); a=gt.impute(gt.arc_table()); b=a[gt.gap_keep(a)].copy(); G=gt.Graph(b); minor=ma.load_minor_arcs(); print("graph",round(time.time()-t0),flush=True)
R=[]
for v,tm in [(15,6),(15,4),(20,6),(10,9)]:
    A=ma.Access(G,minor,v_access=v,t_max=tm); out,inn=A.connectors(lon,lat)
    src=np.unique(oi); row={s:i for i,s in enumerate(src)}
    res=pd.DataFrame({"request_id":m.request_id,"tag":f"acc_v{v}_t{tm}","snap_m":np.maximum(A.last_snap_m[oi],A.last_snap_m[di])})
    for p in ["AM","PM","FF"]:
        T=ma.point_times(G.arc_of[p][1],G.N,out,inn,len(pts),src)
        res[f"t_{p}"]=[T[row[o],d] for o,d in zip(oi,di)]
    print(res.tag[0],"conn/pt out",round(len(out)/len(pts),1),"in",round(len(inn)/len(pts),1),"secs",round(time.time()-t0),flush=True)
    R.append(res)
pd.concat(R).to_parquet(f"{S}/gp_minor_access.parquet",index=False)
