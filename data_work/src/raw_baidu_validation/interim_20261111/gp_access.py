"""Connector access: each query point links to the k nearest road nodes within R, time = detour*dist/v_acc."""
import sys, glob, numpy as np, pandas as pd
sys.path.insert(0, "/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work")
from src.network import build_osm_grid_travel_time as gt
from scipy.spatial import cKDTree
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"; V=f"{O}/grid_pair_baidu_validation_v3"
S="/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
pts=pd.Index(pd.concat([m.origin_text,m.destination_text]).unique())
lat,lon=np.array([s.split(",") for s in pts],float).T
a=gt.impute(gt.arc_table()); b=a[gt.gap_keep(a)].copy()
DET=1.3; K=20; R=2000.0
out=[]
for scheme,df in [("1b",b),("1a",a)]:
    G=gt.Graph(df); N=G.N; P=len(pts)
    px,py=gt.T3857.transform(lon,lat); coslat=np.cos(np.radians(lat))
    dist,idx=cKDTree(np.c_[G.x,G.y]).query(np.c_[px,py],k=K)
    dist=dist*coslat[:,None]
    keep=dist<=np.maximum(R,dist[:,[0]])          # always keep the nearest
    pi=np.repeat(np.arange(P),K)[keep.ravel()]; ni=idx.ravel()[keep.ravel()]; dm=dist.ravel()[keep.ravel()]
    km_arc=G.a.length_m.to_numpy()/1000
    for v_acc in [10,15,20,30]:
        for p in ["AM","PM","FF"]:
            arcs,W=G.arc_of[p]; W=W.tocoo()
            ct=DET*dm/1000/v_acc*60
            # origin virtual nodes N..N+P-1, destination virtual nodes N+P..N+2P-1
            r=np.r_[W.row, N+pi, ni]; c=np.r_[W.col, ni, N+P+pi]; w=np.r_[W.data, ct+1e-9, ct+1e-9]
            M=coo_matrix((w,(r,c)),shape=(N+2*P,)*2).tocsr()
            oi=pts.get_indexer(m.origin_text); di=pts.get_indexer(m.destination_text)
            src=np.unique(oi)
            D=dijkstra(M,directed=True,indices=N+src,limit=240.0)
            row={v:i for i,v in enumerate(src)}
            t=np.array([D[row[o],N+P+d] for o,d in zip(oi,di)])
            out.append(pd.DataFrame({"request_id":m.request_id,"scheme":scheme,"v_acc":v_acc,"period":p,"t":t}))
        print(scheme,v_acc,"done",flush=True)
    # access distance of the nearest node, for reference
    out.append(pd.DataFrame({"request_id":m.request_id,"scheme":scheme,"v_acc":-1,"period":"snap",
                             "t":np.maximum(dist[pts.get_indexer(m.origin_text),0],dist[pts.get_indexer(m.destination_text),0])}))
pd.concat(out).to_parquet(f"{S}/gp_access.parquet",index=False)
