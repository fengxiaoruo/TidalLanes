import glob, numpy as np, pandas as pd
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"
V=f"{O}/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
b=pd.concat([pd.read_csv(f).assign(s=f.split("/")[-2]) for f in glob.glob(f"{V}/api/crawl_sessions/AM_*_d0*/results_flat.csv")])
b=b[b.api_status==0].groupby("request_id").agg(bkm=("distance_m",lambda x:x.mean()/1000),bmin=("duration_sec",lambda x:x.mean()/60))
d=m.set_index("request_id").join(b,how="inner")
p1b=pd.read_parquet(f"{O}/osm_grid_travel_time_v2/pairs_osm1b_L300_sq3000_model_origin.parquet")
p1a=pd.read_parquet(f"{O}/osm_grid_travel_time_v2/pairs_osm1a_sq3000_model_origin.parquet")
k=["grid_from","grid_to"]
d=d.reset_index().merge(p1b[k+["km_AM","t_AM","obs_share_AM","dist_km","tm_AM"]].rename(columns=lambda c:c if c in k else c+"_1b"),on=k,how="left")\
   .merge(p1a[k+["km_AM","t_AM","tm_AM"]].rename(columns=lambda c:c if c in k else c+"_1a"),on=k,how="left")
print("length_m == km_AM_1b*1000 share:",round(float(np.isclose(d.length_m,d.km_AM_1b*1000,rtol=1e-3).mean()),3), " 1A matched:",int(d.km_AM_1a.notna().sum()),"/",len(d))
d["r1b"]=d.km_AM_1b/d.bkm; d["r1a"]=d.km_AM_1a/d.bkm
def cls(r): return pd.cut(r,[0,0.67,1.5,np.inf],labels=["short<0.67","ok","long>1.5"])
print("\n1B length class vs 1A length class (rows=1B, cols=1A):"); print(pd.crosstab(cls(d.r1b),cls(d.r1a),margins=True))
for nm,c in [("1B","km_AM_1b"),("1A","km_AM_1a")]:
    ok=d[c].notna(); print(nm,"corr log km vs Baidu",round(np.corrcoef(np.log(d[c][ok]),np.log(d.bkm[ok]))[0,1],3),"median ratio",round(float((d[c]/d.bkm).median()),3))
for nm,c in [("1B centre","t_AM_1b"),("1B multi","tm_AM_1b"),("1A centre","t_AM_1a"),("1A multi","tm_AM_1a")]:
    ok=d[c].notna()&(d[c]>0); print(nm,"time corr log",round(np.corrcoef(np.log(d[c][ok]),np.log(d.bmin[ok]))[0,1],3))
print("\nobs_share_AM_1b by 1B length class:"); print(d.groupby(cls(d.r1b),observed=True).obs_share_AM_1b.describe()[["count","25%","50%","75%"]].round(2))
print("\nBaidu km / centre distance by 1B class:"); d["bk_over_cd"]=d.bkm/d.dist_km_1b; print(d.groupby(cls(d.r1b),observed=True).bk_over_cd.median().round(2))
print("1B km / centre distance by 1B class:"); print(d.groupby(cls(d.r1b),observed=True).apply(lambda s:(s.km_AM_1b/s.dist_km_1b).median()).round(2))
