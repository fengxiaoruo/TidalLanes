import glob, numpy as np, pandas as pd
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"; V=f"{O}/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
B=[]
for f in glob.glob(f"{V}/api/crawl_sessions/*_d0*/results_flat.csv"):
    r=pd.read_csv(f); r=r[r.api_status==0]; r["panel"]=f.split("/")[-2].split("_")[0]; B.append(r[["request_id","panel","duration_sec"]])
B=pd.concat(B).groupby(["request_id","panel"]).duration_sec.mean().unstack()/60
E=f"{O}/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_square_"
g={p:pd.read_csv(E+p+".csv")[["home_grid","work_grid","tau_obs_min","tau_ff_min","route_length_m"]].rename(columns={"home_grid":"grid_from","work_grid":"grid_to","tau_obs_min":f"g_{p}","tau_ff_min":f"gff_{p}","route_length_m":f"gkm_{p}"}) for p in ["AM","PM"]}
d=m.merge(g["AM"],on=["grid_from","grid_to"],how="left").merge(g["PM"],on=["grid_from","grid_to"],how="left").set_index("request_id").join(B)
print("edgeproj AM in gate5 manifest equal:",round(float(np.isclose(d.g_AM,d.tau_obs_gate5,rtol=1e-6).mean()),3),"PM avail",int(d.g_PM.notna().sum()))
c=lambda x,y:(lambda ok:round(float(np.corrcoef(x[ok],y[ok])[0,1]),3))(np.isfinite(x)&np.isfinite(y))
L=np.log
nets={"1B":("tm_AM_osm1b","tm_PM_osm1b","tm_FF_osm1b"),"edgeproj(gate5)":("g_AM","g_PM","gff_AM")}
d2=d.reset_index(); d2["key"]=d2.grid_from+"|"+d2.grid_to; d2["rkey"]=d2.grid_to+"|"+d2.grid_from
j=d2.merge(d2,left_on="key",right_on="rkey",suffixes=("","_r")); j=j[j.grid_from<j.grid_to]
bt=L(j.AM/j.AM_r)-L(j.PM/j.PM_r)
for nm,(a,p,f) in nets.items():
    mt=L(j[a]/j[a+"_r"])-L(j[p]/j[p+"_r"]); ok=np.isfinite(mt)&np.isfinite(bt)
    print(nm,"AM time",c(L(d[a]),L(d.AM)),"PM time",c(L(d[p]),L(d.PM)),"AM/FF",c(L(d[a]/d[f]),L(d.AM/d.FREE)),
          "tidal",c(mt,bt),"slope",round(np.polyfit(mt[ok],bt[ok],1)[0],2),"n",int(ok.sum()),"mean|tidal| model",round(float(mt[ok].abs().mean()),3),"baidu",round(float(bt[ok].abs().mean()),3))
