import glob, numpy as np, pandas as pd
O="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs"; V=f"{O}/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")[["request_id","grid_from","grid_to"]]
B=[]
for f in glob.glob(f"{V}/api/crawl_sessions/*_d0*/results_flat.csv"):
    r=pd.read_csv(f); r=r[r.api_status==0]; r["panel"]=f.split("/")[-2].split("_")[0]; B.append(r[["request_id","panel","duration_sec"]])
B=(pd.concat(B).groupby(["request_id","panel"]).duration_sec.mean().unstack()/60).rename(columns={"FREE":"FF","AM":"AM_b","PM":"PM_b"}).rename(columns={"FF":"FF_b"})
L=np.log
def c(x,y):
    ok=np.isfinite(x)&np.isfinite(y); return round(float(np.corrcoef(x[ok],y[ok])[0,1]),3)
for nm,src in [("tm nearest (main)","osm_grid_travel_time_v2"),("tm minor access v20","osm_grid_travel_time_v2_acc_minor_v20")]:
    p=pd.read_parquet(f"{O}/{src}/pairs_osm1b_L300_sq3000_model_origin.parquet")[["grid_from","grid_to","tm_AM","tm_PM","tm_FF"]]
    d=m.merge(p,on=["grid_from","grid_to"]).set_index("request_id").join(B)
    d2=d.reset_index(); d2["key"]=d2.grid_from+"|"+d2.grid_to; d2["rkey"]=d2.grid_to+"|"+d2.grid_from
    j=d2.merge(d2,left_on="key",right_on="rkey",suffixes=("","_r")); j=j[j.grid_from<j.grid_to]
    bt=L(j.AM_b/j.AM_b_r)-L(j.PM_b/j.PM_b_r); mt=L(j.tm_AM/j.tm_AM_r)-L(j.tm_PM/j.tm_PM_r)
    print(f"{nm:22s} AM {c(L(d.tm_AM),L(d.AM_b))} PM {c(L(d.tm_PM),L(d.PM_b))} FF {c(L(d.tm_FF),L(d.FF_b))} | ratio AM {np.nanmedian(d.tm_AM/d.AM_b):.2f} | dirAM {c(L(j.tm_AM/j.tm_AM_r),L(j.AM_b/j.AM_b_r))} dirFF {c(L(j.tm_FF/j.tm_FF_r),L(j.FF_b/j.FF_b_r))} | tidal {c(mt,bt)} |mag| {np.nanmean(np.abs(mt)):.3f} vs {np.nanmean(np.abs(bt)):.3f} | AM/FF {c(L(d.tm_AM/d.tm_FF),L(d.AM_b/d.FF_b))} med AM/FF {np.nanmedian(d.tm_AM/d.tm_FF):.3f} vs {np.nanmedian(d.AM_b/d.FF_b):.3f}")
