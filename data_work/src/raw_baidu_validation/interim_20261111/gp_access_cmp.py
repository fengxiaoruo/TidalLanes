import glob, numpy as np, pandas as pd
S="/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad"
V="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")[["request_id","grid_from","grid_to"]]
B=[]
for f in glob.glob(f"{V}/api/crawl_sessions/*_d0*/results_flat.csv"):
    r=pd.read_csv(f); r=r[r.api_status==0]; r["panel"]=f.split("/")[-2].split("_")[0]; B.append(r[["request_id","panel","duration_sec"]])
B=(pd.concat(B).groupby(["request_id","panel"]).duration_sec.mean().unstack()/60).rename(columns={"FREE":"FF"})
A=pd.read_parquet(f"{S}/gp_access.parquet")
NO=pd.read_parquet(f"{S}/gp_same_od.parquet")  # nearest-node, no access leg
L=np.log
def c(x,y):
    ok=np.isfinite(x)&np.isfinite(y); return round(float(np.corrcoef(x[ok],y[ok])[0,1]),3)
def evaluate(T,label):
    d=m.set_index("request_id").join(T).join(B,rsuffix="_b")
    d2=d.reset_index(); d2["key"]=d2.grid_from+"|"+d2.grid_to; d2["rkey"]=d2.grid_to+"|"+d2.grid_from
    j=d2.merge(d2,left_on="key",right_on="rkey",suffixes=("","_r")); j=j[j.grid_from<j.grid_to]
    bt=L(j.AM_b/j.AM_b_r)-L(j.PM_b/j.PM_b_r); mt=L(j.AM/j.AM_r)-L(j.PM/j.PM_r)
    print(f"{label:18s} AM {c(L(d.AM),L(d.AM_b))} PM {c(L(d.PM),L(d.PM_b))} FF {c(L(d.FF),L(d.FF_b))} | ratio AM {np.nanmedian(d.AM/d.AM_b):.2f} | dirAM {c(L(j.AM/j.AM_r),L(j.AM_b/j.AM_b_r))} dirFF {c(L(j.FF/j.FF_r),L(j.FF_b/j.FF_b_r))} | tidal {c(mt,bt)} |mag| {np.nanmean(np.abs(mt)):.3f} vs {np.nanmean(np.abs(bt)):.3f} | AM/FF {c(L(d.AM/d.FF),L(d.AM_b/d.FF_b))}")
for sc in ["1b","1a"]:
    print(f"== {sc}")
    t=NO[NO.scheme==sc].set_index("request_id")[["t_AM","t_PM","t_FF"]]; t.columns=["AM","PM","FF"]
    evaluate(t,"nearest node")
    for v in [10,15,20,30]:
        t=A[(A.scheme==sc)&(A.v_acc==v)].pivot(index="request_id",columns="period",values="t")
        evaluate(t,f"connector v={v}")
print("\n== residual vs access distance (1b, nearest node) ==")
t=NO[NO.scheme=="1b"].set_index("request_id"); d=m.set_index("request_id").join(t).join(B)
G=[]
for f in glob.glob(f"{V}/api/crawl_sessions/AM_*_d0*/results_flat.csv"):
    r=pd.read_csv(f); G.append(r[r.api_status==0][["request_id","distance_m"]])
d=d.join(pd.concat(G).groupby("request_id").distance_m.mean())
d["snap"]=d[["snap_o_m","snap_d_m"]].max(axis=1); d["snapsum"]=d.snap_o_m+d.snap_d_m
d["res"]=L(d.AM/d.t_AM); d["bkm"]=d.distance_m/1000
d["bin"]=pd.cut(d.snapsum,[0,300,600,1000,1500,2500,99999])
print(d.groupby("bin",observed=True).agg(n=("res","size"),med_baidu_over_model=("res",lambda x:round(float(np.exp(x.median())),2)),
      sd_res=("res",lambda x:round(float(x.std()),2)),baidu_min=("AM","median"),model_min=("t_AM","median"),baidu_km=("bkm","median"),model_km=("km_AM","median")).round(2))
print("\n== 1B + minor roads ==")
MN=pd.read_parquet(f"{S}/gp_minor.parquet")
for tag in MN.tag.unique():
    t=MN[MN.tag==tag].set_index("request_id")
    tt=t[["t_AM","t_PM","t_FF"]].copy(); tt.columns=["AM","PM","FF"]
    evaluate(tt,tag)
    sn=t[["snap_o_m","snap_d_m"]].max(axis=1)
    for thr in [150,300]:
        ids=sn.index[sn<=thr]; evaluate(tt.loc[ids],f"  snap<={thr} n={len(ids)}")
print("\n== minor roads for access only ==")
MA=pd.read_parquet(f"{S}/gp_minor_access.parquet")
for tag in MA.tag.unique():
    t=MA[MA.tag==tag].set_index("request_id"); tt=t[["t_AM","t_PM","t_FF"]].copy(); tt.columns=["AM","PM","FF"]
    evaluate(tt,tag)
    for thr in [150,300]:
        ids=t.index[t.snap_m<=thr]; evaluate(tt.loc[ids],f"  snap<={thr} n={len(ids)}")
