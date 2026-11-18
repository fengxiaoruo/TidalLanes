import glob, numpy as np, pandas as pd
V="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")
rows=[]
for f in glob.glob(f"{V}/api/crawl_sessions/*_d0*/results_flat.csv"):
    r=pd.read_csv(f); s=f.split("/")[-2]; r["panel"]=s.split("_")[0]; r["day"]=s.split("_")[-1]
    rows.append(r[(r.api_status==0)&r.duration_sec.notna()][["request_id","panel","day","duration_sec","distance_m"]])
b=pd.concat(rows); b["bmin"]=b.duration_sec/60; b["bkm"]=b.distance_m/1000
print(b.groupby(["panel","day"]).size())
# day-to-day reliability
for p in ["AM","PM"]:
    w=b[b.panel==p].pivot_table(index="request_id",columns="day",values="bmin")
    print(p,"corr log d01~d02",round(np.corrcoef(np.log(w.d01),np.log(w.d02))[0,1],3),"median |ratio-1|",round(float((w.d02/w.d01-1).abs().median()),3))
bm=b.groupby(["request_id","panel"]).agg(bmin=("bmin","mean"),bkm=("bkm","mean")).reset_index()
W=bm.pivot(index="request_id",columns="panel",values=["bmin","bkm"]); W.columns=[f"{a}_{c}" for a,c in W.columns]
d=m.set_index("request_id").join(W,how="inner")
d["km_1b"]=d.length_m/1000
model={"AM":["tm_AM_osm1b","tau_obs_gate5","t_am_centerline_v11"],"FREE":["tm_FF_osm1b","tau_ff_gate5","t_ff_centerline_v11"],"PM":["tm_PM_osm1b"]}
def stats(x,y):
    ok=(x>0)&(y>0)&np.isfinite(x)&np.isfinite(y)
    lx,ly=np.log(x[ok]),np.log(y[ok])
    return dict(n=int(ok.sum()),pearson_log=round(np.corrcoef(lx,ly)[0,1],3),spearman=round(pd.Series(x[ok]).corr(pd.Series(y[ok]),method="spearman"),3),median_ratio=round(float(np.median(x[ok]/y[ok])),3),share_within_25pct=round(float(np.mean(np.abs(np.log(x[ok]/y[ok]))<np.log(1.25))),3))
print("\n== levels: model vs Baidu (mean over days) ==")
for p,cols in model.items():
    for c in cols: print(p,c,stats(d[c].to_numpy(),d[f"bmin_{p}"].to_numpy()))
print("\nBaidu km vs 1B path km:",stats(d.km_1b.to_numpy(),d.bkm_AM.to_numpy()))
print("1B km / Baidu km quantiles:",np.round(np.nanquantile(d.km_1b/d.bkm_AM,[.1,.25,.5,.75,.9,.95,.99]),2))
print("Baidu km quantiles",np.round(np.nanquantile(d.bkm_AM,[.1,.5,.9]),2))
# by stratum
print("\n== AM by stratum_obs_hi ==")
for g,s in d.groupby("stratum_obs_hi"):
    print(g,"1B",stats(s.tm_AM_osm1b.to_numpy(),s.bmin_AM.to_numpy()),"gate5",stats(s.tau_obs_gate5.to_numpy(),s.bmin_AM.to_numpy()))
# excluding detour pairs
ok=(d.km_1b/d.bkm_AM).between(0.67,1.5)
print("\nAM 1B, pairs with 1B km within 0.67-1.5x Baidu km:",stats(d.tm_AM_osm1b[ok].to_numpy(),d.bmin_AM[ok].to_numpy()))
print("AM gate5 same pairs:",stats(d.tau_obs_gate5[ok].to_numpy(),d.bmin_AM[ok].to_numpy()))
# speeds
d["bspd_AM"]=d.bkm_AM/(d.bmin_AM/60); d["spd_1b_AM"]=d.km_1b/(d.tm_AM_osm1b/60)
print("speed km/h median Baidu AM",round(d.bspd_AM.median(),1),"1B AM",round(d.spd_1b_AM.median(),1),"corr log speed",round(np.corrcoef(np.log(d.bspd_AM[ok]),np.log(d.spd_1b_AM[ok]))[0,1],3))
# congestion ratio AM/FREE
print("\n== congestion ratio AM/FREE ==")
for nm,a,f in [("baidu","bmin_AM","bmin_FREE"),("1B","tm_AM_osm1b","tm_FF_osm1b"),("gate5","tau_obs_gate5","tau_ff_gate5"),("cl_v11","t_am_centerline_v11","t_ff_centerline_v11")]:
    print(nm,"median",round(float((d[a]/d[f]).median()),3))
for nm,a,f in [("1B","tm_AM_osm1b","tm_FF_osm1b"),("gate5","tau_obs_gate5","tau_ff_gate5"),("cl_v11","t_am_centerline_v11","t_ff_centerline_v11")]:
    print("corr log congestion ratio baidu~",nm,stats((d[a]/d[f]).to_numpy(),(d.bmin_AM/d.bmin_FREE).to_numpy()))
# direction ratio
print("\n== direction ratio ln(t_kl/t_lk) ==")
d2=d.reset_index(); d2["key"]=d2.grid_from+"|"+d2.grid_to; d2["rkey"]=d2.grid_to+"|"+d2.grid_from
j=d2.merge(d2,left_on="key",right_on="rkey",suffixes=("","_r"))
j=j[j.grid_from<j.grid_to]
for p,cols in [("AM",["bmin_AM","tm_AM_osm1b","tau_obs_gate5","t_am_centerline_v11"]),("PM",["bmin_PM","tm_PM_osm1b"]),("FREE",["bmin_FREE","tm_FF_osm1b","tau_ff_gate5"])]:
    base=np.log(j[cols[0]]/j[cols[0]+"_r"])
    print(p,"baidu mean|dir|",round(float(base.abs().mean()),3),"n",len(j))
    for c in cols[1:]:
        x=np.log(j[c]/j[c+"_r"]); okk=np.isfinite(x)&np.isfinite(base)
        print("  ",c,"mean|dir|",round(float(x[okk].abs().mean()),3),"corr",round(np.corrcoef(x[okk],base[okk])[0,1],3),"slope",round(np.polyfit(x[okk],base[okk],1)[0],3),"sign agree(|baidu|>0.1)",round(float((np.sign(x[okk])==np.sign(base[okk]))[base[okk].abs()>0.1].mean()),3))
# Baidu direction reliability across days
w=b[b.panel=="AM"].pivot_table(index="request_id",columns="day",values="bmin").reset_index().merge(d2[["request_id","key","rkey","grid_from","grid_to"]],on="request_id")
k=w.merge(w,left_on="key",right_on="rkey",suffixes=("","_r")); k=k[k.grid_from<k.grid_to]
x1=np.log(k.d01/k.d01_r); x2=np.log(k.d02/k.d02_r)
print("Baidu AM direction ratio d01~d02 corr",round(np.corrcoef(x1,x2)[0,1],3))
# AM-PM direction reversal
x=np.log(j.bmin_AM/j.bmin_AM_r); y=np.log(j.bmin_PM/j.bmin_PM_r)
print("Baidu AM vs PM direction ratio corr",round(np.corrcoef(x,y)[0,1],3))
x=np.log(j.tm_AM_osm1b/j.tm_AM_osm1b_r); y=np.log(j.tm_PM_osm1b/j.tm_PM_osm1b_r)
print("1B AM vs PM direction ratio corr",round(np.corrcoef(x,y)[0,1],3))
print("\n== congestion-induced direction asymmetry ==")
L=lambda a,b_: np.log(j[a]/j[a+"_r"])-np.log(j[b_]/j[b_+"_r"])
bc=L("bmin_AM","bmin_FREE"); bt=np.log(j.bmin_AM/j.bmin_AM_r)-np.log(j.bmin_PM/j.bmin_PM_r)
print("baidu mean|AM-FREE dir|",round(float(bc.abs().mean()),3),"mean|AM-PM dir|",round(float(bt.abs().mean()),3))
for nm,a,f in [("1B","tm_AM_osm1b","tm_FF_osm1b"),("gate5","tau_obs_gate5","tau_ff_gate5"),("cl_v11","t_am_centerline_v11","t_ff_centerline_v11")]:
    x=L(a,f); print(" AM-FREE",nm,"mean|.|",round(float(x.abs().mean()),3),"corr",round(np.corrcoef(x,bc)[0,1],3),"slope b on m",round(np.polyfit(x,bc,1)[0],3))
x=np.log(j.tm_AM_osm1b/j.tm_AM_osm1b_r)-np.log(j.tm_PM_osm1b/j.tm_PM_osm1b_r)
print(" AM-PM 1B mean|.|",round(float(x.abs().mean()),3),"corr",round(np.corrcoef(x,bt)[0,1],3),"slope",round(np.polyfit(x,bt,1)[0],3))
# reliability of baidu congestion-direction component across days: need FREE only d02; use AM d01 vs d02 minus same FREE
w2=b.pivot_table(index="request_id",columns=["panel","day"],values="bmin"); w2.columns=[f"{p}_{d_}" for p,d_ in w2.columns]
w2=w2.reset_index().merge(d2[["request_id","key","rkey","grid_from","grid_to"]],on="request_id")
k=w2.merge(w2,left_on="key",right_on="rkey",suffixes=("","_r")); k=k[k.grid_from<k.grid_to]
t1=np.log(k.AM_d01/k.AM_d01_r)-np.log(k.PM_d01/k.PM_d01_r); t2=np.log(k.AM_d02/k.AM_d02_r)-np.log(k.PM_d02/k.PM_d02_r)
print("baidu AM-PM tidal component d01~d02 corr",round(np.corrcoef(t1,t2)[0,1],3))
# restrict to pairs with distance-consistent 1B
okp=((j.km_1b/j.bkm_AM).between(0.67,1.5))&((j.km_1b_r/j.bkm_AM_r).between(0.67,1.5))
x=np.log(j.tm_AM_osm1b/j.tm_AM_osm1b_r)
print("distance-consistent pairs n",int(okp.sum()),"AM dir corr 1B",round(np.corrcoef(x[okp],np.log(j.bmin_AM/j.bmin_AM_r)[okp])[0,1],3),
 "AM-FREE",round(np.corrcoef(L('tm_AM_osm1b','tm_FF_osm1b')[okp],bc[okp])[0,1],3),
 "AM-PM",round(np.corrcoef((np.log(j.tm_AM_osm1b/j.tm_AM_osm1b_r)-np.log(j.tm_PM_osm1b/j.tm_PM_osm1b_r))[okp],bt[okp])[0,1],3))
print("\n== by disagreement stratum ==")
for g,s in d.groupby("stratum_disagree"):
    print(g,"AM 1B",stats(s.tm_AM_osm1b.to_numpy(),s.bmin_AM.to_numpy())["pearson_log"],"gate5",stats(s.tau_obs_gate5.to_numpy(),s.bmin_AM.to_numpy())["pearson_log"],
          "km",stats(s.km_1b.to_numpy(),s.bkm_AM.to_numpy())["pearson_log"])
for g,s in j.groupby("stratum_disagree"):
    bt_=np.log(s.bmin_AM/s.bmin_AM_r)-np.log(s.bmin_PM/s.bmin_PM_r); x=np.log(s.tm_AM_osm1b/s.tm_AM_osm1b_r)-np.log(s.tm_PM_osm1b/s.tm_PM_osm1b_r)
    print(g,"tidal AM-PM corr 1B",round(np.corrcoef(x,bt_)[0,1],3),"n",len(s))
