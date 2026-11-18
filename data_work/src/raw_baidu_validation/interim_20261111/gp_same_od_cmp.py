import glob, numpy as np, pandas as pd
S="/private/tmp/claude-502/-Users-fxr-Library-CloudStorage-Dropbox-Projects-TidalLanes/f939346f-8863-48ed-9518-e8ef6245b92a/scratchpad"
V="/Users/fxr/Library/CloudStorage/Dropbox/Projects/TidalLanes/data_work/outputs/grid_pair_baidu_validation_v3"
m=pd.read_csv(f"{V}/data/sample_manifest.csv")[["request_id","grid_from","grid_to"]]
B=[]
for f in glob.glob(f"{V}/api/crawl_sessions/*_d0*/results_flat.csv"):
    r=pd.read_csv(f); p=f.split("/")[-2].split("_")[0]; r=r[r.api_status==0]; r["panel"]=p; B.append(r[["request_id","panel","duration_sec","distance_m"]])
B=pd.concat(B).groupby(["request_id","panel"]).mean().reset_index()
B=B.pivot(index="request_id",columns="panel",values=["duration_sec","distance_m"]); B.columns=[f"{a}_{p}" for a,p in B.columns]
M=pd.read_parquet(f"{S}/gp_same_od.parquet")
def lc(x,y):
    ok=(x>0)&(y>0)&np.isfinite(x)&np.isfinite(y); return round(float(np.corrcoef(np.log(x[ok]),np.log(y[ok]))[0,1]),3), int(ok.sum())
for sc in ["1b","1a"]:
    d=M[M.scheme==sc].merge(m,on="request_id").set_index("request_id").join(B)
    d["bkm"]=d.distance_m_AM/1000; d["snap"]=d[["snap_o_m","snap_d_m"]].max(axis=1)
    print(f"\n===== {sc}: snap distance quantiles (m)", np.round(d.snap.quantile([.25,.5,.75,.9]).values))
    for thr in [np.inf,500,300,150]:
        s=d[d.snap<=thr]
        km=lc(s.km_AM.to_numpy(),s.bkm.to_numpy()); r=(s.km_AM/s.bkm)
        line=f"snap<={thr}: n={len(s)} km corr {km[0]} km ratio med {r.median():.2f} within0.67-1.5 {r.between(.67,1.5).mean():.2f} | time corr"
        for p,bp in [("AM","AM"),("PM","PM"),("FF","FREE")]:
            line+=f" {p} {lc(s[f't_{p}'].to_numpy(),(s[f'duration_sec_{bp}']/60).to_numpy())[0]}"
        line+=f" | AM ratio med {(s.t_AM/(s.duration_sec_AM/60)).median():.2f}"
        # direction and tidal
        s2=s.reset_index(); s2["key"]=s2.grid_from+"|"+s2.grid_to; s2["rkey"]=s2.grid_to+"|"+s2.grid_from
        j=s2.merge(s2,left_on="key",right_on="rkey",suffixes=("","_r")); j=j[j.grid_from<j.grid_to]
        if len(j)>20:
            bd=np.log(j.duration_sec_AM/j.duration_sec_AM_r); md=np.log(j.t_AM/j.t_AM_r)
            bf=np.log(j.duration_sec_FREE/j.duration_sec_FREE_r); mf=np.log(j.t_FF/j.t_FF_r)
            bt=bd-np.log(j.duration_sec_PM/j.duration_sec_PM_r); mt=md-np.log(j.t_PM/j.t_PM_r)
            bc=np.log(j.duration_sec_AM/j.duration_sec_FREE); mc=np.log(j.t_AM/j.t_FF)
            def c(x,y):
                ok=np.isfinite(x)&np.isfinite(y); return round(float(np.corrcoef(x[ok],y[ok])[0,1]),3)
            line+=f"\n      pairs {len(j)}: dirAM {c(md,bd)} dirFF {c(mf,bf)} tidal(AM-PM) {c(mt,bt)} slope {np.polyfit(mt[np.isfinite(mt)&np.isfinite(bt)],bt[np.isfinite(mt)&np.isfinite(bt)],1)[0]:.2f} | congestion AM/FF corr {lc(np.exp(mc).to_numpy(),np.exp(bc).to_numpy())[0]}"
        print(line)
