"""AA-style per-link welfare elasticity + selection-rule comparison (gate5, bpr+pos05).

Welfare (gauge-free Frechet denominator):
    W^θ = Σ_ij B_ij u_i^θ w_j^θ,   B=(I-A)^{-1},  A_ab = 1/(κ τ_ab).
Improving a directed edge a→b (adding a lane, dln n>0) changes edge cost by the
mechanical anchored-congestion law  dln τ_ab = -λ dln n_ab, so
    dA_ab = -A_ab dln τ_ab = +A_ab λ dln n_ab,
    dW^θ = dA_ab · φ_a · ψ_b,   φ = B^T u^θ,  ψ = B w^θ.
Per-directed-edge "improve" elasticity  e_add[a→b] = A_ab λ dln n_add φ_a ψ_b.
Per-pair tidal elasticity (slow +1 / fast -1)  e_tidal = e_add[slow] + e_rem[fast].
This is the ST / first-order (fixed traffic, fixed sorting) welfare response; the
full GE version would add the Ξ→τ and sorting feedback. Validated by brute force below.

Modes:
  setup            : compute elasticities, save rankings + map (no tidal solves)
  validate         : brute-force single-link tidal LT for a few links, compare to predicted
  run:<selection>  : top-100 tidal ST+LT for a selection, append W/commute to compare CSV
                     selection in {bpr_st, speed_delay, welfare_elast, asym}
"""
from __future__ import annotations
import sys
from dataclasses import replace
from pathlib import Path
import numpy as np, pandas as pd

DATA_WORK = Path(__file__).resolve().parents[3]
if str(DATA_WORK) not in sys.path:
    sys.path.insert(0, str(DATA_WORK))
from src.model.full_estimation import build_primitives as bp, invert_fundamentals as inv, parameters as pm, stage2_counterfactuals as s2
from src.model.full_estimation.equilibrium_solve import solve_equilibrium, compute_observation_anchors
from src.model.full_estimation.counterfactual_levels import _iter_eligible_reciprocal_pairs, bpr_st_pair_score
from src.model.full_estimation.tidal_lane_diagnostics import od_cost_decomposition
from src.model.full_estimation.run_alpha_robustness_quick_gate5 import solver_overrides_for_params

EPS=1e-12; TH=6.83; LAM=1.1; VERSION="model_input_edgeproj_v2_lcc_gate5_v1"
OUT=DATA_WORK/"outputs"/"link_welfare_elasticity_v1"; OUT.mkdir(parents=True,exist_ok=True)

def _setup_base():
    prim=bp.load_primitives(DATA_WORK/"outputs"/VERSION,period="AM")
    params=replace(pm.Parameters(),alpha=0.05,beta=-0.10)
    skw={k:v for k,v in solver_overrides_for_params(params).items() if v is not None}
    fund=inv.invert_fundamentals(prim,params,tol=1.5e-4,max_iter=1800,kappa=1.0)
    anc=compute_observation_anchors(prim,fund.ubar_theta,fund.abar_theta,params,chi=fund.chi,kappa=fund.kappa)
    kw=s2.build_congestion_solver_kwargs(prim,fund,params,congestion_form="anchored",congestion_lambda=LAM,**skw)
    kw.update(xi_obs_kl=anc["xi_obs_kl"],n_obs_kl=anc["n_obs_kl"],tau_obs_anchor=anc["tau_obs_anchor"])
    base=s2.solve_baseline_endogenous(prim,fund,params,**kw)
    return prim,params,fund,kw,base

def _uth(eq,fund,params,lbar): return fund.ubar_theta*np.clip(eq.l_R*lbar,EPS,None)**(params.beta*params.theta)
def _wth(eq,fund,params,lbar): return fund.abar_theta*np.clip(eq.l_F*lbar,EPS,None)**(params.alpha*params.theta)
def _frechet(eq,fund,params,prim):
    u=_uth(eq,fund,params,prim.Lbar); w=_wth(eq,fund,params,prim.Lbar)
    return float(np.sum(eq.tau_ij_inv_theta*u[:,None]*w[None,:]))

def _elasticity_tables(prim,params,fund,base):
    B=base.tau_ij_inv_theta
    u=_uth(base,fund,params,prim.Lbar); w=_wth(base,fund,params,prim.Lbar)
    phi=B.T@u; psi=B@w                       # φ_a=Σ_i B_ia u_i ; ψ_b=Σ_j B_bj w_j
    Wth=float(np.sum(u*(B@w)))               # W^θ base
    A_edge=1.0/np.clip(fund.kappa*base.tau_min_kl,EPS,None)
    ei,ej=np.asarray(prim.edge_i),np.asarray(prim.edge_j); n=np.asarray(prim.n_kl,float)
    dln_add=np.log((n+1.0)/np.clip(n,EPS,None))
    # per-directed-edge "improve (add a lane)" welfare semi-elasticity (%ΔW per lane), fixed sorting
    e_add=A_edge*LAM*dln_add*phi[ei]*psi[ej]
    e_add_pct=100.0*e_add/(TH*Wth)
    rows=_iter_eligible_reciprocal_pairs(prim,min_length_m=200.0,delta=1.0)
    recs=[]
    for slow,fast,asym in rows:
        ns,nf=n[slow],n[fast]
        dln_ns=np.log((ns+1.0)/ns); dln_nf=np.log((nf-1.0)/nf)
        e_tidal=LAM*(A_edge[slow]*dln_ns*phi[int(ei[slow])]*psi[int(ej[slow])]
                     +A_edge[fast]*dln_nf*phi[int(ei[fast])]*psi[int(ej[fast])])
        recs.append(dict(slow=slow,fast=fast,asym=asym,
            welfare_elast_pct=100.0*e_tidal/(TH*Wth),
            bpr=bpr_st_pair_score(prim,slow,fast,base.Xi_kl),
            speed_delay=max(float(prim.tau_obs_min[slow]-prim.tau_ff_min[slow]),0.0),
            Xi_slow=float(base.Xi_kl[slow])))
    df=pd.DataFrame(recs)
    return df,e_add_pct,Wth

SELMAP={"bpr_st":"bpr","speed_delay":"speed_delay","welfare_elast":"welfare_elast_pct","asym":"asym"}
def _pick(df,sel,k=100):
    col=SELMAP[sel]; d=df.sort_values(col,ascending=False).head(k)
    return d[["slow","fast"]].to_numpy()

def _solve_tidal(prim,params,fund,kw,base,pairs,horizon):
    ncf=prim.n_kl.copy()
    for s_,f_ in pairs: ncf[s_]+=1; ncf[f_]-=1
    pin=dict(l_R_fixed=base.l_R,l_F_fixed=base.l_F) if horizon=="short_run" else {}
    return solve_equilibrium(prim,fund.ubar_theta,fund.abar_theta,params,n_kl_override=ncf,
        tau_min_init=prim.tau_obs_min.copy(),endogenous_congestion=True,label=horizon,**pin,**kw)

def main():
    mode=sys.argv[1] if len(sys.argv)>1 else "setup"
    prim,params,fund,kw,base=_setup_base()
    df,e_add_pct,Wth=_elasticity_tables(prim,params,fund,base)

    if mode=="setup":
        df.to_csv(OUT/"pair_scores.csv",index=False)
        np.save(OUT/"edge_add_elast_pct.npy",e_add_pct)
        d=df.sort_values("welfare_elast_pct",ascending=False)
        print("top-5 welfare-elast pairs:\n",d.head(5)[["slow","fast","welfare_elast_pct","bpr","speed_delay"]].to_string(index=False))
        print("bottom-5:\n",d.tail(5)[["slow","fast","welfare_elast_pct","bpr","speed_delay"]].to_string(index=False))
        # rank correlations between criteria
        print("Spearman corr (welfare_elast vs):")
        for c in ["bpr","speed_delay","asym","Xi_slow"]:
            print(f"  {c:12s} {df['welfare_elast_pct'].rank().corr(df[c].rank()):.3f}")
        # map of per-directed-edge add-elasticity
        try:
            import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
            from matplotlib.collections import LineCollection
            xs=np.zeros(prim.n_nodes);ys=np.zeros(prim.n_nodes)
            for i,g in enumerate(prim.node_grid_ids):
                p=str(g).split("_");
                if len(p)>=3 and p[0]=="sq": xs[i]=float(p[1]);ys[i]=float(p[2])
            ei,ej=np.asarray(prim.edge_i),np.asarray(prim.edge_j)
            v=e_add_pct; o=np.argsort(np.abs(v))
            segs=[[(xs[ei[i]],ys[ei[i]]),(xs[ej[i]],ys[ej[i]])] for i in o]
            vmax=np.quantile(np.abs(v),0.98)
            fig,ax=plt.subplots(figsize=(8,7.6),dpi=170)
            lc=LineCollection(segs,array=np.clip(v[o],-vmax,vmax),cmap="RdBu_r",linewidths=1.4)
            ax.add_collection(lc); ax.set_aspect("equal");ax.autoscale()
            ax.set_title("Per-link welfare elasticity of adding a lane (fixed sorting)\nred=global welfare ↑, blue=↓  (%ΔW̄ per lane)",fontsize=10)
            plt.colorbar(lc,ax=ax,fraction=0.04,pad=0.02).set_label("%ΔW̄ per lane")
            ax.set_xlabel("grid x");ax.set_ylabel("grid y")
            fig.tight_layout(); fig.savefig(OUT/"map_link_welfare_elasticity.png",bbox_inches="tight")
            print("saved map + pair_scores.csv ->",OUT)
        except Exception as e: print("plot skipped:",e)
        return

    if mode=="validate":
        d=df.sort_values("welfare_elast_pct",ascending=False)
        idx=list(d.index[:2])+[d.index[len(d)//2]]+list(d.index[-2:])   # 5 links: top2, mid, bottom2
        W0=_frechet(base,fund,params,prim)
        p=OUT/"validation.csv"; done=set()
        if p.exists():
            prev=pd.read_csv(p); done=set(prev['slow'].tolist()); out=prev.to_dict('records')
        else: out=[]
        for ix in idx:
            r=df.loc[ix]
            if int(r.slow) in done: continue
            pairs=np.array([[int(r.slow),int(r.fast)]])
            lt=_solve_tidal(prim,params,fund,kw,base,pairs,"long_run")
            Wl=_frechet(lt,fund,params,prim)
            rec=dict(slow=int(r.slow),predicted_pct=float(r.welfare_elast_pct),
                actual_LT_pct=100.0*((Wl/W0)**(1/TH)-1),lt_conv=bool(lt.converged))
            out.append(rec); pd.DataFrame(out).to_csv(p,index=False)
            print(rec,flush=True)
        vdf=pd.DataFrame(out)
        if len(vdf)>=3:
            print("Spearman(predicted, actual_LT) =",vdf['predicted_pct'].rank().corr(vdf['actual_LT_pct'].rank()))
        return

    if mode.startswith("run:"):
        sel=mode.split(":")[1]; pairs=_pick(df,sel,100)
        st=_solve_tidal(prim,params,fund,kw,base,pairs,"short_run")
        lt=_solve_tidal(prim,params,fund,kw,base,pairs,"long_run")
        dec=od_cost_decomposition(base,st,lt,prim,params,fund.abar_theta)
        W0=_frechet(base,fund,params,prim)
        row=dict(selection=sel,n=len(pairs),
            Wbar_ST_pct=100.0*((_frechet(st,fund,params,prim)/W0)**(1/TH)-1),
            Wbar_LT_pct=100.0*((_frechet(lt,fund,params,prim)/W0)**(1/TH)-1),
            ST_direct_sec=dec['direct_st_cost_change_sec'],GE_sec=dec['ge_cost_wedge_sec'],
            sorting_sec=dec['sorting_wedge_sec'],LT_total_sec=dec['total_lt_cost_change_sec'],
            person_min=dec['total_lt_cost_change_sec']/60.0*prim.Lbar,lt_conv=bool(lt.converged))
        p=OUT/"selection_compare.csv"
        df2=pd.read_csv(p) if p.exists() else pd.DataFrame()
        df2=df2[df2.get("selection",pd.Series([],dtype=str))!=sel] if len(df2) else df2
        df2=pd.concat([df2,pd.DataFrame([row])],ignore_index=True); df2.to_csv(p,index=False)
        print(pd.DataFrame([row]).to_string(index=False))
        return

if __name__=="__main__": main()
