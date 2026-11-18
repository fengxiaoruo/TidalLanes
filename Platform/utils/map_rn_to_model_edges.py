"""Map exported RN pair pressure (0460, strict, 0519-0523) onto the gate5 model grid and its directed edges.

Each RN node gets the model cell that contains it (same row/col rule as the platform grid match, polygon test for
clipped cells). An RN pair is then: within one cell, a crossing between rook-adjacent cells (a model edge when both
cells are in the gate5 network), a diagonal or longer jump, or partly outside the grid. Writes pair mapping,
edge loads and a summary to runs/202505_v01/local_model/.
"""
from pathlib import Path
import glob
import json
import math
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import Point

ROOT=Path(__file__).resolve().parents[1]
REPO=ROOT.parent
QR=ROOT/'runs/202505_v01/query-results'
OUT=ROOT/'runs/202505_v01/local_model'
SPEED_DICT=Path('/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/GPS data/Speed202505/02_rn_dictionary/exports')
BUNDLE=REPO/'data_work/outputs/model_input_edgeproj_v2_lcc_gate5_v1/data'
R=6378137.0


def read_parts(pattern):
    return pd.concat([pd.read_csv(f,dtype={'rn_pair_id':str}) for f in sorted(glob.glob(str(QR/pattern)))],ignore_index=True)


def node_cells(nodes,grid,x0,y0):
    col=np.floor((R*np.radians(nodes.lon)-x0)/3000).astype('Int64')
    row=np.floor((R*np.log(np.tan(np.pi/4+np.radians(nodes.lat)/2))-y0)/3000).astype('Int64')
    key=pd.DataFrame({'row_i':row,'col_j':col})
    m=key.merge(grid[['row_i','col_j','grid_id','is_full_rectangle','in_gate5','geom']],how='left',on=['row_i','col_j'])
    m.index=nodes.index
    clipped=m.is_full_rectangle.eq(0)
    inside=[g.contains(Point(x,y)) for g,x,y in zip(m.geom[clipped],nodes.lon[clipped],nodes.lat[clipped])]
    m.loc[clipped[clipped].index[~np.array(inside,dtype=bool)],'grid_id']=None
    return m.grid_id,m.in_gate5.where(m.grid_id.notna())


def length_alloc(pairs,x0,y0,edge_key):
    """Split each pair's displacement into x and y parts in cell units and give each part to the model edge whose
    centre-to-centre span holds the pair midpoint (east half of cell c moving east -> c->east, west half -> west->c)."""
    p=pairs.dropna(subset=['lon_from','lon_to'])
    mx=lambda lon:R*np.radians(lon)
    my=lambda lat:R*np.log(np.tan(np.pi/4+np.radians(lat)/2))
    xa,xb,ya,yb=mx(p.lon_from),mx(p.lon_to),my(p.lat_from),my(p.lat_to)
    ux,uy=((xa+xb)/2-x0)/3000,((ya+yb)/2-y0)/3000
    col,row=np.floor(ux).astype(int),np.floor(uy).astype(int)
    fx,fy=ux-col,uy-row
    out=[]
    for d,f,axis in [((xb-xa)/3000,fx,'x'),((yb-ya)/3000,fy,'y')]:
        step=np.sign(d).astype(int)
        ahead=np.where(step>0,f>=0.5,f<0.5)
        a0,a1=np.where(ahead,0,-step),np.where(ahead,step,0)
        r0,c0=(row,col+a0) if axis=='x' else (row+a0,col)
        r1,c1=(row,col+a1) if axis=='x' else (row+a1,col)
        part=pd.DataFrame({'rn_pair_id':p.rn_pair_id.values,'cells':np.abs(d.values),
                           'grid_from':[f'sq_{r}_{c}' for r,c in zip(r0,c0)],'grid_to':[f'sq_{r}_{c}' for r,c in zip(r1,c1)]})
        out.append(part[step!=0])
    out=pd.concat(out,ignore_index=True)
    out['allocated']=[k in edge_key for k in zip(out.grid_from,out.grid_to)]
    return out


def coverage(d,model_nodes,edge_key):
    """Share of load by purpose: on a model edge, between two model nodes with only the reverse edge, between two model
    nodes with no edge, one end outside the model nodes, both ends outside. Reported only; nothing is reassigned."""
    on=pd.Series([k in edge_key for k in zip(d.grid_from,d.grid_to)],index=d.index)
    rev=pd.Series([(t,f) in edge_key for f,t in zip(d.grid_from,d.grid_to)],index=d.index)
    nf,nt=d.grid_from.isin(model_nodes),d.grid_to.isin(model_nodes)
    cat=np.select([on,nf&nt&rev,nf&nt,nf|nt],['on_model_edge','reverse_edge_only','no_edge_between_model_nodes','one_end_outside'],'both_ends_outside')
    t=d.assign(cat=cat).groupby(['purpose','cat']).load.sum().unstack(0).fillna(0)
    return (t/t.sum()).round(4).to_dict()


def direction_checks(load,edges,col):
    rev=load.merge(load[['grid_from','grid_to',f'{col}_HW',f'{col}_WH','t_min']].rename(
        columns={'grid_from':'grid_to','grid_to':'grid_from',f'{col}_HW':'rev_HW',f'{col}_WH':'rev_WH','t_min':'t_rev'}),on=['grid_from','grid_to'])
    rev=rev[(rev.grid_from<rev.grid_to)&(rev[f'{col}_HW']>0)&(rev.rev_HW>0)&(rev[f'{col}_WH']>0)&(rev.rev_WH>0)]
    am=np.log(rev[f'{col}_HW']/rev.rev_HW);pm=np.log(rev[f'{col}_WH']/rev.rev_WH)
    c=load[(load[f'{col}_HW']>0)&(load.capacity_proxy>0)]
    return {'reciprocal_pairs':int(len(rev)),
            'corr_am_dir_vs_pm_dir':round(float(np.corrcoef(am,pm)[0,1]),4),
            'corr_am_dir_vs_model_am_time_dir':round(float(np.corrcoef(am,np.log(rev.t_min/rev.t_rev))[0,1]),4),
            'edges_for_congestion':int(len(c)),
            'corr_log_am_load_per_capacity_vs_log_t_over_tff':round(float(np.corrcoef(np.log(c[f'{col}_HW']/c.capacity_proxy),np.log(c.t_min/c.t_ff_min))[0,1]),4),
            'corr_log_am_load_vs_log_t_over_tff':round(float(np.corrcoef(np.log(c[f'{col}_HW']),np.log(c.t_min/c.t_ff_min))[0,1]),4)}


def main():
    OUT.mkdir(exist_ok=True)
    meta=json.loads((ROOT/'reference_inputs/manifest.json').read_text())
    grid=pd.read_csv(ROOT/'reference_inputs/model_grid_wgs84_v01.csv')
    grid['geom']=[wkt.loads(s) if f==0 else None for s,f in zip(grid.wkt_wgs84,grid.is_full_rectangle)]

    nodes=pd.concat([pd.read_csv(f,dtype={'rn_id':str}) for f in sorted(SPEED_DICT.glob('rn_node_dictionary_202505_b*.csv'))])
    nodes=nodes[nodes.coordinate_missing.eq(0)].drop_duplicates('rn_id').set_index('rn_id')
    nodes['grid_id'],nodes['in_gate5']=node_cells(nodes,grid,meta['x_origin_3857'],meta['y_origin_3857'])

    win=read_parts('TL25M05_V01_04*_RWIN_b*.csv')
    s30=read_parts('TL25M05_V01_04*_RS30_b*.csv')
    pairs=pd.DataFrame({'rn_pair_id':win.rn_pair_id.unique()})
    pairs[['rn_from','rn_to']]=pairs.rn_pair_id.str.split('_',expand=True)
    for end in ['from','to']:
        n=nodes.reindex(pairs[f'rn_{end}'])
        pairs[f'lon_{end}']=n.lon.values;pairs[f'lat_{end}']=n.lat.values
        pairs[f'grid_{end}']=n.grid_id.values;pairs[f'gate5_{end}']=n.in_gate5.values
    dx=np.radians(pairs.lon_to-pairs.lon_from)*np.cos(np.radians((pairs.lat_to+pairs.lat_from)/2))
    pairs['len_m']=R*np.hypot(dx,np.radians(pairs.lat_to-pairs.lat_from))

    rc=lambda s:s.str.extract(r'sq_(\d+)_(\d+)').astype(float)
    a,b=rc(pairs.grid_from),rc(pairs.grid_to)
    dr,dc=(a[0]-b[0]).abs(),(a[1]-b[1]).abs()
    edges=pd.read_parquet(BUNDLE/'qsm_input_edges_square.parquet',columns=['grid_o','grid_d','t_min','t_ff_min','lanes_directional','capacity_proxy'])
    edge_key=set(zip(edges.grid_o,edges.grid_d))
    pairs['cls']=np.select(
        [pairs.lon_from.isna()|pairs.lon_to.isna(),pairs.grid_from.isna()|pairs.grid_to.isna(),
         pairs.grid_from.eq(pairs.grid_to),(dr+dc).eq(1),dr.eq(1)&dc.eq(1)],
        ['no_coord','outside_grid','within_cell','rook_cross','diagonal_cross'],'longer_jump')
    pairs['model_edge']=[c=='rook_cross' and (o,d) in edge_key for c,o,d in zip(pairs.cls,pairs.grid_from,pairs.grid_to)]
    pairs['within_gate5']=pairs.gate5_from.eq(1)&pairs.gate5_to.eq(1)
    pairs.to_parquet(OUT/'rn_pair_cells.parquet',index=False)

    w=win.merge(pairs[['rn_pair_id','cls','model_edge','within_gate5','grid_from','grid_to','len_m']],on='rn_pair_id',how='left')
    by_cls=w.groupby(['purpose','cls']).n_pass.sum().unstack(0).fillna(0)
    share=(by_cls/by_cls.sum()).round(4)
    gate5=w[w.within_gate5]
    on_edge=w[w.model_edge]

    load=on_edge.groupby(['grid_from','grid_to','purpose']).agg(n_pass=('n_pass','sum'),n_rn_pairs=('rn_pair_id','nunique')).reset_index()
    load=load.pivot_table(index=['grid_from','grid_to'],columns='purpose',values=['n_pass','n_rn_pairs'],fill_value=0)
    load.columns=[f'{a}_{b}' for a,b in load.columns]
    load=edges.rename(columns={'grid_o':'grid_from','grid_d':'grid_to'}).merge(load.reset_index(),on=['grid_from','grid_to'],how='left').fillna({c:0 for c in load.columns})
    load.to_csv(OUT/'model_edge_rn_load_window_strict.csv',index=False)

    s=s30.merge(pairs[['rn_pair_id','model_edge','grid_from','grid_to']],on='rn_pair_id',how='left')
    s=s[s.model_edge].groupby(['grid_from','grid_to','purpose','pass_slot30']).agg(n_pass=('n_pass','sum')).reset_index()
    s.to_csv(OUT/'model_edge_rn_load_slot30_strict.csv',index=False)

    alloc=length_alloc(pairs,meta['x_origin_3857'],meta['y_origin_3857'],edge_key)
    wb=win.merge(alloc,on='rn_pair_id')
    wb['flow']=wb.n_pass*wb.cells
    b=wb[wb.allocated].groupby(['grid_from','grid_to','purpose']).flow.sum().unstack('purpose').fillna(0).add_prefix('len_flow_').reset_index()
    load=load.merge(b,on=['grid_from','grid_to'],how='left').fillna({'len_flow_HW':0,'len_flow_WH':0})
    load.to_csv(OUT/'model_edge_rn_load_window_strict.csv',index=False)
    sb=s30.merge(alloc[alloc.allocated],on='rn_pair_id')
    sb['flow']=sb.n_pass*sb.cells
    sb=sb.groupby(['grid_from','grid_to','purpose','pass_slot30']).flow.sum().rename('len_flow').reset_index()
    s.merge(sb,on=['grid_from','grid_to','purpose','pass_slot30'],how='outer').fillna({'n_pass':0,'len_flow':0}).to_csv(OUT/'model_edge_rn_load_slot30_strict.csv',index=False)

    comparison={'allocated_share_of_length_flow':{p:round(float(g[g.allocated].flow.sum()/g.flow.sum()),4) for p,g in wb.groupby('purpose')},
                'total':{p:{'crossings':int(load[f'n_pass_{p}'].sum()),'length_flow':round(float(load[f'len_flow_{p}'].sum()))} for p in ['HW','WH']}}
    for name,col in [('crossings','n_pass'),('length_flow','len_flow')]:
        comparison[name]=direction_checks(load,edges,col)
    for p in ['HW','WH']:
        a,bb=np.log1p(load[f'n_pass_{p}']),np.log1p(load[f'len_flow_{p}'])
        comparison[f'corr_log_crossings_vs_length_flow_{p}']=round(float(np.corrcoef(a,bb)[0,1]),4)
        r=(load[f'len_flow_{p}']/load[f'n_pass_{p}'].replace(0,np.nan)).dropna()
        comparison[f'ratio_length_flow_to_crossings_{p}_p10_p50_p90']=[round(float(x),3) for x in r.quantile([.1,.5,.9])]
    model_nodes=set(pd.read_parquet(BUNDLE/'qsm_input_nodes_square.parquet',columns=['grid_id']).grid_id)
    comparison['coverage']={'length_flow':coverage(wb.rename(columns={'flow':'load'}),model_nodes,edge_key),
                            'crossings':coverage(w[w.cls.eq('rook_cross')].assign(load=lambda d:d.n_pass),model_nodes,edge_key)}
    (OUT/'rn_model_edge_measure_comparison.json').write_text(json.dumps(comparison,ensure_ascii=False,indent=2))
    print(json.dumps(comparison,ensure_ascii=False,indent=2))

    rev=load.merge(load[['grid_from','grid_to','n_pass_HW','n_pass_WH']].rename(columns={'grid_from':'grid_to','grid_to':'grid_from','n_pass_HW':'rev_HW','n_pass_WH':'rev_WH'}),on=['grid_from','grid_to'])
    rev=rev[(rev.grid_from<rev.grid_to)&(rev.n_pass_HW+rev.rev_HW>0)&(rev.n_pass_WH+rev.rev_WH>0)]
    am_dir=np.log((rev.n_pass_HW+1)/(rev.rev_HW+1))
    pm_dir=np.log((rev.n_pass_WH+1)/(rev.rev_WH+1))
    t_rev=edges.rename(columns={'grid_o':'grid_to','grid_d':'grid_from','t_min':'t_rev'})[['grid_from','grid_to','t_rev']]
    rt=rev.merge(t_rev,on=['grid_from','grid_to'])
    tau_dir=np.log(rt.t_min/rt.t_rev)
    am_rt=np.log((rt.n_pass_HW+1)/(rt.rev_HW+1))
    summary={
        'rn_nodes_with_coord':int(len(nodes)),'rn_nodes_outside_grid':int(nodes.grid_id.isna().sum()),
        'pairs':int(len(pairs)),'pairs_by_class':pairs.cls.value_counts().to_dict(),
        'pair_len_m_p50_p95_p99':[round(float(x),1) for x in pairs.len_m.quantile([.5,.95,.99])],
        'passage_share_by_class':share.to_dict(),
        'passages_strict':{p:int(v) for p,v in w.groupby('purpose').n_pass.sum().items()},
        'passages_within_gate5':{p:int(v) for p,v in gate5.groupby('purpose').n_pass.sum().items()},
        'passages_on_model_edges':{p:int(v) for p,v in on_edge.groupby('purpose').n_pass.sum().items()},
        'model_edges':int(len(edges)),'model_edges_with_load':{'HW':int((load.n_pass_HW>0).sum()),'WH':int((load.n_pass_WH>0).sum())},
        'reciprocal_edge_pairs_with_both_loads':int(len(rev)),
        'corr_am_dir_vs_pm_dir':round(float(np.corrcoef(am_dir,pm_dir)[0,1]),4),
        'corr_am_dir_vs_model_time_dir':round(float(np.corrcoef(am_rt,tau_dir)[0,1]),4),
    }
    (OUT/'rn_model_edge_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps(summary,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
