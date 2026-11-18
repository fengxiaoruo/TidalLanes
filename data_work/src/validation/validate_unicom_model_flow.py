"""Validate saved model grid-link passages against Unicom boundary pressure.

Run from any directory. Inputs are read only; outputs are cached under --output.
Boundary aggregation is deliberately distinct from physical-road link matching.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import heapq
import json
from pathlib import Path
import time

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.spatial import cKDTree
from scipy.stats import spearmanr, pearsonr

ROOT = Path(__file__).resolve().parents[3]
SPEED = Path('/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing')
BASE = ROOT / 'data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1'
METRICS = ['n_valid_transitions', 'n_unique_trips', 'n_unique_users']
DATES = list(range(20250519, 20250526))
WEEKDAYS = DATES[:5]


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def grid_ids(x, y, grid):
    pts = gpd.GeoDataFrame({'position': np.arange(len(x))}, geometry=gpd.points_from_xy(x, y), crs=4326)
    j = gpd.sjoin(pts, grid[['grid_id', 'geometry']], how='left', predicate='within')
    assert not j.position.duplicated().any(), 'Overlapping grid polygons'
    return j.set_index('position').grid_id.reindex(np.arange(len(x))).to_numpy()


def model_data(out):
    e = pd.read_csv(BASE / 'primitives/primitives_edges.csv')
    n = pd.read_csv(BASE / 'primitives/primitives_nodes.csv').set_index('node_idx')
    a = np.load(BASE / 'baseline_endogenous/equilibrium_arrays.npz')
    assert len(e) == len(a['Xi_kl'])
    e.insert(0, 'model_edge_id', np.arange(len(e)))
    e['grid_o'] = e.edge_i.map(n.grid_id)
    e['grid_d'] = e.edge_j.map(n.grid_id)
    e['model_Xi'] = a['Xi_kl']
    assert not e[['grid_o', 'grid_d']].duplicated().any()
    assert np.isfinite(e.model_Xi).all() and (e.model_Xi >= 0).all()
    e.to_csv(out / 'model_baseline_links.csv', index=False)
    return e


def crosswalk(out, model, grid):
    cache = out / 'rn_crosswalk.parquet'
    if cache.exists():
        cached = pd.read_parquet(cache)
        if 'osm_node_path' in cached.columns:
            return cached
    r = pd.read_csv(SPEED / 'GPS data/outputs/grid_od_and_roads_20250519_20250525/tables/road_pair_geometry.csv')
    assert not r.rn_pair_id.duplicated().any()
    r['grid_o'] = grid_ids(r.from_lon, r.from_lat, grid)
    r['grid_d'] = grid_ids(r.to_lon, r.to_lat, grid)
    r['grid_crossing'] = r.grid_o.notna() & r.grid_d.notna() & r.grid_o.ne(r.grid_d)
    r = r.merge(model[['grid_o', 'grid_d', 'model_edge_id']], how='left', on=['grid_o', 'grid_d'], validate='many_to_one')
    r['endpoint_candidate'] = r.model_edge_id.notna() & r.chord_length_m.between(1, 1000)
    r['osm_snap_from_m'] = np.nan
    r['osm_snap_to_m'] = np.nan
    r['osm_path_length_m'] = np.nan
    r['osm_grid_sequence_ok'] = False
    r['osm_from_node_idx'] = np.nan
    r['osm_to_node_idx'] = np.nan
    r['osm_node_path'] = ''
    r['osm_grid_sequence'] = ''

    arcs = pd.read_parquet(SPEED / 'V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet')
    nodes = pd.concat([
        arcs[['from_node_idx', 'from_node_id']].rename(columns={'from_node_idx': 'idx', 'from_node_id': 'id'}),
        arcs[['to_node_idx', 'to_node_id']].rename(columns={'to_node_idx': 'idx', 'to_node_id': 'id'})
    ]).drop_duplicates('idx').sort_values('idx').set_index('idx')
    xy = nodes.id.str.split('|', regex=False).str[0].str.split(',', expand=True).astype(float)
    tr = Transformer.from_crs(4326, 32650, always_xy=True)
    nx, ny = tr.transform(xy[0].to_numpy(), xy[1].to_numpy())
    tree = cKDTree(np.column_stack([nx, ny]))
    nodes['grid_id'] = grid_ids(xy[0], xy[1], grid)
    # Different grade levels at identical coordinates are ambiguous for RN points.
    dup_xy = pd.Series(list(zip(nx, ny))).duplicated(keep=False).to_numpy()
    idxvals = nodes.index.to_numpy()
    graph = defaultdict(dict)
    for a, b, length in arcs[['from_node_idx', 'to_node_idx', 'length_m']].itertuples(index=False, name=None):
        old = graph[a].get(b, np.inf)
        graph[a][b] = min(old, length)
    nodegrids = nodes.grid_id.to_dict()
    cand = r.loc[r.endpoint_candidate].copy()
    fx, fy = tr.transform(cand.from_lon.to_numpy(), cand.from_lat.to_numpy())
    tx, ty = tr.transform(cand.to_lon.to_numpy(), cand.to_lat.to_numpy())
    fd, fi = tree.query(np.column_stack([fx, fy]))
    td, ti = tree.query(np.column_stack([tx, ty]))
    r.loc[cand.index, 'osm_snap_from_m'] = fd
    r.loc[cand.index, 'osm_snap_to_m'] = td
    r.loc[cand.index, 'osm_from_node_idx'] = idxvals[fi]
    r.loc[cand.index, 'osm_to_node_idx'] = idxvals[ti]
    cand['source'] = idxvals[fi]
    cand['target'] = idxvals[ti]
    eligible = (fd <= 10) & (td <= 10) & ~dup_xy[fi] & ~dup_xy[ti] & (fi != ti)
    cand = cand.loc[eligible]
    log(f'RN endpoint candidates: {r.endpoint_candidate.sum():,}; unambiguous OSM snaps: {len(cand):,}')
    # Bounded Dijkstra once per source; no model flow enters the match rules.
    for num, (source, group) in enumerate(cand.groupby('source')):
        targets = set(group.target)
        cutoff = min(3000.0, float((group.chord_length_m * 3 + 100).max()))
        dist, parent, queue = {source: 0.0}, {}, [(0.0, source)]
        found = set()
        while queue and found != targets:
            d, u = heapq.heappop(queue)
            if d != dist[u]:
                continue
            if u in targets:
                found.add(u)
            for v, w in graph.get(u, {}).items():
                nd = d + w
                if nd <= cutoff and nd < dist.get(v, np.inf):
                    dist[v] = nd
                    parent[v] = u
                    heapq.heappush(queue, (nd, v))
        for row in group.itertuples():
            target = row.target
            if target not in found or dist[target] > min(3000, row.chord_length_m * 3 + 100):
                continue
            path = [target]
            while path[-1] != source:
                path.append(parent[path[-1]])
            path.reverse()
            seq = []
            for u in path:
                g = nodegrids[u]
                if not seq or g != seq[-1]:
                    seq.append(g)
            r.at[row.Index, 'osm_node_path'] = ','.join(map(str, path))
            r.at[row.Index, 'osm_grid_sequence'] = ','.join(map(str, seq))
            r.at[row.Index, 'osm_path_length_m'] = dist[target]
            r.at[row.Index, 'osm_grid_sequence_ok'] = seq == [row.grid_o, row.grid_d]
        if num and num % 2000 == 0:
            log(f'OSM source paths checked: {num:,}')
    r['primary'] = r.endpoint_candidate & r.chord_length_m.le(500) & r.osm_grid_sequence_ok
    r['strict_250m'] = r.primary & r.chord_length_m.le(250) & r.osm_snap_from_m.le(1) & r.osm_snap_to_m.le(1)
    r['osm_1000m'] = r.endpoint_candidate & r.osm_grid_sequence_ok
    r['endpoint_500m'] = r.endpoint_candidate & r.chord_length_m.le(500)
    r['endpoint_1000m'] = r.endpoint_candidate
    r.to_parquet(cache, index=False)
    log(f'Crosswalk saved: primary RN pairs {r.primary.sum():,}')
    return r


def aggregate(out, cross):
    cache = out / 'rn_daily_am.parquet'
    if cache.exists():
        return pd.read_parquet(cache)
    folder = SPEED / 'GPS data/Speed202505/03_rn_pair_10min_pressure/exports/formal_counts'
    files = sorted(folder.glob('*.csv'))
    assert len(files) == 389
    allrows, audit = [], []
    cols = ['date', 'rn_pair_id', 'time_block_6h'] + [f'{m}_s{s:02}' for m in METRICS for s in range(6, 18)]
    # 07:00 <= passage time < 09:00: block 1, slots 6..17.
    for k, file in enumerate(files):
        d = pd.read_csv(file, usecols=cols)
        assert set(d.date.unique()).issubset(DATES)
        assert not d[['date', 'rn_pair_id', 'time_block_6h']].duplicated().any()
        am = d.loc[d.time_block_6h.eq(1)].copy()
        z = am[['date', 'rn_pair_id']].copy()
        for m in METRICS:
            c = [f'{m}_s{s:02}' for s in range(6, 18)]
            vals = am[c]
            assert not vals.lt(0).any().any()
            z[m] = vals.sum(axis=1, min_count=1)
            z[m + '_slots'] = vals.notna().sum(axis=1)
        allrows.append(z)
        audit.append({'path': str(file), 'bytes': file.stat().st_size, 'mtime_ns': file.stat().st_mtime_ns,
                      'rows': len(d), 'am_block_rows': len(am), 'sha256': hashlib.sha256(file.read_bytes()).hexdigest()})
        if (k+1) % 50 == 0:
            log(f'Formal files read: {k+1}/{len(files)}')
    d = pd.concat(allrows, ignore_index=True)
    assert not d[['date', 'rn_pair_id']].duplicated().any(), 'Duplicate date/pair across export parts'
    assert (d.n_valid_transitions.fillna(0) >= d.n_unique_trips.fillna(0)).all()
    pd.DataFrame(audit).to_csv(out / 'formal_file_audit.csv', index=False)
    d.to_parquet(cache, index=False)
    log(f'AM aggregation saved: {len(d):,} date/RN records')
    return d


def stats(x, y):
    ok = np.isfinite(x) & np.isfinite(y)
    x, y = np.asarray(x)[ok], np.asarray(y)[ok]
    if len(x) < 5 or np.std(x) == 0 or np.std(y) == 0:
        return {'n': int(len(x))}
    xs, ys = x/x.sum(), y/y.sum()
    k = max(1, int(np.ceil(len(x)*.1)))
    topx, topy = set(np.argsort(x)[-k:]), set(np.argsort(y)[-k:])
    return {'n': int(len(x)), 'spearman': float(spearmanr(x, y).statistic),
            'pearson_levels': float(pearsonr(x, y).statistic),
            'pearson_log1p': float(pearsonr(np.log1p(x), np.log1p(y)).statistic),
            'top_decile_overlap': len(topx & topy)/k,
            'share_total_variation': float(np.abs(xs-ys).sum()/2),
            'model_sum': float(x.sum()), 'observed_sum': float(y.sum())}


def compare(out, model, cross, daily):
    versions = ['primary', 'strict_250m', 'osm_1000m', 'endpoint_500m', 'endpoint_1000m']
    joined = daily.merge(cross[['rn_pair_id', 'model_edge_id']+versions], on='rn_pair_id', how='left', validate='many_to_one')
    assert joined.model_edge_id.notna().sum() > 0
    rows, coverage, saved, directions = [], [], [], []
    for v in versions:
        c = cross.loc[cross[v]]
        j = joined.loc[joined[v].fillna(False)]
        a = j.groupby(['date','model_edge_id'])[METRICS].sum(min_count=1).reset_index()
        slots = j.groupby(['date','model_edge_id'])['n_valid_transitions_slots'].sum().rename('observed_rn_slots').reset_index()
        a = a.merge(slots, validate='one_to_one', on=['date','model_edge_id'])
        for date in DATES:
            dd = a.loc[a.date.eq(date)].merge(model, on='model_edge_id', validate='one_to_one')
            for m in METRICS:
                rows.append({'sample':v,'period':str(date),'metric':m,**stats(dd.model_Xi,dd[m])})
        # Balanced panel across five weekdays, with missing days left missing.
        wk = a.loc[a.date.isin(WEEKDAYS)]
        n = wk.groupby('model_edge_id').n_valid_transitions.count()
        balanced_ids = n.index[n.eq(5)]
        w = wk.loc[wk.model_edge_id.isin(balanced_ids)].groupby('model_edge_id')[METRICS].mean().reset_index()
        w = w.merge(model,on='model_edge_id',validate='one_to_one')
        w['sample'] = v
        saved.append(w)
        for m in METRICS:
            rows.append({'sample':v,'period':'weekday_balanced_mean','metric':m,**stats(w.model_Xi,w[m])})
        coverage.append({'sample':v,'rn_pairs':len(c),'model_edges_geometry':c.model_edge_id.nunique(),
                         'balanced_model_edges':len(w),'all_model_edges':len(model),
                         'model_passage_share_balanced':w.model_Xi.sum()/model.model_Xi.sum(),
                         'model_length_share_balanced':w.ell_kl_km.sum()/model.ell_kl_km.sum(),
                         'weekday_observed_transitions':j.loc[j.date.isin(WEEKDAYS),'n_valid_transitions'].sum(),
                         'weekday_all_transitions':daily.loc[daily.date.isin(WEEKDAYS),'n_valid_transitions'].sum()})
        # Canonical direction uses node indices; both directions observed on all five days.
        a1 = w.loc[w.edge_i.lt(w.edge_j)].copy()
        rev = w[['edge_i','edge_j','model_Xi']+METRICS].rename(columns={
            'edge_i':'edge_j','edge_j':'edge_i','model_Xi':'model_reverse',
            **{m:m+'_reverse' for m in METRICS}})
        pair = a1.merge(rev,on=['edge_i','edge_j'],validate='one_to_one')
        pair['model_log_ratio'] = np.log(pair.model_Xi / pair.model_reverse)
        for m in METRICS:
            x = pair.loc[(pair[m]>0)&(pair[m+'_reverse']>0)].copy()
            x['observed_log_ratio'] = np.log(x[m]/x[m+'_reverse'])
            res = {'n':len(x)}
            if len(x)>4:
                res.update(spearman=float(spearmanr(x.model_log_ratio,x.observed_log_ratio).statistic),
                           pearson=float(pearsonr(x.model_log_ratio,x.observed_log_ratio).statistic),
                           dominant_direction_agreement=float((np.sign(x.model_log_ratio)==np.sign(x.observed_log_ratio)).mean()),
                           median_abs_log_ratio_error=float(np.abs(x.model_log_ratio-x.observed_log_ratio).median()))
            directions.append({'sample':v,'metric':m,**res})
            if v=='primary' and m=='n_valid_transitions':
                x.to_csv(out/'primary_direction_pairs.csv',index=False)
        if v=='primary':
            a.merge(model,on='model_edge_id',validate='many_to_one').to_csv(out/'primary_daily_links.csv',index=False)
    pd.DataFrame(rows).to_csv(out/'flow_metrics.csv',index=False)
    pd.DataFrame(coverage).to_csv(out/'sample_coverage.csv',index=False)
    pd.DataFrame(directions).to_csv(out/'direction_metrics.csv',index=False)
    pd.concat(saved).to_csv(out/'weekday_link_comparison.csv',index=False)
    # Day-to-day agreement of Unicom counts on the same common link support.
    a = pd.read_csv(out/'primary_daily_links.csv')
    piv = a.pivot(index='model_edge_id',columns='date',values='n_valid_transitions').dropna(subset=WEEKDAYS)
    piv[WEEKDAYS].corr(method='spearman').to_csv(out/'unicom_daily_rank_stability.csv')
    # Pair-cluster bootstrap for weekday ranks, preserving two directions together.
    w = pd.concat(saved).query('sample == "primary"').copy()
    w['pair'] = [f'{min(a,b)}_{max(a,b)}' for a,b in zip(w.edge_i,w.edge_j)]
    groups = [g.index.to_numpy() for _,g in w.reset_index(drop=True).groupby('pair')]
    w = w.reset_index(drop=True)
    rng = np.random.default_rng(20261029)
    bs=[]
    for _ in range(1000):
        ix=np.concatenate([groups[i] for i in rng.integers(0,len(groups),len(groups))])
        bs.append(spearmanr(w.iloc[ix].model_Xi,w.iloc[ix].n_valid_transitions).statistic)
    ci={'spearman_ci95_pair_bootstrap':np.quantile(bs,[.025,.975]).tolist(),'replicates':1000,'seed':20261029}
    (out/'bootstrap.json').write_text(json.dumps(ci,indent=2))
    return pd.DataFrame(rows), pd.DataFrame(coverage), pd.DataFrame(directions)


def figures(out, grid):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    from matplotlib.collections import LineCollection
    plt.rcParams.update({'font.family':'sans-serif','font.sans-serif':['Arial','DejaVu Sans'],
                         'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                         'svg.fonttype':'none','pdf.fonttype':42})
    w=pd.read_csv(out/'weekday_link_comparison.csv').query('sample == "primary"').copy()
    pair=pd.read_csv(out/'primary_direction_pairs.csv')
    metrics=pd.read_csv(out/'flow_metrics.csv')
    r=metrics.query('sample == "primary" and metric == "n_valid_transitions" and period == "weekday_balanced_mean"').iloc[0]
    fig,axs=plt.subplots(1,3,figsize=(13,4.1),layout='constrained')
    axs[0].scatter(np.log1p(w.model_Xi),np.log1p(w.n_valid_transitions),s=9,alpha=.42,color='#346888',linewidths=0)
    axs[0].set(xlabel='Model passage, log(1 + Xi)',ylabel='Unicom AM transitions, log(1 + count)',title=f'a   Flow ranking (n = {len(w):,})')
    axs[0].text(.04,.94,f'Spearman = {r.spearman:.3f}',transform=axs[0].transAxes,va='top')
    axs[1].scatter(pair.model_log_ratio,pair.observed_log_ratio,s=10,alpha=.5,color='#346888',linewidths=0)
    axs[1].axhline(0,c='0.65',lw=.6);axs[1].axvline(0,c='0.65',lw=.6)
    axs[1].set(xlabel='Model log forward / reverse flow',ylabel='Unicom log forward / reverse count',title=f'b   Directional imbalance (n = {len(pair):,})')
    d=metrics.query('sample == "primary" and metric == "n_valid_transitions" and period != "weekday_balanced_mean"').copy()
    axs[2].plot(np.arange(len(d)),d.spearman,'o-',c='#346888',lw=1.2)
    axs[2].set_xticks(np.arange(len(d)),[x[-2:] for x in d.period.astype(str)])
    axs[2].set(xlabel='May 2025 passage date',ylabel='Spearman correlation with model',title='c   Daily comparison',ylim=(-1,1))
    fig.savefig(out/'flow_validation.png',dpi=300);fig.savefig(out/'flow_validation.pdf');fig.savefig(out/'flow_validation.svg');plt.close(fig)
    # Map a canonical boundary midpoint with both directions aggregated to one color.
    g=grid.to_crs(32650).set_index('grid_id')
    cent=g.geometry.centroid
    w['pair']=[f'{min(a,b)}_{max(a,b)}' for a,b in zip(w.edge_i,w.edge_j)]
    p=w.groupby('pair').agg(grid_o=('grid_o','first'),grid_d=('grid_d','first'),model=('model_Xi','sum'),obs=('n_valid_transitions','sum'))
    p['gap']=np.log2((p.obs/p.obs.sum())/(p.model/p.model.sum()))
    fig,ax=plt.subplots(figsize=(8,7),layout='constrained')
    g.loc[g.index.isin(set(w.grid_o)|set(w.grid_d))].boundary.plot(ax=ax,color='0.88',linewidth=.35)
    segments=[[(cent.loc[a].x,cent.loc[a].y),(cent.loc[b].x,cent.loc[b].y)] for a,b in zip(p.grid_o,p.grid_d)]
    lc=LineCollection(segments,cmap='RdBu_r',norm=Normalize(-3,3),linewidths=1.5)
    lc.set_array(p.gap.to_numpy());ax.add_collection(lc)
    fig.colorbar(lc,ax=ax,label='log2(Unicom share / model share), clipped at +/-3',shrink=.7)
    ax.set(title='Relative boundary pressure: weekday 07:00-09:00',xlabel='UTM 50N easting (m)',ylabel='UTM 50N northing (m)',aspect='equal')
    fig.savefig(out/'flow_share_gap_map.png',dpi=300);fig.savefig(out/'flow_share_gap_map.pdf');plt.close(fig)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=ROOT/'data_work/outputs/unicom_model_flow_validation_v1')
    args=parser.parse_args();out=args.output;out.mkdir(parents=True,exist_ok=True)
    grid=gpd.read_parquet(ROOT/'data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet')
    model=model_data(out);log(f'Model export: {len(model)} directed links')
    cross=crosswalk(out,model,grid)
    daily=aggregate(out,cross)
    metrics,cov,direction=compare(out,model,cross,daily)
    figures(out,grid)
    inputs=[BASE/'primitives/primitives_edges.csv',BASE/'primitives/primitives_nodes.csv',BASE/'baseline_endogenous/equilibrium_arrays.npz',
            ROOT/'data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet',
            SPEED/'V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet',
            SPEED/'GPS data/outputs/grid_od_and_roads_20250519_20250525/tables/road_pair_geometry.csv']
    pd.DataFrame([{'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in inputs]).to_csv(out/'input_manifest.csv',index=False)
    (out/'run_config.json').write_text(json.dumps({'model_base':str(BASE),'speed_root':str(SPEED),'dates':DATES,
        'am_window':'07:00 <= Asia/Shanghai passage time < 09:00','primary_chord_max_m':500,'osm_snap_max_m':10,
        'model_flow_used_for_matching':False,'missing_values':'retain missing; no zero imputation',
        'comparison_unit':'directed model grid boundary; Unicom is a boundary-crossing pressure proxy',
        'balanced_weekdays':WEEKDAYS,'no_per_link_scaling':True},indent=2))
    log('DONE')
    print(metrics.query('sample == "primary" and metric == "n_valid_transitions"').to_string(index=False))
    print(cov.to_string(index=False));print(direction.query('metric == "n_valid_transitions"').to_string(index=False))


if __name__=='__main__':
    main()
