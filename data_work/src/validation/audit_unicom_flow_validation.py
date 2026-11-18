"""Independent source checks and prespecified measurement diagnostics."""
import csv
import json
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'data_work/outputs/unicom_model_flow_validation_v1'


def main():
    cross=pd.read_parquet(OUT/'rn_crosswalk.parquet')
    daily=pd.read_parquet(OUT/'rn_daily_am.parquet')
    audit=pd.read_csv(OUT/'formal_file_audit.csv')
    model=pd.read_csv(OUT/'model_baseline_links.csv')
    comparison=pd.read_csv(OUT/'weekday_link_comparison.csv')
    checks={}
    checks['formal_files_389']=bool(len(audit)==389)
    checks['formal_rows_match_source_guide']=bool(audit.rows.sum()==13288340)
    checks['no_duplicate_rn_date']=bool(not daily[['date','rn_pair_id']].duplicated().any())
    checks['missing_am_cells_retained']=bool(daily.n_valid_transitions.isna().any())
    p=cross.loc[cross.primary]
    checks['primary_osm_snap_within_10m']=bool((p.osm_snap_from_m<=10).all() and (p.osm_snap_to_m<=10).all())
    checks['primary_endpoint_length_rule']=bool(p.chord_length_m.between(1,500).all())
    checks['primary_osm_paths_cross_only_requested_grids']=bool(p.osm_grid_sequence_ok.all())
    checks['all_model_links_cross_grids']=bool(model.grid_o.ne(model.grid_d).all())
    arcs_path=Path('/Users/fxr/Library/CloudStorage/Dropbox/Projects/Projects_FengGu/Speed/Road_processing/V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet')
    arcs=pd.read_parquet(arcs_path)
    lengths=arcs.groupby(['from_node_idx','to_node_idx']).length_m.min().to_dict()
    for row in p.itertuples():
        path=list(map(int,row.osm_node_path.split(',')))
        assert path[0]==int(row.osm_from_node_idx) and path[-1]==int(row.osm_to_node_idx)
        actual=sum(lengths[(a,b)] for a,b in zip(path[:-1],path[1:]))
        assert abs(actual-row.osm_path_length_m)<1e-7
        assert row.osm_grid_sequence==row.grid_o+','+row.grid_d
    checks['saved_osm_paths_are_valid_directed_arcs']=True
    # Check source arithmetic with Python's CSV parser, independently of pandas.
    cache=daily.set_index(['date','rn_pair_id'])
    checked=0
    for date in range(20250519,20250526):
        file=next(Path(f) for f in audit.path if str(date) in Path(f).name)
        with file.open(newline='',encoding='utf-8-sig') as stream:
            for row in csv.DictReader(stream):
                if int(row['time_block_6h'])!=1:
                    continue
                key=(int(row['date']),row['rn_pair_id'])
                record=cache.loc[key]
                for m in ['n_valid_transitions','n_unique_trips','n_unique_users']:
                    values=[float(row[f'{m}_s{s:02}']) for s in range(6,18) if row[f'{m}_s{s:02}']]
                    expected=sum(values) if values else np.nan
                    actual=record[m]
                    assert (np.isnan(expected) and np.isnan(actual)) or expected==actual
                checked+=1
                if checked % 100==0:
                    break
    checks['independent_csv_arithmetic_700_rows']=checked==700
    # Independent rank-correlation formula.
    w=comparison.loc[comparison['sample'].eq('primary')].copy()
    x=w.model_Xi.rank().to_numpy();y=w.n_valid_transitions.rank().to_numpy()
    x=x-x.mean();y=y-y.mean()
    independent=float(x@y/np.sqrt((x@x)*(y@y)))
    checks['independent_rank_formula']=bool(abs(independent-spearmanr(w.model_Xi,w.n_valid_transitions).statistic)<1e-12)
    # Separate the effect of changing match rules from changing the link universe.
    common=set.intersection(*[set(g.model_edge_id) for _,g in comparison.groupby('sample')])
    common_metrics=[]
    for name,g in comparison.groupby('sample'):
        g=g.loc[g.model_edge_id.isin(common)]
        common_metrics.append({'sample':name,'n_common_links':len(g),
            'spearman':float(spearmanr(g.model_Xi,g.n_valid_transitions).statistic)})
    pd.DataFrame(common_metrics).to_csv(OUT/'common_support_sensitivity.csv',index=False)
    # Coverage stages retain the full RN universe, including within-grid passages.
    d=daily.loc[daily.date.between(20250519,20250523)].merge(
        cross[['rn_pair_id','grid_o','grid_d','grid_crossing','model_edge_id','endpoint_candidate','primary']],
        on='rn_pair_id',how='left',validate='many_to_one')
    stages={
        'all_observed_RN_pairs':np.ones(len(d),dtype=bool),
        'both_endpoints_in_grid_universe':d.grid_o.notna()&d.grid_d.notna(),
        'different_endpoint_grids':d.grid_crossing.fillna(False),
        'requested_model_grid_pair_any_length':d.model_edge_id.notna(),
        'requested_model_grid_pair_1_to_1000m':d.endpoint_candidate.fillna(False),
        'primary_osm_supported_up_to_500m':d.primary.fillna(False)}
    coverage=[]
    for stage,mask in stages.items():
        z=d.loc[mask]
        coverage.append({'stage':stage,'date_pair_rows':len(z),'unique_rn_pairs':z.rn_pair_id.nunique(),
                         'weekday_am_valid_transitions':float(z.n_valid_transitions.sum())})
    pd.DataFrame(coverage).to_csv(OUT/'observation_coverage_stages.csv',index=False)
    pair=pd.read_csv(OUT/'primary_direction_pairs.csv')
    direction_size={'n':len(pair),
        'median_abs_model_log_ratio':float(pair.model_log_ratio.abs().median()),
        'median_abs_unicom_log_ratio':float(pair.observed_log_ratio.abs().median()),
        'model_median_major_minor_ratio':float(np.exp(pair.model_log_ratio.abs()).median()),
        'unicom_median_major_minor_ratio':float(np.exp(pair.observed_log_ratio.abs()).median())}
    (OUT/'direction_magnitude.json').write_text(json.dumps(direction_size,indent=2))
    # Day agreement: this is temporal repeatability of the observation proxy.
    z=pd.read_csv(OUT/'primary_daily_links.csv').pivot(index='model_edge_id',columns='date',values='n_valid_transitions')
    z=z.dropna(subset=list(range(20250519,20250524)))
    daychecks=[]
    for date in range(20250520,20250524):
        ratio=z[date]/z[20250519]
        daychecks.append({'date':date,'n_links':len(z),'equal_to_monday_fraction':float((z[date]==z[20250519]).mean()),
                          'median_ratio_to_monday':float(ratio.median()),'ratio_iqr':float(ratio.quantile(.75)-ratio.quantile(.25))})
    pd.DataFrame(daychecks).to_csv(OUT/'daily_repeatability_audit.csv',index=False)
    (OUT/'verification_checks.json').write_text(json.dumps(checks,indent=2))
    assert all(checks.values()),checks
    print(json.dumps(checks,indent=2))
    print(pd.DataFrame(common_metrics).to_string(index=False))
    print(pd.DataFrame(coverage).to_string(index=False))
    print(json.dumps(direction_size,indent=2))


if __name__=='__main__':
    main()
