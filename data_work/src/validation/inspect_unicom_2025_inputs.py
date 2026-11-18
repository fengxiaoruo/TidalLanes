"""Read-only inventory of 2025 commute inputs in SubwayMobility and the shared library."""
from pathlib import Path
import hashlib
import json
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
PROJECTS=ROOT.parent
SUBWAY=PROJECTS/'SubwayMobility'
QUERY=SUBWAY/'Documents/Validation/PlatformRuns/ng_v01_two_wave/query-results'
SQL=SUBWAY/'Platform/ng_v01_two_wave/20_replication'
OUT=ROOT/'data_work/outputs/unicom_2025_input_feasibility_v1'


def main():
    OUT.mkdir(exist_ok=True)
    files=sorted(QUERY.glob('TW_NG103D_tau2_tb25_exp_*_v01.csv'))
    assert len(files)==6
    inventory=[]
    parts=[]
    for file in files:
        d=pd.read_csv(file)
        inventory.append({'path':str(file),'rows':len(d),'trips':int(d.n_trip.sum()),
                          'sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'columns':'|'.join(d.columns)})
        parts.append(d)
    d=pd.concat(parts,ignore_index=True)
    key=['dir','period','rail_detected','dictionary_mode_group']
    assert not d[key+['o_cx','o_cy','d_cx','d_cy']].duplicated().any()
    assert d.n_people.ge(5).all()
    group=d.groupby(key).agg(rows=('n_trip','size'),trips=('n_trip','sum')).reset_index()
    qc=pd.read_csv(QUERY/'TW_NG103C_tau2_tb25_build_v01_qc.csv')
    comparison=group.merge(qc.loc[qc.n_cells_kept.gt(0)],on=key,validate='one_to_one',how='outer',indicator=True)
    assert comparison['_merge'].eq('both').all()
    assert comparison.rows.eq(comparison.n_cells_kept).all()
    assert comparison.trips.eq(comparison.n_trip_kept).all()
    comparison.to_csv(OUT/'ng103_export_qc.csv',index=False)
    group.to_csv(OUT/'ng103_counts_by_direction_period_mode.csv',index=False)
    sources=[QUERY/'TW_NG74B_odmode_exp25_v01.csv',
             PROJECTS/'ChinaUnicomProjects/Projects/03_subway_mobility/raw/201911-202505/TW_NG74A_cell_counts_exp_v01.csv',
             PROJECTS/'ChinaUnicomProjects/Projects/03_subway_mobility/raw/202505/SM25M05_P14_R01_od_result.csv']
    for p in sources:
        z=pd.read_csv(p)
        inventory.append({'path':str(p),'rows':len(z),'trips':None,
                          'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'columns':'|'.join(z.columns)})
    for p in [SQL/'TW_NG103C_tau2_tb25_build_v01.sql',SQL/'TW_NG103D_tau2_tb25_exp_hw0709_v01.sql']:
        inventory.append({'path':str(p),'rows':None,'trips':None,
                          'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'columns':None})
    pd.DataFrame(inventory).to_csv(OUT/'source_inventory.csv',index=False)
    # Interpret explicit integer-hour SQL predicates; never infer intervals from labels.
    bounds=pd.DataFrame([{'period_label':'07-09','actual_start_inclusive':'07:00','actual_end_exclusive':'10:00'},
                         {'period_label':'10-16','actual_start_inclusive':'10:00','actual_end_exclusive':'17:00'},
                         {'period_label':'17-19','actual_start_inclusive':'17:00','actual_end_exclusive':'20:00'}])
    sql=(SQL/'TW_NG103C_tau2_tb25_build_v01.sql').read_text()
    assert 'mm.hh BETWEEN 7 AND 9' in sql and 'mm.hh BETWEEN 10 AND 16' in sql and 'mm.hh BETWEEN 7 AND 19' in sql
    bounds.to_csv(OUT/'ng103_actual_time_bounds.csv',index=False)
    result={'source_files':6,'rows':len(d),'trips':int(d.n_trip.sum()),'qc_matched':True,
            'spatial_grid_km':5,'sample_days':19,'actual_time_bounds':bounds.to_dict('records'),
            'has_date_column':bool('date' in d),'has_hour_column':bool('hour' in d),
            'exact_0709_recoverable_from_export':False,
            'mode1_label':'m1_ground_unspecified; not validated pure car',
            'home_work_min_distance_m':1000,'trip_min_distance_m':500,
            'ng74b25_rows':19127,'shared_p14_25_month_value':'16875-5; raw anomalous label, uncorrected'}
    (OUT/'feasibility_audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
