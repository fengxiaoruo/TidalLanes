"""Write the 0400 export-table build and the per-part export SELECTs, and register them in queue.json.

Scheme chosen 2026-11-01: pooled 19 weekdays x slot30, pooled x peak window, date x peak window,
daily participation, monthly joint N with margins. Every exported count below 5 distinct SIMs is blanked.
"""
from pathlib import Path
import hashlib
import json

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'202505_v01/04_aggregate_exports'
CFG=json.loads((ROOT/'config/extraction_202505_v01.json').read_text())
DATES=CFG['ordinary_weekdays']
P='tl25m05_v01_'

TRIPS=f'''FROM (
SELECT x.uid, x.`date`, x.slot10, x.purpose, x.mode, v.variant
FROM {P}trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1
  AND ((x.purpose='HW' AND x.period='AM') OR (x.purpose='WH' AND x.period='PM'))
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN {P}person_internal p ON p.uid=t.uid AND p.in_joint=1'''
GO="if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT'))"
GD="if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT'))"
MG="if(t.mode=1,'mode1','other')"


def dep(table,keys,keynames,person_days):
    sel=',\n       '.join(f'{k} {n}' for k,n in zip(keys,keynames))
    pd_col=",\n       count(DISTINCT concat(t.uid,'_',cast(t.`date` AS string))) n_person_days" if person_days else ''
    return f'''DROP TABLE IF EXISTS {P}{table};
CREATE TABLE {P}{table} AS
SELECT {sel},
       count(1) n_trips,
       count(DISTINCT t.uid) n_people{pd_col}
{TRIPS}
GROUP BY {', '.join(keys)}
HAVING count(DISTINCT t.uid)>=5;
'''


def build_sql():
    s30=dep('x_dep_slot30_exp',['t.variant','t.purpose','cast(floor(t.slot10/3) AS int)',GO,GD,MG],
            ['variant','purpose','slot30','grid_o','grid_d','mode_group'],True)
    win=dep('x_dep_window_exp',['t.variant','t.purpose',GO,GD,MG],['variant','purpose','grid_o','grid_d','mode_group'],True)
    dw=dep('x_dep_date_window_exp',['t.`date`','t.variant','t.purpose',GO,GD,MG],['`date`','variant','purpose','grid_o','grid_d','mode_group'],False)
    n=f'''DROP TABLE IF EXISTS {P}x_n_exp;
CREATE TABLE {P}x_n_exp AS
SELECT home_grid_id, work_grid_id, home_in_gate5, work_in_gate5, n_people
FROM {P}hw_od_internal
WHERE n_people>=5;
'''
    nm=f'''DROP TABLE IF EXISTS {P}x_n_margin_exp;
CREATE TABLE {P}x_n_margin_exp AS
SELECT side, grid_id, n_people_total,
       if(n_people_suppressed>=5,n_people_suppressed,NULL) n_people_suppressed,
       if(n_people_suppressed>0 AND n_people_suppressed<5,1,0) suppressed_lt5
FROM (
SELECT 'home' side, home_grid_id grid_id, sum(n_people) n_people_total, sum(if(n_people<5,n_people,0)) n_people_suppressed
FROM {P}hw_od_internal
GROUP BY home_grid_id
UNION ALL
SELECT 'work' side, work_grid_id grid_id, sum(n_people) n_people_total, sum(if(n_people<5,n_people,0)) n_people_suppressed
FROM {P}hw_od_internal
GROUP BY work_grid_id
) m
WHERE n_people_total>=5;
'''
    pt=f'''DROP TABLE IF EXISTS {P}x_part_exp;
CREATE TABLE {P}x_part_exp AS
SELECT `date`, variant, home_grid_id, work_grid_id, n_people_any,
       if(n_people_hw>=5,n_people_hw,NULL) n_people_hw,
       if(n_people_wh>=5,n_people_wh,NULL) n_people_wh,
       if(n_people_hw_am>=5,n_people_hw_am,NULL) n_people_hw_am,
       if(n_people_wh_pm>=5,n_people_wh_pm,NULL) n_people_wh_pm
FROM {P}participation_internal
WHERE n_people_any>=5;
'''
    def agg(table,alias,extra=''):
        return f'''SELECT count(1) {alias}_rows, sum(if(variant='strict',1,0)) {alias}_rows_strict{extra}
FROM {P}{table}'''
    summary=f'''SELECT a.s30_rows, a.s30_rows_strict, a.s30_trips_strict,
       b.win_rows, b.win_rows_strict, b.win_trips_strict,
       c.dw_rows, c.dw_rows_strict, c.dw_trips_strict,
       d.pt_rows, d.pt_rows_strict, d.pt_person_days_strict,
       e.n_rows, e.n_people,
       f.nm_rows, f.nm_lt5_cells
FROM (
{agg('x_dep_slot30_exp','s30',", sum(if(variant='strict',n_trips,0)) s30_trips_strict")}
) a
CROSS JOIN (
{agg('x_dep_window_exp','win',", sum(if(variant='strict',n_trips,0)) win_trips_strict")}
) b
CROSS JOIN (
{agg('x_dep_date_window_exp','dw',", sum(if(variant='strict',n_trips,0)) dw_trips_strict")}
) c
CROSS JOIN (
{agg('x_part_exp','pt',", sum(if(variant='strict',n_people_any,0)) pt_person_days_strict")}
) d
CROSS JOIN (
SELECT count(1) n_rows, sum(n_people) n_people FROM {P}x_n_exp
) e
CROSS JOIN (
SELECT count(1) nm_rows, sum(suppressed_lt5) nm_lt5_cells FROM {P}x_n_margin_exp
) f;
'''
    return s30+'\n'+win+'\n'+dw+'\n'+n+'\n'+nm+'\n'+pt+'\n'+summary


COLS={
 'x_n_exp':'home_grid_id, work_grid_id, home_in_gate5, work_in_gate5, n_people',
 'x_n_margin_exp':'side, grid_id, n_people_total, n_people_suppressed, suppressed_lt5',
 'x_dep_slot30_exp':'variant, purpose, slot30, grid_o, grid_d, mode_group, n_trips, n_people, n_person_days',
 'x_dep_window_exp':'variant, purpose, grid_o, grid_d, mode_group, n_trips, n_people, n_person_days',
 'x_dep_date_window_exp':'`date`, variant, purpose, grid_o, grid_d, mode_group, n_trips, n_people',
 'x_part_exp':'`date`, variant, home_grid_id, work_grid_id, n_people_any, n_people_hw, n_people_wh, n_people_hw_am, n_people_wh_pm',
}


def parts():
    out=[('N','x_n_exp',None),('NM','x_n_margin_exp',None)]
    for v in ['strict','relaxed']:
        for pu in ['HW','WH']:
            out.append((f'S30_{v}_{pu}','x_dep_slot30_exp',f"variant='{v}' AND purpose='{pu}'"))
    for v in ['strict','relaxed']:
        out.append((f'WIN_{v}','x_dep_window_exp',f"variant='{v}'"))
    for d in DATES:
        out.append((f'DW_{d}','x_dep_date_window_exp',f'`date`={d}'))
    for d in DATES:
        out.append((f'PART_{d}','x_part_exp',f'`date`={d}'))
    return out


def main():
    OUT.mkdir(exist_ok=True)
    build=OUT/'TL25M05_V01_0400B_export_tables.sql'
    build.write_text(build_sql())
    entries=[{'id':'0400B','file':str(build.relative_to(ROOT)),'kind':'internal_build','depends_on':['0120','0130','0500A'],
              'creates':[P+t for t in COLS],'note':'export tables with >=5 suppression; summary row gives totals for part checks'}]
    for k,(tag,table,where) in enumerate(parts(),1):
        f=OUT/f'TL25M05_V01_04{k:02d}_{tag}.sql'
        f.write_text(f'SELECT {COLS[table]}\nFROM {P}{table}' + (f'\nWHERE {where}' if where else '') + ';\n')
        entries.append({'id':f'04{k:02d}','file':str(f.relative_to(ROOT)),'kind':'aggregate_export','depends_on':['0400B'],
                        'table':P+table,'part':tag})
    qp=ROOT/'202505_v01/queue.json';q=json.loads(qp.read_text())
    have={x['id'] for x in q}
    assert not any(e['id'] in have and next(x for x in q if x['id']==e['id'])['status']!='not_run' for e in entries),'export already started'
    q=[x for x in q if x['id'] not in {e['id'] for e in entries}]
    i=[k for k,x in enumerate(q) if x['id']=='0400'][0]
    for e in entries:
        e.update(status='not_run',local_ready=True,sha256=hashlib.sha256((ROOT/e['file']).read_bytes()).hexdigest())
    q[i:i]=entries
    for x in q:
        if x['id']=='0400':
            x['depends_on']=[e['id'] for e in entries[1:]]
            x['note']='umbrella: passes when all parts pass and part row sums equal 0400B totals'
    qp.write_text(json.dumps(q,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'build':str(build.relative_to(ROOT)),'parts':len(entries)-1},indent=2))


if __name__=='__main__':
    main()
