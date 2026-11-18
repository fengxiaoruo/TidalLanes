"""Write the 0460 RN export-table build and the per-part export SELECTs, and register them in queue.json.

Scheme chosen 2026-11-02: strict only, pooled 0519-0523 x slot30, pooled x peak window, date x peak window,
plus the grid-OD linked-trip coverage table (both variants). Rows need >=5 distinct SIMs; parts split by a
hash bucket of rn_pair_id so each part stays near 150k rows.
"""
from pathlib import Path
import hashlib
import json

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'202505_v01/04_aggregate_exports'
P='tl25m05_v01_'
DAYS=[20250519,20250520,20250521,20250522,20250523]
NB={'rn_x_slot30_exp':16,'rn_x_window_exp':4,'rn_x_date_window_exp':3}
BUCKET='pmod(hash(rn_pair_id),{n}) part_bucket'


def rn(table,src,keys,n):
    return f'''DROP TABLE IF EXISTS {P}{table};
CREATE TABLE {P}{table} AS
SELECT {', '.join(keys)}, rn_pair_id, n_pass, n_trips, n_people, {BUCKET.format(n=n)}
FROM {P}{src}
WHERE variant='strict' AND n_people>=5;
'''


def build_sql():
    s30=rn('rn_x_slot30_exp','rn_p_slot30_internal',['purpose','pass_slot30'],NB['rn_x_slot30_exp'])
    win=rn('rn_x_window_exp','rn_p_window_internal',['purpose'],NB['rn_x_window_exp'])
    dw=rn('rn_x_date_window_exp','rn_p_date_window_internal',['`date`','purpose'],NB['rn_x_date_window_exp'])
    cov=f'''DROP TABLE IF EXISTS {P}rn_x_od_cover_exp;
CREATE TABLE {P}rn_x_od_cover_exp AS
SELECT variant, purpose, grid_o, grid_d, n_trips,
       if(n_linked_trips>=5,n_linked_trips,NULL) n_linked_trips,
       n_people
FROM {P}rn_od_cover_internal
WHERE n_people>=5;
'''
    def agg(table,alias):
        return f'''SELECT count(1) {alias}_rows, sum(n_pass) {alias}_pass, max(part_rows) {alias}_max_part_rows
FROM (
SELECT n_pass, count(1) OVER (PARTITION BY part_bucket{', `date`' if alias=='dw' else ''}) part_rows
FROM {P}{table}
) z'''
    summary=f'''SELECT a.s30_rows, a.s30_pass, a.s30_max_part_rows,
       b.win_rows, b.win_pass, b.win_max_part_rows,
       c.dw_rows, c.dw_pass, c.dw_max_part_rows,
       d.cov_rows, d.cov_rows_strict, d.cov_trips_strict
FROM (
{agg('rn_x_slot30_exp','s30')}
) a
CROSS JOIN (
{agg('rn_x_window_exp','win')}
) b
CROSS JOIN (
{agg('rn_x_date_window_exp','dw')}
) c
CROSS JOIN (
SELECT count(1) cov_rows, sum(if(variant='strict',1,0)) cov_rows_strict, sum(if(variant='strict',n_trips,0)) cov_trips_strict
FROM {P}rn_x_od_cover_exp
) d;
'''
    return s30+'\n'+win+'\n'+dw+'\n'+cov+'\n'+summary


COLS={
 'rn_x_slot30_exp':'purpose, pass_slot30, rn_pair_id, n_pass, n_trips, n_people',
 'rn_x_window_exp':'purpose, rn_pair_id, n_pass, n_trips, n_people',
 'rn_x_date_window_exp':'`date`, purpose, rn_pair_id, n_pass, n_trips, n_people',
 'rn_x_od_cover_exp':'variant, purpose, grid_o, grid_d, n_trips, n_linked_trips, n_people',
}


def parts():
    out=[]
    for b in range(NB['rn_x_slot30_exp']):
        out.append((f'RS30_b{b:02d}','rn_x_slot30_exp',f'part_bucket={b}'))
    for b in range(NB['rn_x_window_exp']):
        out.append((f'RWIN_b{b}','rn_x_window_exp',f'part_bucket={b}'))
    for d in DAYS:
        for b in range(NB['rn_x_date_window_exp']):
            out.append((f'RDW_{d}_b{b}','rn_x_date_window_exp',f'`date`={d} AND part_bucket={b}'))
    for v in ['strict','relaxed']:
        out.append((f'RCOV_{v}','rn_x_od_cover_exp',f"variant='{v}'"))
    return out


def main():
    build=OUT/'TL25M05_V01_0460B_rn_export_tables.sql'
    build.write_text(build_sql())
    entries=[{'id':'0460B','file':str(build.relative_to(ROOT)),'kind':'internal_build','depends_on':['0150'],
              'creates':[P+t for t in COLS],'note':'RN export tables (strict, >=5 people) with hash buckets; summary row gives totals for part checks'}]
    for k,(tag,table,where) in enumerate(parts(),61):
        f=OUT/f'TL25M05_V01_04{k:02d}_{tag}.sql'
        f.write_text(f'SELECT {COLS[table]}\nFROM {P}{table}\nWHERE {where};\n')
        entries.append({'id':f'04{k:02d}','file':str(f.relative_to(ROOT)),'kind':'aggregate_export_rn','depends_on':['0460B'],
                        'table':P+table,'part':tag})
    assert len(entries)-1<=39,'part ids would pass 0499'
    qp=ROOT/'202505_v01/queue.json';q=json.loads(qp.read_text())
    have={x['id']:x for x in q}
    assert not any(e['id'] in have and have[e['id']]['status']!='not_run' for e in entries),'RN export already started'
    q=[x for x in q if x['id'] not in {e['id'] for e in entries} and x['id']!='0460']
    for e in entries:
        e.update(status='not_run',local_ready=True,sha256=hashlib.sha256((ROOT/e['file']).read_bytes()).hexdigest())
    i=[k for k,x in enumerate(q) if x['id']=='0400'][0]+1
    umbrella={'id':'0460','name':'RN aggregate export','kind':'planned_stage','depends_on':[e['id'] for e in entries[1:]],
              'status':'not_run','local_ready':False,'file':None,
              'note':'umbrella: passes when all RN parts pass and part row/passage sums equal 0460B totals'}
    q[i:i]=entries+[umbrella]
    qp.write_text(json.dumps(q,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'build':str(build.relative_to(ROOT)),'parts':len(entries)-1},indent=2))


if __name__=='__main__':
    main()
