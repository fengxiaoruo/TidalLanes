"""Write 0020 grid/fixture upload parts and the 0021 match test from reference_inputs, and register them in queue.json."""
from pathlib import Path
import csv
import hashlib
import json

ROOT=Path(__file__).resolve().parents[1]
REF=ROOT/'reference_inputs'
OUT=ROOT/'202505_v01/01_geo_inputs'
PART_BYTES=100000
GRID_TABLE='tl25m05_v01_grid_ref'
FIX_TABLE='tl25m05_v01_geo_fixture_ref'


def q(s):
    assert "'" not in s and '\\' not in s
    return "'"+s+"'"


def parts(rows,fmt):
    out,cur,size=[],[],0
    for r in rows:
        line=fmt(r)
        if cur and size+len(line)>PART_BYTES:
            out.append(cur);cur,size=[],0
        cur.append(line);size+=len(line)
    if cur:out.append(cur)
    return out


def upload_sql(table,cols,casts,chunk,first):
    head=(f'DROP TABLE IF EXISTS {table};\nCREATE TABLE {table} AS\n' if first else f'INSERT INTO TABLE {table}\n')
    sel=', '.join(f'cast(s.{c} AS {t}) {c}' for c,t in zip(cols,casts))
    body=',\n'.join('  '+l for l in chunk)
    return (f'{head}SELECT {sel}\nFROM (SELECT stack({len(chunk)},\n{body}\n) AS ({", ".join(cols)})) s;\n'
            f"SELECT 'n_rows' metric, count(1) n_count FROM {table};\n")


def main():
    meta=json.loads((REF/'manifest.json').read_text())
    x0,y0=meta['x_origin_3857'],meta['y_origin_3857']
    with (REF/'model_grid_wgs84_v01.csv').open() as f:grid=list(csv.DictReader(f))
    with (REF/'synthetic_grid_fixture_v01.csv').open() as f:fix=list(csv.DictReader(f))
    # Full 3000 m squares are recovered from row/col; only clipped cells need their polygon.
    gparts=parts(grid,lambda r:f"{q(r['grid_id'])}, {int(r['row_i'])}, {int(r['col_j'])}, {int(r['in_gate5'])}, {int(r['is_full_rectangle'])}, "
                 +q('' if r['is_full_rectangle']=='1' else r['wkt_wgs84']))
    fparts=parts(fix,lambda r:f"{q(r['fixture_id'])}, {q(repr(float(r['lon_wgs84'])))}, {q(repr(float(r['lat_wgs84'])))}")
    files=[]
    for k,chunk in enumerate(gparts,1):
        name=f'TL25M05_V01_0020G{k}_grid_upload.sql'
        (OUT/name).write_text(upload_sql(GRID_TABLE,['grid_id','row_i','col_j','in_gate5','is_full_rectangle','wkt_wgs84'],
                                         ['string','int','int','int','int','string'],chunk,k==1))
        files.append((f'0020G{k}',name,[f'0020G{k-1}'] if k>1 else ['0009'],len(chunk)))
    for k,chunk in enumerate(fparts,1):
        name=f'TL25M05_V01_0020F{k}_fixture_upload.sql'
        (OUT/name).write_text(upload_sql(FIX_TABLE,['fixture_id','lon_wgs84','lat_wgs84'],['string','double','double'],chunk,k==1))
        files.append((f'0020F{k}',name,[f'0020F{k-1}'] if k>1 else ['0009'],len(chunk)))
    match=f'''SELECT f.fixture_id,
       CASE WHEN g.is_full_rectangle=1 THEN g.grid_id
            WHEN g.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(g.wkt_wgs84),Default.ST_Point(f.lon_wgs84,f.lat_wgs84)) THEN g.grid_id
       END grid_id,
       g.grid_id candidate_grid_id,
       f.row_i,
       f.col_j
FROM (
SELECT fixture_id, lon_wgs84, lat_wgs84,
       cast(floor((6378137.0*radians(lon_wgs84)-({x0!r}))/3000.0) AS int) col_j,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(lat_wgs84)/2.0))-({y0!r}))/3000.0) AS int) row_i
FROM {FIX_TABLE}
) f
LEFT JOIN {GRID_TABLE} g ON g.row_i=f.row_i AND g.col_j=f.col_j;
'''
    (OUT/'TL25M05_V01_0021_grid_fixture_match.sql').write_text(match)
    last_g=[i for i,_,_,_ in files if i.startswith('0020G')][-1]
    last_f=[i for i,_,_,_ in files if i.startswith('0020F')][-1]
    qp=ROOT/'202505_v01/queue.json';queue=json.loads(qp.read_text())
    old={r['id']:r for r in queue}
    assert all(old.get(i,{}).get('status','not_run')=='not_run' for i,_,_,_ in files),'upload parts already submitted'
    new=[]
    for r in queue:
        if r['id'].startswith('0020') or r['id']=='0021':continue
        if r['id']=='0100':
            for i,name,dep,n in files:
                p=OUT/name
                new.append({'id':i,'file':f'202505_v01/01_geo_inputs/{name}','kind':'public_reference_upload','depends_on':dep,
                            'status':'not_run','local_ready':True,'rows':n,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
            p=OUT/'TL25M05_V01_0021_grid_fixture_match.sql'
            new.append({'id':'0021','file':'202505_v01/01_geo_inputs/TL25M05_V01_0021_grid_fixture_match.sql','kind':'read_only_probe',
                        'depends_on':[last_g,last_f],'status':'not_run','local_ready':True,
                        'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                        'validate':'python3 Platform/utils/validate_probe_result.py --kind geo --csv <result>'})
            r['depends_on']=['0021' if d=='0020' else d for d in r['depends_on']]
        new.append(r)
    qp.write_text(json.dumps(new,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'grid_parts':len(gparts),'fixture_parts':len(fparts),'grid_rows':len(grid),'fixture_rows':len(fix),
                      'max_part_bytes':max((OUT/n).stat().st_size for _,n,_,_ in files)},indent=2))


if __name__=='__main__':
    main()
