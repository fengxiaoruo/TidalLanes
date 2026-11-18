"""Check the extraction plan and local references without network or SQL execution."""
from pathlib import Path
import csv
import hashlib
import json
import re
from datetime import datetime
from daas_task import lint

ROOT=Path(__file__).resolve().parents[1]


def main():
    cfg=json.loads((ROOT/'config/extraction_202505_v01.json').read_text())
    q=json.loads((ROOT/'202505_v01/queue.json').read_text())
    ids=[r['id'] for r in q]
    assert len(ids)==len(set(ids))
    state={r['id']:r for r in q}
    done=set()
    def visit(k,trail):
        assert k not in trail,f'Dependency cycle: {trail} {k}'
        if k in done:return
        for dep in state[k]['depends_on']:
            assert dep in state,f'Missing dependency {dep}'
            visit(dep,trail+[k])
        done.add(k)
    for k in ids:visit(k,[])
    assert len(cfg['ordinary_weekdays'])==19
    assert len(set(cfg['ordinary_weekdays']))==19
    for date in cfg['ordinary_weekdays']:
        assert datetime.strptime(str(date),'%Y%m%d').weekday()<5
    assert cfg['validation_passage_dates']==[20250519,20250520,20250521,20250522,20250523]
    assert cfg['departure_windows']['AM']=={'start':'07:00:00','end_exclusive':'09:00:00'}
    assert cfg['departure_windows']['PM']=={'start':'17:00:00','end_exclusive':'19:00:00'}
    assert cfg['export_min_distinct_people']==5
    for r in q:
        if not r['file'] or r['status']=='satisfied_by_reference':continue
        file=ROOT/r['file'];sql=file.read_text()
        assert not lint(sql),f'{file}: {lint(sql)}'
        if r['kind']!='read_only_probe':continue
        assert not file.is_symlink()
        assert hashlib.sha256(file.read_bytes()).hexdigest()==r['sha256']
        assert sql.count(';')==1,f'{file}: must contain exactly one statement'
        assert not re.search(r'\b(DROP|CREATE|INSERT|DELETE|ALTER)\b',sql,re.I),file
        assert '--' not in sql and '/*' not in sql and '{{' not in sql
        assert not re.search(r'\bSELECT\s+\*',sql,re.I)
        assert not re.search(r'\bSELECT\s+(?:cast\()?uid\b',sql,re.I)
    meta=json.loads((ROOT/'reference_inputs/manifest.json').read_text())
    assert meta['gate5_nodes']==1104 and meta['gate5_edges']==2642
    assert meta['clipped_gate5_cells']==44
    for rec in meta['files']:
        file=ROOT/'reference_inputs'/rec['path']
        assert hashlib.sha256(file.read_bytes()).hexdigest()==rec['sha256']
    with (ROOT/'reference_inputs/time_fixture_v01.csv').open() as f:
        tests=list(csv.DictReader(f))
    for t in tests:
        dt=datetime.fromisoformat(t['stamp'])
        p='AM' if 7<=dt.hour<9 else 'PM' if 17<=dt.hour<19 else 'OTHER'
        assert p==t['expected_period']
        assert int(t['expected_slot10'])==dt.hour*6+dt.minute//10
        assert int(t['expected_slot30'])==dt.hour*2+dt.minute//30
    ready=[r['id'] for r in q if r['local_ready'] and r['status']=='not_run' and all(state[d]['status'] in ('passed','satisfied_by_reference') for d in r['depends_on'])]
    result={'local_checks_passed':True,'planned_tasks':len(q),'read_only_sql_files':sum(bool(r['file']) for r in q),
            'dependency_eligible_probe_ids':ready,'remote_execution_confirmed':False,
            'reference_grid_count':meta['grid_count'],'public_geo_fixture_rows':meta['fixture_rows'],
            'note':'Local checks do not establish live schema, function availability, login or platform approval.'}
    out=ROOT/'runs/202505_v01/checks/workspace_check.json'
    out.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
