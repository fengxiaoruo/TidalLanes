"""Check downloaded 0460 RN export parts against the 0460B summary row; writes runs/202505_v01/checks/rn_export_check.json."""
from pathlib import Path
import json
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
QR=ROOT/'runs/202505_v01/query-results'
COUNT_COLS=['n_people','n_linked_trips']


def main():
    queue=json.loads((ROOT/'202505_v01/queue.json').read_text())
    summ=pd.read_csv(QR/'TL25M05_V01_0460B_rn_export_tables.csv').iloc[0]
    parts=[x for x in queue if x.get('kind')=='aggregate_export_rn']
    got={};problems=[];sizes=[]
    for x in parts:
        f=QR/(Path(x['file']).name.replace('.sql','.csv'))
        if not f.exists():
            problems.append(f"missing {f.name}");continue
        d=pd.read_csv(f,dtype=str,keep_default_na=False)
        sizes.append(f.stat().st_size)
        if any(c.lower()=='uid' for c in d.columns):problems.append(f'uid column in {f.name}')
        for c in COUNT_COLS:
            if c in d.columns:
                v=pd.to_numeric(d[c].replace('',None),errors='coerce').dropna()
                if (v<5).any():problems.append(f'{f.name}: {c} has values below 5')
        g=got.setdefault(x['table'],{'rows':0,'pass':0,'rows_strict':0,'trips_strict':0})
        g['rows']+=len(d)
        if 'n_pass' in d.columns:g['pass']+=int(pd.to_numeric(d.n_pass).sum())
        if 'variant' in d.columns:
            s=d[d.variant=='strict'];g['rows_strict']+=len(s);g['trips_strict']+=int(pd.to_numeric(s.n_trips).sum())
        if len(d)==10:problems.append(f'{f.name}: exactly 10 rows, check for platform truncation')
    expect={'tl25m05_v01_rn_x_slot30_exp':('s30_rows','s30_pass'),'tl25m05_v01_rn_x_window_exp':('win_rows','win_pass'),
            'tl25m05_v01_rn_x_date_window_exp':('dw_rows','dw_pass')}
    table_check={}
    for t,(a,b) in expect.items():
        g=got.get(t,{'rows':0,'pass':0})
        ok=g['rows']==int(summ[a]) and g['pass']==int(summ[b])
        table_check[t]={'expected_rows':int(summ[a]),'downloaded_rows':g['rows'],'expected_pass':int(summ[b]),'downloaded_pass':g['pass'],'ok':ok}
        if not ok:problems.append(f'mismatch {t}')
    g=got.get('tl25m05_v01_rn_x_od_cover_exp',{'rows':0,'rows_strict':0,'trips_strict':0})
    ok=g['rows']==int(summ.cov_rows) and g['rows_strict']==int(summ.cov_rows_strict) and g['trips_strict']==int(summ.cov_trips_strict)
    table_check['tl25m05_v01_rn_x_od_cover_exp']={'expected_rows':int(summ.cov_rows),'downloaded_rows':g['rows'],
        'expected_trips_strict':int(summ.cov_trips_strict),'downloaded_trips_strict':g['trips_strict'],'ok':ok}
    if not ok:problems.append('mismatch tl25m05_v01_rn_x_od_cover_exp')
    out={'parts':len(parts),'total_bytes':sum(sizes),'max_part_bytes':max(sizes) if sizes else 0,
         'tables':table_check,'problems':problems,'passed':not problems}
    (ROOT/'runs/202505_v01/checks/rn_export_check.json').write_text(json.dumps(out,ensure_ascii=False,indent=2))
    print(json.dumps(out,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
