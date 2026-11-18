"""Check downloaded 0400 export parts against the 0400B summary row; writes runs/202505_v01/checks/export_check.json."""
from pathlib import Path
import json
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
QR=ROOT/'runs/202505_v01/query-results'
COUNT_COLS=['n_people','n_people_total','n_people_suppressed','n_people_any','n_people_hw','n_people_wh','n_people_hw_am','n_people_wh_pm']


def main():
    queue=json.loads((ROOT/'202505_v01/queue.json').read_text())
    summ=pd.read_csv(QR/'TL25M05_V01_0400B_export_tables.csv').iloc[0]
    parts=[x for x in queue if x.get('kind')=='aggregate_export']
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
        key=x['table'];got.setdefault(key,{'rows':0,'rows_strict':0})
        got[key]['rows']+=len(d)
        if 'variant' in d.columns:got[key]['rows_strict']+=int((d.variant=='strict').sum())
        if len(d)==10:problems.append(f'{f.name}: exactly 10 rows, check for platform truncation')
    expect={'tl25m05_v01_x_dep_slot30_exp':('s30_rows','s30_rows_strict'),'tl25m05_v01_x_dep_window_exp':('win_rows','win_rows_strict'),
            'tl25m05_v01_x_dep_date_window_exp':('dw_rows','dw_rows_strict'),'tl25m05_v01_x_part_exp':('pt_rows','pt_rows_strict'),
            'tl25m05_v01_x_n_exp':('n_rows',None),'tl25m05_v01_x_n_margin_exp':('nm_rows',None)}
    table_check={}
    for t,(a,b) in expect.items():
        g=got.get(t,{'rows':0,'rows_strict':0})
        ok=g['rows']==int(summ[a]) and (b is None or g['rows_strict']==int(summ[b]))
        table_check[t]={'expected_rows':int(summ[a]),'downloaded_rows':g['rows'],
                        'expected_strict':None if b is None else int(summ[b]),'downloaded_strict':g['rows_strict'] if b else None,'ok':ok}
        if not ok:problems.append(f'row mismatch {t}')
    out={'parts':len(parts),'total_bytes':sum(sizes),'max_part_bytes':max(sizes) if sizes else 0,
         'tables':table_check,'problems':problems,'passed':not problems}
    (ROOT/'runs/202505_v01/checks/export_check.json').write_text(json.dumps(out,ensure_ascii=False,indent=2))
    print(json.dumps(out,ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
