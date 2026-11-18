"""Compare downloaded synthetic time or geometry probe output against local fixtures."""
import argparse
import json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
BOUNDARY_TOL_DEG=1e-9


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--kind',choices=['time','geo'],required=True)
    p.add_argument('--csv',type=Path,required=True)
    a=p.parse_args()
    actual=pd.read_csv(a.csv,keep_default_na=False)
    expected=pd.read_csv(ROOT/'reference_inputs'/('time_fixture_v01.csv' if a.kind=='time' else 'synthetic_grid_fixture_v01.csv'),keep_default_na=False)
    assert actual.fixture_id.is_unique,'Duplicate matches: geometry assignment is ambiguous'
    j=expected.merge(actual,on='fixture_id',how='outer',suffixes=('_expected','_actual'),indicator=True,validate='one_to_one')
    assert j['_merge'].eq('both').all(),'Probe result missing fixtures or containing extra IDs'
    if a.kind=='time':
        for exp,col in [('expected_period','period'),('expected_slot10','slot10'),('expected_slot30','slot30')]:
            assert j[exp].astype(str).eq(j[col].astype(str)).all(),f'Mismatch in {col}'
        near=pd.Series(False,index=j.index)
    else:
        near=near_boundary(j)
        bad=j[~near&j.expected_grid_id.ne(j.grid_id)]
        assert bad.empty,f'Polygon mismatch away from boundaries: {bad.fixture_id.head(10).tolist()}'
    print(json.dumps({'kind':a.kind,'rows':len(j),'matches_expected':True,'near_boundary_excluded':int(near.sum()),
                      'near_boundary_ids':j.loc[near,'fixture_id'].tolist(),'input':str(a.csv)},ensure_ascii=False))


def near_boundary(j,tol=BOUNDARY_TOL_DEG):
    # Points this close to an edge are boundary cases under the spec; platform and local float paths may split them either way.
    from shapely import wkt
    from shapely.geometry import Point
    grid=pd.read_csv(ROOT/'reference_inputs/model_grid_wgs84_v01.csv').set_index('grid_id').wkt_wgs84.map(wkt.loads)
    out=[]
    for r in j.itertuples():
        p=Point(float(r.lon_wgs84),float(r.lat_wgs84))
        cells=[c for c in {r.expected_grid_id,r.grid_id} if c]
        out.append(any(grid[c].boundary.distance(p)<tol for c in cells))
    return pd.Series(out,index=j.index)


if __name__=='__main__':
    main()
