"""Prepare nonpersonal model geometry inputs and public spatial fixtures locally."""
from pathlib import Path
import hashlib
import json
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

ROOT=Path(__file__).resolve().parents[2]
DEST=ROOT/'Platform/reference_inputs'


def main():
    DEST.mkdir(exist_ok=True)
    src=ROOT/'data_work/outputs/manual_centerline_rules_v11/data/grid_square_master.parquet'
    base=ROOT/'data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_bpr_pos05_v1/primitives'
    grid=gpd.read_parquet(src)
    nodes=pd.read_csv(base/'primitives_nodes.csv')
    edges=pd.read_csv(base/'primitives_edges.csv')
    assert grid.grid_id.is_unique and nodes.grid_id.is_unique
    projected=grid.to_crs(3857)
    rc=projected.grid_id.str.extract(r'^sq_(\d+)_(\d+)$').astype(int)
    b=projected.bounds
    full=((b.maxx-b.minx-3000).abs()<1e-4)&((b.maxy-b.miny-3000).abs()<1e-4)&((projected.area-9e6).abs()<.1)
    x0=float((b.minx-rc[1]*3000)[full].median())
    y0=float((b.miny-rc[0]*3000)[full].median())
    assert ((b.minx-rc[1]*3000)[full]-x0).abs().max()<1e-6
    assert ((b.miny-rc[0]*3000)[full]-y0).abs().max()<1e-6
    reg=pd.DataFrame({'grid_id':grid.grid_id,'row_i':rc[0],'col_j':rc[1],
                      'in_gate5':grid.grid_id.isin(nodes.grid_id).astype(int),'is_full_rectangle':full.astype(int),
                      'wkt_wgs84':grid.geometry.to_wkt(rounding_precision=15)})
    reg.to_csv(DEST/'model_grid_wgs84_v01.csv',index=False)
    nodes[['node_idx','grid_id']].to_csv(DEST/'model_nodes_gate5_v01.csv',index=False)
    e=edges[['edge_i','edge_j']].copy();e.insert(0,'model_edge_id',np.arange(len(e)))
    lookup=nodes.set_index('node_idx').grid_id
    e['grid_o']=e.edge_i.map(lookup);e['grid_d']=e.edge_j.map(lookup)
    e.to_csv(DEST/'model_links_gate5_v01.csv',index=False)
    # Nonpersonal synthetic coordinates; never user POIs or extracted trajectories.
    fixtures=[]
    for idx,row in grid.iterrows():
        point=row.geometry.representative_point()
        fixtures.append({'fixture_id':f'{row.grid_id}_inside','lon_wgs84':point.x,'lat_wgs84':point.y,
                         'expected_grid_id':row.grid_id,'kind':'interior'})
    # Include clipped-cell bbox points, which a bare floor formula can misclassify.
    for idx in grid.index[~full]:
        polygon=grid.loc[idx,'geometry'];a,c,z,d=polygon.bounds
        for ix,fx in enumerate([.05,.5,.95]):
            for iy,fy in enumerate([.05,.5,.95]):
                point=Point(a+(z-a)*fx,c+(d-c)*fy)
                fixtures.append({'fixture_id':f'{grid.loc[idx,"grid_id"]}_bbox{ix}{iy}',
                                 'lon_wgs84':point.x,'lat_wgs84':point.y,'expected_grid_id':'',
                                 'kind':'clipped_bbox'})
    f=pd.DataFrame(fixtures)
    pts=gpd.GeoDataFrame(f,geometry=gpd.points_from_xy(f.lon_wgs84,f.lat_wgs84),crs=grid.crs)
    joined=gpd.sjoin(pts,grid[['grid_id','geometry']],how='left',predicate='within')
    assert not joined.index.duplicated().any()
    f['expected_grid_id']=joined.grid_id.fillna('').to_numpy()
    assert f.loc[f.kind.eq('interior'),'expected_grid_id'].ne('').all()
    f.to_csv(DEST/'synthetic_grid_fixture_v01.csv',index=False)
    floor_case_count=0
    # Show why floor arithmetic alone cannot handle the clipped study boundary.
    test=pts.to_crs(3857)
    rr=np.floor((test.geometry.y.to_numpy()-y0)/3000).astype(int)
    cc=np.floor((test.geometry.x.to_numpy()-x0)/3000).astype(int)
    arithmetic=np.array([f'sq_{i}_{j}' for i,j in zip(rr,cc)])
    floor_case_count=int((arithmetic!=f.expected_grid_id.to_numpy()).sum())
    rn=ROOT/'data_work/outputs/unicom_model_flow_validation_v1/rn_crosswalk.parquet'
    if rn.exists():
        r=pd.read_parquet(rn)
        # Full observed target-grid-pair scope, without selecting on count or match quality.
        target=r.loc[r.model_edge_id.notna(),['rn_pair_id','rn_from','rn_to','grid_o','grid_d','model_edge_id']]
        assert target.rn_pair_id.is_unique
        target.to_csv(DEST/'rn_target_grid_pairs_observed_v01.csv',index=False)
    manifest=[]
    for file in sorted(DEST.glob('*.csv')):
        manifest.append({'path':file.name,'rows':len(pd.read_csv(file)),
                         'sha256':hashlib.sha256(file.read_bytes()).hexdigest()})
    meta={'grid_crs':'EPSG:4326','construction_crs':'EPSG:3857','projected_cell_size_m':3000,
          'cell_size_is_not_geodesic_3000m':True,'x_origin_3857':x0,'y_origin_3857':y0,
          'grid_count':len(reg),'gate5_nodes':len(nodes),'gate5_edges':len(edges),
          'clipped_gate5_cells':int(((~full)&grid.grid_id.isin(nodes.grid_id)).sum()),
          'fixture_rows':len(f),'bare_floor_disagreements':floor_case_count,
          'boundary_rule':'strict interior; unresolved boundary points retained as QC, no arbitrary assignment',
          'upload_performed':False,'rn_target_scope':'observed RN dictionary only; not full physical road universe',
          'files':manifest,'upstream':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
                                    for p in [src,base/'primitives_nodes.csv',base/'primitives_edges.csv']]}
    (DEST/'manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in meta.items() if k not in ['files','upstream']},ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
