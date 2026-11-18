SELECT f.fixture_id,
       CASE WHEN g.is_full_rectangle=1 THEN g.grid_id
            WHEN g.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(g.wkt_wgs84),Default.ST_Point(f.lon_wgs84,f.lat_wgs84)) THEN g.grid_id
       END grid_id,
       g.grid_id candidate_grid_id,
       f.row_i,
       f.col_j
FROM (
SELECT fixture_id, lon_wgs84, lat_wgs84,
       cast(floor((6378137.0*radians(lon_wgs84)-(12848142.373323334))/3000.0) AS int) col_j,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(lat_wgs84)/2.0))-(4785196.001162674))/3000.0) AS int) row_i
FROM tl25m05_v01_geo_fixture_ref
) f
LEFT JOIN tl25m05_v01_grid_ref g ON g.row_i=f.row_i AND g.col_j=f.col_j;
