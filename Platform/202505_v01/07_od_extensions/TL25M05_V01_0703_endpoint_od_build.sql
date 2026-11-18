DROP TABLE IF EXISTS tl25m05_v01_endpoint_od_exp;
CREATE TABLE tl25m05_v01_endpoint_od_exp AS
SELECT q.purpose purpose,
       q.band band,
       if(q.n_people>=5 OR q.grid_o IN ('NOPOI','POICONF','BADXY','OUT'),q.grid_o,'SUPP') grid_o,
       if(q.n_people>=5 OR q.grid_d IN ('NOPOI','POICONF','BADXY','OUT'),q.grid_d,'SUPP') grid_d,
       count(1) n_cells,
       sum(q.n_trips) n_trips,
       sum(q.n_people) n_people,
       sum(q.n_trips_in_r) n_trips_in_r,
       sum(q.sum_dur_s) sum_dur_s,
       sum(q.sum_distance_m) sum_distance_m
FROM (
SELECT y.purpose purpose,
       y.band band,
       y.grid_o grid_o,
       y.grid_d grid_d,
       count(1) n_trips,
       count(DISTINCT y.uid) n_people,
       sum(y.in_r) n_trips_in_r,
       sum(y.dur_s) sum_dur_s,
       sum(y.distance_m) sum_distance_m
FROM (
SELECT x.uid, x.purpose, x.dur_s, x.distance_m, x.in_r,
       if(x.slot10<60,'dep_0610','dep_1620') band,
       CASE WHEN x.o_lon IS NULL THEN 'NOPOI'
            WHEN x.o_conf=1 THEN 'POICONF'
            WHEN NOT (x.o_lon BETWEEN 115.4 AND 117.8 AND x.o_lat BETWEEN 39.3 AND 41.2) THEN 'BADXY'
            WHEN gro.is_full_rectangle=1 THEN gro.grid_id
            WHEN gro.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(gro.wkt_wgs84),Default.ST_Point(x.o_lon,x.o_lat)) THEN gro.grid_id
            ELSE 'OUT' END grid_o,
       CASE WHEN x.d_lon IS NULL THEN 'NOPOI'
            WHEN x.d_conf=1 THEN 'POICONF'
            WHEN NOT (x.d_lon BETWEEN 115.4 AND 117.8 AND x.d_lat BETWEEN 39.3 AND 41.2) THEN 'BADXY'
            WHEN grd.is_full_rectangle=1 THEN grd.grid_id
            WHEN grd.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(grd.wkt_wgs84),Default.ST_Point(x.d_lon,x.d_lat)) THEN grd.grid_id
            ELSE 'OUT' END grid_d
FROM (
SELECT m.uid, m.purpose, m.slot10, m.dur_s, m.distance_m,
       if(pr.in_r=1,1,0) in_r,
       so.lon o_lon, so.lat o_lat, so.xy_conflict o_conf,
       se.lon d_lon, se.lat d_lat, se.xy_conflict d_conf,
       cast(floor((6378137.0*radians(so.lon)-(12848142.373323334))/3000.0) AS int) o_col,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(so.lat)/2.0))-(4785196.001162674))/3000.0) AS int) o_row,
       cast(floor((6378137.0*radians(se.lon)-(12848142.373323334))/3000.0) AS int) d_col,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(se.lat)/2.0))-(4785196.001162674))/3000.0) AS int) d_row
FROM (
SELECT u.uid, u.start_poi, u.end_poi, u.dur_s, u.distance_m,
       CASE WHEN u.start_ptype IS NULL OR u.end_ptype IS NULL OR u.start_ptype<0 OR u.end_ptype<0 THEN 'unknown'
            WHEN u.start_ptype=1 AND u.end_ptype=2 THEN 'HW'
            WHEN u.start_ptype=2 AND u.end_ptype=1 THEN 'WH'
            WHEN u.start_ptype=1 OR u.end_ptype=1 THEN 'home_other'
            WHEN u.start_ptype=2 OR u.end_ptype=2 THEN 'work_other'
            ELSE 'other' END purpose,
       cast(substr(u.stime_str,12,2) AS int)*6+cast(floor(cast(substr(u.stime_str,15,2) AS int)/10.0) AS int) slot10
FROM (
SELECT cast(uid AS string) uid, `date`, cast(move_id AS string) move_id,
       if(max(cast(mode AS int))<>min(cast(mode AS int))
          OR max(cast(distance AS double))<>min(cast(distance AS double))
          OR max(cast(stime AS string))<>min(cast(stime AS string))
          OR max(cast(etime AS string))<>min(cast(etime AS string))
          OR max(cast(start_ptype AS int))<>min(cast(start_ptype AS int))
          OR max(cast(end_ptype AS int))<>min(cast(end_ptype AS int))
          OR max(cast(start_poi AS string))<>min(cast(start_poi AS string))
          OR max(cast(end_poi AS string))<>min(cast(end_poi AS string)),1,0) key_conflict,
       min(cast(mode AS int)) mode,
       min(cast(distance AS double)) distance_m,
       min(unix_timestamp(cast(etime AS string))-unix_timestamp(cast(stime AS string))) dur_s,
       min(cast(stime AS string)) stime_str,
       min(cast(start_ptype AS int)) start_ptype,
       min(cast(end_ptype AS int)) end_ptype,
       min(cast(start_poi AS string)) start_poi,
       min(cast(end_poi AS string)) end_poi
FROM move_month
WHERE city='V0110000'
  AND `date` IN (20250506,20250507,20250508,20250509,20250512,20250513,20250514,20250515,20250516,
                 20250519,20250520,20250521,20250522,20250523,20250526,20250527,20250528,20250529,20250530)
GROUP BY cast(uid AS string), `date`, cast(move_id AS string)
) u
WHERE u.key_conflict=0 AND u.mode=1 AND u.dur_s>0 AND u.distance_m>0 AND 3.6*u.distance_m/u.dur_s<=200
  AND ((substr(u.stime_str,12,8)>='06:00:00' AND substr(u.stime_str,12,8)<'10:00:00')
       OR (substr(u.stime_str,12,8)>='16:00:00' AND substr(u.stime_str,12,8)<'20:00:00'))
) m
LEFT JOIN (
SELECT cast(uid AS string) uid, cast(poi_id AS string) poi_id,
       min(cast(weighted_centroid_lon AS double)) lon,
       min(cast(weighted_centroid_lat AS double)) lat,
       if(max(cast(weighted_centroid_lon AS double))<>min(cast(weighted_centroid_lon AS double))
          OR max(cast(weighted_centroid_lat AS double))<>min(cast(weighted_centroid_lat AS double)),1,0) xy_conflict
FROM stay_poi
WHERE city='V0110000' AND date=20250501
GROUP BY cast(uid AS string), cast(poi_id AS string)
) so ON so.uid=m.uid AND so.poi_id=m.start_poi
LEFT JOIN (
SELECT cast(uid AS string) uid, cast(poi_id AS string) poi_id,
       min(cast(weighted_centroid_lon AS double)) lon,
       min(cast(weighted_centroid_lat AS double)) lat,
       if(max(cast(weighted_centroid_lon AS double))<>min(cast(weighted_centroid_lon AS double))
          OR max(cast(weighted_centroid_lat AS double))<>min(cast(weighted_centroid_lat AS double)),1,0) xy_conflict
FROM stay_poi
WHERE city='V0110000' AND date=20250501
GROUP BY cast(uid AS string), cast(poi_id AS string)
) se ON se.uid=m.uid AND se.poi_id=m.end_poi
LEFT JOIN tl25m05_v01_person_internal pr ON pr.uid=m.uid
) x
LEFT JOIN tl25m05_v01_grid_ref gro ON gro.row_i=x.o_row AND gro.col_j=x.o_col
LEFT JOIN tl25m05_v01_grid_ref grd ON grd.row_i=x.d_row AND grd.col_j=x.d_col
) y
GROUP BY y.purpose, y.band, y.grid_o, y.grid_d
) q
GROUP BY q.purpose, q.band,
         if(q.n_people>=5 OR q.grid_o IN ('NOPOI','POICONF','BADXY','OUT'),q.grid_o,'SUPP'),
         if(q.n_people>=5 OR q.grid_d IN ('NOPOI','POICONF','BADXY','OUT'),q.grid_d,'SUPP');

SELECT purpose, band,
       sum(n_cells) n_cells,
       sum(n_trips) n_trips,
       sum(n_trips_in_r) n_trips_in_r,
       sum(if(grid_o='NOPOI' OR grid_d='NOPOI',n_trips,0)) n_trips_nopoi,
       sum(if(grid_o IN ('POICONF','BADXY') OR grid_d IN ('POICONF','BADXY'),n_trips,0)) n_trips_badxy,
       sum(if(grid_o='OUT' OR grid_d='OUT',n_trips,0)) n_trips_out,
       sum(if(grid_o='SUPP' OR grid_d='SUPP',n_trips,0)) n_trips_supp,
       sum(if(grid_o NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT') AND grid_d NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT'),n_trips,0)) n_trips_export,
       sum(if(grid_o NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT') AND grid_d NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT'),1,0)) n_rows_export
FROM tl25m05_v01_endpoint_od_exp
GROUP BY purpose, band;
