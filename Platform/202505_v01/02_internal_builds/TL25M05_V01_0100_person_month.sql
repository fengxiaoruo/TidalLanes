DROP TABLE IF EXISTS tl25m05_v01_poi_internal;
CREATE TABLE tl25m05_v01_poi_internal AS
SELECT k.uid, k.poi_id, k.n_rows, k.ptype, k.days_poi, k.stay_minutes, k.lon_wgs84, k.lat_wgs84, k.native_grid_id, k.key_conflict,
       if(k.key_conflict=0,
          rank() OVER (PARTITION BY k.uid, k.ptype, k.key_conflict ORDER BY k.days_poi DESC, k.stay_minutes DESC),
          NULL) poi_rank
FROM (
SELECT cast(uid AS string) uid,
       cast(poi_id AS string) poi_id,
       count(1) n_rows,
       min(cast(ptype AS int)) ptype,
       min(cast(days_poi AS int)) days_poi,
       min(coalesce(weekday_day_time,0)+coalesce(weekday_eve_time,0)+coalesce(weekend_day_time,0)+coalesce(weekend_eve_time,0)) stay_minutes,
       min(cast(weighted_centroid_lon AS double)) lon_wgs84,
       min(cast(weighted_centroid_lat AS double)) lat_wgs84,
       min(cast(final_grid_id AS string)) native_grid_id,
       if(max(cast(ptype AS int))<>min(cast(ptype AS int))
          OR max(cast(days_poi AS int))<>min(cast(days_poi AS int))
          OR max(cast(weighted_centroid_lon AS double))<>min(cast(weighted_centroid_lon AS double))
          OR max(cast(weighted_centroid_lat AS double))<>min(cast(weighted_centroid_lat AS double))
          OR max(coalesce(weekday_day_time,0)+coalesce(weekday_eve_time,0)+coalesce(weekend_day_time,0)+coalesce(weekend_eve_time,0))
             <>min(coalesce(weekday_day_time,0)+coalesce(weekday_eve_time,0)+coalesce(weekend_day_time,0)+coalesce(weekend_eve_time,0)),
          1,0) key_conflict
FROM stay_poi
WHERE city='V0110000' AND date=20250501 AND cast(ptype AS int) IN (1,2)
GROUP BY cast(uid AS string), cast(poi_id AS string)
) k;

DROP TABLE IF EXISTS tl25m05_v01_primary_point_internal;
CREATE TABLE tl25m05_v01_primary_point_internal AS
SELECT uid, ptype,
       count(1) n_candidates,
       sum(key_conflict) n_conflict_keys,
       sum(if(poi_rank=1,1,0)) n_top,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,poi_id,NULL)), NULL) poi_id,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,days_poi,NULL)), NULL) days_poi,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,stay_minutes,NULL)), NULL) stay_minutes,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,lon_wgs84,NULL)), NULL) lon_wgs84,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,lat_wgs84,NULL)), NULL) lat_wgs84,
       if(sum(if(poi_rank=1,1,0))=1, max(if(poi_rank=1,native_grid_id,NULL)), NULL) native_grid_id
FROM tl25m05_v01_poi_internal
GROUP BY uid, ptype;

DROP TABLE IF EXISTS tl25m05_v01_person_internal;
CREATE TABLE tl25m05_v01_person_internal AS
SELECT p.uid, p.n_attr_rows, p.attr_conflict, p.age_code, p.is_core_attr, p.adult_19_59,
       p.home_n_candidates, p.home_n_top, p.home_n_conflict_keys, p.home_poi, p.home_days, p.home_minutes, p.home_lon, p.home_lat, p.home_native_grid, p.home_xy_valid,
       p.work_n_candidates, p.work_n_top, p.work_n_conflict_keys, p.work_poi, p.work_days, p.work_minutes, p.work_lon, p.work_lat, p.work_native_grid, p.work_xy_valid,
       CASE WHEN gh.is_full_rectangle=1 THEN gh.grid_id
            WHEN gh.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(gh.wkt_wgs84),Default.ST_Point(p.home_lon,p.home_lat)) THEN gh.grid_id
       END home_grid_id,
       CASE WHEN gw.is_full_rectangle=1 THEN gw.grid_id
            WHEN gw.is_full_rectangle=0 AND Default.ST_Contains(Default.ST_GeomCollection(gw.wkt_wgs84),Default.ST_Point(p.work_lon,p.work_lat)) THEN gw.grid_id
       END work_grid_id,
       gh.in_gate5 home_cand_in_gate5,
       gw.in_gate5 work_cand_in_gate5,
       if(p.attr_conflict=0 AND p.adult_19_59=1 AND p.is_core_attr='Y' AND p.home_n_top=1 AND p.home_xy_valid=1,1,0) in_r,
       if(p.attr_conflict=0 AND p.adult_19_59=1 AND p.is_core_attr='Y' AND p.home_n_top=1 AND p.home_xy_valid=1
          AND p.work_n_top=1 AND p.work_xy_valid=1,1,0) in_joint
FROM (
SELECT a.uid, a.n_attr_rows, a.attr_conflict, a.age_code, a.is_core_attr,
       if(a.age_code BETWEEN '05' AND '12',1,0) adult_19_59,
       coalesce(h.n_candidates,0) home_n_candidates, coalesce(h.n_top,0) home_n_top, coalesce(h.n_conflict_keys,0) home_n_conflict_keys,
       h.poi_id home_poi, h.days_poi home_days, h.stay_minutes home_minutes, h.lon_wgs84 home_lon, h.lat_wgs84 home_lat, h.native_grid_id home_native_grid,
       if(h.lon_wgs84 BETWEEN 115.4 AND 117.8 AND h.lat_wgs84 BETWEEN 39.3 AND 41.2,1,0) home_xy_valid,
       coalesce(w.n_candidates,0) work_n_candidates, coalesce(w.n_top,0) work_n_top, coalesce(w.n_conflict_keys,0) work_n_conflict_keys,
       w.poi_id work_poi, w.days_poi work_days, w.stay_minutes work_minutes, w.lon_wgs84 work_lon, w.lat_wgs84 work_lat, w.native_grid_id work_native_grid,
       if(w.lon_wgs84 BETWEEN 115.4 AND 117.8 AND w.lat_wgs84 BETWEEN 39.3 AND 41.2,1,0) work_xy_valid,
       cast(floor((6378137.0*radians(h.lon_wgs84)-(12848142.373323334))/3000.0) AS int) home_col,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(h.lat_wgs84)/2.0))-(4785196.001162674))/3000.0) AS int) home_row,
       cast(floor((6378137.0*radians(w.lon_wgs84)-(12848142.373323334))/3000.0) AS int) work_col,
       cast(floor((6378137.0*ln(tan(pi()/4.0+radians(w.lat_wgs84)/2.0))-(4785196.001162674))/3000.0) AS int) work_row
FROM (
SELECT cast(uid AS string) uid,
       count(1) n_attr_rows,
       if(max(lpad(cast(age AS string),2,'0'))<>min(lpad(cast(age AS string),2,'0'))
          OR max(coalesce(trim(upper(cast(is_core AS string))),'missing'))<>min(coalesce(trim(upper(cast(is_core AS string))),'missing')),1,0) attr_conflict,
       min(lpad(cast(age AS string),2,'0')) age_code,
       min(coalesce(trim(upper(cast(is_core AS string))),'missing')) is_core_attr
FROM user_attribute
WHERE city='V0110000' AND date=20250501
GROUP BY cast(uid AS string)
) a
LEFT JOIN tl25m05_v01_primary_point_internal h ON h.uid=a.uid AND h.ptype=1
LEFT JOIN tl25m05_v01_primary_point_internal w ON w.uid=a.uid AND w.ptype=2
) p
LEFT JOIN tl25m05_v01_grid_ref gh ON gh.row_i=p.home_row AND gh.col_j=p.home_col
LEFT JOIN tl25m05_v01_grid_ref gw ON gw.row_i=p.work_row AND gw.col_j=p.work_col;

SELECT pp.n_attr_uid, pp.n_attr_dup_uid, pp.n_attr_conflict_uid, pp.n_adult_19_59, pp.n_adult_core, pp.n_adult_core_any_home, pp.n_adult_core_home_tie, pp.n_adult_core_home_xy_bad, pp.n_r, pp.n_r_any_work, pp.n_r_work_tie, pp.n_r_work_xy_bad, pp.n_joint, pp.n_joint_same_poi, pp.n_joint_same_grid, pp.n_r_home_in_grid, pp.n_r_home_in_gate5, pp.n_joint_both_in_grid, pp.n_joint_both_in_gate5, pp.n_r_home_cells, pp.n_joint_work_cells, pq.n_poi_keys, pq.n_poi_dup_keys, pq.n_poi_conflict_keys, pq.n_home_poi_keys, pq.n_work_poi_keys, pq.n_poi_id_zero, pq.n_poi_xy_missing
FROM (
SELECT count(1) n_attr_uid,
       sum(if(n_attr_rows>1,1,0)) n_attr_dup_uid,
       sum(attr_conflict) n_attr_conflict_uid,
       sum(adult_19_59) n_adult_19_59,
       sum(if(adult_19_59=1 AND is_core_attr='Y',1,0)) n_adult_core,
       sum(if(adult_19_59=1 AND is_core_attr='Y' AND home_n_candidates>0,1,0)) n_adult_core_any_home,
       sum(if(adult_19_59=1 AND is_core_attr='Y' AND home_n_top>1,1,0)) n_adult_core_home_tie,
       sum(if(adult_19_59=1 AND is_core_attr='Y' AND home_n_top=1 AND home_xy_valid=0,1,0)) n_adult_core_home_xy_bad,
       sum(in_r) n_r,
       sum(if(in_r=1 AND work_n_candidates>0,1,0)) n_r_any_work,
       sum(if(in_r=1 AND work_n_top>1,1,0)) n_r_work_tie,
       sum(if(in_r=1 AND work_n_top=1 AND work_xy_valid=0,1,0)) n_r_work_xy_bad,
       sum(in_joint) n_joint,
       sum(if(in_joint=1 AND home_poi=work_poi,1,0)) n_joint_same_poi,
       sum(if(in_joint=1 AND home_grid_id=work_grid_id,1,0)) n_joint_same_grid,
       sum(if(in_r=1 AND home_grid_id IS NOT NULL,1,0)) n_r_home_in_grid,
       sum(if(in_r=1 AND home_grid_id IS NOT NULL AND home_cand_in_gate5=1,1,0)) n_r_home_in_gate5,
       sum(if(in_joint=1 AND home_grid_id IS NOT NULL AND work_grid_id IS NOT NULL,1,0)) n_joint_both_in_grid,
       sum(if(in_joint=1 AND home_grid_id IS NOT NULL AND work_grid_id IS NOT NULL AND home_cand_in_gate5=1 AND work_cand_in_gate5=1,1,0)) n_joint_both_in_gate5,
       count(DISTINCT if(in_r=1,home_grid_id,NULL)) n_r_home_cells,
       count(DISTINCT if(in_joint=1,work_grid_id,NULL)) n_joint_work_cells
FROM tl25m05_v01_person_internal
) pp
CROSS JOIN (
SELECT count(1) n_poi_keys,
       sum(if(n_rows>1,1,0)) n_poi_dup_keys,
       sum(key_conflict) n_poi_conflict_keys,
       sum(if(ptype=1,1,0)) n_home_poi_keys,
       sum(if(ptype=2,1,0)) n_work_poi_keys,
       sum(if(poi_id='0',1,0)) n_poi_id_zero,
       sum(if(lon_wgs84 IS NULL OR lat_wgs84 IS NULL,1,0)) n_poi_xy_missing
FROM tl25m05_v01_poi_internal
) pq;
