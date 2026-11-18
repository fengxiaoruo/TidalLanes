DROP TABLE IF EXISTS tl25m05_v01_trip_p0519_internal;
CREATE TABLE tl25m05_v01_trip_p0519_internal AS
SELECT t.uid, t.`date`, t.move_id, t.n_rows, t.key_conflict, t.mode, t.distance_m, t.time_raw, t.stime_str, t.etime_str, t.dur_s,
       t.start_ptype, t.end_ptype, t.start_poi, t.end_poi, t.start_native_grid, t.end_native_grid,
       CASE WHEN t.start_ptype IS NULL OR t.end_ptype IS NULL OR t.start_ptype<0 OR t.end_ptype<0 THEN 'unknown'
            WHEN t.start_ptype=1 AND t.end_ptype=2 THEN 'HW'
            WHEN t.start_ptype=2 AND t.end_ptype=1 THEN 'WH'
            WHEN t.start_ptype=1 OR t.end_ptype=1 THEN 'home_other'
            WHEN t.start_ptype=2 OR t.end_ptype=2 THEN 'work_other'
            ELSE 'other' END purpose,
       cast(substr(t.stime_str,12,2) AS int)*6+cast(floor(cast(substr(t.stime_str,15,2) AS int)/10.0) AS int) slot10,
       cast(substr(t.stime_str,12,2) AS int)*2+cast(floor(cast(substr(t.stime_str,15,2) AS int)/30.0) AS int) slot30,
       CASE WHEN substr(t.stime_str,12,8)>='07:00:00' AND substr(t.stime_str,12,8)<'09:00:00' THEN 'AM'
            WHEN substr(t.stime_str,12,8)>='17:00:00' AND substr(t.stime_str,12,8)<'19:00:00' THEN 'PM'
            ELSE 'OTHER' END period,
       if(regexp_replace(substr(t.stime_str,1,10),'-','')<>cast(t.`date` AS string),1,0) stime_off_partition,
       if(substr(t.etime_str,1,10)<>substr(t.stime_str,1,10),1,0) etime_next_day,
       if(t.start_native_grid IS NULL OR trim(t.start_native_grid) IN ('','-1','null','NULL')
          OR t.end_native_grid IS NULL OR trim(t.end_native_grid) IN ('','-1','null','NULL'),1,0) native_grid_bad,
       if(t.key_conflict=0 AND t.dur_s>0 AND t.distance_m>0 AND 3.6*t.distance_m/t.dur_s<=200,1,0) valid_trip,
       coalesce(p.in_r,0) in_r,
       coalesce(p.in_joint,0) in_joint,
       p.home_poi, p.work_poi, p.home_grid_id, p.work_grid_id,
       sp.ptype start_poi_ptype, sp.lon_wgs84 start_lon, sp.lat_wgs84 start_lat,
       ep.ptype end_poi_ptype, ep.lon_wgs84 end_lon, ep.lat_wgs84 end_lat
FROM (
SELECT cast(uid AS string) uid, `date`, cast(move_id AS string) move_id,
       count(1) n_rows,
       if(max(cast(mode AS int))<>min(cast(mode AS int))
          OR max(cast(distance AS double))<>min(cast(distance AS double))
          OR max(cast(`time` AS double))<>min(cast(`time` AS double))
          OR max(cast(stime AS string))<>min(cast(stime AS string))
          OR max(cast(etime AS string))<>min(cast(etime AS string))
          OR max(cast(start_ptype AS int))<>min(cast(start_ptype AS int))
          OR max(cast(end_ptype AS int))<>min(cast(end_ptype AS int))
          OR max(cast(start_poi AS string))<>min(cast(start_poi AS string))
          OR max(cast(end_poi AS string))<>min(cast(end_poi AS string)),1,0) key_conflict,
       min(cast(mode AS int)) mode,
       min(cast(distance AS double)) distance_m,
       min(cast(`time` AS double)) time_raw,
       min(cast(stime AS string)) stime_str,
       min(cast(etime AS string)) etime_str,
       min(unix_timestamp(cast(etime AS string))-unix_timestamp(cast(stime AS string))) dur_s,
       min(cast(start_ptype AS int)) start_ptype,
       min(cast(end_ptype AS int)) end_ptype,
       min(cast(start_poi AS string)) start_poi,
       min(cast(end_poi AS string)) end_poi,
       min(cast(start_grid_id AS string)) start_native_grid,
       min(cast(end_grid_id AS string)) end_native_grid
FROM move_month
WHERE city='V0110000' AND `date`=20250519
GROUP BY cast(uid AS string), `date`, cast(move_id AS string)
) t
LEFT JOIN tl25m05_v01_person_internal p ON p.uid=t.uid
LEFT JOIN tl25m05_v01_poi_internal sp ON sp.uid=t.uid AND sp.poi_id=t.start_poi
LEFT JOIN tl25m05_v01_poi_internal ep ON ep.uid=t.uid AND ep.poi_id=t.end_poi;

SELECT count(1) n_trip_keys,
       sum(n_rows) n_raw_rows,
       sum(if(n_rows>1,1,0)) n_dup_keys,
       sum(key_conflict) n_conflict_keys,
       sum(stime_off_partition) n_stime_off_partition,
       sum(etime_next_day) n_etime_next_day,
       sum(native_grid_bad) n_native_grid_bad,
       sum(if(dur_s IS NULL,1,0)) n_dur_missing,
       sum(if(dur_s<=0,1,0)) n_dur_nonpos,
       sum(if(abs(time_raw-dur_s)<=1,1,0)) n_time_eq_dur_seconds,
       sum(if(abs(time_raw*60-dur_s)<=60,1,0)) n_time_eq_dur_minutes,
       sum(if(distance_m IS NULL OR distance_m<=0,1,0)) n_dist_nonpos,
       sum(if(dur_s>0 AND distance_m>0 AND 3.6*distance_m/dur_s>200,1,0)) n_speed_gt200_dur,
       sum(valid_trip) n_valid,
       sum(if(valid_trip=1 AND mode=1,1,0)) n_valid_mode1,
       sum(if(valid_trip=1 AND purpose='HW',1,0)) n_valid_hw,
       sum(if(valid_trip=1 AND purpose='WH',1,0)) n_valid_wh,
       sum(if(valid_trip=1 AND purpose='home_other',1,0)) n_valid_home_other,
       sum(if(valid_trip=1 AND purpose='work_other',1,0)) n_valid_work_other,
       sum(if(valid_trip=1 AND purpose='other',1,0)) n_valid_other,
       sum(if(valid_trip=1 AND purpose='unknown',1,0)) n_valid_unknown,
       sum(if(valid_trip=1 AND in_r=1,1,0)) n_valid_r,
       sum(if(valid_trip=1 AND in_joint=1,1,0)) n_valid_joint,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='HW',1,0)) n_valid_joint_hw,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='WH',1,0)) n_valid_joint_wh,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='HW' AND period='AM',1,0)) n_valid_joint_hw_am,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='WH' AND period='PM',1,0)) n_valid_joint_wh_pm,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='HW' AND period='AM' AND mode=1,1,0)) n_valid_joint_hw_am_mode1,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='WH' AND period='PM' AND mode=1,1,0)) n_valid_joint_wh_pm_mode1,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='HW' AND start_poi=home_poi,1,0)) n_joint_hw_start_is_primary_home,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='HW' AND end_poi=work_poi,1,0)) n_joint_hw_end_is_primary_work,
       sum(if(valid_trip=1 AND in_joint=1 AND purpose='WH' AND start_poi=work_poi AND end_poi=home_poi,1,0)) n_joint_wh_both_primary,
       sum(if(valid_trip=1 AND purpose IN ('HW','WH') AND start_lon IS NOT NULL AND end_lon IS NOT NULL,1,0)) n_valid_hwwh_both_endpoints_xy,
       sum(if(valid_trip=1 AND purpose IN ('HW','WH') AND (start_poi_ptype<>start_ptype OR end_poi_ptype<>end_ptype),1,0)) n_valid_hwwh_ptype_mismatch,
       count(DISTINCT if(valid_trip=1 AND in_joint=1 AND purpose IN ('HW','WH'),uid,NULL)) n_joint_people_with_hwwh_trip
FROM tl25m05_v01_trip_p0519_internal;
