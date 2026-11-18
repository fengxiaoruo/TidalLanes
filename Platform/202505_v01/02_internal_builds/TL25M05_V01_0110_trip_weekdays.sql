DROP TABLE IF EXISTS tl25m05_v01_trip_internal;
CREATE TABLE tl25m05_v01_trip_internal AS
SELECT u.uid, u.`date`, u.move_id, u.mode, u.distance_m, u.dur_s, u.purpose, u.slot10, u.period,
       u.key_conflict,
       if(u.key_conflict=0 AND u.dur_s>0 AND u.distance_m>0 AND 3.6*u.distance_m/u.dur_s<=200,1,0) valid_strict,
       if(u.key_conflict=0 AND u.dur_s>=0 AND u.distance_m>0
          AND NOT (u.dur_s>=60 AND 3.6*u.distance_m/u.dur_s>200),1,0) valid_relaxed,
       if(u.dur_s>=0 AND u.dur_s<60 AND u.distance_m>0 AND (u.dur_s=0 OR 3.6*u.distance_m/u.dur_s>200),1,0) degenerate_duration,
       coalesce(p.in_r,0) in_r,
       coalesce(p.in_joint,0) in_joint,
       CASE WHEN u.purpose='HW' AND u.start_poi=p.home_poi AND u.end_poi=p.work_poi THEN 1
            WHEN u.purpose='WH' AND u.start_poi=p.work_poi AND u.end_poi=p.home_poi THEN 1
            ELSE 0 END primary_endpoints
FROM (
SELECT t.uid, t.`date`, t.move_id, t.key_conflict, t.mode, t.distance_m, t.dur_s, t.start_poi, t.end_poi,
       CASE WHEN t.start_ptype IS NULL OR t.end_ptype IS NULL OR t.start_ptype<0 OR t.end_ptype<0 THEN 'unknown'
            WHEN t.start_ptype=1 AND t.end_ptype=2 THEN 'HW'
            WHEN t.start_ptype=2 AND t.end_ptype=1 THEN 'WH'
            WHEN t.start_ptype=1 OR t.end_ptype=1 THEN 'home_other'
            WHEN t.start_ptype=2 OR t.end_ptype=2 THEN 'work_other'
            ELSE 'other' END purpose,
       cast(substr(t.stime_str,12,2) AS int)*6+cast(floor(cast(substr(t.stime_str,15,2) AS int)/10.0) AS int) slot10,
       CASE WHEN substr(t.stime_str,12,8)>='07:00:00' AND substr(t.stime_str,12,8)<'09:00:00' THEN 'AM'
            WHEN substr(t.stime_str,12,8)>='17:00:00' AND substr(t.stime_str,12,8)<'19:00:00' THEN 'PM'
            ELSE 'OTHER' END period
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
) t
) u
LEFT JOIN tl25m05_v01_person_internal p ON p.uid=u.uid;

SELECT `date`,
       count(1) n_trip_keys,
       sum(key_conflict) n_conflict_keys,
       sum(valid_strict) n_valid_strict,
       sum(valid_relaxed) n_valid_relaxed,
       sum(degenerate_duration) n_degenerate_duration,
       sum(if(valid_strict=1 AND mode=1,1,0)) n_strict_mode1,
       sum(if(in_joint=1 AND valid_strict=1 AND purpose='HW',1,0)) n_joint_hw_strict,
       sum(if(in_joint=1 AND valid_strict=1 AND purpose='WH',1,0)) n_joint_wh_strict,
       sum(if(in_joint=1 AND valid_relaxed=1 AND purpose='HW',1,0)) n_joint_hw_relaxed,
       sum(if(in_joint=1 AND valid_relaxed=1 AND purpose='WH',1,0)) n_joint_wh_relaxed,
       sum(if(in_joint=1 AND valid_strict=1 AND purpose='HW' AND period='AM' AND mode=1,1,0)) n_joint_hw_am_mode1_strict,
       sum(if(in_joint=1 AND valid_strict=1 AND purpose='WH' AND period='PM' AND mode=1,1,0)) n_joint_wh_pm_mode1_strict,
       sum(if(in_joint=1 AND valid_strict=1 AND purpose IN ('HW','WH') AND primary_endpoints=1,1,0)) n_joint_hwwh_primary_strict,
       count(DISTINCT if(in_joint=1 AND valid_strict=1 AND purpose IN ('HW','WH'),uid,NULL)) n_joint_people_hwwh_strict,
       count(DISTINCT if(in_joint=1 AND valid_relaxed=1 AND purpose IN ('HW','WH'),uid,NULL)) n_joint_people_hwwh_relaxed
FROM tl25m05_v01_trip_internal
GROUP BY `date`;
