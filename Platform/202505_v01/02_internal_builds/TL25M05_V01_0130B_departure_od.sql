DROP TABLE IF EXISTS tl25m05_v01_departure_od_internal;
CREATE TABLE tl25m05_v01_departure_od_internal AS
SELECT t.`date`,
       cast(floor(t.slot10/3) AS int) slot30,
       t.period,
       t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       t.primary_endpoints,
       t.variant,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM (
SELECT x.uid, x.`date`, x.slot10, x.period, x.purpose, x.mode, x.primary_endpoints, v.variant
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1 AND x.purpose IN ('HW','WH')
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.`date`, cast(floor(t.slot10/3) AS int), t.period, t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other'), t.primary_endpoints, t.variant;

DROP TABLE IF EXISTS tl25m05_v01_participation_internal;
CREATE TABLE tl25m05_v01_participation_internal AS
SELECT t.`date`,
       coalesce(p.home_grid_id,'OUT') home_grid_id,
       coalesce(p.work_grid_id,'OUT') work_grid_id,
       t.variant,
       count(DISTINCT if(t.purpose='HW',t.uid,NULL)) n_people_hw,
       count(DISTINCT if(t.purpose='WH',t.uid,NULL)) n_people_wh,
       count(DISTINCT t.uid) n_people_any,
       count(DISTINCT if(t.purpose='HW' AND t.period='AM',t.uid,NULL)) n_people_hw_am,
       count(DISTINCT if(t.purpose='WH' AND t.period='PM',t.uid,NULL)) n_people_wh_pm
FROM (
SELECT x.uid, x.`date`, x.slot10, x.period, x.purpose, x.mode, x.primary_endpoints, v.variant
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1 AND x.purpose IN ('HW','WH')
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.`date`, coalesce(p.home_grid_id,'OUT'), coalesce(p.work_grid_id,'OUT'), t.variant;

SELECT d.n_dep_rows, d.n_dep_rows_strict, d.n_dep_trips_strict, d.n_dep_rows_strict_ge5, d.n_dep_trips_strict_ge5,
       d.n_dep_rows_strict_window, d.n_dep_rows_strict_window_ge5, d.n_dep_trips_strict_window, d.n_dep_trips_strict_window_ge5,
       d.n_dep_rows_strict_window_mode1, d.n_dep_rows_strict_window_mode1_ge5, d.n_dep_trips_strict_window_mode1, d.n_dep_trips_strict_window_mode1_ge5,
       d.n_dep_trips_relaxed, d.n_dates,
       r.n_part_rows, r.n_part_rows_strict, r.n_person_days_any_strict, r.n_part_rows_strict_ge5, r.n_person_days_any_strict_ge5
FROM (
SELECT count(1) n_dep_rows,
       sum(if(variant='strict',1,0)) n_dep_rows_strict,
       sum(if(variant='strict',n_trips,0)) n_dep_trips_strict,
       sum(if(variant='strict' AND n_people>=5,1,0)) n_dep_rows_strict_ge5,
       sum(if(variant='strict' AND n_people>=5,n_trips,0)) n_dep_trips_strict_ge5,
       sum(if(variant='strict' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')),1,0)) n_dep_rows_strict_window,
       sum(if(variant='strict' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')) AND n_people>=5,1,0)) n_dep_rows_strict_window_ge5,
       sum(if(variant='strict' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')),n_trips,0)) n_dep_trips_strict_window,
       sum(if(variant='strict' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')) AND n_people>=5,n_trips,0)) n_dep_trips_strict_window_ge5,
       sum(if(variant='strict' AND mode_group='mode1' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')),1,0)) n_dep_rows_strict_window_mode1,
       sum(if(variant='strict' AND mode_group='mode1' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')) AND n_people>=5,1,0)) n_dep_rows_strict_window_mode1_ge5,
       sum(if(variant='strict' AND mode_group='mode1' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')),n_trips,0)) n_dep_trips_strict_window_mode1,
       sum(if(variant='strict' AND mode_group='mode1' AND ((purpose='HW' AND period='AM') OR (purpose='WH' AND period='PM')) AND n_people>=5,n_trips,0)) n_dep_trips_strict_window_mode1_ge5,
       sum(if(variant='relaxed',n_trips,0)) n_dep_trips_relaxed,
       count(DISTINCT `date`) n_dates
FROM tl25m05_v01_departure_od_internal
) d
CROSS JOIN (
SELECT count(1) n_part_rows,
       sum(if(variant='strict',1,0)) n_part_rows_strict,
       sum(if(variant='strict',n_people_any,0)) n_person_days_any_strict,
       sum(if(variant='strict' AND n_people_any>=5,1,0)) n_part_rows_strict_ge5,
       sum(if(variant='strict' AND n_people_any>=5,n_people_any,0)) n_person_days_any_strict_ge5
FROM tl25m05_v01_participation_internal
) r;
