SELECT scheme, n_rows, n_rows_ge5, n_trips, n_trips_ge5, n_trips_mode1, n_trips_mode1_ge5, n_rows_mode1, n_rows_mode1_ge5
FROM (
SELECT 'A_date_slot30' scheme,
       count(1) n_rows,
       sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_trips,
       sum(if(n_people>=5,n_trips,0)) n_trips_ge5,
       sum(if(mode_group='mode1',n_trips,0)) n_trips_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,n_trips,0)) n_trips_mode1_ge5,
       sum(if(mode_group='mode1',1,0)) n_rows_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,1,0)) n_rows_mode1_ge5
FROM (
SELECT t.`date` dkey, cast(floor(t.slot10/3) AS int) skey, t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.in_joint=1 AND t.valid_strict=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
GROUP BY t.`date`, cast(floor(t.slot10/3) AS int), t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other')
) g
UNION ALL
SELECT 'B_pool_slot30' scheme,
       count(1) n_rows,
       sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_trips,
       sum(if(n_people>=5,n_trips,0)) n_trips_ge5,
       sum(if(mode_group='mode1',n_trips,0)) n_trips_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,n_trips,0)) n_trips_mode1_ge5,
       sum(if(mode_group='mode1',1,0)) n_rows_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,1,0)) n_rows_mode1_ge5
FROM (
SELECT 'pool' dkey, cast(floor(t.slot10/3) AS int) skey, t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.in_joint=1 AND t.valid_strict=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
GROUP BY 'pool', cast(floor(t.slot10/3) AS int), t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other')
) g
UNION ALL
SELECT 'C_pool_slot60' scheme,
       count(1) n_rows,
       sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_trips,
       sum(if(n_people>=5,n_trips,0)) n_trips_ge5,
       sum(if(mode_group='mode1',n_trips,0)) n_trips_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,n_trips,0)) n_trips_mode1_ge5,
       sum(if(mode_group='mode1',1,0)) n_rows_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,1,0)) n_rows_mode1_ge5
FROM (
SELECT 'pool' dkey, cast(floor(t.slot10/6) AS int) skey, t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.in_joint=1 AND t.valid_strict=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
GROUP BY 'pool', cast(floor(t.slot10/6) AS int), t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other')
) g
UNION ALL
SELECT 'D_pool_window' scheme,
       count(1) n_rows,
       sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_trips,
       sum(if(n_people>=5,n_trips,0)) n_trips_ge5,
       sum(if(mode_group='mode1',n_trips,0)) n_trips_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,n_trips,0)) n_trips_mode1_ge5,
       sum(if(mode_group='mode1',1,0)) n_rows_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,1,0)) n_rows_mode1_ge5
FROM (
SELECT 'pool' dkey, -1 skey, t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.in_joint=1 AND t.valid_strict=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
GROUP BY 'pool', -1, t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other')
) g
UNION ALL
SELECT 'E_date_window' scheme,
       count(1) n_rows,
       sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_trips,
       sum(if(n_people>=5,n_trips,0)) n_trips_ge5,
       sum(if(mode_group='mode1',n_trips,0)) n_trips_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,n_trips,0)) n_trips_mode1_ge5,
       sum(if(mode_group='mode1',1,0)) n_rows_mode1,
       sum(if(mode_group='mode1' AND n_people>=5,1,0)) n_rows_mode1_ge5
FROM (
SELECT t.`date` dkey, -1 skey, t.purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.in_joint=1 AND t.valid_strict=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
GROUP BY t.`date`, -1, t.purpose,
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')),
         if(t.mode=1,'mode1','other')
) g
) u;
