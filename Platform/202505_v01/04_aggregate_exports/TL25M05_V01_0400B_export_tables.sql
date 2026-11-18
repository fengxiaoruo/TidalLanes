DROP TABLE IF EXISTS tl25m05_v01_x_dep_slot30_exp;
CREATE TABLE tl25m05_v01_x_dep_slot30_exp AS
SELECT t.variant variant,
       t.purpose purpose,
       cast(floor(t.slot10/3) AS int) slot30,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people,
       count(DISTINCT concat(t.uid,'_',cast(t.`date` AS string))) n_person_days
FROM (
SELECT x.uid, x.`date`, x.slot10, x.purpose, x.mode, v.variant
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1
  AND ((x.purpose='HW' AND x.period='AM') OR (x.purpose='WH' AND x.period='PM'))
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.variant, t.purpose, cast(floor(t.slot10/3) AS int), if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')), if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')), if(t.mode=1,'mode1','other')
HAVING count(DISTINCT t.uid)>=5;

DROP TABLE IF EXISTS tl25m05_v01_x_dep_window_exp;
CREATE TABLE tl25m05_v01_x_dep_window_exp AS
SELECT t.variant variant,
       t.purpose purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people,
       count(DISTINCT concat(t.uid,'_',cast(t.`date` AS string))) n_person_days
FROM (
SELECT x.uid, x.`date`, x.slot10, x.purpose, x.mode, v.variant
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1
  AND ((x.purpose='HW' AND x.period='AM') OR (x.purpose='WH' AND x.period='PM'))
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.variant, t.purpose, if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')), if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')), if(t.mode=1,'mode1','other')
HAVING count(DISTINCT t.uid)>=5;

DROP TABLE IF EXISTS tl25m05_v01_x_dep_date_window_exp;
CREATE TABLE tl25m05_v01_x_dep_date_window_exp AS
SELECT t.`date` `date`,
       t.variant variant,
       t.purpose purpose,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       if(t.mode=1,'mode1','other') mode_group,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people
FROM (
SELECT x.uid, x.`date`, x.slot10, x.purpose, x.mode, v.variant
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE x.in_joint=1
  AND ((x.purpose='HW' AND x.period='AM') OR (x.purpose='WH' AND x.period='PM'))
  AND ((v.variant='strict' AND x.valid_strict=1) OR (v.variant='relaxed' AND x.valid_relaxed=1))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.`date`, t.variant, t.purpose, if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')), if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')), if(t.mode=1,'mode1','other')
HAVING count(DISTINCT t.uid)>=5;

DROP TABLE IF EXISTS tl25m05_v01_x_n_exp;
CREATE TABLE tl25m05_v01_x_n_exp AS
SELECT home_grid_id, work_grid_id, home_in_gate5, work_in_gate5, n_people
FROM tl25m05_v01_hw_od_internal
WHERE n_people>=5;

DROP TABLE IF EXISTS tl25m05_v01_x_n_margin_exp;
CREATE TABLE tl25m05_v01_x_n_margin_exp AS
SELECT side, grid_id, n_people_total,
       if(n_people_suppressed>=5,n_people_suppressed,NULL) n_people_suppressed,
       if(n_people_suppressed>0 AND n_people_suppressed<5,1,0) suppressed_lt5
FROM (
SELECT 'home' side, home_grid_id grid_id, sum(n_people) n_people_total, sum(if(n_people<5,n_people,0)) n_people_suppressed
FROM tl25m05_v01_hw_od_internal
GROUP BY home_grid_id
UNION ALL
SELECT 'work' side, work_grid_id grid_id, sum(n_people) n_people_total, sum(if(n_people<5,n_people,0)) n_people_suppressed
FROM tl25m05_v01_hw_od_internal
GROUP BY work_grid_id
) m
WHERE n_people_total>=5;

DROP TABLE IF EXISTS tl25m05_v01_x_part_exp;
CREATE TABLE tl25m05_v01_x_part_exp AS
SELECT `date`, variant, home_grid_id, work_grid_id, n_people_any,
       if(n_people_hw>=5,n_people_hw,NULL) n_people_hw,
       if(n_people_wh>=5,n_people_wh,NULL) n_people_wh,
       if(n_people_hw_am>=5,n_people_hw_am,NULL) n_people_hw_am,
       if(n_people_wh_pm>=5,n_people_wh_pm,NULL) n_people_wh_pm
FROM tl25m05_v01_participation_internal
WHERE n_people_any>=5;

SELECT a.s30_rows, a.s30_rows_strict, a.s30_trips_strict,
       b.win_rows, b.win_rows_strict, b.win_trips_strict,
       c.dw_rows, c.dw_rows_strict, c.dw_trips_strict,
       d.pt_rows, d.pt_rows_strict, d.pt_person_days_strict,
       e.n_rows, e.n_people,
       f.nm_rows, f.nm_lt5_cells
FROM (
SELECT count(1) s30_rows, sum(if(variant='strict',1,0)) s30_rows_strict, sum(if(variant='strict',n_trips,0)) s30_trips_strict
FROM tl25m05_v01_x_dep_slot30_exp
) a
CROSS JOIN (
SELECT count(1) win_rows, sum(if(variant='strict',1,0)) win_rows_strict, sum(if(variant='strict',n_trips,0)) win_trips_strict
FROM tl25m05_v01_x_dep_window_exp
) b
CROSS JOIN (
SELECT count(1) dw_rows, sum(if(variant='strict',1,0)) dw_rows_strict, sum(if(variant='strict',n_trips,0)) dw_trips_strict
FROM tl25m05_v01_x_dep_date_window_exp
) c
CROSS JOIN (
SELECT count(1) pt_rows, sum(if(variant='strict',1,0)) pt_rows_strict, sum(if(variant='strict',n_people_any,0)) pt_person_days_strict
FROM tl25m05_v01_x_part_exp
) d
CROSS JOIN (
SELECT count(1) n_rows, sum(n_people) n_people FROM tl25m05_v01_x_n_exp
) e
CROSS JOIN (
SELECT count(1) nm_rows, sum(suppressed_lt5) nm_lt5_cells FROM tl25m05_v01_x_n_margin_exp
) f;
