DROP TABLE IF EXISTS tl25m05_v01_od_band_exp;
CREATE TABLE tl25m05_v01_od_band_exp AS
SELECT c.purpose purpose,
       c.band band,
       c.mode_group mode_group,
       if(c.n_people>=5,c.grid_o,'SUPP') grid_o,
       if(c.n_people>=5,c.grid_d,'SUPP') grid_d,
       count(1) n_cells,
       sum(c.n_trips) n_trips,
       sum(c.n_people) n_people,
       sum(c.n_person_days) n_person_days,
       sum(c.sum_dur_s) sum_dur_s,
       sum(c.sum_distance_m) sum_distance_m
FROM (
SELECT t.purpose purpose,
       t.band band,
       if(t.mode=1,'mode1','other') mode_group,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d,
       count(1) n_trips,
       count(DISTINCT t.uid) n_people,
       count(DISTINCT concat(t.uid,'_',cast(t.`date` AS string))) n_person_days,
       sum(t.dur_s) sum_dur_s,
       sum(t.distance_m) sum_distance_m
FROM (
SELECT x.uid, x.`date`, x.purpose, x.mode, x.dur_s, x.distance_m, b.band
FROM tl25m05_v01_trip_internal x
LATERAL VIEW explode(array('all_day','dep_0610','dep_0709','onroad_0610','onroad_0709','dep_1620','dep_1719','onroad_1620','onroad_1719')) b AS band
WHERE x.in_joint=1 AND x.valid_strict=1 AND x.purpose IN ('HW','WH')
  AND (b.band='all_day'
       OR (x.purpose='HW' AND b.band='dep_0610' AND x.slot10>=36 AND x.slot10<60)
       OR (x.purpose='HW' AND b.band='dep_0709' AND x.slot10>=42 AND x.slot10<54)
       OR (x.purpose='HW' AND b.band='onroad_0610' AND x.slot10*600+300<36000 AND x.slot10*600+300+x.dur_s>21600)
       OR (x.purpose='HW' AND b.band='onroad_0709' AND x.slot10*600+300<32400 AND x.slot10*600+300+x.dur_s>25200)
       OR (x.purpose='WH' AND b.band='dep_1620' AND x.slot10>=96 AND x.slot10<120)
       OR (x.purpose='WH' AND b.band='dep_1719' AND x.slot10>=102 AND x.slot10<114)
       OR (x.purpose='WH' AND b.band='onroad_1620' AND x.slot10*600+300<72000 AND x.slot10*600+300+x.dur_s>57600)
       OR (x.purpose='WH' AND b.band='onroad_1719' AND x.slot10*600+300<68400 AND x.slot10*600+300+x.dur_s>61200))
) t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
GROUP BY t.purpose, t.band, if(t.mode=1,'mode1','other'),
         if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')),
         if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT'))
) c
GROUP BY c.purpose, c.band, c.mode_group, if(c.n_people>=5,c.grid_o,'SUPP'), if(c.n_people>=5,c.grid_d,'SUPP');

SELECT purpose, band, mode_group,
       sum(n_cells) n_cells,
       sum(if(grid_o<>'SUPP',n_cells,0)) n_cells_ge5,
       sum(n_trips) n_trips,
       sum(if(grid_o<>'SUPP',n_trips,0)) n_trips_ge5,
       sum(if(grid_o<>'SUPP' AND grid_o<>'OUT' AND grid_d<>'OUT',n_trips,0)) n_trips_ge5_in_grid,
       sum(if(grid_o<>'SUPP',1,0)) n_rows_export
FROM tl25m05_v01_od_band_exp
GROUP BY purpose, band, mode_group;
