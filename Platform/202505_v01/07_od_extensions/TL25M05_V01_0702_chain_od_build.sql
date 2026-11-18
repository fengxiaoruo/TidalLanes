DROP TABLE IF EXISTS tl25m05_v01_chain_od_exp;
CREATE TABLE tl25m05_v01_chain_od_exp AS
SELECT k.purpose purpose,
       k.band band,
       k.chain_kind chain_kind,
       k.mode_group mode_group,
       if(k.n_people>=5,k.grid_o,'SUPP') grid_o,
       if(k.n_people>=5,k.grid_d,'SUPP') grid_d,
       count(1) n_cells,
       sum(k.n_chains) n_chains,
       sum(k.n_people) n_people,
       sum(k.sum_elapsed_s) sum_elapsed_s,
       sum(k.sum_legs) sum_legs,
       sum(k.n_primary_end) n_primary_end,
       sum(k.n_le_90m) n_le_90m
FROM (
SELECT z.chain_dir purpose,
       z.band band,
       z.chain_kind chain_kind,
       z.mode_group mode_group,
       if(z.chain_dir='HW',coalesce(z.hg,'OUT'),coalesce(z.wg,'OUT')) grid_o,
       if(z.chain_dir='HW',coalesce(z.wg,'OUT'),coalesce(z.hg,'OUT')) grid_d,
       count(1) n_chains,
       count(DISTINCT z.uid) n_people,
       sum(z.elapsed_s) sum_elapsed_s,
       sum(z.n_legs) sum_legs,
       sum(z.primary_end) n_primary_end,
       sum(if(z.elapsed_s<=5400,1,0)) n_le_90m
FROM (
SELECT y.uid, y.chain_dir, y.hg, y.wg, y.n_legs, y.elapsed_s, y.primary_end, y.chain_kind, y.mode_group, bb.band
FROM (
SELECT h.uid, h.chain_dir, h.hg, h.wg, h.n_legs, h.elapsed_s, h.start_sod, h.primary_end,
       CASE WHEN h.n_legs=1 AND h.n_direct=1 THEN 'direct'
            WHEN h.jmatch=1 THEN 'transfer_only'
            ELSE 'activity_stop' END chain_kind,
       CASE WHEN h.n_mode1=h.n_legs THEN 'mode1'
            WHEN h.n_mode1>0 THEN 'mixed'
            ELSE 'other' END mode_group
FROM (
SELECT e.uid, e.`date`, e.tour_id, e.chain_dir,
       max(e.hg) hg,
       max(e.wg) wg,
       count(1) n_legs,
       sum(e.invalid_movement) n_invalid,
       sum(if(e.mode=1,1,0)) n_mode1,
       sum(if((e.chain_dir='HW' AND e.start_ptype=1 AND e.end_ptype=2) OR (e.chain_dir='WH' AND e.start_ptype=2 AND e.end_ptype=1),1,0)) n_direct,
       max(if(e.chain_dir='HW',if(e.j_first_work=e.j_tour_first,1,0),if(e.j_after_last_work=e.j_tour_last,1,0))) jmatch,
       min(e.sod) start_sod,
       max(e.t_end)-min(e.t_start) elapsed_s,
       max(if((e.chain_dir='HW' AND e.end_ptype=2 AND e.end_poi=e.wpoi) OR (e.chain_dir='WH' AND e.end_ptype=1 AND e.end_poi=e.hpoi),1,0)) primary_end
FROM (
SELECT c.uid, c.`date`, c.tour_id, c.rn_time, c.mode, c.start_ptype, c.end_ptype, c.end_poi, c.sod, c.t_start, c.t_end,
       c.invalid_movement, c.hg, c.wg, c.hpoi, c.wpoi, c.j_first_work, c.j_tour_first, c.j_after_last_work, c.j_tour_last,
       CASE WHEN c.tour_start_ptype=1 AND c.first_work_rn IS NOT NULL AND c.rn_time<=c.first_work_rn THEN 'HW'
            WHEN c.tour_end_ptype=1 AND c.last_work_rn IS NOT NULL AND c.rn_time>c.last_work_rn THEN 'WH'
            WHEN c.start_ptype=1 AND c.end_ptype=2 THEN 'HW'
            WHEN c.start_ptype=2 AND c.end_ptype=1 THEN 'WH'
            ELSE 'none' END chain_dir
FROM (
SELECT b.uid, b.`date`, b.tour_id, b.rn_time, b.mode, b.start_ptype, b.end_ptype, b.end_poi, b.sod, b.t_start, b.t_end,
       b.invalid_movement, b.hg, b.wg, b.hpoi, b.wpoi,
       b.first_work_rn, b.last_work_rn, b.tour_start_ptype, b.tour_end_ptype, b.j_tour_first, b.j_tour_last,
       max(if(b.rn_time=b.first_work_rn, b.j600, NULL)) OVER (PARTITION BY b.uid, b.`date`, b.tour_id) j_first_work,
       max(if(b.rn_time=b.last_work_rn+1, b.j600, NULL)) OVER (PARTITION BY b.uid, b.`date`, b.tour_id) j_after_last_work
FROM (
SELECT a.uid, a.`date`, a.tour_id, a.rn_time, a.mode, a.start_ptype, a.end_ptype, a.end_poi, a.sod, a.t_start, a.t_end,
       a.invalid_movement, a.j600, a.hg, a.wg, a.hpoi, a.wpoi,
       min(if(a.end_ptype=2, a.rn_time, NULL)) OVER (PARTITION BY a.uid, a.`date`, a.tour_id) first_work_rn,
       max(if(a.end_ptype=2, a.rn_time, NULL)) OVER (PARTITION BY a.uid, a.`date`, a.tour_id) last_work_rn,
       first_value(a.start_ptype) OVER (PARTITION BY a.uid, a.`date`, a.tour_id ORDER BY a.rn_time
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) tour_start_ptype,
       last_value(a.end_ptype) OVER (PARTITION BY a.uid, a.`date`, a.tour_id ORDER BY a.rn_time
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) tour_end_ptype,
       first_value(a.j600) OVER (PARTITION BY a.uid, a.`date`, a.tour_id ORDER BY a.rn_time
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) j_tour_first,
       last_value(a.j600) OVER (PARTITION BY a.uid, a.`date`, a.tour_id ORDER BY a.rn_time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) j_tour_last
FROM (
SELECT g.uid, g.`date`, g.rn_time, g.mode, g.start_ptype, g.end_ptype, g.end_poi, g.sod, g.t_start, g.t_end,
       g.invalid_movement, g.hg, g.wg, g.hpoi, g.wpoi,
       coalesce(sum(if(g.end_ptype=1,1,0)) OVER (PARTITION BY g.uid, g.`date` ORDER BY g.rn_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)+1 tour_id,
       coalesce(sum(g.end_j) OVER (PARTITION BY g.uid, g.`date` ORDER BY g.rn_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)+1 j600
FROM (
SELECT f.uid, f.`date`, f.rn_time, f.mode, f.start_ptype, f.end_ptype, f.end_poi, f.t_start, f.t_end,
       f.hg, f.wg, f.hpoi, f.wpoi,
       cast(substr(f.hms,1,2) AS int)*3600+cast(substr(f.hms,4,2) AS int)*60+cast(substr(f.hms,7,2) AS int) sod,
       if(f.key_conflict=0 AND f.dur_s>0 AND f.distance_m>0 AND 3.6*f.distance_m/f.dur_s<=200,0,1) invalid_movement,
       if(f.rn_time=f.n_trip_day OR f.end_ptype IS NULL OR f.end_ptype IN (1,2) OR f.next_t_start-f.t_end>=600,1,0) end_j
FROM (
SELECT t.uid, t.`date`, t.move_id, t.t_start, t.t_end, t.mode, t.start_ptype, t.end_ptype, t.end_poi, t.hms,
       t.distance_m, t.dur_s, t.key_conflict, t.hg, t.wg, t.hpoi, t.wpoi,
       row_number() OVER (PARTITION BY t.uid, t.`date` ORDER BY t.t_start, t.t_end, t.move_id) rn_time,
       count(1) OVER (PARTITION BY t.uid, t.`date`) n_trip_day,
       lead(t.t_start) OVER (PARTITION BY t.uid, t.`date` ORDER BY t.t_start, t.t_end, t.move_id) next_t_start
FROM (
SELECT m.uid, m.`date`, m.move_id, m.key_conflict, m.t_start, m.t_end, m.mode, m.start_ptype, m.end_ptype, m.end_poi,
       substr(m.stime_str,12,8) hms,
       m.distance_m, m.t_end-m.t_start dur_s,
       p.home_grid_id hg, p.work_grid_id wg, p.home_poi hpoi, p.work_poi wpoi
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
       min(unix_timestamp(cast(stime AS string))) t_start,
       min(cast(stime AS string)) stime_str,
       min(unix_timestamp(cast(etime AS string))) t_end,
       min(cast(mode AS int)) mode,
       min(cast(start_ptype AS int)) start_ptype,
       min(cast(end_ptype AS int)) end_ptype,
       min(cast(end_poi AS string)) end_poi,
       min(cast(distance AS double)) distance_m
FROM move_month
WHERE city='V0110000'
  AND `date` IN (20250506,20250507,20250508,20250509,20250512,20250513,20250514,20250515,20250516,
                 20250519,20250520,20250521,20250522,20250523,20250526,20250527,20250528,20250529,20250530)
GROUP BY cast(uid AS string), `date`, cast(move_id AS string)
) m
INNER JOIN tl25m05_v01_person_internal p ON p.uid=m.uid
WHERE p.in_joint=1
) t
) f
) g
) a
) b
) c
) e
WHERE e.chain_dir<>'none'
GROUP BY e.uid, e.`date`, e.tour_id, e.chain_dir
) h
WHERE h.n_invalid=0
) y
LATERAL VIEW explode(array('all_day','dep_0610','dep_0709','onroad_0610','dep_1620','dep_1719','onroad_1620')) bb AS band
WHERE bb.band='all_day'
   OR (y.chain_dir='HW' AND bb.band='dep_0610' AND y.start_sod>=21600 AND y.start_sod<36000)
   OR (y.chain_dir='HW' AND bb.band='dep_0709' AND y.start_sod>=25200 AND y.start_sod<32400)
   OR (y.chain_dir='HW' AND bb.band='onroad_0610' AND y.start_sod<36000 AND y.start_sod+y.elapsed_s>21600)
   OR (y.chain_dir='WH' AND bb.band='dep_1620' AND y.start_sod>=57600 AND y.start_sod<72000)
   OR (y.chain_dir='WH' AND bb.band='dep_1719' AND y.start_sod>=61200 AND y.start_sod<68400)
   OR (y.chain_dir='WH' AND bb.band='onroad_1620' AND y.start_sod<72000 AND y.start_sod+y.elapsed_s>57600)
) z
GROUP BY z.chain_dir, z.band, z.chain_kind, z.mode_group,
         if(z.chain_dir='HW',coalesce(z.hg,'OUT'),coalesce(z.wg,'OUT')),
         if(z.chain_dir='HW',coalesce(z.wg,'OUT'),coalesce(z.hg,'OUT'))
) k
GROUP BY k.purpose, k.band, k.chain_kind, k.mode_group, if(k.n_people>=5,k.grid_o,'SUPP'), if(k.n_people>=5,k.grid_d,'SUPP');

SELECT purpose, band, chain_kind, mode_group,
       sum(n_cells) n_cells,
       sum(n_chains) n_chains,
       sum(if(grid_o<>'SUPP',n_chains,0)) n_chains_ge5,
       sum(if(grid_o<>'SUPP' AND grid_o<>'OUT' AND grid_d<>'OUT',n_chains,0)) n_chains_ge5_in_grid,
       sum(n_primary_end) n_primary_end,
       sum(n_le_90m) n_le_90m,
       sum(sum_legs) sum_legs,
       sum(if(grid_o<>'SUPP',1,0)) n_rows_export
FROM tl25m05_v01_chain_od_exp
GROUP BY purpose, band, chain_kind, mode_group;
