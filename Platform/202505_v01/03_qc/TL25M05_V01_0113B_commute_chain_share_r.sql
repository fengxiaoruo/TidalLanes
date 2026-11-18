SELECT d.period, d.mode_group, d.purpose, d.leg_class, d.chain_kind,
       count(1) n_trips,
       count(DISTINCT d.uid) n_people
FROM (
SELECT c.uid,
       CASE WHEN c.hms>='07:00:00' AND c.hms<'09:00:00' THEN 'AM'
            WHEN c.hms>='17:00:00' AND c.hms<'19:00:00' THEN 'PM'
            ELSE 'OTHER' END period,
       if(c.mode=1,'mode1','other') mode_group,
       CASE WHEN c.start_ptype IS NULL OR c.end_ptype IS NULL OR c.start_ptype<0 OR c.end_ptype<0 THEN 'unknown'
            WHEN c.start_ptype=1 AND c.end_ptype=2 THEN 'HW'
            WHEN c.start_ptype=2 AND c.end_ptype=1 THEN 'WH'
            WHEN c.start_ptype=1 OR c.end_ptype=1 THEN 'home_other'
            WHEN c.start_ptype=2 OR c.end_ptype=2 THEN 'work_other'
            ELSE 'other' END purpose,
       CASE WHEN c.start_ptype=1 AND c.end_ptype=2 THEN 'hw_direct'
            WHEN c.start_ptype=2 AND c.end_ptype=1 THEN 'wh_direct'
            WHEN c.tour_start_ptype=1 AND c.first_work_rn IS NOT NULL AND c.rn_time<=c.first_work_rn THEN 'hw_chain'
            WHEN c.tour_end_ptype=1 AND c.last_work_rn IS NOT NULL AND c.rn_time>c.last_work_rn THEN 'wh_chain'
            WHEN c.first_work_rn IS NOT NULL AND c.rn_time>c.first_work_rn AND c.rn_time<=c.last_work_rn THEN 'work_between'
            ELSE 'non_commute' END leg_class,
       CASE WHEN c.start_ptype=1 AND c.end_ptype=2 THEN 'na'
            WHEN c.start_ptype=2 AND c.end_ptype=1 THEN 'na'
            WHEN c.tour_start_ptype=1 AND c.first_work_rn IS NOT NULL AND c.rn_time<=c.first_work_rn
            THEN if(c.j_first_work=c.j_tour_first,'transfer_only','activity_stop')
            WHEN c.tour_end_ptype=1 AND c.last_work_rn IS NOT NULL AND c.rn_time>c.last_work_rn
            THEN if(c.j_after_last_work=c.j_tour_last,'transfer_only','activity_stop')
            ELSE 'na' END chain_kind
FROM (
SELECT b.uid, b.rn_time, b.mode, b.start_ptype, b.end_ptype, b.hms, b.invalid_movement,
       b.first_work_rn, b.last_work_rn, b.tour_start_ptype, b.tour_end_ptype, b.j_tour_first, b.j_tour_last,
       max(if(b.rn_time=b.first_work_rn, b.j600, NULL)) OVER (PARTITION BY b.uid, b.`date`, b.tour_id) j_first_work,
       max(if(b.rn_time=b.last_work_rn+1, b.j600, NULL)) OVER (PARTITION BY b.uid, b.`date`, b.tour_id) j_after_last_work
FROM (
SELECT a.uid, a.`date`, a.tour_id, a.rn_time, a.mode, a.start_ptype, a.end_ptype, a.hms, a.invalid_movement, a.j600,
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
SELECT g.uid, g.`date`, g.rn_time, g.mode, g.start_ptype, g.end_ptype, g.invalid_movement, g.hms,
       coalesce(sum(if(g.end_ptype=1,1,0)) OVER (PARTITION BY g.uid, g.`date` ORDER BY g.rn_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)+1 tour_id,
       coalesce(sum(g.end_j) OVER (PARTITION BY g.uid, g.`date` ORDER BY g.rn_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)+1 j600
FROM (
SELECT f.uid, f.`date`, f.rn_time, f.mode, f.start_ptype, f.end_ptype,
       from_unixtime(f.t_start, 'HH:mm:ss') hms,
       if(f.key_conflict=0 AND f.dur_s>0 AND f.distance_m>0 AND 3.6*f.distance_m/f.dur_s<=200,0,1) invalid_movement,
       if(f.rn_time=f.n_trip_day OR f.end_ptype IS NULL OR f.end_ptype IN (1,2) OR f.next_t_start-f.t_end>=600,1,0) end_j
FROM (
SELECT t.uid, t.`date`, t.move_id, t.t_start, t.t_end, t.mode, t.start_ptype, t.end_ptype,
       t.distance_m, t.dur_s, t.key_conflict,
       row_number() OVER (PARTITION BY t.uid, t.`date` ORDER BY t.t_start, t.t_end, t.move_id) rn_time,
       count(1) OVER (PARTITION BY t.uid, t.`date`) n_trip_day,
       lead(t.t_start) OVER (PARTITION BY t.uid, t.`date` ORDER BY t.t_start, t.t_end, t.move_id) next_t_start
FROM (
SELECT m.uid, m.`date`, m.move_id, m.key_conflict, m.t_start, m.t_end, m.mode, m.start_ptype, m.end_ptype,
       m.distance_m, m.t_end-m.t_start dur_s
FROM (
SELECT cast(uid AS string) uid, `date`, cast(move_id AS string) move_id,
       if(max(cast(mode AS int))<>min(cast(mode AS int))
          OR max(cast(distance AS double))<>min(cast(distance AS double))
          OR max(cast(stime AS string))<>min(cast(stime AS string))
          OR max(cast(etime AS string))<>min(cast(etime AS string))
          OR max(cast(start_ptype AS int))<>min(cast(start_ptype AS int))
          OR max(cast(end_ptype AS int))<>min(cast(end_ptype AS int)),1,0) key_conflict,
       min(unix_timestamp(cast(stime AS string))) t_start,
       min(unix_timestamp(cast(etime AS string))) t_end,
       min(cast(mode AS int)) mode,
       min(cast(start_ptype AS int)) start_ptype,
       min(cast(end_ptype AS int)) end_ptype,
       min(cast(distance AS double)) distance_m
FROM move_month
WHERE city='V0110000'
  AND `date` IN (20250506,20250507,20250508,20250509,20250512,20250513,20250514,20250515,20250516,
                 20250519,20250520,20250521,20250522,20250523,20250526,20250527,20250528,20250529,20250530)
GROUP BY cast(uid AS string), `date`, cast(move_id AS string)
) m
INNER JOIN tl25m05_v01_person_internal p ON p.uid=m.uid
WHERE p.in_r=1
) t
) f
) g
) a
) b
) c
WHERE c.invalid_movement=0
) d
GROUP BY d.period, d.mode_group, d.purpose, d.leg_class, d.chain_kind
HAVING count(DISTINCT d.uid)>=5;
