DROP TABLE IF EXISTS tl25m05_v01_rn_link_p0519_internal;
CREATE TABLE tl25m05_v01_rn_link_p0519_internal AS
SELECT t.uid, t.`date`, t.move_id, t.purpose, t.period, t.mode, t.slot10,
       t.valid_strict, t.valid_relaxed, t.degenerate_duration,
       if(c.move_id IS NULL,0,1) in_speed_clean,
       coalesce(c.n_rows,0) speed_clean_rows,
       coalesce(a.n_pass,0) n_pass,
       coalesce(a.n_valid_pass,0) n_valid_pass,
       a.first_pass_unix, a.last_pass_unix, a.min_passage_date, a.max_passage_date,
       cast(from_unixtime(a.first_pass_unix,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.first_pass_unix,'mm') AS int)/10.0) AS int) first_pass_slot10
FROM (
SELECT uid, `date`, move_id, purpose, period, mode, slot10, valid_strict, valid_relaxed, degenerate_duration
FROM tl25m05_v01_trip_internal
WHERE `date`=20250519 AND in_joint=1 AND purpose IN ('HW','WH')
) t
LEFT JOIN (
SELECT cast(uid AS string) uid, cast(move_id AS string) move_id, count(1) n_rows
FROM speed202505_clean_drive_trip
WHERE city='V0110000' AND `date`=20250519 AND mode=1 AND qc_basic_valid=1
GROUP BY cast(uid AS string), cast(move_id AS string)
) c ON c.uid=t.uid AND c.move_id=t.move_id
LEFT JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id,
       count(1) n_pass,
       sum(is_valid) n_valid_pass,
       min(if(is_valid=1,passage_unix_seconds,NULL)) first_pass_unix,
       max(if(is_valid=1,passage_unix_seconds,NULL)) last_pass_unix,
       min(passage_date) min_passage_date,
       max(passage_date) max_passage_date
FROM speed202505_pair_passage_day_20250519_all
WHERE city='V0110000' AND `date`=20250519
GROUP BY cast(uid AS string), cast(moi_id AS string)
) a ON a.uid=t.uid AND a.moi_id=t.move_id;

SELECT purpose, period, mode,
       count(1) n_trips,
       sum(valid_strict) n_strict,
       sum(valid_relaxed) n_relaxed,
       sum(in_speed_clean) n_in_speed_clean,
       sum(if(speed_clean_rows>1,1,0)) n_speed_clean_dup,
       sum(if(n_pass>0,1,0)) n_any_pass,
       sum(if(n_valid_pass>0,1,0)) n_valid_pass_trips,
       sum(if(valid_strict=1 AND n_valid_pass>0,1,0)) n_strict_linked,
       sum(if(valid_relaxed=1 AND n_valid_pass>0,1,0)) n_relaxed_linked,
       sum(if(valid_strict=1 AND in_speed_clean=1 AND n_valid_pass=0,1,0)) n_strict_clean_no_pass,
       sum(if(valid_strict=1 AND in_speed_clean=0,1,0)) n_strict_not_clean,
       sum(if(degenerate_duration=1 AND n_valid_pass>0,1,0)) n_degenerate_linked,
       sum(n_valid_pass) n_valid_passages,
       sum(if(n_valid_pass>0 AND min_passage_date<20250519,1,0)) n_pass_prev_day,
       sum(if(n_valid_pass>0 AND first_pass_slot10=slot10,1,0)) n_clock_same_slot,
       sum(if(n_valid_pass>0 AND abs(first_pass_slot10-slot10)<=1,1,0)) n_clock_within1,
       sum(if(n_valid_pass>0 AND abs(first_pass_slot10-slot10)<=3,1,0)) n_clock_within3,
       sum(if(n_valid_pass>0 AND abs(abs(first_pass_slot10-slot10)-48)<=1,1,0)) n_clock_off8h,
       sum(if(n_valid_pass>0,first_pass_slot10-slot10,0)) sum_clock_diff_slots,
       sum(if(n_valid_pass>0,last_pass_unix-first_pass_unix,0)) sum_pass_span_s
FROM tl25m05_v01_rn_link_p0519_internal
GROUP BY purpose, period, mode;
