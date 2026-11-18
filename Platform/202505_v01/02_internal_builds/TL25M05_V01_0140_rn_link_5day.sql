DROP TABLE IF EXISTS tl25m05_v01_rn_trip_internal;
CREATE TABLE tl25m05_v01_rn_trip_internal AS
SELECT t.uid, t.`date`, t.move_id, t.purpose, t.slot10, t.valid_strict, t.valid_relaxed,
       if(t.purpose='HW',coalesce(p.home_grid_id,'OUT'),coalesce(p.work_grid_id,'OUT')) grid_o,
       if(t.purpose='HW',coalesce(p.work_grid_id,'OUT'),coalesce(p.home_grid_id,'OUT')) grid_d
FROM tl25m05_v01_trip_internal t
JOIN tl25m05_v01_person_internal p ON p.uid=t.uid AND p.in_joint=1
WHERE t.`date` IN (20250519,20250520,20250521,20250522,20250523)
  AND t.in_joint=1 AND t.mode=1
  AND ((t.purpose='HW' AND t.period='AM') OR (t.purpose='WH' AND t.period='PM'))
  AND (t.valid_strict=1 OR t.valid_relaxed=1);

DROP TABLE IF EXISTS tl25m05_v01_rn_pass_internal;
CREATE TABLE tl25m05_v01_rn_pass_internal AS
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.valid_strict, k.valid_relaxed,
       a.rn_pair_id, a.passage_date,
       cast(from_unixtime(a.passage_unix_seconds,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.passage_unix_seconds,'mm') AS int)/10.0) AS int) pass_slot10
FROM tl25m05_v01_rn_trip_internal k
JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id, rn_pair_id, passage_unix_seconds, passage_date
FROM speed202505_pair_passage_day_20250519_all
WHERE city='V0110000' AND `date`=20250519 AND is_valid=1
) a ON a.uid=k.uid AND a.moi_id=k.move_id
WHERE k.`date`=20250519;

INSERT INTO TABLE tl25m05_v01_rn_pass_internal
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.valid_strict, k.valid_relaxed,
       a.rn_pair_id, a.passage_date,
       cast(from_unixtime(a.passage_unix_seconds,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.passage_unix_seconds,'mm') AS int)/10.0) AS int) pass_slot10
FROM tl25m05_v01_rn_trip_internal k
JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id, rn_pair_id, passage_unix_seconds, passage_date
FROM speed202505_pair_passage_week_src20250520
WHERE city='V0110000' AND `date`=20250520 AND is_valid=1
) a ON a.uid=k.uid AND a.moi_id=k.move_id
WHERE k.`date`=20250520;

INSERT INTO TABLE tl25m05_v01_rn_pass_internal
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.valid_strict, k.valid_relaxed,
       a.rn_pair_id, a.passage_date,
       cast(from_unixtime(a.passage_unix_seconds,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.passage_unix_seconds,'mm') AS int)/10.0) AS int) pass_slot10
FROM tl25m05_v01_rn_trip_internal k
JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id, rn_pair_id, passage_unix_seconds, passage_date
FROM speed202505_pair_passage_week_src20250521
WHERE city='V0110000' AND `date`=20250521 AND is_valid=1
) a ON a.uid=k.uid AND a.moi_id=k.move_id
WHERE k.`date`=20250521;

INSERT INTO TABLE tl25m05_v01_rn_pass_internal
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.valid_strict, k.valid_relaxed,
       a.rn_pair_id, a.passage_date,
       cast(from_unixtime(a.passage_unix_seconds,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.passage_unix_seconds,'mm') AS int)/10.0) AS int) pass_slot10
FROM tl25m05_v01_rn_trip_internal k
JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id, rn_pair_id, passage_unix_seconds, passage_date
FROM speed202505_pair_passage_week_src20250522
WHERE city='V0110000' AND `date`=20250522 AND is_valid=1
) a ON a.uid=k.uid AND a.moi_id=k.move_id
WHERE k.`date`=20250522;

INSERT INTO TABLE tl25m05_v01_rn_pass_internal
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.valid_strict, k.valid_relaxed,
       a.rn_pair_id, a.passage_date,
       cast(from_unixtime(a.passage_unix_seconds,'HH') AS int)*6
         +cast(floor(cast(from_unixtime(a.passage_unix_seconds,'mm') AS int)/10.0) AS int) pass_slot10
FROM tl25m05_v01_rn_trip_internal k
JOIN (
SELECT cast(uid AS string) uid, cast(moi_id AS string) moi_id, rn_pair_id, passage_unix_seconds, passage_date
FROM speed202505_pair_passage_week_src20250523
WHERE city='V0110000' AND `date`=20250523 AND is_valid=1
) a ON a.uid=k.uid AND a.moi_id=k.move_id
WHERE k.`date`=20250523;

SELECT k.`date`, k.purpose, k.n_trips, k.n_trips_strict, k.n_people,
       s.n_linked_trips, s.n_linked_trips_strict, s.n_linked_people, s.n_passages, s.n_passages_strict,
       s.n_rn_pairs, s.n_pass_other_date, s.min_pass_slot10, s.max_pass_slot10
FROM (
SELECT `date`, purpose, count(1) n_trips, sum(valid_strict) n_trips_strict, count(DISTINCT uid) n_people
FROM tl25m05_v01_rn_trip_internal
GROUP BY `date`, purpose
) k
LEFT JOIN (
SELECT `date`, purpose,
       count(DISTINCT concat(uid,'_',move_id)) n_linked_trips,
       count(DISTINCT if(valid_strict=1,concat(uid,'_',move_id),NULL)) n_linked_trips_strict,
       count(DISTINCT uid) n_linked_people,
       count(1) n_passages,
       sum(valid_strict) n_passages_strict,
       count(DISTINCT rn_pair_id) n_rn_pairs,
       sum(if(passage_date<>`date`,1,0)) n_pass_other_date,
       min(pass_slot10) min_pass_slot10,
       max(pass_slot10) max_pass_slot10
FROM tl25m05_v01_rn_pass_internal
GROUP BY `date`, purpose
) s ON s.`date`=k.`date` AND s.purpose=k.purpose;
