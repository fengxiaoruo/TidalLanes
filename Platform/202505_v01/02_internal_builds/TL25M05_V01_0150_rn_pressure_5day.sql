DROP TABLE IF EXISTS tl25m05_v01_rn_p_slot10_internal;
CREATE TABLE tl25m05_v01_rn_p_slot10_internal AS
SELECT x.variant, x.purpose, x.pass_slot10, x.rn_pair_id,
       count(1) n_pass,
       count(DISTINCT concat(x.uid,'_',cast(x.`date` AS string),'_',x.move_id)) n_trips,
       count(DISTINCT x.uid) n_people
FROM (
SELECT r.uid, r.`date`, r.move_id, r.purpose, r.pass_slot10, r.rn_pair_id, v.variant
FROM tl25m05_v01_rn_pass_internal r
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE (v.variant='strict' AND r.valid_strict=1) OR (v.variant='relaxed' AND r.valid_relaxed=1)
) x
GROUP BY x.variant, x.purpose, x.pass_slot10, x.rn_pair_id;

DROP TABLE IF EXISTS tl25m05_v01_rn_p_slot30_internal;
CREATE TABLE tl25m05_v01_rn_p_slot30_internal AS
SELECT x.variant, x.purpose, cast(floor(x.pass_slot10/3) AS int) pass_slot30, x.rn_pair_id,
       count(1) n_pass,
       count(DISTINCT concat(x.uid,'_',cast(x.`date` AS string),'_',x.move_id)) n_trips,
       count(DISTINCT x.uid) n_people
FROM (
SELECT r.uid, r.`date`, r.move_id, r.purpose, r.pass_slot10, r.rn_pair_id, v.variant
FROM tl25m05_v01_rn_pass_internal r
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE (v.variant='strict' AND r.valid_strict=1) OR (v.variant='relaxed' AND r.valid_relaxed=1)
) x
GROUP BY x.variant, x.purpose, cast(floor(x.pass_slot10/3) AS int), x.rn_pair_id;

DROP TABLE IF EXISTS tl25m05_v01_rn_p_window_internal;
CREATE TABLE tl25m05_v01_rn_p_window_internal AS
SELECT x.variant, x.purpose, x.rn_pair_id,
       count(1) n_pass,
       count(DISTINCT concat(x.uid,'_',cast(x.`date` AS string),'_',x.move_id)) n_trips,
       count(DISTINCT x.uid) n_people
FROM (
SELECT r.uid, r.`date`, r.move_id, r.purpose, r.rn_pair_id, v.variant
FROM tl25m05_v01_rn_pass_internal r
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE (v.variant='strict' AND r.valid_strict=1) OR (v.variant='relaxed' AND r.valid_relaxed=1)
) x
GROUP BY x.variant, x.purpose, x.rn_pair_id;

DROP TABLE IF EXISTS tl25m05_v01_rn_p_date_window_internal;
CREATE TABLE tl25m05_v01_rn_p_date_window_internal AS
SELECT x.`date`, x.variant, x.purpose, x.rn_pair_id,
       count(1) n_pass,
       count(DISTINCT concat(x.uid,'_',x.move_id)) n_trips,
       count(DISTINCT x.uid) n_people
FROM (
SELECT r.uid, r.`date`, r.move_id, r.purpose, r.rn_pair_id, v.variant
FROM tl25m05_v01_rn_pass_internal r
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE (v.variant='strict' AND r.valid_strict=1) OR (v.variant='relaxed' AND r.valid_relaxed=1)
) x
GROUP BY x.`date`, x.variant, x.purpose, x.rn_pair_id;

DROP TABLE IF EXISTS tl25m05_v01_rn_od_cover_internal;
CREATE TABLE tl25m05_v01_rn_od_cover_internal AS
SELECT x.variant, x.purpose, x.grid_o, x.grid_d,
       count(1) n_trips,
       sum(if(l.move_id IS NULL,0,1)) n_linked_trips,
       count(DISTINCT x.uid) n_people
FROM (
SELECT k.uid, k.`date`, k.move_id, k.purpose, k.grid_o, k.grid_d, v.variant
FROM tl25m05_v01_rn_trip_internal k
LATERAL VIEW explode(array('strict','relaxed')) v AS variant
WHERE (v.variant='strict' AND k.valid_strict=1) OR (v.variant='relaxed' AND k.valid_relaxed=1)
) x
LEFT JOIN (
SELECT DISTINCT uid, `date`, move_id
FROM tl25m05_v01_rn_pass_internal
) l ON l.uid=x.uid AND l.`date`=x.`date` AND l.move_id=x.move_id
GROUP BY x.variant, x.purpose, x.grid_o, x.grid_d;

SELECT 'slot10' scheme, variant, count(1) n_rows, sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_pass) n_pass, sum(if(n_people>=5,n_pass,0)) n_pass_ge5, count(DISTINCT rn_pair_id) n_pairs, count(DISTINCT if(n_people>=5,rn_pair_id,NULL)) n_pairs_ge5
FROM tl25m05_v01_rn_p_slot10_internal
GROUP BY variant
UNION ALL
SELECT 'slot30' scheme, variant, count(1) n_rows, sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_pass) n_pass, sum(if(n_people>=5,n_pass,0)) n_pass_ge5, count(DISTINCT rn_pair_id) n_pairs, count(DISTINCT if(n_people>=5,rn_pair_id,NULL)) n_pairs_ge5
FROM tl25m05_v01_rn_p_slot30_internal
GROUP BY variant
UNION ALL
SELECT 'window' scheme, variant, count(1) n_rows, sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_pass) n_pass, sum(if(n_people>=5,n_pass,0)) n_pass_ge5, count(DISTINCT rn_pair_id) n_pairs, count(DISTINCT if(n_people>=5,rn_pair_id,NULL)) n_pairs_ge5
FROM tl25m05_v01_rn_p_window_internal
GROUP BY variant
UNION ALL
SELECT 'date_window' scheme, variant, count(1) n_rows, sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_pass) n_pass, sum(if(n_people>=5,n_pass,0)) n_pass_ge5, count(DISTINCT rn_pair_id) n_pairs, count(DISTINCT if(n_people>=5,rn_pair_id,NULL)) n_pairs_ge5
FROM tl25m05_v01_rn_p_date_window_internal
GROUP BY variant
UNION ALL
SELECT 'od_cover' scheme, variant, count(1) n_rows, sum(if(n_people>=5,1,0)) n_rows_ge5,
       sum(n_trips) n_pass, sum(if(n_people>=5,n_linked_trips,0)) n_pass_ge5, sum(n_linked_trips) n_pairs, 0 n_pairs_ge5
FROM tl25m05_v01_rn_od_cover_internal
GROUP BY variant;
