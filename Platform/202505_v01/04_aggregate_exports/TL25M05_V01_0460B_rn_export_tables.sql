DROP TABLE IF EXISTS tl25m05_v01_rn_x_slot30_exp;
CREATE TABLE tl25m05_v01_rn_x_slot30_exp AS
SELECT purpose, pass_slot30, rn_pair_id, n_pass, n_trips, n_people, pmod(hash(rn_pair_id),16) part_bucket
FROM tl25m05_v01_rn_p_slot30_internal
WHERE variant='strict' AND n_people>=5;

DROP TABLE IF EXISTS tl25m05_v01_rn_x_window_exp;
CREATE TABLE tl25m05_v01_rn_x_window_exp AS
SELECT purpose, rn_pair_id, n_pass, n_trips, n_people, pmod(hash(rn_pair_id),4) part_bucket
FROM tl25m05_v01_rn_p_window_internal
WHERE variant='strict' AND n_people>=5;

DROP TABLE IF EXISTS tl25m05_v01_rn_x_date_window_exp;
CREATE TABLE tl25m05_v01_rn_x_date_window_exp AS
SELECT `date`, purpose, rn_pair_id, n_pass, n_trips, n_people, pmod(hash(rn_pair_id),3) part_bucket
FROM tl25m05_v01_rn_p_date_window_internal
WHERE variant='strict' AND n_people>=5;

DROP TABLE IF EXISTS tl25m05_v01_rn_x_od_cover_exp;
CREATE TABLE tl25m05_v01_rn_x_od_cover_exp AS
SELECT variant, purpose, grid_o, grid_d, n_trips,
       if(n_linked_trips>=5,n_linked_trips,NULL) n_linked_trips,
       n_people
FROM tl25m05_v01_rn_od_cover_internal
WHERE n_people>=5;

SELECT a.s30_rows, a.s30_pass, a.s30_max_part_rows,
       b.win_rows, b.win_pass, b.win_max_part_rows,
       c.dw_rows, c.dw_pass, c.dw_max_part_rows,
       d.cov_rows, d.cov_rows_strict, d.cov_trips_strict
FROM (
SELECT count(1) s30_rows, sum(n_pass) s30_pass, max(part_rows) s30_max_part_rows
FROM (
SELECT n_pass, count(1) OVER (PARTITION BY part_bucket) part_rows
FROM tl25m05_v01_rn_x_slot30_exp
) z
) a
CROSS JOIN (
SELECT count(1) win_rows, sum(n_pass) win_pass, max(part_rows) win_max_part_rows
FROM (
SELECT n_pass, count(1) OVER (PARTITION BY part_bucket) part_rows
FROM tl25m05_v01_rn_x_window_exp
) z
) b
CROSS JOIN (
SELECT count(1) dw_rows, sum(n_pass) dw_pass, max(part_rows) dw_max_part_rows
FROM (
SELECT n_pass, count(1) OVER (PARTITION BY part_bucket, `date`) part_rows
FROM tl25m05_v01_rn_x_date_window_exp
) z
) c
CROSS JOIN (
SELECT count(1) cov_rows, sum(if(variant='strict',1,0)) cov_rows_strict, sum(if(variant='strict',n_trips,0)) cov_trips_strict
FROM tl25m05_v01_rn_x_od_cover_exp
) d;
