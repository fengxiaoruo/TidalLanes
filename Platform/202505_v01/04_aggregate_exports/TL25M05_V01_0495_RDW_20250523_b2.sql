SELECT `date`, purpose, rn_pair_id, n_pass, n_trips, n_people
FROM tl25m05_v01_rn_x_date_window_exp
WHERE `date`=20250523 AND part_bucket=2;
