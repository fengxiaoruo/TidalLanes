SELECT purpose, rn_pair_id, n_pass, n_trips, n_people
FROM tl25m05_v01_rn_x_window_exp
WHERE part_bucket=3;
