SELECT purpose, band, mode_group, grid_o, grid_d, n_trips, n_people, n_person_days, sum_dur_s, sum_distance_m
FROM tl25m05_v01_od_band_exp
WHERE grid_o<>'SUPP' AND mode_group='mode1' AND purpose='HW' AND band IN ('all_day');
