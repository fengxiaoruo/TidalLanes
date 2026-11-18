SELECT variant, purpose, grid_o, grid_d, mode_group, n_trips, n_people, n_person_days
FROM tl25m05_v01_x_dep_window_exp
WHERE variant='strict';
