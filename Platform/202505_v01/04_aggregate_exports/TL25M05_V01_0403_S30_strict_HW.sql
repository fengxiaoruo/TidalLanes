SELECT variant, purpose, slot30, grid_o, grid_d, mode_group, n_trips, n_people, n_person_days
FROM tl25m05_v01_x_dep_slot30_exp
WHERE variant='strict' AND purpose='HW';
