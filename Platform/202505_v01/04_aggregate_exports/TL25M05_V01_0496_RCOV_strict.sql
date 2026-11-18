SELECT variant, purpose, grid_o, grid_d, n_trips, n_linked_trips, n_people
FROM tl25m05_v01_rn_x_od_cover_exp
WHERE variant='strict';
