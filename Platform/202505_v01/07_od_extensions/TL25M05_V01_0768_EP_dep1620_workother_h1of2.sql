SELECT purpose, band, grid_o, grid_d, n_trips, n_people, n_trips_in_r, sum_dur_s, sum_distance_m
FROM tl25m05_v01_endpoint_od_exp
WHERE band='dep_1620' AND purpose IN ('work_other')
  AND grid_o NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT') AND grid_d NOT IN ('SUPP','NOPOI','POICONF','BADXY','OUT')
  AND pmod(hash(grid_o),2)=1;
