SELECT sum(if(fast=1,1,0)) n_fast,
       sum(if(fast=1 AND dur_s<60,1,0)) n_fast_dur_lt60s,
       sum(if(fast=1 AND dur_s>=60 AND dur_s<300,1,0)) n_fast_dur_60_300s,
       sum(if(fast=1 AND dur_s>=300 AND dur_s<1800,1,0)) n_fast_dur_5_30min,
       sum(if(fast=1 AND dur_s>=1800,1,0)) n_fast_dur_ge30min,
       sum(if(fast=1 AND distance_m<1000,1,0)) n_fast_dist_lt1km,
       sum(if(fast=1 AND distance_m>=1000 AND distance_m<5000,1,0)) n_fast_dist_1_5km,
       sum(if(fast=1 AND distance_m>=5000 AND distance_m<20000,1,0)) n_fast_dist_5_20km,
       sum(if(fast=1 AND distance_m>=20000 AND distance_m<100000,1,0)) n_fast_dist_20_100km,
       sum(if(fast=1 AND distance_m>=100000,1,0)) n_fast_dist_ge100km,
       sum(if(fast=1 AND mode=1,1,0)) n_fast_mode1,
       sum(if(fast=1 AND mode=0,1,0)) n_fast_mode0,
       sum(if(fast=1 AND mode>=1000,1,0)) n_fast_mode1000plus,
       sum(if(fast=1 AND purpose IN ('HW','WH'),1,0)) n_fast_hwwh,
       sum(if(fast=1 AND in_joint=1 AND purpose IN ('HW','WH'),1,0)) n_fast_joint_hwwh,
       sum(if(fast=1 AND start_native_grid=end_native_grid,1,0)) n_fast_same_native_grid,
       sum(if(fast=1 AND native_grid_bad=1,1,0)) n_fast_native_grid_bad,
       percentile_approx(if(fast=1,dur_s,NULL),0.5) p50_fast_dur_s,
       percentile_approx(if(fast=1,distance_m,NULL),0.5) p50_fast_dist_m,
       percentile_approx(if(fast=0 AND dur_s>0 AND distance_m>0,3.6*distance_m/dur_s,NULL),0.5) p50_ok_speed_kmh,
       percentile_approx(if(fast=0 AND dur_s>0 AND distance_m>0,3.6*distance_m/dur_s,NULL),0.99) p99_ok_speed_kmh
FROM (
SELECT dur_s, distance_m, mode, purpose, in_joint, start_native_grid, end_native_grid, native_grid_bad,
       if(dur_s>0 AND distance_m>0 AND 3.6*distance_m/dur_s>200,1,0) fast
FROM tl25m05_v01_trip_p0519_internal
) t;
