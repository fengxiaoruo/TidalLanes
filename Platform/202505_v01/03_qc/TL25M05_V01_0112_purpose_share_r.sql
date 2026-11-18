SELECT slot10, if(mode=1,'mode1','other') mode_group, purpose, in_r, in_joint,
       count(1) n_trips,
       count(DISTINCT uid) n_people,
       sum(if(primary_endpoints=1,1,0)) n_primary_endpoints
FROM tl25m05_v01_trip_internal
WHERE valid_strict=1
  AND `date` IN (20250506,20250507,20250508,20250509,20250512,20250513,20250514,20250515,20250516,
                 20250519,20250520,20250521,20250522,20250523,20250526,20250527,20250528,20250529,20250530)
GROUP BY slot10, if(mode=1,'mode1','other'), purpose, in_r, in_joint
HAVING count(DISTINCT uid)>=5;
