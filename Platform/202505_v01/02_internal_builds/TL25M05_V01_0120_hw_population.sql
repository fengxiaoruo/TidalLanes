DROP TABLE IF EXISTS tl25m05_v01_hw_od_internal;
CREATE TABLE tl25m05_v01_hw_od_internal AS
SELECT coalesce(home_grid_id,'OUT') home_grid_id,
       coalesce(work_grid_id,'OUT') work_grid_id,
       max(if(home_grid_id IS NOT NULL AND home_cand_in_gate5=1,1,0)) home_in_gate5,
       max(if(work_grid_id IS NOT NULL AND work_cand_in_gate5=1,1,0)) work_in_gate5,
       count(1) n_people
FROM tl25m05_v01_person_internal
WHERE in_joint=1
GROUP BY coalesce(home_grid_id,'OUT'), coalesce(work_grid_id,'OUT');

SELECT count(1) n_pairs,
       sum(n_people) n_people,
       sum(if(n_people>=5,1,0)) n_pairs_ge5,
       sum(if(n_people>=5,n_people,0)) n_people_in_ge5_pairs,
       sum(if(home_grid_id=work_grid_id AND home_grid_id<>'OUT',1,0)) n_same_cell_pairs,
       sum(if(home_grid_id=work_grid_id AND home_grid_id<>'OUT',n_people,0)) n_same_cell_people,
       sum(if(home_grid_id='OUT' OR work_grid_id='OUT',n_people,0)) n_people_outside_grid,
       sum(if(home_in_gate5=1 AND work_in_gate5=1,1,0)) n_gate5_pairs,
       sum(if(home_in_gate5=1 AND work_in_gate5=1,n_people,0)) n_gate5_people,
       sum(if(home_in_gate5=1 AND work_in_gate5=1 AND n_people>=5,1,0)) n_gate5_pairs_ge5,
       sum(if(home_in_gate5=1 AND work_in_gate5=1 AND n_people>=5,n_people,0)) n_gate5_people_in_ge5_pairs,
       max(n_people) max_pair_people
FROM tl25m05_v01_hw_od_internal;
