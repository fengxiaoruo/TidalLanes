SELECT purpose, band, chain_kind, mode_group, grid_o, grid_d, n_chains, n_people, sum_elapsed_s, sum_legs, n_primary_end, n_le_90m
FROM tl25m05_v01_chain_od_exp
WHERE grid_o<>'SUPP' AND purpose='WH' AND band='dep_1620';
