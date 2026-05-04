# MODEL_replication_tidallanes.tex audit, 2026-04-29

## 核心结论

`Documents/L1_manuscripts/MODEL_replication_tidallanes.tex` 的 Spec C 数值结果大体能和当前 `Replication_apply/results/tidal_lane/` 输出对上，但模型口径和数据输入版本说明需要修正。最重要的风险是：正文把 `sparse_adjmat_seattle.csv` 解释成 iceberg commuting cost 或 `tau_ij`，而当前 Python port 实际把第三列读成 `Xi_ij` / baseline traffic input。这个差异会影响读者对公式、输入和 counterfactual shock 的理解。

## 当前实际输入版本

- 当前 Spec C 脚本：`Replication_apply/python_commuting/tidal_lane_specC.py`
- 当前 Spec C 数据源：`data_work/outputs/manual_centerline_rules_v11/data/qsm_input_edges_square.parquet`
- 当前 AA baseline input bundle：`Replication_apply/aa_input_square_v2/`
- 当前同步给原始相对路径的 bundle：`Replication_apply/counterfactuals/seattle/`
- 当前节点映射：`Replication_apply/prepare_from_data_work_square_v2/node_index_mapping_square.csv`
- 当前结果：`Replication_apply/results/tidal_lane/selected_pairs_specC.csv` 和 `chi_lr_lf_specC.csv`

`aa_input_square_v2` 和 `counterfactuals/seattle` 的三份 CSV 行数一致：

- `node_lr_lf_seattle.csv`: 2,729 rows
- `sparse_adjmat_seattle.csv`: 4,087 rows
- `sparse_commute_seattle.csv`: 377,708 rows

`v2` 的 edge traffic input 与 `v1` 不同。`v1` 的 `sparse_adjmat` 第三列总和为 2,837.10，`v2` 和 `counterfactuals/seattle` 为 24,375.11。当前 Spec C 使用的是 `v2`。

## data_work lineage

当前北京输入来自 `manual_centerline_rules_v11` 的 square grid QSM outputs：

- nodes: `qsm_input_nodes_square.parquet`, 2,736 rows, 2,729 active rows进入 AA input
- edges: `qsm_input_edges_square.parquet`, 4,632 rows across periods, AM rows进入 AA input和 Spec C selection
- OD: `qsm_input_od_square.parquet`, 377,708 rows, `sample_definition == reachable_AM`
- parameter file: `qsm_input_parameters_square.json`

关键口径：

- `version_id`: `manual_centerline_rules_v11`
- `grid_type`: `square`
- `travel_time_definition`: `grid_centroid_distance_over_v_harm_min`
- `free_flow_definition`: `grid_centroid_distance_over_v_ff_harm_min`
- `iceberg_transform_definition`: `not_applied_stage08_minutes_only`
- `road_mode_rule`: `car_only_baseline_with_car_bus_alternative_saved`

`aa_input_square_v2` 不是单纯用 `lanes_directional` 构造 traffic input。构造脚本优先使用：

1. `data_work/outputs/raw_connected_v1_grid_travel_time_edgeproj_v2/data/raw_connected_vs_old_adjacent_adjacent_square_AM.csv` 的 `lane_est_path_raw_edgeproj_len_weighted`
2. 同一 CSV 的 `lane_est_path_raw_edgeproj_bottleneck`
3. `qsm_input_edges_square.parquet` 的 `lanes_directional`
4. fallback `2.0`

因此正文若说 v2 的 lane count 只来自 `lanes_directional`，不够准确。

## TeX 中需要修正的关键点

1. `sparse_adjmat_seattle.csv` 的含义写错。
   - TeX line 138--140 写成 `i, j, tau_ij` 和 iceberg commuting cost。
   - 当前 `data_io.py` 直接把第三列读为 `xi_ij`。
   - 应改为：`i, j, Xi_ij`，表示 edge-level observed traffic / traffic proxy input，而不是 `tau_ij` 或 `\bar{t}_{ij}`。

2. `sparse_commute_seattle.csv` 的含义需要更精确。
   - TeX line 144--146 写成 OD commute shares `pi_ij`。
   - 当前 `data_io.py` 把第三列读成 `l_ij` dense flow matrix；`prepare_from_data_work_square_v2` 写入的是 `commuters_road` levels，不是 row-normalized shares。

3. BPR inversion 的位置需要重写。
   - TeX line 151--159 说 `data_io.py` 由 commute matrix 和 adjacency costs recover flows。
   - 实际上 BPR inversion 发生在 input builder 和 Spec C shock construction；`data_io.py` 只读取 CSV。
   - 正确表述：`prepare_from_data_work_square_v2/build_inputs_from_data_work_square_v2.py` 用 `lanes * (tau_obs/tau_ff)^(1/delta1)` 构造 `sparse_adjmat` 第三列；`tidal_lane_specC.py` 再用同一类 BPR inversion构造 treated pair 的 fixed-flow shock。

4. v2 vs v1 的说明过度概括。
   - TeX line 267--274 说 square grid 替代 Voronoi、OD shares 更新。
   - 但 `v1` 也是 square grid，且 node/OD 行数和 `v2` 一致。主要变化是 `sparse_adjmat` traffic proxy 从 first-pass capacity proxy 改成基于 lane estimates 和 BPR inversion 的 traffic input。

5. top-100 skipped 的原因写错。
   - TeX line 309--310 写成 most asymmetric pair `r=14.76`，100 selected 中 1 skipped due lane constraint。
   - 当前 `selected_pairs_specC.csv` 的 treated range 是 2.08--12.49，rank 1 的 `r=14.76` 没有进入结果。
   - 我复核了 rank 1 pair 的 lane constraint，fast direction lanes = 4，不是 lane constraint 问题。它被跳过是因为 raw nodes 3247 和 3295 不在 `node_index_mapping_square.csv`，也就是不在 AA input node set。

6. Spec C 的 lane source 口径不一致。
   - `tidal_lane_specC.py` 构造 treated shock 时直接读取 `qsm_input_edges_square.parquet` 的 `lanes_directional`。
   - `aa_input_square_v2` baseline traffic input 由 v2 builder 的 lane priority 构造，可能使用 edgeproj lane estimates。
   - 因此正文应明确：baseline `Xi_ij` 用 `aa_input_square_v2` 的 BPR traffic input；Spec C treated-pair travel-time shock 当前用 `qsm_input_edges_square.parquet` 的 `lanes_directional` 计算。若要完全一致，脚本需要把 edgeproj lane estimates merge 到 Spec C shock construction。

7. “solver handles route redistribution endogenously” 表述过强。
   - TeX line 354--356 写成 solver allows commuters to shift routes and locations。
   - 当前 counterfactual solver调整的是 spatial equilibrium中的 resident/workplace distribution 和 edge traffic hats，不是显式 road-network route assignment。更稳妥的说法是：solver re-solves the spatial commuting equilibrium over the supplied cost matrix, but does not impose Wardrop route choice on the physical road network。

8. `chi` welfare equation可能过度简化。
   - TeX line 77--84 的 `chi^{-1} = sum (u_i v_j / t_ij)^theta` 可以作为 intuition，但当前 implemented equations 包含 `l_R`、`l_F`、`lambda`、`alpha`、`beta` 和 `Xi_ij` terms。
   - 建议将该式标成 simplified intuition，或改为“welfare-related closure scalar”并引用 implemented hat system。

9. “inbound congested / outbound underutilised” 口径需要谨慎。
   - TeX line 286--287 把 AM peak 的方向解释成 inbound congested、outbound underutilised。
   - 当前 selection 只基于 pairwise travel-time asymmetry，不校验 CBD/inbound方向。除非另有空间方向分类，应改成“the slower AM direction gains capacity and the faster opposite direction loses capacity”。

10. LaTeX 编译无严重错误，但有 overfull hbox。
    - log 只显示 overfull table warnings，未见 missing figure 或 undefined reference。
    - 长表格可后续用 `tabularx` 或缩小列宽处理。

## 当前 Spec C 数值核对

来自 `Replication_apply/results/tidal_lane/selected_pairs_specC.csv` 和 `chi_lr_lf_specC.csv`：

- treated pairs: 99
- selected rank range: 2--100
- selected asymmetry ratio range: 2.0824--12.4872
- `tbar_slow`: 0.8205--0.9247, mean 0.8624
- `tbar_fast`: 1.0931--1.7543, mean 1.2664
- average slow-direction travel-time change: -13.759%
- average fast-direction travel-time change: +26.641%
- `chi_hat`: 0.9996313829
- `(chi_hat - 1) * 1e6`: -368.6171
- `l_R` max gain: `sq_36_72`, +50.250%
- `l_R` max loss: `sq_14_10`, -27.588%
- `l_F` max gain: `sq_36_65`, +73.226%
- `l_F` max loss: `sq_53_46`, -21.841%

这些数值和 TeX 的 main result table 基本一致。需要改的是 asymmetry range、skipped原因、以及输入版本和公式口径。

## 建议正文改法

优先只修 Section 1--3，暂不扩写 Open Questions：

1. 将 input table 改为 `Xi_ij` / `L_ij` levels，而不是 `tau_ij` / `pi_ij` shares。
2. 在 Section 2 加一个 “Current Beijing input bundle” 小段，明确当前用 `aa_input_square_v2`，并说明它覆盖了 `counterfactuals/seattle`。
3. 重写 v1/v2 段落：v1 是 first-pass capacity proxy；v2 是 lane-priority + BPR inversion traffic input。
4. 修正 treated pair selection：candidate top-100 after length filter，rank 1 dropped because not in AA node mapping，final treated ranks 2--100。
5. 把 solver redistribution 表述从 route choice 改成 spatial commuting equilibrium adjustment。
6. 把 Spec C shock lane source和 baseline traffic lane source分开说明，或者修改脚本使二者使用同一 lane priority。

