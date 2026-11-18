# model_v1 补充与修改执行清单

用途：这是 `model_v1_review_and_data_gap_temp_20260422.md` 的执行版清单，供模型定义、代码入口和数据输入包对齐使用。完成后可删除，不作为长期 handoff 文档。

## 0. 当前执行原则

- 先对齐模型对象，再补数据，再估计；不要在 `t_{kl}` 定义未锁定前解释结构参数。
- 若目标是“严格遵照 AA”，模型内部的 `t_{kl}` 应是 iceberg transport factor，不应直接等同于分钟 travel time。
- 观测分钟 travel time 建议统一记为 `tau_*_min`，模型成本统一记为 `t_*_iceberg`。
- 当前可先保留 route assignment 的工程近似，但文档和输出中必须明确标成 approximation / diagnostic，不能写成已经完成的 AA route block。

## 1. 第一批必须补齐的输入包

目标目录建议：

- `data_work/outputs/model_v1_input_bundle_current/`

第一批先做两个 bundle，避免 centerline-grid 和 raw-grid 混在一起：

- `centerline_grid_v11/`：基于 `manual_centerline_rules_v11`，用于让现有 solver 先跑在当前锁定中心线版本上。
- `raw_edgeproj_v2/`：基于 `raw_connected_v1_grid_travel_time_edgeproj_v2`，用于后续把模型接到当前 raw-grid 主推进方向。

### 1.1 `centerline_grid_v11` 最小表

需要生成或复制以下文件：

- `nodes_square.parquet`
- `edges_square.parquet`
- `od_square.parquet`
- `grid_links_square_long.csv`
- `parameters_square.json`
- `data_quality_square.csv`

若继续支持 `hex` 和 `voronoi`，同名替换 grid type。

### 1.2 `raw_edgeproj_v2` 最小表

需要生成或复制以下文件：

- `raw_nodes.parquet`
- `raw_edges_AM.parquet`
- `raw_edges_PM.parquet`
- `raw_edges_FF.parquet`
- `grid_connectors_square.parquet`
- `adjacent_grid_cost_square_AM.parquet`
- `adjacent_grid_cost_square_PM.parquet`
- `raw_edge_to_lane_capacity.parquet`
- `parameters_square.json`
- `data_quality_square.csv`

当前 `raw_edgeproj_v2` 仍有若干输出文件名保留 `kunpeng_*` 前缀。执行层面可以先读取这些现有文件，但新 bundle 内部统一另存为 `raw_connected_v1_*` 或不带旧前缀的中性名称。

## 2. 字段级补充清单

### 2.1 nodes 表

必需字段：

- `node_id`
- `grid_id`
- `grid_type`
- `x_m`
- `y_m`
- `residents`
- `jobs`
- `component`
- `version_id`

建议字段：

- `job_resident_ratio`
- `node_source`
- `keep_for_model`
- `drop_reason`

完成判据：

- `node_id` 唯一且无缺失。
- `residents` 和 `jobs` 为非负数值。
- 每个 OD 中出现的 origin/destination 都能在 nodes 表找到。

### 2.2 edges 表

必需字段：

- `edge_id`
- `from_node`
- `to_node`
- `length_m`
- `tau_obs_min`
- `tau_ff_min`
- `t_obs_iceberg`
- `t_ff_iceberg`
- `lanes_total`
- `lanes_directional`
- `capacity_proxy`
- `road_class`
- `period`
- `version_id`

建议字段：

- `roadseg_id`
- `cline_id`
- `skel_dir`
- `direction`
- `speed_obs_kmh`
- `speed_ff_kmh`
- `n_obs_speed`
- `lane_quality_flag`
- `capacity_quality_flag`
- `tau_ff_source`
- `tau_obs_source`
- `geometry_source`
- `is_connector`
- `connector_dist_m`
- `connector_time_min`

完成判据：

- `tau_obs_min > 0`。
- `tau_ff_min > 0`。
- `tau_obs_min >= tau_ff_min` 的违反比例需要单独报告；若存在违反，要有 `tau_consistency_flag`。
- `t_obs_iceberg >= 1` 且 `t_ff_iceberg >= 1`。
- 不允许静默使用 `tau_ff_min = tau_obs_min`；如需回填，必须有 `tau_ff_imputed_flag = 1` 和 `tau_ff_source = imputed_from_obs`。

### 2.3 OD 表

必需字段：

- `origin_node`
- `destination_node`
- `origin_grid`
- `destination_grid`
- `commuters_total`
- `commuters_road`
- `commuters_car`
- `grid_type`
- `sample_definition`
- `version_id`

建议字段：

- `type_walk`
- `type_bike`
- `type_sub`
- `type_bus`
- `type_car`
- `identified`
- `identified_share_of_pop`
- `reachable_flag`
- `od_source`
- `road_mode_rule`

完成判据：

- `commuters_road` 的构造规则写入 `parameters_*.json`。
- 第一版建议同时保存两套 road mass：
  - `commuters_car_only = type_car`
  - `commuters_car_bus = type_car + type_bus`
- 正式 baseline 先用哪一套必须在 `parameters_*.json` 中锁定。

### 2.4 lane / capacity crosswalk

必需字段：

- `model_edge_id`
- `roadseg_id`
- `cline_id`
- `skel_dir`
- `lanes_total`
- `lanes_directional`
- `capacity_proxy`
- `lane_source`
- `lane_quality_flag`
- `matched_length_m`
- `match_share`

建议字段：

- `opposite_model_edge_id`
- `opposite_dir_lane_ratio`
- `lane_symmetry_flag`
- `tidal_candidate_flag`
- `policy_feasible_flag`
- `policy_feasible_reason`

完成判据：

- 每条 model edge 都有 lane/capacity 状态：
  - observed
  - imputed
  - missing
- 缺失比例按 length-weighted 和 count-weighted 两种口径都输出。
- 潮汐车道 counterfactual 不能直接对 lane missing 的 edge 生效，除非明确标成 imputed scenario。

### 2.5 policy eligibility 表

必需字段：

- `model_edge_id`
- `opposite_model_edge_id`
- `eligible_tidal_lane`
- `policy_period`
- `add_lane_to_edge`
- `remove_lane_from_opposite`
- `min_lanes_after_removal`
- `eligibility_source`

建议字段：

- `asymmetry_ratio_AM`
- `asymmetry_ratio_PM`
- `faster_dir_AM`
- `faster_dir_PM`
- `reversal_flag`
- `geometry_feasible_flag`
- `manual_review_flag`

完成判据：

- 对每一个被政策冲击的 edge，都能找到反向 edge。
- 反向减 lane 后 `lanes_directional >= min_lanes_after_removal`。
- 若没有 geometry feasibility 数据，必须显式写成 `geometry_feasible_flag = unknown`，不能默认可行。

## 3. 代码修改清单

### 3.1 `stage08_build_qsm_inputs.py`

路径：

- `data_work/src/stages/stage08_build_qsm_inputs.py`

修改动作：

- 在输出 `qsm_input_edges_*` 时加入 `tau_obs_min` 和 `tau_ff_min`，不要只保留 `t_min`。
- 在输出时加入 `tau_ff_source`、`tau_ff_imputed_flag`、`lane_quality_flag`。
- 将 `travel_time_definition` 拆成：
  - `tau_obs_definition`
  - `tau_ff_definition`
  - `iceberg_transform_definition`
- 输出 `data_quality_*_qsm.csv`，至少包括：
  - edge count
  - missing `tau_ff_min` share
  - imputed `tau_ff_min` share
  - missing lane share
  - `tau_obs_min < tau_ff_min` share

第一条可执行命令：

```bash
python3 data_work/src/stages/run_full_pipeline.py --config data_work/config/manual_centerline_plan1.json --version-id manual_centerline_rules_v11 --output-dir data_work/outputs --from-stage stage05 --to-stage stage08 --grid-type square
```

注意：当前 `manual_centerline_rules_v11/data` 里尚未看到 `grid_nodes_*`、`t_edges_*`、`OD_*` 和 `qsm_input_*`，所以需要先让 `stage05-08` 在 v11 上跑通，再谈 model baseline。

### 3.2 `spatial_equilibrium.py`

路径：

- `data_work/src/model/spatial_equilibrium.py`

修改动作：

- `ModelInputs` 中新增：
  - `edge_tau_obs_min`
  - `edge_tau_ff_min`
  - `edge_t_obs_iceberg`
  - `edge_t_ff_iceberg`
  - `edge_tau_ff_imputed_flag`
- `load_model_inputs()` 中新增显式转换：
  - `t_obs_iceberg = exp(delta0 * tau_obs_min)`
  - `t_ff_iceberg = exp(delta0 * tau_ff_min)`
- 若 `tau_ff_min` 缺失，不再静默回填；先打 flag，再输出 warning / diagnostics。
- `compute_soft_shortest_path_assignment()` 的输入改为 iceberg cost 或显式标注输入是 `tau` 还是 `t`。
- 拥堵更新改成 free-flow 有下界的形式，例如：
  - `tau_new = tau_ff_min * (1 + alpha * density ** lambda)`
  - 再转换成 `t_new_iceberg`
- `estimate_theta_two_way_fe()` 和 `estimate_lambda_cross_section()` 改名或在输出里标注为 diagnostic calibration。

完成判据：

- 模型内部不再出现含义混乱的 `edge_t_ff_min` 名称。
- 输出文件同时保存分钟对象和 iceberg 对象。
- baseline equilibrium summary 中单独报告：
  - convergence status
  - max error
  - iterations
  - share of imputed free-flow edges
  - share of missing/imputed lane edges

### 3.3 model runner

路径：

- `data_work/src/model/run_spatial_equilibrium.py`
- `data_work/src/model/run_counterfactual_suite.py`

修改动作：

- 默认 `--version-id` 不再写死为 `raw_rebuild_validation`。
- 增加 `--input-bundle` 参数，指向明确的模型输入包。
- 输出目录命名包含：
  - graph version
  - grid type
  - tau/iceberg convention
  - route assignment version
- `calibration_summary.json` 中将 `theta_hat` / `lambda_hat` 改成：
  - `theta_diagnostic`
  - `lambda_diagnostic`
  - `theta_used`
  - `lambda_used`

完成判据：

- 跑出来的结果目录名能直接看出模型用的是 `centerline_grid_v11` 还是 `raw_edgeproj_v2`。
- 任何 counterfactual 输出都能回溯到对应 input bundle 和 parameters json。

## 4. 数据构造顺序

### Step A. 先补 `centerline_grid_v11`

目的：让现有 solver 先跑在当前锁定中心线版本上。

执行：

- 跑 `manual_centerline_rules_v11` 的 `stage05-08`。
- 检查 `qsm_input_edges_square.parquet` 是否包含 `tau_ff_min`。
- 若没有，则修改 `stage08_build_qsm_inputs.py` 后重跑。
- 用 `centerline_speed_master.parquet` 的 late-night / free-flow 口径补 `tau_ff_min`。
- 用 `centerline_lane_master.parquet` 补 lane/capacity。

产出：

- `data_work/outputs/model_v1_input_bundle_current/centerline_grid_v11/`

### Step B. 再补 `raw_edgeproj_v2`

目的：把模型接到当前 raw-grid 主推进方向。

执行：

- 从 `raw_connected_v1_grid_travel_time_edgeproj_v2/data/` 读取当前 raw edge、connector、adjacent cost。
- 将旧前缀文件另存到新 bundle 中，字段名统一改为中性名称。
- 用 `kunpeng_speed_ff_by_roadseg.csv` 生成 `tau_ff_min`。
- 用 AM/PM raw edge 文件生成 `tau_obs_min`。
- 为 raw edge 构造 lane/capacity crosswalk；若暂时只能从 centerline lane 间接映射，必须输出 `lane_source = centerline_proxy`。

产出：

- `data_work/outputs/model_v1_input_bundle_current/raw_edgeproj_v2/`

### Step C. 再跑最小 baseline

目的：先测试模型可跑、收敛、字段完整，不先解释 welfare。

执行：

- 跑 square grid。
- 固定外生 `theta_used` 和 `lambda_used`。
- 暂不把 diagnostic regression 当作正式估计。
- 输出 convergence 和 data quality summary。

完成判据：

- `converged = True` 或清楚记录失败原因。
- 无静默 free-flow 回填。
- 无无法解释的 negative / zero cost。
- counterfactual 前 baseline diagnostics 先过关。

## 5. 最小数据质量报告

每个 bundle 至少输出一张 `data_quality_square.csv`，字段包括：

- `n_nodes`
- `n_edges`
- `n_od_pairs`
- `od_total_commuters`
- `od_total_road_commuters`
- `edge_missing_tau_obs_share`
- `edge_missing_tau_ff_share`
- `edge_tau_ff_imputed_share`
- `edge_tau_obs_lt_tau_ff_share`
- `edge_missing_lane_share_count`
- `edge_missing_lane_share_length`
- `edge_missing_capacity_share_count`
- `edge_missing_capacity_share_length`
- `od_nodes_not_in_graph_count`
- `largest_component_node_share`
- `reachable_od_pair_share`

这个报告是后续是否可以进入估计的 gate。若这些指标没过，不建议继续解释模型结果。

## 6. 暂时不要做的事

- 不要直接用 `raw_rebuild_validation` 的旧 model output 写正式结论。
- 不要把 `lambda_diagnostic` 写成结构估计。
- 不要在 `tau_ff_min` 缺失时静默令其等于 `tau_obs_min`。
- 不要把 car / bus / total commuters 混成一个未说明的 OD mass。
- 不要在 raw-grid connector long-tail 还未诊断完时，把 raw-grid 版本当作最终 welfare baseline。

## 7. 推荐的最近三步

1. 先修改 `stage08_build_qsm_inputs.py`，让 v11 的 QSM inputs 含有 `tau_obs_min`、`tau_ff_min`、lane/capacity quality flags 和 data-quality report。
2. 跑 `manual_centerline_rules_v11` 的 `stage05-08`，生成 `centerline_grid_v11` input bundle。
3. 修改 `spatial_equilibrium.py` 的 cost loader，把分钟 travel time 与 AA iceberg cost 分开，再跑一个只看收敛和数据质量的 square-grid baseline。
