# 主线输出清单（2026-11-18 冻结）

`data_work/outputs/` 不进 Git。下面这些目录是当前主线（1B、全日 OD＋07–09 用时、`ce_floor` β_c = 0.35）的输入、结果和支撑检查，备份时必须包含；其余输出目录可以从代码加输入重建，或属于历史版本。口径与命令的唯一权威表在 [`DOCUMENT_INDEX.md`](../../DOCUMENT_INDEX.md) 的“Current mainline”一节。

新增主线结果时，在对应分组里加一行；口径变了就整份换版，旧版改名为 `MAINLINE_MANIFEST_<日期>.md` 留存。

## A. 不可重建（与 raw_data 同等对待，先备份）

| 目录 | 内容 |
|---|---|
| `grid_pair_baidu_validation_v3` | 百度格对外部基准，5 天 × 3 时段，11-16 18:01 采集完成（`logs/week_master.log`）。重采需要重新调 API，且时间点无法复现。`backup_to_drive.sh` 不备份它；11-18 决定靠 Dropbox 共享文件夹同步保护（见 `BACKUP_WORKFLOW.md` 的 Known gaps） |

## B. 主线输入链（按依赖顺序）

| 目录 | 作用 | 构造脚本 |
|---|---|---|
| `model_input_edgeproj_v2_lcc_gate5_od2025_v2_odallday` | 2025-05 全日家→单位 OD 包 | 见 WORK_LOG 2026-11-11 |
| `osm_grid_travel_time_v2` | OSM 1A/1B 格对用时：早高峰 07–09 点，0519–0523 五个工作日；畅通 02–05 点。summary 无参数块，参数见同结果的 `osm_grid_travel_time_v2_recheck0709/summary.json` | `src/network/build_osm_grid_travel_time.py` |
| `model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709` | **主线输入包**：924 格、2,872 条边 | `build_osm_model_bundle.py --scheme osm1b --od-bundle model_input_edgeproj_v2_lcc_gate5_od2025_v2_odallday --tt-source osm_grid_travel_time_v2 --version-id model_input_osm1b_L300_gate5_od2025_v2_alldaytt0709`（在 `src/model/travel_cost_bundle/`）。2026-11-18 逐边核对：2,872 条边的早高峰与畅通用时与 `osm_grid_travel_time_v2` 完全一致（差为 0），包内 `tau_obs_source` 列也写的是它；`_recheck0709`、`_nb8` 在这些边上同样完全一致，其余 `v2_*` 变体都不一致。`_recheck0709` 的 summary 记录了参数（早高峰 07–09 点、0519–0523 五个工作日、L = 300 m），据此确认 v2 就是 07–09 用时 |
| `stage2_endogenous_osm1b_ce_floor_b025_fl001_ks_od2025_v1` | 主线只从这里的 `parameters.json` 取 α、β、θ。它们是文献值（θ = 6.83、β = −0.10 取 AA 2022 西雅图，α = 0.05 取文献中值），不是在北京数据上估计的，与 β_c 无关。该目录自身的输入是 `model_input_osm1b_L300_gate5_od2025_v2`（07–09 出发 OD），见 `baseline_summary.json` 的 `version_id`；其 `mainline_config.json` 里的 `travel_cost` 是写死的旧标签，不可信 | `src/model/full_estimation/run_stage2_gate5.py`（脚本默认值仍是旧主线 anchored λ = 1.1，必须显式传参） |

## C. 主线结果（β_c = 0.35）

| 目录 | 内容 |
|---|---|
| `tidal_lane_diagnostics_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` | 潮汐 top100 主结果：+0.627% / +0.397% |
| `policy_comparison_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` | 七项政策比较与排序 |
| `beta_c_rerun_b035_summary_v1` | 0.25 / 0.29 / 0.35 / 0.44 汇总、收敛与多起点检查、**运行命令** |
| `beta_c_b035_downstream_rerun_v1` | θ 敏感性、η = 0.21 标定、对称输入在 0.35 下的重跑 |
| `theta_first_second_order_osm1b_b035_alldaytt0709_v1` | θ 敏感性与一阶 / 二阶分解 |
| `selection_split_day_check_osm1b_ce_floor_b035_fl001_ks_od2025_v1` | 分日选边互换 |
| `policy_multistart_osm1b_ce_floor_b035_fl001_ks_od2025_alldaytt0709_v1` | 政策均衡多起点、收紧容差 |
| `symmetric_input_reestimated_osm1b_ce_floor_b035_fl001_ks_arithmetic_od2025_v1` | 对称输入（算术平均） |
| `symmetric_input_reestimated_osm1b_ce_floor_b035_fl001_ks_geometric_od2025_v1` | 对称输入（几何平均） |
| `tidal_lane_diagnostics_osm1b_ce_floor_b029_fl001_ks_od2025_alldaytt0709_v1` | 弹性区间下端 |
| `tidal_lane_diagnostics_osm1b_ce_floor_b044_fl001_ks_od2025_alldaytt0709_v1` | 弹性区间上端 |

## D. 弹性依据与待定口径的试算

| 目录 | 内容 |
|---|---|
| `congestion_floor_sensitivity_v1` | β_c 由 0.25 改为 0.35 的依据 |
| `background_traffic_trial_v1` | 背景车流进入拥堵函数的两种做法 |
| `load_dependent_elasticity_trial_v1` | 弹性随负荷变化；潮汐走廊轻方向车流过少 |
| `flow_density_per_lane_v1` | 流量–密度曲线 |
| `link_time_gamma_eta_scan_osm1b_alldaytt0709_v1`、`…_fine_v1` | OD 距离衰减 η 扫描 |

## E. β_c = 0.25 对照（旧值，保留以便对比）

| 目录 | 内容 |
|---|---|
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v3` | 0.25 下潮汐 top100：+0.441% / +0.313% |
| `policy_comparison_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1` | 0.25 下政策比较 |
| `pricing_additive_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_v1` | 0.25 下定价 |
| `tidal_lane_diagnostics_osm1b_ce_floor_b025_fl001_ks_od2025_alldaytt0709_eta0p21_v1` | η = 0.21 标定（0.25 下） |

同名 `_v2` 和部分 `_v1` 只有失败的 `run.log`，已在 WORK_LOG 标为作废，不在清单内。
