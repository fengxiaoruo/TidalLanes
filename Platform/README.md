# TidalLanes 平台数据工作区

本目录服务于2025年通勤模型、分时拥堵与模型flow验证。组织方式参考SubwayMobility的现行NG批次：审计、空间输入、内部构造、QC、聚合导出、单一运行记录。没有复制其历史任务或共享数据目录。

2026-10-30起已在智慧足迹DaaS平台 `TidalLanes/TL25M05_V01` 目录提交首批只读探针（0006、0008、0009、0010），状态见queue.json与run-log。字段名、move_rn覆盖与时钟问题已由Speed既有导出回答，0001–0005与0007标 `satisfied_by_reference`，不再重交。用户已授权为本项目从平台提取数据，后续按现有范围推进，无需为每个常规步骤重复请求许可。

## 从这里开始

1. 读 [提取口径](current-spec.md)、[本批计划](202505_v01/PLAN.md)、[运行队列](202505_v01/RUN_QUEUE.md) 和 [唯一运行记录](runs/202505_v01/run-log.md)。
2. 本地运行 `python3 Platform/utils/check_workspace.py`。检查文件、依赖、哈希与平台防火墙规则，不执行SQL、不联网。
3. 平台操作用 `python3 Platform/utils/daas_task.py`（submit/recent/status/fetch）。它通过已登录的Chrome DaaS标签页发请求，不读写凭据；submit只交queue中依赖已满足、哈希一致、lint通过的项目，并写submissions.jsonl与queue状态。fetch只写新文件，拒绝uid列。
4. 按真实结果更新queue.json与run-log，再准备0020网格上传与正式构造SQL。

## 目录

| 路径 | 内容 |
| --- | --- |
| `config/` | 数据期、样本、半开时间窗、输出粒度和来源合同 |
| `202505_v01/00_audit/` | 9条独立单语句只读探针 |
| `202505_v01/01_geo_inputs/` | 原网格上传与平台匹配检查模板 |
| `202505_v01/02_internal_builds/` | 本项目独立内部表构造规则 |
| `202505_v01/03_qc/` | 样本、重复键、时钟、缺失和抑制核验规则 |
| `202505_v01/04_aggregate_exports/` | 聚合字段、导出模板；没有个人明细 |
| `202505_v01/05_capacity/` | pilot、容量和冻结分片计划 |
| `reference_inputs/` | 本地生成的公开网格、模型连接与合成测试坐标 |
| `runs/202505_v01/` | 本批唯一日志、原样聚合查询结果、提交记录、校验 |
| `utils/` | 准备空间参考、生成网格上传SQL（build_geo_upload.py）、本地检查、平台提交/链式运行/状态/下载（daas_task.py）、下载后测试结果验证 |
| `docs/` | 来源复用与提取边界 |

生产表统一 `tl25m05_v01_` 前缀。正式构造与导出均在依赖核实后出新文件并登记哈希；`.sql.in`含未绑定变量，不能直接提交。目前没有可执行的生产建表SQL，不能把本地检查通过写成平台已跑通。

主要产物是：月度joint职住人数、分时实际出发OD、相同trip样本的RN经过压力和逐日通勤参与率。2022年模型、既有验证、其他项目与共享CSV均保留。模型切换在提取验收后单独版本化进行。
