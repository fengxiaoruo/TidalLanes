# 来源复用边界

SubwayMobility只读参考现行NG00–23、NG103与对应run-log/current-spec，不复制早期01_pull_data/02_variable_construction的legacy年龄、夜窗家识别或旧线路设定。其Platform组织方式被采用，统计样本与研究暴露没有机械移植。

NG103D六份2025数据已经下载且QC通过，本项目不重跑。它们是5公里格、原SQL标签07-09实际07至10时、19日月度汇总且有职住≥1公里限制；适合描述/时长benchmark，不是原3公里精确AM输入。

NG74A家与工作边际来自不同有效子集，不能代替joint OD；NG74B有当期joint和HW次数，但没有精确时间或纯汽车字段。共享P14是旧快照，不改贴为当前输入。

Speed已有RN经过、同月OD、RN字典与路线时钟审计可只读复用；七列OD没有purpose，RN聚合没有本项目purpose。需在平台内部将同一trip样本与purpose接回RN后生成所需聚合，不能仅靠本地相减。

生产构造使用tl25m05_v01_独立表。复用源内部表前先确认真实表名/schema和来源算法；没有线上确认时标unverified，不凭文件名声称表存在。source_contracts.json和source_registry.csv给出本地来源与哈希，不意味着线上状态已验证。

同源的Q和RN可以作条件于OD/速度的路由验证；若用RN校准参数，需要日期/道路留出。模型验证不能通过逐link缩放或以flow筛匹配机械改善。

## 2026-10-30核对的已知事实

- 字段名：Speed `00_schema_audit/exports/00_columns`。move_month有move_id与moi_id、start/end_poi、start/end_ptype、start/end_grid_id、is_core；stay_poi有weighted_centroid_lon/lat与days_poi；user_attribute有age、is_core、home/work_lat/lon；move_rn无move_id。字段类型未导出。
- trip键：SubwayMobility NG03（20250506）3,120.6万trip键全部唯一、无冲突；mode1占2,327.0万。
- RN：move_rn全月31日；rn_seq锚点上uid+date+moi_id与move_month的move_id当日100%匹配（Speed，20250515）。RN time为秒级Unix；trip原始stime显示当日00–23时，经CAST BIGINT与FROM_UNIXTIME显示为08时至次日07时，因此trip时段用原始stime字符串，RN时刻用供应方转换，不互相平移。
- route_node：2025年5月RN几乎只匹配2026版字典。white_route_node不能用作2025 RN边字典。
- Speed passage表 `speed202505_pair_passage_day_20250519_all` 与 `_week_src20250520`–`26` 含city、uid、date、moi_id、rn_seq、rn_from/to、passage_unix_seconds、passage_date、slot_10min及质量旗标；是否仍在由0010确认。
