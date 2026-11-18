# 质量核验合同

本地文件检查仅说明准备完整，不能替代本页的平台QC。

| 检查 | 必须保留的证据 |
| --- | --- |
| 实际字段和日期 | schema原样结果、city/date/月键与时刻类型 |
| 唯一性 | attr/POI/trip/RN各自键、完全重复与冲突数量；join前后计数，无重复膨胀 |
| 空间 | 7569个公共fixture全部与原多边形一致；个人点缺失/多匹配/mask外计数 |
| 时间 | 12个合成时刻的AM/PM及slot10/30全匹配；source day×passage day联合分布及跨窗量 |
| N与人口边际 | joint人数=行和=列和；同格/短距离、外部支持及被抑制人数另列 |
| 参与与Q | 月唯一人数、逐日参与、trip计数不同；distinct不跨槽直接加总 |
| RN | 时间与route候选、覆盖分母、raw/valid/trips/users次序、缺失不补零 |
| 样本一致 | Q和RN用同trip universe；全体mode1参考与R通勤各自标sample_id |
| 导出 | 抑制前后粗分组总量、片段互斥/完备、SHA、唯一键、行数和字节 |

不设置为了跑过的随意相关系数或覆盖阈值。空间fixture应完全一致，计数恒等式与分片对账应精确闭合；缺少覆盖必须量化后限制解释，不通过手工补零抹平。

细单元抑制按人数而非trip门槛。QC仅导出允许的粗汇总，不通过互补表恢复小单元。某组n_cells_kept=0时区分真正空导出组与下载丢失。

时间结果验证：`python3 Platform/utils/validate_probe_result.py --kind time --csv 实际下载路径`。失败保留原文件和真实差异，不修改原样结果来通过。
