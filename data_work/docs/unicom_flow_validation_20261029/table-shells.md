# 已填充表与图来源

| 表/图 | 直接来源 |
| --- | --- |
| 主结果及计数敏感性 | flow_metrics.csv |
| 方向与符号一致性 | direction_metrics.csv |
| 不平衡量级 | direction_magnitude.json |
| 模型覆盖 | sample_coverage.csv |
| 观测覆盖分层 | observation_coverage_stages.csv |
| 共同支持敏感性 | common_support_sensitivity.csv |
| 日稳定性与复制排查 | unicom_daily_rank_stability.csv、daily_repeatability_audit.csv |
| 区间 | bootstrap.json |
| 散点/方向/每日图 | flow_validation.pdf/svg/png |
| 差异地图 | flow_share_gap_map.pdf/png |

上述文件位于data_work/outputs/unicom_model_flow_validation_v1。L2结果报告由write_unicom_flow_report.py读取这些表生成，不手工转录结果数值。
