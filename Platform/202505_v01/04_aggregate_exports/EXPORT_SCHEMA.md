# 聚合导出字段

本页与export_manifest.csv定义需求，正式字段应在实际内部schema核实后冻结，不能直接运行含变量的.sql.in。

## N

month、grid_home、grid_work、sample_id、mode_definition、n_people、n_people_with_hw、n_people_with_wh、n_people_day5、same_grid_flag。不同资格子组不要反复相加计人口；主边际从选定joint cohort计算。

## 分时Q

date、slot30（0至47）、grid_o、grid_d、od_basis、purpose、dictionary_mode_group、rail_detected、sample_id、n_trip、n_people、n_trip_from_day5_people、sum_time_s、sum_distance_m。实际端点未通过审计的记录不填主点冒充。

n_trip_from_day5_people为该单元内day5资格人群贡献的trip次数，不是人数。单元人数/次数与时长非缺失分母分别保存，不能把缺时长当0算均值。时长分位数可选，聚合后不能平均中位数；需要粗粒度分位数则在平台重新计算。

## RN

passage_date、slot10（0至143）、rn_pair_id、purpose、dictionary_mode_group、sample_id、n_raw_transitions、n_valid_transitions、n_unique_trips、n_unique_people、n_source_trip_keys、qc_match_scope。RN字典/坐标已有合规聚合可只读复用；没有必要把全量路段几何每日重复导出。

source date与passage date分别留粗QC，不把一个日期字段同时用作两个时钟。未知purpose/join失败、全体mode1、R通勤各自保存；转换到小时/AM窗口时去重人数重算。

## 保存与版本

下载原样放Platform/runs/202505_v01/query-results，不经符号链接写共享源。逐份文件在冻结manifest中记录目标scope、hash范围、expected rows/count、真实hash/字节和审核状态。模型适配及地图放data_work新版本，平台导出不直接替换2022原始输入。
