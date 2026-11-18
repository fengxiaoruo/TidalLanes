# 空间和路网口径

网格：manual_centerline_rules_v11/data/grid_square_master.parquet，3357个原始3公里网格（边界裁剪）。模型LCC采用其中1104节点。经纬度点与网格均为WGS84；距离吸附和地图使用EPSG:32650。

RN：Speed GPS data中已保存的road_pair_geometry.csv，端点连线不是真实路形；RN ID不等于OSM ID。当前字典route_year=2026与2025年OSM有时间差。

OSM：V1-OSM/data/intermediate/route_v3/beijing_arcs.parquet，663802条有向弧。按from/to node坐标建立空间索引；不同图节点同坐标的层级歧义不纳入主匹配。沿有向图求有界最短可行路径，保存节点序列、网格序列、长度与吸附距离。

路径筛查检查节点网格顺序，不逐弧检查曲线是否短暂离格；最短可行路径不是供应方真实路径证明。主结果属于粗网格有向边界代理，未声称完成RN→Speed→模型的逐物理link映射。Speed原始segment到OSM映射未用于给每条模型Xi分配物理道路。

每个跨界RN pair在同一模型有向边界只贡献一次该RN transition计数；同格内部RN不计入跨界量。供应方真实长路径跨越多个网格、出界后回返或未恢复弧仍可能遗漏。宽距离口径单列，不自动并入主样本。

地图连线为网格质心连接，不是路形；双向按已有共同观测支持加总，颜色为归一化份额比，显示范围±3但原表不截尾。
