# 变量字典

| 变量 | 单位与含义 | 聚合规则 |
| --- | --- | --- |
| model_edge_id | primitives_edges.csv零起始行号 | 保持与保存Xi数组顺序一致 |
| grid_o/grid_d | 模型原始网格ID | 起点与终点有方向 |
| model_Xi | 模型person-equivalent期望link passage | 不当作实测车辆数；可能重复经过 |
| n_valid_transitions | 联通有效RN transition经过数 | AM槽和匹配RN pair求和，跨日取均值 |
| n_unique_trips/users | 原RN pair×10分钟单元去重数 | 累加为替代压力；不能解释为全窗独立数 |
| *_slots | 原AM12槽内非缺失槽数 | 仅用于观测覆盖 |
| chord_length_m | RN端点直线距离 | 不是实际道路距离 |
| osm_snap_from/to_m | RN点到最近无歧义OSM图节点距离 | WGS84投影UTM50N米制 |
| osm_path_length_m | 图上最短可行路径弧长之和 | 用于连通筛查，不声称真实路径 |
| osm_node_path | 保存的有向OSM节点序列 | 可逐弧检查 |
| osm_grid_sequence | 恢复路径节点压缩后的网格顺序 | 主样本必须仅起点格、终点格 |
| primary | 500米、10米吸附和OSM路径规则 | 完全不使用模型流量决定匹配 |
| strict_250m | primary中250米、1米吸附子集 | 更严格敏感性 |
| osm_1000m | 1000米、10米吸附和路径规则 | 更宽OSM敏感性 |
| endpoint_500m/1000m | 只按端点空间归属和距离 | 低置信度，未核实路径 |

空槽保留NaN。全窗口无观测的RN pair保持NaN。网格link若至少一个选定RN pair有值才得到观测值；link缺某日不补零。周平衡要求五日都有观测值，不意味着所有RN槽完全覆盖。归一化share只在对应比较样本内计算。

方向比使用node_i<node_j作为固定正方向，比较log(q_forward/q_reverse)。高流量方向是该比值的符号。没有按模型“快/慢向”翻转观测方向。
