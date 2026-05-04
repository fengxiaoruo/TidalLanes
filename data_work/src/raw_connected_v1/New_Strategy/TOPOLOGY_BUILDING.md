# 交通拓扑（节点-有向边）构建流程

本流程使用生成的 `road_list.xlsx`（每个 `road_id` 一条有方向的 `LINESTRING`），把所有 `LINESTRING` 的首尾端点聚类成节点（忽略方向用于“节点识别”，但保留方向通过有向边体现），最终得到节点-有向边网络。

## run 顺序

1. 提取道路清单（得到 `road_list.xlsx`）
   - 脚本：`extract_road_list.py`
   - 运行示例：
     ```powershell
     cd "b:\RA工作\SpeedNow"
     python .\New_Strategy\extract_road_list.py --input "B:\RA工作\SpeedNow\Raw_data\speed_Beijing_all_wgs84.csv"
     ```

2. 节点识别（端点聚类，得到 `nodes.xlsx` 与 `segment_endpoints_nodes.xlsx`）
   - 脚本：`node_identification.py`
   - 运行示例（当前用 25m）：
     ```powershell
     python .\New_Strategy\node_identification.py --input ".\New_Strategy\Processed_Data\road_list.xlsx" --eps_m 25
     ```

3. 构建有向边（得到 `directed_edges.xlsx`）
   - 脚本：`build_directed_edges.py`
   - 运行示例：
     ```powershell
     python .\New_Strategy\build_directed_edges.py
     ```
   - 逻辑：每条 `road_id` 作为一条有向边，起点节点为该 `road_id` 的起端聚类结果 `start_node_id`，终点节点为 `end_node_id`。

4. 交互式可视化（生成单个 HTML，浏览器打开）
   - 脚本：`visualize_network_html.py`
   - 运行示例：
     ```powershell
     python .\New_Strategy\visualize_network_html.py
     ```

## 可控参数

### 1) 端点聚类阈值
- `node_identification.py --eps_m`：端点在米制坐标下的距离阈值（当前你用的是 `25`）

### 2) 计算开销
- `extract_road_list.py --chunksize`：分块读取大小
- `node_identification.py --limit`：仅用于快速测试（限制处理前 N 条 `road_id`）

### 3) 可视化筛选与样式（当前写在脚本内部常量）
- `visualize_network_html.py` 里的 `main_urban_bbox`：主城区范围筛选（WGS84，经纬度 bbox）
- 样式常量：
  - `raw_width`：黄色原始 `LINESTRING` 的线宽
  - `conn_width`：蓝色虚线“连接”的线宽
  - 节点红点半径（circleMarker 的 `radius`）

## 节点对双向/单向连接统计（来自 directed_edges）

统计对象：无向节点对 `{u, v}`（只统计至少存在一个有向边连接的节点对；不存在连接的节点对不统计）。

使用的数据：`New_Strategy/Processed_Data/directed_edges.xlsx` 中的 `from_node_id/to_node_id`。

- 无向节点对总数：`18661`
- 双向节点对（u->v 且 v->u 都存在）：`6106`大部分为常规路口
- 单向节点对（仅存在一个方向）：`12555`，大部分系辅道/匝道/立交桥节点密集导致

