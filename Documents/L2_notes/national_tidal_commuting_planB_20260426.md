# 全国潮汐通勤 motivation evidence：方案 B 实施备忘

## 主数据与地理底图

- **乡镇 OD 流量（全国）**：`data_work/raw_data/commuting_town_2021.dta`  
  - 列为：`commuters_2021`、起点/讫点 省-市-县-镇名称与 `origin_town_code` / `dest_town_code`（9 位乡镇级代码）。
- **县级几何与 2019 审图号底图**（同目录 bundle）：  
  `data_work/raw_data/gis/map/2019中国地图-审图号GS(2019)1822号/县（等积投影）.shp` 及配套 `.prj`（Krasovsky 1940 Albers），用于由乡镇码映射到**县级**多边形质心，再投影到 WGS84，供 `routematrix` 的 `coord_type=wgs84` 使用。

## 样本定义（全国 50 城，每城 50 条无向对）

1. **城怎么选**  
   在**同城内部**乡镇对（O、D 的省、市名相同）上，对 `commuters_2021` 求和，按总量从高到低取 **全国前 50 个地级单元**，见 `metrics/national_top_cities_intra_commute_2021.csv`。

2. **无向对怎么定、按什么 rank**  
   - 在城内将 directed 记录聚合成**无向** pair：`A↔B` 与 `A→B`/`B→A` 的通勤量合起来，在 `build_top_pairs.collapse_unordered_pairs` 里记为 `pop_ab`（与 canonical `home` 方向一致的一向）和 `pop_ba`（反向）。  
   - **排名指标**为  
     \[
     \text{commuter\_undir\_mean} = \frac{\text{OD} + \text{DO}}{2} = \frac{pop\_{ab} + pop\_{ba}}{2}
     \]  
     与按 **\(pop\_{ab}+pop\_{ba}\)** 排序**等价**（仅差常数因子 2），全城按该量**降序**，取前 **`top_n_pairs`**（默认 50）条无向对。  
   - **不**做城市核心圈、直线距离带、1 km 近重复等任何过滤；若某城无向对总数不足 50，则全写入并 `sample_shortfall_flag` 为真。

3. **坐标**  
   乡镇 9 位码 → 前 6 位 `PAC` 对县级 shp 取质心；同县多镇仍可能**共点**，靠 `home_plot_id` / `work_plot_id` 区分乡镇。中山等在旧版中曾因「核心圈半径为 0」被整城丢弃的**现可正常出对**（已不再计算 `invalid_city_core`）。

## 依赖（Python）

读 shp 与投影需：

```bash
python3 -m pip install pyshp pyproj
```

## 主配置

- `data_work/config/national_tidal_commuting_top50cities_2021_v1.json`  
  - `source: "town_2021"`，`top_n_cities: 50`，`top_n_pairs: 50`  
  - `town_2021` 内填写 dta 与 shp 路径（相对 `data_work/` 根）
- 两城小测：`data_work/config/national_tidal_commuting_top2cities_2021_test.json`
- 北京+深圳点数据试跑：`data_work/config/national_tidal_commuting_planb_trial_v1.json`（`per_city_csv`，**同一** `(OD+DO)/2` 排名后取前 `top_n_pairs` 条，无核心圈/距离/去重）

## 已实现的脚本

- `build_top_pairs.py` / `town_2021_top_pairs.py`：聚合、无向 fold、`finalize_undirected_top_n` 排名并截断。
- `county_centroids_wgs84.py`：县级质心 WGS84。
- `query_baidu_routematrix.py` / `summarize_results.py`：读当版 `top_commuting_pairs.csv` 后拉矩阵、汇总作图。

## 推荐命令

```bash
cd data_work
python3 -m pip install pyshp pyproj
```

```bash
python3 -m src.national_tidal_commuting.build_top_pairs \
  --config config/national_tidal_commuting_top50cities_2021_v1.json
```

## 输出说明

- `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/`
- `data/top_commuting_pairs.csv`：至多为 **50×50=2500** 行；列含 `commuter_undir_mean`、`rank_by_commuter_undir_mean`、`rank_in_city` 等。
- 百度 `routematrix` 不伪造未来时刻，仍分 AM/PM/NIGHT 三次实跑。

## 下一步

- 在三个时间窗口跑 `query_baidu_routematrix.py`，再 `summarize_results.py`。
- 若需更细空间点，可后续换乡镇级几何或更精确质心。
