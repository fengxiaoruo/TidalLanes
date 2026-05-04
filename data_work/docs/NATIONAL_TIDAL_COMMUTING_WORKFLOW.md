# 全国潮汐通勤工作流

所有命令在**本仓库的 `data_work/` 目录**下执行（先 `cd` 到该目录）。

---

## 0. 规范输出目录

配置 `version_id: national_tidal_commuting_top50cities_2021_v1` 时，结果在：

- **`data_work/outputs/national_tidal_commuting_top50cities_2021_v1/`**  
  - `data/` 样本与矩阵输入  
  - `api/` 百度返回  
  - `metrics/` 表与汇报  
  - `figures/` 图  

**不要**用 `data_work/data_work/outputs/...`（历史误跑出的嵌套目录）。百度与汇总只认 **`data_work/outputs/.../data/top_commuting_pairs.csv`** 这一套。

**行数检查**：`top_commuting_pairs.csv` 在满样本、当前主线逻辑下为 **2500 行**（无表头）。若明显偏少，核对是否跑错 `version_id` 或建样本是否中断。旧试验目录里曾出现约 800 多行，与主线无关。

若改用百度官方 2024 百城通勤高峰拥堵指数版本，使用：

- config: `data_work/config/national_tidal_commuting_baidu2024_top100_congestion_v1.json`
- output: `data_work/outputs/national_tidal_commuting_baidu2024_top100_congestion_v1/`
- source PDF: `data_work/raw_data/baidu_2024_china_city_traffic_report.pdf`
- extracted city base: `data_work/raw_data/baidu_2024_city_congestion_base.csv`
- city ranking rule: by `commute_peak_congestion_index` from attachment 1 of Baidu Map's 2024 annual city traffic report
- extraction script: `data_work/src/national_tidal_commuting/extract_baidu_2024_city_base.py`

The extracted city base also keeps weekend congestion rank/index and commuting time/distance from attachments 2--3. The current build exports `7461` OD pairs; Sanya remains in the 100-city source table but has zero exportable non-same-town pairs after the existing town-coordinate and cleaning rules.

旧的本地 Excel 版本仍可复现：

- config: `data_work/config/national_tidal_commuting_top100_congestion_2023_v1.json`
- source: `data_work/raw_data/20190909-15百城拥堵数据-20230616.xlsx`

注意：旧 Excel 文件名含 `20230616`，但表内 `日期` 字段为 `20190909`--`20190915`。除非专门做历史对照，后续 national city base 优先使用百度官方 2024 版本。

---

## 1. 数据与建对（在跑百度**之前**完成）

### 1.1 你需要的文件

| 内容 | 路径/说明 |
|------|-----------|
| 乡镇流 | `raw_data/commuting_town_2021.dta`（与 config 中 `commute_dta_path` 一致） |
| 乡镇点位 | **`raw_data/commuting_town_2021_centers.parquet`**（`config` 中 `town_2021.town_centers_parquet`） |
| 可选回退 | 不配 parquet 时可用县级 shp 或三列 `town_centroids_csv`；主线推荐 **parquet 乡镇点** |

Parquet 列至少含：`towncode`，`office_lon`/`office_lat`，`centroid_lon`/`centroid_lat`。解析规则：**优先 office**，无有效 office 再 **centroid**；全量一乡一行会写入 metrics（见下）。

### 1.2 建样本命令

```bash
cd data_work
python src/national_tidal_commuting/build_top_pairs.py --config config/national_tidal_commuting_top50cities_2021_v1.json
```

2023 前 100 拥堵城市版本：

```bash
cd data_work
python src/national_tidal_commuting/build_top_pairs.py --config config/national_tidal_commuting_top100_congestion_2023_v1.json
```

百度 2024 前 100 拥堵城市版本：

```bash
cd data_work
python src/national_tidal_commuting/extract_baidu_2024_city_base.py
python src/national_tidal_commuting/build_top_pairs.py --config config/national_tidal_commuting_baidu2024_top100_congestion_v1.json
```

**主要产出**：

- `outputs/.../data/top_commuting_pairs.csv`：约 2500 对，含 `home_lon/lat, work_lon/lat` 及 **`home_coord_source` / `work_coord_source` / `pair_uses_centroid_fallback`**（乡镇点来源）。  
- `.../metrics/top_commuting_pairs_city_summary.csv`：按城行数、样本是否不足等。  
- `.../metrics/national_top_cities_intra_commute_2021.csv`：被选城市清单。默认版本为通勤量前 50 城；拥堵版本为拥堵指数前 100 城，并附带 `source_congestion_rank` / `source_congestion_index`。  
- `.../metrics/source_congestion_city_ranking.csv`：仅拥堵版本生成，记录 Excel 里的完整拥堵城市排序。  
- 若使用 `town_centers_parquet`，另有：  
  - `commuting_town_2021_towns_resolved_to_centroid.csv`：解析为 **centroid 备用** 的乡镇（与 office 全量比大约 21 条量级，以你表为准）  
  - `commuting_town_2021_center_resolution_by_towncode.csv`：约 4 万+ 行乡镇解析表  
  - `commuting_town_2021_centers_load_summary.json`：office/centroid 条数等  

若本批 top 对里**没有**含「centroid 乡镇」为端点，`pair_uses_centroid_fallback` 可能全为 `False`，**正常**；乡镇级 centroid 清单仍见上表。

**注意**：需能读 parquet（`pip install pyarrow` 等）。读 DTA 时若部分镇码不在 parquet 的 4 万+ 键里，日志会提示 **dropping ... town code missing in lookup**——属数据覆盖差异，不阻塞跑通。

### 1.3 与百度的关系

百度矩阵脚本**只读**上一步的 `top_commuting_pairs.csv` 里的端点坐标；**不需要**在调百度时重复配乡镇 parquet。但 **没建好对、或路径不对，矩阵会报错**。

---

## 2. 已有百度 AK / token 之后：拉矩阵 + 汇总（核心操作）

以下假设 **`top_commuting_pairs.csv` 已在 §1 生成**，且你已在[百度地图开放平台](https://lbsyun.baidu.com/)为应用开通可用的驾车路线规划能力并注意**配额**。

### 2.1 AK 的两种写法

- **本机 .py 文件（推荐，勿提交）**：复制 `src/national_tidal_commuting/ak_local.py.example` 为同目录 **`ak_local.py`**，把其中的 `BAIDU_MAP_AUTH_TOKEN` 填成你的 AK。`ak_local.py` 已列入 `.gitignore`。
- **或环境变量**（新终端要重新 `export`）：`export BAIDU_MAP_AUTH_TOKEN='你的AK'`。

当前脚本默认 `--ak-source auto`：**若 `ak_local.py` 存在，则优先使用本地文件；否则回退到环境变量。**
若两者都存在且不一致，终端会打印所选来源。若你确实想强制环境变量，可显式传 `--ak-source env`。

### 2.2 一键全量爬三档（推荐：进度 + 每批落盘）

对当前 `top_commuting_pairs.csv` 里**全部** OD，顺序跑你指定的 panel；**每发一条 OD 请求就追加**写入，终端打印总进度。一轮输出在：

`outputs/.../api/crawl_sessions/<crawl_run_id>/`

含 `session_matrix_flat.csv`（与旧矩阵列一致，**额外有** `crawl_run_id`、`crawl_label`、`crawl_session_start_local`，便于后处理按「第几轮、哪档、何时爬」切分）、`session_raw.jsonl`、`progress.jsonl`、`run_manifest.json`。

```bash
cd data_work
python -m src.national_tidal_commuting.run_full_crawl
# 可选: --label 早高峰_2026Q2
# 若想强制环境变量而不是 ak_local.py: 追加 --ak-source env
```

`--label` 会写入每行，便于和别的轮次在表里区分。

**汇总** `summarize_results.py` 会同时识别旧文件 `baidu_matrix_results_*.csv` 与 `crawl_sessions/*/session_matrix_flat.csv`（多轮/多源时同 key 取**最新**一条）。

### 2.3 分步跑（仍支持）

与 `--ak-env-var` 一致。

**方式 A / B：shell 与单档脚本** 见下（仍可用 environment variable 或 `ak_local.py` 中的 `BAIDU_MAP_AUTH_TOKEN`）。

### 2.4 三档请求（若重视时段口径，推荐拆成 `AM / PM / NIGHT` 三次）

当前脚本默认接口：**`directionlite/v1/driving`**。  
- **FREE**：`tactics=0`（常规路线 / 时间最短），**任意时间**可跑。注意这只是轻量基线，**不等同于** `routematrix/v2` 的“无路况”口径。  
- **AM / PM**：`tactics=2`（躲避拥堵），**默认不按本地时间拦你**；7 点起跑到中午跨峰也可以。需要「只在建议窗开跑」时用 `--strict-clock-window` 或 `STRICT=1`（见脚本注释）。

**未来爬取版本的保留决策（2026-04-29）**：修改爬取代码时，优先支持同一批 OD、同一真实时段下同时爬 `tactics=0`、`tactics=2`、`tactics=3` 三套结果。当前解释是：
- `tactics=0`：常规路线 / 默认较快路线，可作为统一策略口径。
- `tactics=2`：躲避拥堵，更接近“考虑实时拥堵后的导航推荐最短时间路线”。
- `tactics=3`：最短距离路线，可作为距离优先的对照口径。

后续论文主口径可在 `tactics=0` 与 `tactics=2` 之间选择，并把另一套作为 robustness；`tactics=3` 主要用于检查路线策略变化是否驱动方向性差异。

- **若要更严格的论文口径**：建议把三档拆成三次真实运行，使用 **`AM / PM / NIGHT`**，并传 `--strict-live-window`。此时脚本会：
  - 一次只允许一个 panel
  - 禁止 `FREE`
  - 要求北京时间落在更窄的真实时段窗：
    - `AM`: `07:00–09:00`
    - `PM`: `17:00–19:30`
    - `NIGHT`: `22:00–次日01:00`

推荐命令：

```bash
cd data_work
python -m src.national_tidal_commuting.run_full_crawl --panels AM --strict-live-window --label am_$(date +%Y%m%d)
python -m src.national_tidal_commuting.run_full_crawl --panels PM --strict-live-window --label pm_$(date +%Y%m%d)
python -m src.national_tidal_commuting.run_full_crawl --panels NIGHT --strict-live-window --label night_$(date +%Y%m%d)
```

如果你当前只是想**测试能不能跑通**，而不在乎 panel 名字本身，那么直接跑任意单个 panel 都可以；后续可以根据 `query_time_local` 再把它解释为当时所处时段。最简单的测试命令是：

```bash
cd data_work
python -m src.national_tidal_commuting.run_full_crawl \
  --config config/national_tidal_commuting_top2cities_2021_test.json \
  --panels AM \
  --label smoke_$(date +%Y%m%d_%H%M%S) \
  --sleep-sec 0.05
```

之所以推荐 `AM` 只是因为它是合法单 panel 名字之一；如果你更顺手，也可以把 `AM` 换成 `PM` 或 `NIGHT`。在你当前说的这套口径下，**panel 标签本身不必过度解读，真正可用的是结果文件里的实际请求时间 `query_time_local`。**

**方式 A：一键跑三档**（在 `data_work` 下）：

```bash
cd data_work
export BAIDU_MAP_AUTH_TOKEN='你的AK'
./scripts/run_baidu_travel_panels.sh
# 或把 AK 作第一个参数: ./scripts/run_baidu_travel_panels.sh '你的AK'
# 需要严格时间窗: STRICT=1 ./scripts/run_baidu_travel_panels.sh
```

**方式 B：分三次手动跑**（便于断点续跑、改间隔）：

```bash
cd data_work
export BAIDU_MAP_AUTH_TOKEN='你的AK'

python src/national_tidal_commuting/query_baidu_routematrix.py \
  --config config/national_tidal_commuting_top50cities_2021_v1.json --query-window FREE

python src/national_tidal_commuting/query_baidu_routematrix.py \
  --config config/national_tidal_commuting_top50cities_2021_v1.json --query-window AM

python src/national_tidal_commuting/query_baidu_routematrix.py \
  --config config/national_tidal_commuting_top50cities_2021_v1.json --query-window PM
```

**每跑完一档**，在 `outputs/.../api/` 会新增（时间戳因运行时刻变化）：

- `baidu_matrix_results_<FREE|AM|PM>_<时间戳>.csv`：展平 OD、时长、距离、`tactics`、名义时刻说明、**实际发请求时间** 等。  
- `baidu_matrix_raw_<同窗口>_<时间戳>.jsonl`：整包响应用于排错。

### 2.5 汇总 + 出图

在 **FREE、AM、PM 三套矩阵 CSV 都至少成功跑过一次**（可跨天、多时间戳；汇总会对同一 OD×窗口×方向取**最后一条**）后：

```bash
cd data_work
python src/national_tidal_commuting/summarize_results.py \
  --config config/national_tidal_commuting_top50cities_2021_v1.json
```

**产出**（均在 `outputs/.../` 下）：

- `metrics/top_commuting_pairs_pair_summary.csv`：每 **pair** 一行的宽表与 AM/PM/畅通 等衍生指标。  
- `metrics/top_commuting_pairs_city_metrics.csv`：按城汇总。  
- `figures/figure1_*.png` … `figure6_*.png`。  
- 版本根下 `config_snapshot.summarize_results.json`。

**慎删 `api/` 里旧结果**：`summarize` 会读 `baidu_matrix_results_*.csv` 与 `crawl_sessions/*/session_matrix_flat.csv`；多轮/废文件会干扰「取最新」逻辑。

---

## 3. 含义与排期（便于写论文/脚注）

| 目标 | 做法 |
|------|------|
| 畅通/对照基线 | 跑 **FREE**（`tactics=13`），任意时刻。 |
| 早/晚高峰路况含义 | 跑 **AM/PM**；**更贴峰**可在 7–9 点、17–20 点（北京时）开跑；默认**不因**「不在 8/18 点」而禁止长任务。 |
| 发请求真实时刻 | 看矩阵 CSV 里的 `query_time_local` / `query_date`。 |

v1 路径规划与矩阵的区分见 **`data_work/docs/BAIDU_ROUTE_API_NOTES.md`**。

---

## 4. 最短检查清单（「token 已就绪」到可写结果）

- [ ] `data_work/outputs/national_tidal_commuting_top50cities_2021_v1/data/top_commuting_pairs.csv` 存在且行数约 2500。  
- [ ] 已设 AK（`ak_local.py` 或 `export`）。  
- [ ] 已用 **`run_full_crawl`** 三档、或分档脚本 / `run_baidu_travel_panels.sh`。  
- [ ] 已跑 `summarize_results.py`，检查 `pair_summary` 与 `city_metrics`、图件。  
