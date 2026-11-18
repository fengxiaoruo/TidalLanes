# 全国爬取定时 / 指定时刻启动

目标：在**你指定的北京时间**启动 `run_full_crawl`，并把结果写入当前 `version_id` 下的 `api/crawl_sessions/<run_id>/`。

## 1. 时段与参数（与代码一致）

- **`--strict-live-window`**：启动时检查一次当前北京时间是否在「窄窗」内；通过后整轮任务会持续跑完（可能跨窗），每条请求仍会带真实 `query_time_local`。
  - **AM**：07:00–09:00  
  - **PM**：17:00–19:30  
  - **NIGHT**（若以后跑夜间档）：22:00–次日 01:00  
  - 与 `--strict-live-window` 同时使用时，**一次只能一个 panel**：`--panels AM` 或 `--panels PM`（不要写 `FREE,AM,PM`）。

- **不按窄窗、只定时启动**：去掉 `--strict-live-window`，仍可用 `--panels AM` 等；适合机器不在国内时做测试（口径上弱一些）。

## 1b. 三轮（FREE / AM / PM）只设开始时间、不设结束时间

需求：**不启用** `--strict-live-window`；只配置三个「北京时间」**开始**时刻；每轮跑满该 panel 的全量 OD；**实际爬取时段**由落盘数据记录，无需人工设结束时间。

- **一键脚本**：`data_work/scripts/run_scheduled_triple_crawl.sh`  
  - 从 `scripts/scheduled_triple_crawl.env` 读配置（请复制 `scheduled_triple_crawl.env.example` 后改名编辑）。  
  - 后台启动 **3 个子进程**，分别睡到 `START_FREE_HHMM` / `START_AM_HHMM` / `START_PM_HHMM` 后执行一轮 `run_full_crawl`（`--panels FREE`、`AM`、`PM` 各一次，**不带** `--strict-live-window`）。  
  - 日志：`data_work/logs/scheduled_triple_crawl/`（可在 `.env` 里改 `CRAWL_LOG_DIR`）。

- **记录爬取时间段**（每轮自动生成，无需改爬虫代码）：  
  - 每轮目录：`api/crawl_sessions/<crawl_run_id>/run_manifest.json` 中有 `crawl_session_start_local`、`finished_at_local`。  
  - 更细：同目录 `session_matrix_flat.csv` 里每条请求的 `query_time_local`；对该列取 **min / max** 即为本轮 API 实际覆盖的本地时间区间。

补好 `ak_local.py` 或 `BAIDU_MAP_AUTH_TOKEN` 后，在 `data_work` 下执行：

```bash
cd /Users/fxr/Dropbox/Projects/TidalLanes/data_work
./scripts/run_scheduled_triple_crawl.sh
```

注意：三轮为 **3 个并行长任务**（若都在同一天且间隔不够，可能同时打百度配额）；若希望 **串行**（上一轮跑完再睡下一轮），可改用三次 `cron` 分三天或间隔足够长的三个时刻分别调用单 panel，或自行改 shell 为顺序 `wait`（单进程）。

## 2. 推荐：cron 在固定时刻直接启动（最简单）

编辑本机 crontab（`crontab -e`），示例为**工作日** 07:05 跑早高峰、17:10 跑晚高峰（请把路径改成你的仓库绝对路径）：

```cron
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# 早高峰：单档 AM + 严格窗（07:05 在 07:00–09:00 内）
5 7 * * 1-5 cd /Users/fxr/Dropbox/Projects/TidalLanes/data_work && /usr/bin/python3.11 -m src.national_tidal_commuting.run_full_crawl --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json --panels AM --strict-live-window --label cron_am >> /tmp/tidallanes_national_am.log 2>&1

# 晚高峰：单档 PM + 严格窗
10 17 * * 1-5 cd /Users/fxr/Dropbox/Projects/TidalLanes/data_work && /usr/bin/python3.11 -m src.national_tidal_commuting.run_full_crawl --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json --panels PM --strict-live-window --label cron_pm >> /tmp/tidallanes_national_pm.log 2>&1
```

说明：

- `python3.11` 路径用 `which python3.11` 核对。  
- AK 仍走 `ak_local.py` 或环境变量（与现有一致）。  
- 日志建议重定向到固定文件便于事后查。  
- **整轮 AM 或 PM 各自约 1.5 万 batch**，耗时常达数小时；cron 不会杀长任务，但**电脑需开机、休眠策略勿在中途睡眠**。

## 3. 「等到指定时刻再跑」：辅助脚本

仓库内：`data_work/scripts/wait_until_shanghai_hhmm_then_exec.py`  

在 `data_work` 目录下示例（等到上海 07:10 再启动 AM 严格窗）：

```bash
cd /Users/fxr/Dropbox/Projects/TidalLanes/data_work
python3.11 scripts/wait_until_shanghai_hhmm_then_exec.py 07:10 -- \
  python3.11 -m src.national_tidal_commuting.run_full_crawl \
  --config config/national_tidal_commuting_baidu2025_top100_congestion_v1.json \
  --panels AM \
  --strict-live-window \
  --label wait_am_$(date +%Y%m%d)
```

可与 **launchd** 组合：在开机登录时加载一个 `StartCalendarInterval` 很宽的任务，实际内部先 `wait` 再 `exec`（适合「每天同一脚本、但要在 07:12 才开跑」这类需求）。

模板 shell（可复制改名后改 `REPO_ROOT` / `WAIT_HHMM`）：

- `data_work/scripts/national_crawl_am_strict.example.sh`  
- `data_work/scripts/national_crawl_pm_strict.example.sh`  

## 4. macOS launchd（可选）

1. 复制一份 plist，把 `ProgramArguments` 里的路径改成你的绝对路径。  
2. `WorkingDirectory` 设为 `.../TidalLanes/data_work`。  
3. `StartCalendarInterval` 设 `Hour` / `Minute` / `Weekday`（0=周日，若需工作日请用多个 plist 或改用 shell 内判断）。  
4. `launchctl load` / `bootstrap` 加载后，用 `log stream` 或重定向日志观察。

若单次任务超过系统对 `StartInterval` 的默认预期，可把 `AbandonProcessGroup` 等保持默认，避免被误杀；长任务关键是**电源与睡眠**。

## 5. FREE / 夜间档

- **FREE**：不要用 `--strict-live-window`；可单独定时，例如凌晨跑：  
  `--panels FREE --label cron_free`  
- **NIGHT**：`--panels NIGHT --strict-live-window`，且须在 22:00–次日 01:00 内启动。

## 6. 与「100 城」样本的关系

定时任务里 `--config` 指向含已生成 `data/top_commuting_pairs.csv` 的 `version_id` 即可（例如 `national_tidal_commuting_baidu2025_top100_congestion_v1.json`）。换版本时只改 config 路径。
