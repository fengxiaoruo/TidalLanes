# 车流视频批处理说明

## 1. 放视频

把新采集的视频放到：

```bash
flowvedio/input_videos/
```

支持 `.mov`、`.MOV`、`.mp4`、`.m4v`、`.avi`。可以放子文件夹。

## 2. 准备环境

如果还没有安装依赖：

```bash
python -m venv flowvedio/.venv-video
source flowvedio/.venv-video/bin/activate
python -m pip install -r flowvedio/requirements-video.txt
```

已经装过的话，直接激活：

```bash
source flowvedio/.venv-video/bin/activate
```

## 3. 可选：先把长视频拆成短片段

稳定阶段建议按 5 分钟拆：

```bash
python flowvedio/split_traffic_videos.py \
  --input flowvedio/input_videos/20260710_formal \
  --output flowvedio/split_outputs/20260710_formal_5min \
  --chunk-seconds 300
```

先不真正切视频、只看 manifest：

```bash
python flowvedio/split_traffic_videos.py \
  --input flowvedio/input_videos/20260710_formal \
  --output flowvedio/split_outputs/20260710_formal_5min \
  --chunk-seconds 300 \
  --dry-run
```

如果潮汐车道布设导致线位变化，先写一个切分计划表，例如 `flowvedio/split_plan_20260710.csv`：

```csv
video,start,end,label,keep
formal1.MOV,00:00,15:00,pre,1
formal1.MOV,15:00,17:00,transition,1
formal1.MOV,17:00,45:00,post,1
formal1.MOV,45:00,55:00,unfilmed_gap,0
formal1.MOV,55:00,01:10:00,post,1
```

然后按计划表切：

```bash
python flowvedio/split_traffic_videos.py \
  --input flowvedio/input_videos/20260710_formal \
  --output flowvedio/split_outputs/20260710_formal_planned \
  --plan flowvedio/split_plan_20260710.csv
```

`keep=0` 的行只作为记录，不会输出视频。切分结果总表在输出目录的 `segments_manifest.csv`。

## 4. 先检查计数线

先 dry-run，不跑模型，只生成每个视频的线位预览：

```bash
python flowvedio/run_traffic_batch.py \
  --input flowvedio/input_videos \
  --output flowvedio/batch_outputs \
  --profile auto \
  --device mps \
  --dry-run
```

看每个输出文件夹里的 `line_preview.jpg`。如果黄线方向或位置不对，先改 `flowvedio/configs/caitian_default.yaml` 里的 `line_ratio`。

横屏视频如果文件名包含“出城”，`--profile auto` 会优先用 `horizontal_outbound_right`；如果文件名包含“进城”或“入城”，会优先用 `horizontal_inbound_left`。也可以手动指定：

```bash
--profile horizontal_outbound_right
--profile horizontal_inbound_left
```

也可以不改配置文件，直接手动指定计数线。先 dry-run 生成坐标网格：

```bash
python flowvedio/run_traffic_batch.py \
  --input flowvedio/input_videos/20260710_test \
  --output flowvedio/batch_outputs/20260710_test \
  --profile auto \
  --config flowvedio/configs/caitian_default.yaml \
  --dry-run
```

打开每个输出文件夹里的 `coordinate_preview.jpg`，读出计数线两个端点的像素坐标 `(x1, y1)` 和 `(x2, y2)`，然后用 `--line` 覆盖 profile：

```bash
python flowvedio/run_traffic_batch.py \
  --input flowvedio/input_videos/20260710_test \
  --output flowvedio/batch_outputs/20260710_test_manual \
  --profile auto \
  --config flowvedio/configs/caitian_default.yaml \
  --line 1035,635,1620,790 \
  --dry-run
```

确认新的 `line_preview.jpg` 合适后，去掉 `--dry-run` 正式运行。`--line` 使用的是视频进入计数算法后的像素坐标；竖屏 profile 会先旋转，所以竖屏视频的手动坐标要看旋转后的 `coordinate_preview.jpg`。

## 5. 正式跑

```bash
python flowvedio/run_traffic_batch.py \
  --input flowvedio/input_videos \
  --output flowvedio/batch_outputs \
  --profile auto \
  --device mps \
  --config flowvedio/configs/caitian_default.yaml
```

Apple Silicon 用 `--device mps`。如果报 MPS 问题，改成 `--device cpu`。

## 6. 看结果

批处理总表：

```bash
flowvedio/batch_outputs/summary_all_videos.csv
```

每个视频会有一个单独文件夹，里面通常包括：

- `line_preview.jpg`
- `*_annotated_count.mp4`
- `*_vehicle_crossings.csv`
- `*_vehicle_counts_by_minute.csv`
- `*_traffic_summary.json`
- `run_meta.json`

## 7. 当前默认 profile

`horizontal_mainline`：横屏楼上俯拍，使用 YOLO11s + ByteTrack，保留之前 `test.MOV` 的线位。

`horizontal_outbound_right`：横屏右半幅出城方向，适合当前 `test出城方向1619开始.MOV` 这种机位。

`horizontal_inbound_left`：横屏左半幅进城方向，沿用之前左侧车流线位。

`vertical_rotated_dense`：竖屏密集车流，先顺时针旋转，再用 YOLO11m + 简单轨迹关联。这个 profile 主要用于可视化 QA 和粗算，正式数值建议抽样人工校准。
