# Raw Segment Baidu Validation

本目录用于 **baseline raw 路网 segment** 与百度驾车 API 返回 travel cost 的对照验证。

与 `centerline_baidu_validation` 的区别：

- 几何单元：`raw_segment_master`（约 2.4 万条）而非 centerline（约 1.2 万条）
- 对照侧：百度 speed CSV 直接观测（`speed_Beijing_all_wgs84.csv`），而非 centerline 聚合推断
- 输出根：`outputs/raw_baidu_validation_beijing_v1/`（与 centerline 结果分离）

完整说明见 [`../../docs/RAW_BAIDU_VALIDATION.md`](../../docs/RAW_BAIDU_VALIDATION.md)。

## 快速入口

```bash
cd data_work

# 1) 生成 manifest（含途经点）
python -m src.raw_baidu_validation.build_crawl_manifest \
  --config config/raw_baidu_validation_beijing_v1.json

# 2) smoke test
python -m src.raw_baidu_validation.query_baidu_direction \
  --config config/raw_baidu_validation_beijing_v1.json \
  --panel FREE --limit 5 --require-waypoints --steps-info --label smoke_wp
```
