# Centerline Baidu Validation

本目录用于 **baseline centerline** 与百度驾车 API 返回 travel cost 的对照验证。

完整说明见 [`../../docs/CENTERLINE_BAIDU_VALIDATION.md`](../../docs/CENTERLINE_BAIDU_VALIDATION.md)。

## 快速入口

```bash
cd data_work

# 1) 从 baseline centerline 生成双向爬取 manifest（含固定途经点）
python -m src.centerline_baidu_validation.build_crawl_manifest \
  --config config/centerline_baidu_validation_beijing_v1.example.json

# 2) 后续：按 manifest 调用百度 API
python -m src.centerline_baidu_validation.query_baidu_direction \
  --config config/centerline_baidu_validation_beijing_v1.json \
  --panel FREE --limit 5 --require-waypoints --label smoke_wp
```

## 目录约定

- 代码：`src/centerline_baidu_validation/`
- 配置：`config/centerline_baidu_validation_*.json`
- 输出：`outputs/centerline_baidu_validation_<version>/`
- AK：复用 `src/national_tidal_commuting/ak_local.py`（或环境变量 `BAIDU_MAP_AUTH_TOKEN`）
