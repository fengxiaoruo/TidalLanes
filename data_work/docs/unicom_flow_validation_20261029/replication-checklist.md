# 复现与验收

1. 使用含pandas、numpy、scipy、geopandas、shapely、pyproj、pyarrow、matplotlib的Python环境。
2. 核对脚本内ROOT与SPEED路径，源数据只读。input_manifest.csv和formal_file_audit.csv保存SHA-256；input_manifest不包含大原始通勤CSV（模型使用保存的Xi，不重估）。
3. 全量重建先将data_work/outputs/unicom_model_flow_validation_v1改名保留，避免使用旧缓存。按validate_unicom_model_flow.py、audit_unicom_flow_validation.py、write_unicom_flow_report.py顺序运行，均位于data_work/src/validation。
4. 缓存重用仅适用于输入、配置与脚本未变；变化后必须用新生成目录完整重建。脚本首步支持--output，但审计与报告默认指向正式v1目录，另设输出时应同步调整这两个脚本的OUT常量。
5. 检查verification_checks.json全部为true。独立Python CSV解析器复算七天各100条AM记录；独立秩相关公式复算主结果；逐条检查所有主匹配保存的OSM路径有向弧与长度。
6. 检查flow_metrics、direction_metrics、sample_coverage键与样本数，保留缺失值；不把unobserved当0。
7. 图已目视检查：字体、轴标签、点数、变换及地图图例可读；PDF/SVG为矢量，PNG为300dpi。
8. 本次不运行平台SQL、不新增下载、不写入Speed源项目、不改变主模型或既有基线数组。没有训练/测试缩放参数，也没有政策因果解释。

当前结果：389份与13288340源行对齐；11项检查通过。主要测量限制见结果memo，不由自动检查通过而消失。
