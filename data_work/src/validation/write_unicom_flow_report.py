"""Build the retained research memo from saved validation tables."""
import json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'data_work/outputs/unicom_model_flow_validation_v1'


def main():
    checks=json.loads((OUT/'verification_checks.json').read_text())
    assert all(checks.values()),checks
    metrics=pd.read_csv(OUT/'flow_metrics.csv')
    dmetrics=pd.read_csv(OUT/'direction_metrics.csv')
    coverage=pd.read_csv(OUT/'sample_coverage.csv')
    ci=json.loads((OUT/'bootstrap.json').read_text())['spearman_ci95_pair_bootstrap']
    magnitude=json.loads((OUT/'direction_magnitude.json').read_text())
    stage=pd.read_csv(OUT/'observation_coverage_stages.csv').set_index('stage')
    common=pd.read_csv(OUT/'common_support_sensitivity.csv').set_index('sample')
    w=metrics.query('period == "weekday_balanced_mean" and metric == "n_valid_transitions"').set_index('sample')
    dr=dmetrics.query('metric == "n_valid_transitions"').set_index('sample')
    c=coverage.set_index('sample').loc['primary']
    r=w.loc['primary'];direction=dr.loc['primary']
    labels={'primary':'主样本：OSM支持，1–500米，吸附≤10米',
            'strict_250m':'更严格：1–250米，吸附≤1米',
            'osm_1000m':'OSM支持，1–1000米',
            'endpoint_500m':'仅端点，1–500米（较低置信度）',
            'endpoint_1000m':'仅端点，1–1000米（较低置信度）'}
    rows=[]
    for key in labels:
        rr=w.loc[key];dd=dr.loc[key]
        rows.append(f'| {labels[key]} | {int(rr.n):,} | {rr.spearman:.3f} | {int(dd.n):,} | {dd.spearman:.3f} | {dd.dominant_direction_agreement:.1%} |')
    table='\n'.join(rows)
    conditional=stage.loc['primary_osm_supported_up_to_500m','weekday_am_valid_transitions']/stage.loc['requested_model_grid_pair_any_length','weekday_am_valid_transitions']
    text=f'''# 联通与模型flow交叉验证结果

日期：2026-10-29。状态：有向网格连接层面的跨期验证已完成；尚非逐条物理道路流量验证。

**模型与联通的空间流量排序部分一致，但方向性吻合较弱。** 主样本{int(r.n):,}条有向连接的Spearman相关为{r.spearman:.3f}；{int(direction.n):,}对双向连接的方向比相关为{direction.spearman:.3f}，高流量方向一致率为{direction.dominant_direction_agreement:.1%}。本轮结果支持模型捕捉部分空间分布，尚不支持“模型flow已充分验证”的表述。

模型基线需求来自2022年11月通勤数据，观测压力来自2025年5月机动车出行；二者年份、目的与单位不同。匹配筛选还会遗漏部分道路经过量。因此，不能将差异全部归为模型误差，也不能用本轮结果确认绝对车辆流量。

## 1. 主结果

| 指标 | 结果 |
| --- | ---: |
| 平衡工作日样本中的有向模型连接 | {int(r.n):,} / 2,642 |
| 覆盖的模型基线link passage份额 | {c.model_passage_share_balanced:.1%} |
| 覆盖的模型link长度份额 | {c.model_length_share_balanced:.1%} |
| 流量排序Spearman相关 | {r.spearman:.3f} |
| 按无向网格对重抽样的95%区间 | [{ci[0]:.3f}, {ci[1]:.3f}] |
| 水平Pearson相关 | {r.pearson_levels:.3f} |
| log(1+flow) Pearson相关 | {r.pearson_log1p:.3f} |
| 两侧各自流量最高10%连接的重叠率 | {r.top_decile_overlap:.1%} |
| 归一化share的total variation距离 | {r.share_total_variation:.3f} |
| 双向流量比Spearman相关 | {direction.spearman:.3f} |
| 高流量方向一致率 | {direction.dominant_direction_agreement:.1%} |

方向不平衡的量级也有差异。在同一组{int(direction.n):,}对连接中，模型高/低方向流量比的中位数为{magnitude['model_median_major_minor_ratio']:.2f}，联通为{magnitude['unicom_median_major_minor_ratio']:.2f}。模型所呈现的不平衡更强；匹配在两个方向的覆盖差异、全部机动车与通勤样本的差别，以及route choice都可能影响这一比较。

工作日逐日相关约0.536–0.546，周末约0.504–0.506，主结果不由某个工作日独自驱动。联通工作日之间的排序相关超过0.998，属于同一观测代理的时间稳定性，不能当作五次独立模型验证。复核中只有约1%–2%的连接计数与周一完全相同，并非简单复制日文件。

重抽样区间仅描述给定观测和匹配规则下、按网格对重抽样的不确定性，不覆盖网格对之间的空间相关、手机样本误差或系统性匹配误差。本轮不设置任意相关系数“通过线”。

![流量、方向与逐日比较]({OUT}/flow_validation.png)

## 2. 测量单位与匹配规则

模型link是相邻3公里网格间的抽象有向连接；Ξ是模型route kernel隐含的期望link passage，允许路径中的重复经过。联通RN pair更细。直接按RN ID、OSM ID或原始Speed segment ID连接模型Ξ会比较不同经济对象。

本轮先将RN起终坐标分别放入模型原始网格多边形，保留属于模型有向连接的跨界RN pair。再将端点吸附至Speed当前OSM有向图节点，排除同坐标多层节点歧义；在不使用模型flow的条件下，寻找有向最短可行路径。主样本要求RN端点距离1至500米、两端吸附距离均不超过10米、路径长度不超过min(3000米，3×端点距离+100米)，且恢复路径节点的网格顺序仅为目标起点格、终点格，各一次。更严格和更宽的规则在下表单列。

这些OSM路径提供连通性筛查，不证明供应方实际使用了该最短路径。筛查依据的是路径节点的网格序列，未逐弧审计曲线是否短暂离开网格。模型连接本身的质心路由也未必等同于全部真实跨界道路，因此本轮称为“有向网格边界压力代理验证”。逐条物理道路验证还需要把模型passage分配至原始道路路径，并恢复RN的实际路段归属。

窗口为北京当地时间07:00≤经过时刻<09:00，与模型AM速度窗口一致；读取6小时block=1内s06至s17的12个10分钟槽。按2025-05-19至23日分别聚合，周结果只保留五日都有有效观测的模型连接；缺失不补零。24、25日作为周末诊断。

主指标为n_valid_transitions之和，保留重复有效经过。n_unique_trips/users仅在原始RN pair×10分钟单元内去重，本轮将其累加作为替代压力指标，不能称为全窗口独立行程或独立人数。三种指标的流量排序相关几乎相同。没有使用weight/gw扩样，也没有逐link缩放。

## 3. 覆盖与敏感性

| 匹配样本 | 平衡有向连接数 | 流量排序相关 | 双向连接对数量 | 方向比相关 | 高流量方向一致率 |
| --- | ---: | ---: | ---: | ---: | ---: |
{table}

在各版本共同的1,738条连接上，主样本相关为{common.loc['primary','spearman']:.3f}，严格短段口径为{common.loc['strict_250m','spearman']:.3f}，OSM支持的1000米口径为{common.loc['osm_1000m','spearman']:.3f}。匹配口径会影响数值；不能将更宽口径的相关提高全部解释为模型改进。

完整389份源文件共13,288,340行，工作日AM全RN有效经过合计744,502,916次。端点分属不同网格的经过为68,102,526次；端点属于模型已有有向连接、长度不限的经过为43,045,074次；主匹配保留25,535,214次，为后者的{conditional:.1%}，为全部跨网格经过的37.5%。主样本覆盖96.9%的模型passage，不能据此声称也覆盖96.9%的实际道路流量。

同格内部经过不应加进跨界总量。多条并行道路可共同贡献网格边界压力；本轮不把串联RN段的沿线计数求和称为独立行程。缺乏逐行程联合路线时，仍无法检验重复跨界和非最短路径造成的全部测量误差。

![相对流量份额差异地图]({OUT}/flow_share_gap_map.png)

地图颜色为共同支持集内log2(联通份额/模型份额)，双向连接按已有观测支持汇总至无向网格对。图中的线连接网格质心，不是实际道路；色标限制在±3，完整数值保留于比较表。地图表示相对份额差异，不是绝对误差或拥堵程度。

## 4. 输入独立性与解释边界

- 基线沿用gate5 anchored、lambda=1.1、既有模型Ξ，未重估模型或调整参数来提高相关。
- 模型职住/汽车通勤样本来自`commute_202211.csv`，主线stage08记录汽车口径；联通2025年mode=1为算法判定机动车，包含非通勤活动。
- 模型已使用2025年同期Speed速度构造成本；联通RN路线又是基于OSM的后加工路径，两侧共享路网环境。这是新增计数对象的外部一致性检查，不是完全独立的路网或速度测试。
- 源资料登记当前RN字典采用route_year=2026，当前OSM几何文件为2025年版本；存在网络版本差异。短距离吸附和路径筛查不能消除所有变化。
- 没有用联通2025年道路计数重新估计模型，也没有以流量高低筛选匹配。不存在在全部道路上先拟合每条比例再验证的做法。
- 无需训练缩放系数，本轮没有绝对水平预测检验。结果也不是政策因果效应或拥堵函数参数识别。

## 5. 已完成核验与复现

{len(checks)}项核验全部通过，包括389份源文件及总行数、RN×日期唯一性、缺失值保留、700条原始CSV的独立计数复算、独立排序相关公式，以及全部主样本保存的OSM路径方向和长度检查。源文件SHA-256、输入manifest、模型Ξ、RN crosswalk、恢复路径、每日比较和敏感性表均已保存。

复现脚本按顺序运行：

1. `data_work/src/validation/validate_unicom_model_flow.py`
2. `data_work/src/validation/audit_unicom_flow_validation.py`
3. `data_work/src/validation/write_unicom_flow_report.py`

输出：`data_work/outputs/unicom_model_flow_validation_v1/`。现有缓存用于快速复核；从源完整重建时，先将该生成目录改名保留，再运行上述脚本。输入文件和既有主线结果均只读，未运行平台SQL或新增下载。

详细口径与复现说明：`data_work/docs/unicom_flow_validation_20261029/`。本报告由已保存比较表生成；不需要重解均衡即可复核。

## 6. 后续优先级

第一优先核对方向性差异：在同一有向连接上报告主匹配相对全部RN跨界记录的覆盖率，区分双向测量不均、通勤/全部机动车差异和模型路由造成的差异。逐条物理道路验证应先完成模型到原始道路路径的流量分配，保持本次主结果作为固定基准。当前证据不支持直接据此重校准拥堵弹性、福利权重或选边规则。
'''
    report=ROOT/'Documents/L2_notes/unicom_model_flow_validation_results_20261029.md'
    report.write_text(text)
    print(report)


if __name__=='__main__':
    main()
