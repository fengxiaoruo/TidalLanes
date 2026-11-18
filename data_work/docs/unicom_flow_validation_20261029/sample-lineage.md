# 样本来源与合并

1. 读取2642条primitives edge、1104个node和保存的baseline endogenous Xi数组，按原写出行序连接。
2. 全周389份正式CSV，共13288340行；只读取block1的s06至s17。源表和AM聚合均检查date/RN pair相关键唯一性。工作日AM有2579260个date/RN记录，部分记录AM槽全空。
3. RN几何字典621376对，按rn_pair_id一对一。坐标空间连接使用原始完整网格多边形，within规则；边界点不任意归格。
4. 模型grid_o/grid_d唯一；RN到模型many-to-one，daily到crosswalk many-to-one，不展开复制计数。
5. 主样本14317个几何合格RN pair，对应2277条模型link；其中有2198条模型link在五个工作日均有有效计数；双向都在该平衡样本中的无向对964个。
6. RN pair来源内的有效经过允许重复；不同RN pair汇总为边界压力。无法在聚合数据上重新做全路径去重。

observation_coverage_stages.csv列示全部RN、跨格、模型目标pair、距离限制与主匹配的计数损失；sample_coverage.csv同时列示模型passage与长度覆盖。两侧分母不互换。
