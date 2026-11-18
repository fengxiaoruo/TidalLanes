# 独立内部表构造规则

本目录当前是构造合同，尚无已运行的生产SQL。上游核实后用tl25m05_v01_独立表名和CREATE生成；不复制源项目DROP语句。

## 0100 月度人和地点

user_attribute按(city,date,uid)唯一，POI按(city,month,uid,poi_id)唯一；重复完全一致可折叠，冲突单列，不按列MIN拼接。使用ptype=1/2，poi_id=0有效。主点按已验证days_poi/停留证据选取，实质并列留诊断；工作地不因同格被清空。

保留R旗标、has_home/work、坐标/类型/并列、是否在原网格和模型mask。样本人数为SIM，不称物理人口。主join内的有效家班人数是同一cohort，N的行列边际由其joint表导出。

## 0110 trip和地点关联

按(uid,date,move_id)检查各实质字段一致后折叠；冲突键不入主样本，逐日输出影响。分别保留raw/valid/model_eligible，时长距离>0与≤200km/h为候选有效门槛，缺失/零/负/异常及native格缺失分别QC，不预设500米或职住1公里下限。

purpose取原始start_ptype/end_ptype；时刻用原始stime半开分箱，并保留etime跨日QC，不把timestamp转Unix后再无依据加减8小时。

长期主点HW/WH的grid_o/d随出行方向反转，标od_basis=primary_hw。实际trip端点按同城同月同uid的poi_id关联坐标，标od_basis=trip_endpoint；该路线若未通过schema/唯一性/覆盖审计，不能伪造实际端点。多家/多工作地trip端点与主点差异单列。

## 0120/0130 N、Q和参与

N为固定joint cohort的月度不同uid计数。另留有HW/WH行为、mode1候选通勤者和day5旗标，避免以一条trip定义所有就业者。逐日出发次数Q与唯一参与人数/人日分开。

30分钟、60分钟及2小时人数都在平台相应单元重新去重，不加总小槽distinct。全方式、R主样本及全体clean mode1分别有sample_id；mode1保留是否地铁检出。unknown不并入非通勤或汽车。

## 0140/0150 RN

优先核对Speed已验证源日passage实现，若只读复用需冻结来源合同。生产采用本项目trip universe关联，不修改Speed表。

RN候选键(uid,date,moi_id↔move_id)只是已验证方案的候选，仍核当前数据期一致性、rn_seq重复、route_id多候选、is_end和next_rn_id。不能仅挑第一条route或删掉差的路径来提高相关。

经过日依据RN时刻独立生成，并核邻近来源日对目标周的贡献。先pilot单来源日，再延伸来源窗口直到目标经过日覆盖可解释；无需人为让RN首末节点等于trip时刻。

计数关系为raw transitions≥valid transitions≥cell-unique trips≥cell-unique people。trip键含来源日期，跨日不能只按uid+move_id去重。每次合法重复经过保留在transition计数，不能拿去重trip数替代路径经过。

可以先全RN覆盖QC，再导出已声明的目标模型范围与更宽范围摘要；RN未匹配、mask外、部分路线覆盖的损失各有分母。未知purpose与join失败单列，不以“全部经过−已识别通勤”自动叫非通勤。
