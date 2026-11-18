# 202505_v01运行队列

机器状态唯一来源是 [queue.json](queue.json)，真实过程写 [run-log](../runs/202505_v01/run-log.md)。共20项。0001–0005与0007已由Speed既有导出回答（satisfied_by_reference，路径与哈希写在queue.json），不再提交；0006、0008、0009、0010已于2026-10-30提交；其余为带依赖的计划。

| 编号 | 工作 | 进入条件 |
| --- | --- | --- |
| 0001–0005 | 字段名 | 已由Speed `00_schema_audit/exports/00_columns` 回答（只有字段名，没有类型）。move_month同时有move_id与moi_id；move_rn只有moi_id |
| 0006 | 20250519 move_month按原始stime小时的计数、两种时钟显示、moi_id=move_id比例、start/end_poi缺失与0值、mode1数 | 防火墙改写为sum(if)；SubwayMobility NG03已证20250506 trip键唯一且无冲突 |
| 0007 | move_rn覆盖与时间范围 | 已由Speed 01库存、04时间、PLATFORM_NOTES回答：全月31日；RN time为秒级Unix，显示与trip时钟不同步，不任意加8小时 |
| 0008 | 合成07/09、17/19边界与10/30分钟槽 | 不依赖个人数据 |
| 0009 | 公开多边形内部/边界ST_Contains | SubwayMobility TW_G01C已用Default.ST_GeomCollection；ST_Contains仍需本账号实测 |
| 0010 | Speed UID级passage表（19日all、src20250520）是否仍在及日期范围 | 只读；决定0140是复用Speed passage还是重扫42亿行/日级move_rn |
| 0020 | 原网格和合成fixture上传与精确匹配 | 0009、真实上传表/schema、fixture零误配 |
| 0100 | 独立月度person与主点 | attr/POI/schema、0020 |
| 0110 | trip去重、purpose、departure_slot | move schema/时刻、0100；0008必须匹配fixture |
| 0120 | 月度joint HW人口 | 0100/0110 |
| 0130 | 分时OD和每日参与 | 0110；端点分为主点/实际trip |
| 0140 | RN trip连接与经过时钟 | 0004/7/10、0110；优先以uid+date+moi_id把本项目trip/purpose接到Speed passage，先测覆盖率与Speed候选过滤造成的缺口 |
| 0150 | 同样本purpose RN压力 | 0140，不以模型flow筛记录 |
| 0300 | 闭合与损失QC | N、Q、RN内部表 |
| 0500 | 单日pilot容量与分片 | 0300 |
| 0400 | 冻结聚合导出 | 0500 |

状态含义：not_run、submitted、running、passed、failed、blocked_dependency、satisfied_by_reference（已有项目导出回答同一问题，不再提交）。passed必须有实际结果、任务ID、输出路径和对应QC；本地检查不能推进为passed。读字段与函数探针可以在同一已登录会话内依次执行；不得用计划顺序代替真实依赖。

SQL改动后更新对应文件哈希。提交时复制确切单语句，不把说明段/模板变量提交到平台。模板实例化后登记新文件与真实来源，不手工跳过检查。
