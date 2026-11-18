# Platform执行规则

适用TidalLanes本目录。先读项目根AGENTS、writing、Platform/current-spec、该批queue.json和唯一run-log。

- 本项目数据提取已有用户授权。按已授权范围执行常规只读、独立内部表构造与聚合验收；不为这些动作新增确认步骤。
- 先查已有任务与记录，再执行依赖已满足的任务。超时后先核任务状态，不盲目重交。没有实际提交/通过，不得标submitted/passed。
- 只写独立 `tl25m05_v01_` 表。不得DROP、覆盖或追加SubwayMobility、Speed及其他项目前缀的表。已跑过的本项目表也不自动DROP；新算法用新版本。
- 生产构造从原表独立生成，已有项目内部表只读作benchmark。确需复用内部表时先记录schema、内容QC、来源SQL/哈希与数据期，禁止依赖会随其他任务变化的上游。
- SQL只提交单语句或逐语句明确顺序，探针文件无注释。`.sql.in`不能执行；绑定实际参数后保存具体SQL和哈希，检查依赖再运行。
- UID、UID hash、用户坐标、个人行程/轨迹只在平台内部使用，不导出。这里只保存审核允许的聚合；合成空间fixture编号不是个人标识。
- 探针只返回聚合或公共样例。若SHOW或函数被拒，记录真实错误，换可用的元数据入口；不把未验证字段填成猜测的别名。
- API key/登录信息只使用既有安全环境，不写入文件、SQL、日志或提交快照。平台请求只经 `utils/daas_task.py` 在已登录Chrome标签页内发出；登录过期（LOGIN_EXPIRED）时停下等用户重新登录，不另存token。
- 平台防火墙（SubwayMobility 2026-09至10实测）会拦截：顶层WITH、SELECT *、uid IS [NOT] NULL、`--`或`#`注释、DESCRIBE、sum(CASE WHEN…)（改用sum(if(…))）、1=1、同一行的UNION ALL SELECT。每个任务只有一条最终SELECT；事实表必须带city与明确日期。lint已写入daas_task.py与check_workspace.py。
- 账号共享Hive存储配额（Speed 2026-10-15曾因RN候选表满额失败）。不复制整周/整月RN长表；UID级内部表建前估算体量，验收后只DROP本项目已验收的中间表并记入run-log。
- 共享库只读，不自动同步新代码/SQL/数据到ChinaUnicomProjects。仅在本项目保存聚合输出，不通过来源项目符号链接覆盖文件。
- query-results原样保留；派生验收放checks，模型适配器放data_work。输入、输出、过滤损失、日期和计数单位始终可追溯。
- queue.json是状态单一来源；RUN_QUEUE.md解释依赖，run-log记录真实过程。平台task ID、显示时区、SQL哈希、输出路径/行数、QC和错误均写同一日志。
- 任何新版本改变时窗、mode、样本或空间分辨率，先更新规格、版本和来源记录，不改贴历史结果。
