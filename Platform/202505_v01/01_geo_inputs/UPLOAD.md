# 原网格上传与空间测试

本地生成：`python3 Platform/utils/prepare_references.py`。manifest保存输入/输出SHA-256、数量、EPSG:3857原点和裁剪信息；不执行上传。

必须准备的公开文件：model_grid_wgs84_v01.csv（3357个原始裁剪网格）、model_nodes_gate5_v01.csv（1104节点）、model_links_gate5_v01.csv（2642连接）、synthetic_grid_fixture_v01.csv（7569个非个人点）。time_fixture用于纯时间测试。RN target文件来自已有观察字典，只是比较范围参考，不代表全部物理道路；不得据此静默截断正式RN分母。

原点由完整矩形格恢复：x=12848142.373323334、y=4785196.001162674，间距3000投影米。恢复算式仅用于索引或候选格；现用44个裁剪格必须做精确点落面。合成样例中裸floor归格与原裁剪多边形有1363处不一致，不能用公式替代边界。

上传后记录真实表名、字段类型、行数与哈希对应关系，再绑定TL25M05_V01_0020_grid_fixture.sql.in。fixture输出应包含每个fixture_id及唯一grid_id，无匹配用空值，不能丢掉无匹配fixture。

下载后运行：`python3 Platform/utils/validate_probe_result.py --kind geo --csv 实际路径`。全部fixture匹配后才用于个人点内部映射。可按row/col或bbox筛候选减少空间join，但精确多边形仍是最终判定。

Default.ST_GeomCollection/ST_Point/ST_Contains仅有团队示例依据；0009先核账号实际可用。若失败，先查现有空间函数/允许上传方式，再做等价实现和同一fixture验证，不能退回5公里质心分配。

## 上传方式

沿用SubwayMobility NG65U/NG67U已通过的SQL字面量上传做法：把公开表写成 `DROP TABLE IF EXISTS tl25m05_v01_<name>` 加 `CREATE TABLE … AS SELECT … FROM (SELECT stack(n, …) AS (…)) s`，第一段建表、后续段 `INSERT INTO`，每段约700行、约100KB，每段末尾一条计数SELECT核对累计行数。完整3000米格（2889个）只上传行列号与grid_id；WKT只上传468个裁剪格（其中44个在gate5内）。平台按EPSG:3857公式算出候选行列号，完整格直接归格，裁剪格再做ST_Contains。上传SQL由 `utils/build_geo_upload.py` 从reference_inputs生成（网格4段、fixture 5段，每段≤107KB）并写入queue，不手工编辑；`.sql.in` 模板已由0021取代。

验收：`validate_probe_result.py --kind geo`。距格边界小于1e-9度（约0.1毫米）的fixture按规格列为边界点，单独报告、不计入一致性判定；本地按同一SQL逻辑模拟时有22个此类点，其余7547个全部一致。
