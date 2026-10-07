# 燃气管线泄漏检测与隔离协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8333`。

- `app.py`：参数、依赖和服务生命周期。
- `src/domain.py`：管段、传感值和来源记录校验。
- `src/rules.py`：泄漏评分、阀门顺序、修复、试压、恢复状态机。
- `src/repository.py`：SQLite、重复保护、乐观版本、阀门占用与排队、审计链。
- `src/service.py`：角色权限和业务编排。
- `src/http_api.py`：JSON 接口与首页。
- `src/audit.py`：可校验的审计事件。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8333 --crew-capacity 2
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。

## 抢修排班与阀门资源

- 阀门是互斥资源：一个阀门同一时间只归一个隔离方案。`isolate` 在单个事务内按上游到下游顺序逐个占位；任一阀门被占则整体失败（409 `valve_unavailable`），报错列出被占阀门及持有工单，不会出现两个方案各关一半。
- 属地释放（`restore`）前禁止越权抢占：带 `force`/`preempt` 的请求一律拒绝（403 `preempt_forbidden`）。
- 班组容量由 `--crew-capacity` 控制（默认 2）。容量满员时方案进入队列（状态 `queued`）；有工单恢复供气释放阀门后，按提交顺序自动晋级，轮到时按当时阀门状态判断：阀门仍被占则方案驳回，工单回到 `verified` 并在 `isolation_rejection` 中记录被占阀门，可修改方案后重新提交。
- 占用（`valve_locks`）与排队顺序（`work_queue`）持久化在 SQLite，重启后自动读回；同一工单重复提交同一阀门顺序是幂等操作，不重复占位、不重复排队。
- `GET /api/state` 返回 `valve_locks`、`queue` 与 `crew_capacity`；工单详情带 `valves_held` 与 `queue_position`。

测试覆盖完整抢修流程、重复事件、阀门顺序、阀门互斥与抢占拒绝、排队晋级与驳回、重启读回、并发占位、试压阈值、现场危险条件、权限和版本冲突。模型不替代 SCADA、管网水力计算或正式应急预案。
