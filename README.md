# 温室灌溉决策服务

把种植区、作物生育期、传感器质量、蒸散估算与水肥配方组织成**可追溯的灌溉计划**，
并在乱序遥测、跨午夜取水、共用管线并发、传感器降级等现场条件下保证**水量守恒**。

## 解决的现场问题

- **探头延迟补报 → 不再重复浇已湿畦面**：遥测一律按 `occurred_at`（业务时间）定序，
  决策只使用决策时点之前最新鲜的读数；计划发布后到达的迟到读数只重算**尚未锁定**的时段，
  已签发/已执行的时段不可回改、不二次扣水。
- **取水额度不可突破**：命令**签发即预占**当日额度，关阀按实测流量×历时结算、
  余额当日释放；重复执据、失联、人工停灌都不会二次扣水。
- **人工优先**：`/manual-stop` 立即结算活动命令（未确认开阀按 0 L）并置阀门接管，
  接管期间自动指令一律被拦截，直到 `/valves/{id}/resume`。
- **并发开阀**：同 `shared_line` 阀门强制串行排产，异管线可并行；
  阀门实际超时占用时，同线伙伴排队等待，关阀后下一节拍才放行。
- **重启续跑**：所有状态变化写 JSONL 事件日志，重启重放后从同一计划继续，
  包括执据去重集合。

## 运行

```bash
python3 -m unittest discover -s tests     # 22 个核对测试
python3 -m irrigation \
  --domain reference/domain.json \
  --state data/events.jsonl \
  --host 0.0.0.0 --port 8080
```

零第三方依赖（仅标准库，Python ≥ 3.11）。冷启动若状态文件为空，会把资料包
`telemetry` 作为历史读数导入；之后启动从事件日志重放。

## 领域约定（`reference/domain.json`）

- 时间：带偏移量的 ISO 8601；水量升；流量 L/min；配额按 `Asia/Shanghai` 本地日期分桶。
- 遥测质量：`good` / `suspect`（仍可用于决策，记入审计）/ `offline`。
- 好读数超过新鲜度窗口（默认 240 分钟）视为 `stale`，与 offline 一样走降级维持量。
- 传感器降级时不使用任何含水率，改用保守蒸散维持量，并**扣除当日已浇水量**。

## 决策与追溯

每个时段携带结构化 `reason`：触发因子、依据的读数事件 id 与质量、ET0/Kc、
亏缺换算（mm）、单次/阶段封顶、额定流量与水肥投加克数——回答主管问的"**为何浇**"。

水量换算：`1 mm 水深 × 面积(m²) = 1 L`；补水量 = 含水率亏缺 × 根深(mm) × 面积，
依次受**单次封顶**、**作物阶段日总量封顶**约束。

## HTTP 接口

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/readings` | 上报遥测（支持批量，自动按业务时间定序） |
| POST | `/et0?date=` | 录入当日参考蒸散 |
| POST | `/plans?date=` | 生成预演计划（proposed） |
| POST | `/preview?date=` | 用水预演，不落任何状态 |
| POST | `/plans/{id}/thresholds` | 农艺师修订阈值（发布前后均可） |
| POST | `/plans/{id}/publish` | 发布计划 |
| POST | `/plans/{id}/revise` | 吸收迟到读数，只改未锁定时段 |
| GET  | `/plans/{id}` | 计划明细（为何浇、依据哪条读数） |
| POST | `/tick` | 控制器节拍：到时签发 + 失联巡检 |
| POST | `/receipts` | 阀门回执（`receipt_id` 幂等去重） |
| POST | `/manual-stop` | 人工停灌并接管阀门 |
| POST | `/valves/{id}/resume` | 解除接管 |
| GET  | `/commands/pending` | 待确认命令 |
| GET  | `/quota?date=` | 当日配额台账（已结/预占/剩余/命令清单） |
| GET  | `/zones`、`/status` | 各区状态与主管总览 |

## 典型时序

```
ingest_readings → generate_plan（预演）→ update_thresholds（农艺师修订）
→ preview（用水预演）→ publish_plan
→ dispatch_due（签发、预占配额、管线门控）
   ├─ 迟到读数 → revise_plan（只改未锁定时段）
   ├─ open/close 回执 → 按实结算（重复执据忽略）
   ├─ 超时无回执 → reconcile 判失联，全额释放预占
   └─ manual_stop → 立即压过自动 + 阀门接管
```

## 代码结构

```
irrigation/
  clock.py    时区/时间语义（业务时间 vs 接收时间、本地日期分桶）
  config.py   分区、阀门、阈值、阶段系数、水肥配方（从 domain.json 加载）
  models.py   Reading / Slot / Command 及状态机
  et.py       蒸散折算与单时段决策（输出可追溯 reason）
  quota.py    跨午夜配额台账（预占/结算两段式，命令幂等）
  store.py    JSONL 事件日志（追加 + 重放）
  service.py  计划生命周期、迟到重算、签发/回执/失联/人工、并发门控
  api.py      标准库 HTTP 适配层
  __main__.py 启动入口
```
