# Streaming Tick Aggregator

本地行情事件接入与时间序列聚合服务（Spring Boot 3 / Java 21，进程内存储）。
在事件**乱序、重复、延迟到达**的情况下，聚合与查询结果保持一致且可解释；
支持**拒绝留存、可控补算与运行统计**，可长期在线运行且内存有界。

## 构建与验证

```bash
mvn test          # 运行全部测试（28 个用例，日志打印每条事件输入与判定依据）
mvn spring-boot:run
```

手动验证：

```bash
# 接入事件
curl -X POST localhost:8080/api/ticks -H 'Content-Type: application/json' -d '{
  "eventId":"e1","symbol":"BTCUSDT","sequence":1,
  "timestampEpochMs":1700000041000,"price":100.5,"volume":2.0}'

# 查询聚合（includeUnconfirmed=false 时只返回已确认窗口）
curl 'localhost:8080/api/ticks/aggregates?symbol=BTCUSDT&from=1700000040000&to=1700000100000&includeUnconfirmed=true'

# 查询留存下来的被拒事件（按事件时间范围过滤）
curl 'localhost:8080/api/ticks/rejections?symbol=BTCUSDT&from=0&to=4102444800000'

# 放宽允许延迟并触发补算（allowedLatenessMs 可省略，省略则按当前配置补算）
curl -X POST 'localhost:8080/api/ticks/reprocess?symbol=BTCUSDT&allowedLatenessMs=120000'

# 查看运行统计（symbol 可省略，省略返回全部标的）
curl 'localhost:8080/api/ticks/stats?symbol=BTCUSDT'
```

## 接入协议

`POST /api/ticks`，请求体为单个事件（JSON）：

| 字段 | 类型 | 约束 |
|---|---|---|
| `eventId` | string | 必填，同一 symbol 内唯一，用于幂等去重 |
| `symbol` | string | 必填，非空白 |
| `sequence` | long | 必填，源端按 symbol 单调递增的流序号 |
| `timestampEpochMs` | long | 必填，事件时间，需在 `(0, 4102444800000]`（2100-01-01）内 |
| `price` / `volume` | double | 必填，正的有限数值 |

响应 `IngestionResult`：`status` ∈ `ACCEPTED | DUPLICATE | REJECTED`，
`rejectionReason`（仅 REJECTED 时非空）与 `detail`（人类可读的判定依据，含
watermark、sequenceFloor 等现场值）。判定按以下**固定顺序**执行，首个命中的规则决定结果，
因此任何事件的判定都是确定且可解释的：

1. **幂等去重**：`eventId` 已被接受过 → `DUPLICATE`（不改变任何状态，已确认结果不变）。
2. **序列回退**：`sequence <= sequenceFloor`（已确认窗口中的最大 sequence）→
   `REJECTED / SEQUENCE_REGRESSION`。
3. **超时迟到**：`eventTs < watermark - allowedLatenessMs` →
   `REJECTED / LATE_BEYOND_ALLOWED_LATENESS`（不计入任何窗口，但会进入拒绝留存，见下）。
4. **窗口已关闭**：目标窗口已确认（`windowEnd <= confirmedHorizon`）→
   `REJECTED / WINDOW_CLOSED`。已确认结果不可变，即使放宽延迟配置也不会被改写。
5. **接受**：归入 `floor(eventTs / windowSizeMs)` 对应的滚动窗口，并推进
   `watermark = max(watermark, eventTs)`。

字段缺失（`MISSING_FIELD`）、数值非法（`INVALID_VALUE`）、时间戳非法
（`INVALID_TIMESTAMP`）在进入聚合器之前由接入层校验拒绝；这类结构性非法事件
**不会**进入留存（它们永远不可能变合法）。

## 窗口与延迟判定规则

- **滚动窗口**：按事件时间对齐到 `windowSizeMs` 的整数倍边界，输出 OHLC + 成交量 +
  tick 数。open/close 取窗口内事件时间最早/最晚的 tick（同时间戳按 sequence 决胜），
  因此**与到达顺序无关**：同一批在允许迟到范围内的事件，任意到达顺序收敛到同一结果。
- **水位线**：`watermark = 已接受事件的最大事件时间`（按 symbol 独立维护）。
- **允许乱序**：事件时间落后水位线不超过 `allowedLatenessMs` 的事件仍被接受并归入正确窗口；
  超出者被显式拒绝（规则 3），不会计入错误窗口。
- **窗口确认（粘性）**：`confirmedHorizon = 历史最大 (watermark - allowedLatenessMs)`，
  只前进不后退。当 `windowEnd <= confirmedHorizon` 时窗口**确认（confirmed）**，此后不可变；
  即使运行时把 `allowedLatenessMs` 调大，确认边界也**不会回退**——已对外确认的结果永远稳定。
  查询默认只返回已确认窗口，`includeUnconfirmed=true` 时才返回进行中的窗口（`confirmed=false` 标记）。
- **序列语义**：`sequence` 是源端的流完整性序号。允许乱序的是**事件时间**；
  序列回退仅在相对**已确认**窗口判定时拒绝（规则 2），因此水位线 horizon 内的
  重排序不会被误判为回退，而重放/损坏的流会被确定性拒绝。

## 拒绝留存（rejection retention）

聚合层拒绝的事件（规则 2/3/4）会被**单独留存**，可按标的与事件时间范围查询：

```
GET /api/ticks/rejections?symbol=BTCUSDT&from=<epochMs>&to=<epochMs>
```

每条留存记录（`RejectedTickRecord`）包含：

- `event`：原始事件全文；
- `reason` / `detail`：被拒原因与判定依据；
- `watermarkMs` / `sequenceFloor` / `allowedLatenessMs`：判定当时的现场信息；
- `rejectedAtEpochMs`：判定的墙钟时间。

边界与淘汰规则：

- 留存按 symbol 独立，上限 `max-retained-rejections-per-symbol`（默认 4096），
  超出时 **FIFO 淘汰最老记录**（淘汰计数见 stats 的 `rejectionsEvictedTotal`）。
- 留存是一个**旁路存储**：写入与淘汰只影响留存自身，绝不触碰窗口、去重表或水位线。
  即使把上限压到很小，已确认的窗口结果也不会因此改变，留存查询与聚合查询也不会
  互相矛盾（两者都在同一把 symbol 锁内生成一致快照）。
- 接入层校验拒绝（字段缺失等）不留存——它们不可能通过补算变合法。

## 可控补算（reprocess）

事后放宽允许延迟后，可把留存的事件重新纳入聚合：

```
POST /api/ticks/reprocess?symbol=BTCUSDT&allowedLatenessMs=120000
```

- `allowedLatenessMs` 可选：给出时先把**全服务**的允许延迟调整为该值，再触发补算；
  省略时按当前配置补算。`symbol` 可选，省略时对所有标的逐个补算。
- 每条留存事件**走与实时接入完全相同的判定路径**，因此：
  - **幂等**：已被接受过的事件命中去重，不会被算两遍；对同一批留存重复触发补算，
    结果稳定不变（返回的 `scanned/accepted/stillRejected` 可观察每轮效果）；
  - **顺序无关**：补算结果与留存事件的重放顺序无关，且与"这些事件一开始就在
    允许范围内到达"的结果完全一致（窗口聚合本身与到达顺序无关）；
  - **已确认不改写**：目标窗口已确认的事件被判定为 `WINDOW_CLOSED` 并带新现场
    重新留存，已确认结果保持不变；
  - **不阻塞在线流量**：补算逐条处理、条与条之间释放锁，补算期间新的接入与查询
    照常工作；每条事件的应用是原子的，查询永远看不到算到一半的中间状态。
- 响应 `ReprocessResult`：`scanned`（本轮扫描的留存条数）、`accepted`、
  `alreadyCounted`（已被实时流接受过的）、`stillRejected` 及按原因分布。

## 运行统计（stats）

```
GET /api/ticks/stats?symbol=BTCUSDT   # symbol 省略时返回全部标的
```

每个标的返回一份**一致快照**（在 symbol 锁内组装，不会看到中间态，也不会卡住写入——
锁只在拷贝期间持有）：`watermarkMs`、`sequenceFloor`、`confirmedHorizonMs`、
`allowedLatenessMs`、`retainedWindows` / `confirmedWindows`、`dedupEntries`、
`retainedRejections`、`acceptedTotal` / `duplicateTotal`、
`rejectionsByReason`（各类拒绝计数）、`windowsEvictedTotal` / `dedupEvictedTotal` /
`rejectionsEvictedTotal`（各类淘汰计数）、`reprocessRunsTotal` / `reprocessAcceptedTotal`。

## 并发与内存

- 每个 symbol 一个 `SymbolWindows` 状态机，其监视器锁串行化该 symbol 的写入、查询、
  留存与补算；查询在锁内生成不可变快照，**不会暴露中间态**。
- **窗口数**上限 `maxRetainedWindowsPerSymbol`，仅淘汰**已确认**的最老窗口。
- **去重表**记录 `eventId -> 所属窗口`，上限 `maxDedupEntriesPerSymbol`；只淘汰
  **所属窗口已确认**的条目——此类事件的重投会被规则 4 确定性拒绝，淘汰不会导致
  重复计数。属于未确认窗口的条目是"被钉住的"：即使投递量超过上限也**不会**被淘汰，
  这是"同一笔事件只算一次"在容量打满时依然成立的保证；窗口确认后这些条目重新变得
  可淘汰，长期运行下占用收敛回上限以内。
- **拒绝留存**上限 `maxRetainedRejectionsPerSymbol`，FIFO 淘汰，与聚合状态完全隔离。
- 拒绝路径不修改任何聚合状态；只有聚合成功后才推进去重表、序列与水位线。
- 边界说明：若水位线长期不推进而新事件持续涌入，被钉住的去重条目会随未确认窗口内的
  去重事件数增长（任何 exactly-once 系统都必须记住这些事件）；水位线恢复推进后即收敛。
  symbol 基数本身不设上限（每个新 symbol 一份状态机）。

## 可配置项（`application.properties`，前缀 `tick.aggregator`）

| 配置 | 默认 | 含义 |
|---|---|---|
| `window-size-ms` | 60000 | 滚动窗口大小（毫秒） |
| `allowed-lateness-ms` | 30000 | 允许的最大迟到（相对水位线） |
| `max-retained-windows-per-symbol` | 1024 | 每 symbol 最多保留的窗口数 |
| `max-dedup-entries-per-symbol` | 65536 | 每 symbol 去重表上限（仅淘汰已确认窗口的条目） |
| `max-retained-rejections-per-symbol` | 4096 | 每 symbol 拒绝留存上限（FIFO 淘汰） |

## 测试覆盖

- `TickIngestionServiceTest`：字段缺失、非法时间戳/数值、重复投递幂等、序列回退，
  每类拒绝原因可区分。
- `WindowAggregationTest`：乱序归入正确窗口、同批事件任意顺序收敛同一结果、
  重复投递不改变聚合、超时迟到显式拒绝且不污染窗口、迟到边界、窗口确认边界。
- `RejectionRetentionTest`：拒绝留存携带完整判定现场、按时间范围查询、校验拒绝不留存、
  留存上限 FIFO 淘汰且淘汰不影响已确认结果。
- `ReprocessTest`：放宽延迟后补算结果与"从未迟到"的参考完全一致、已确认窗口不被改写
  （`WINDOW_CLOSED`）、重复触发幂等稳定、补算顺序无关、补算与实时写入/查询并发安全。
- `CapacitySafetyTest`：去重容量打满时重复投递仍只算一次、被钉住条目不被淘汰、
  容量淘汰后重投被确定性拒绝且已确认结果不变、stats 快照各字段与运行状态一致。
- `ConcurrencyTest`：多线程写入 + 并发查询（读者永不见中间态）、并发重复风暴
  （每事件恰好计数一次）、窗口保留有界。
- `TickApiIntegrationTest` / `OpsApiIntegrationTest`：HTTP 端到端（接入→去重→拒绝→查询；
  留存查询→放宽延迟补算→统计）。

所有测试日志均打印事件输入与判定依据（`status`/`rejectionReason`/`detail`），
新增场景的每条留存记录与每轮补算结果（`scanned/accepted/stillRejected`）也会打印，
失败时可直接定位判定现场。
