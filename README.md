# Streaming Tick Aggregator

本地行情事件接入与时间序列聚合服务（Spring Boot 3 / Java 21，进程内存储）。
在事件**乱序、重复、延迟到达**的情况下，聚合与查询结果保持一致且可解释，并支持
长期在线运行所需的三件事：**超时迟到事件留存、放宽延迟后的可控补算、有界且可观测**。

## 构建与验证

```bash
mvn test          # 全部 31 个用例；日志逐条打印事件输入、判定结果与现场值
mvn spring-boot:run
```

手动验证：

```bash
# 1) 接入事件
curl -X POST localhost:8080/api/ticks -H 'Content-Type: application/json' -d '{
  "eventId":"e1","symbol":"BTCUSDT","sequence":1,
  "timestampEpochMs":1700000041000,"price":100.5,"volume":2.0}'

# 2) 查询聚合（includeUnconfirmed=false 时只返回已确认窗口）
curl 'localhost:8080/api/ticks/aggregates?symbol=BTCUSDT&from=1700000040000&to=1700000100000&includeUnconfirmed=true'

# 3) 查询被留存的超时迟到事件（按标的 + 事件时间半开区间 [from,to)）
curl 'localhost:8080/api/ticks/retained?symbol=BTCUSDT&from=1700000000000&to=1700000200000'

# 4) 运行时把允许延迟放宽到 120s（只允许放宽、不会收窄；带 symbol 只影响该标的）
curl -X POST 'localhost:8080/api/ticks/admin/lateness?allowedLatenessMs=120000&symbol=BTCUSDT'

# 5) 触发补算：把该标的留存的迟到事件按新延迟折回仍开放的窗口
curl -X POST 'localhost:8080/api/ticks/admin/recompute?symbol=BTCUSDT'

# 6) 随时查看每个标的的水位、窗口、去重/留存占用与各类计数
curl 'localhost:8080/api/ticks/stats?symbol=BTCUSDT'
curl 'localhost:8080/api/ticks/stats/all'
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

1. **幂等去重**：`eventId` 已计入某个**尚未封印**的窗口 → `DUPLICATE`
   （不改变任何状态，已确认结果不变）。窗口封印时其全部事件 id 被**整批回收**；
   之后该窗口事件的重放由规则 2 兜底——任何容量淘汰都不会让同一笔事件被计第二次。
2. **序列回退**：`sequence <= sequenceFloor`（已封印窗口中的最大 sequence）→
   `REJECTED / SEQUENCE_REGRESSION`。
3. **超时迟到**：`eventTs < watermark - allowedLatenessMs` →
   `REJECTED / LATE_BEYOND_ALLOWED_LATENESS`。事件不计入任何窗口，同时被**留存**
   （见下），拒绝原因与现场显式返回。
4. **接受**：归入 `floor(eventTs / windowSizeMs)` 对应的滚动窗口，并推进
   `watermark = max(watermark, eventTs)`。

字段缺失（`MISSING_FIELD`）、数值非法（`INVALID_VALUE`）、时间戳非法
（`INVALID_TIMESTAMP`）在进入聚合器之前由接入层校验拒绝。

## 超时迟到事件留存（dead-letter）

规则 3 拒绝的事件会写入该标的独立的**有界 FIFO 留存区**，每条记录包含：

- 原始内容：`eventId / sequence / eventTimestampEpochMs / price / volume`；
- 拒绝原因 `reason` 与人类可读判定说明 `decisionDetail`；
- 判定现场：`watermarkAtDecisionMs`、`latenessFloorAtDecisionMs`、
  `allowedLatenessAtDecisionMs`、`rejectedAtWallClockMs`；
- `incorporated`：是否已被补算折回。

`GET /api/ticks/retained?symbol=..&from=..&to=..` 按事件时间半开区间查询。
留存容量为 `max-retained-rejections-per-symbol`（默认 4096），超出后**最老的记录先被
淘汰**；同一 eventId 重复被拒不会产生重复记录。淘汰**只影响留存区自身**：不改窗口、
不改去重、不改已确认结果，因此留存查询不可能与已确认结果自相矛盾。容量设为 0 表示
不留存（拒绝行为本身不变）。

## 可控补算（recompute）

先通过 `POST /api/ticks/admin/lateness` 把允许延迟**放宽**（只能放宽；收窄请求被
忽略，因为更窄的视界会与已接受事件矛盾），再调用
`POST /api/ticks/admin/recompute?symbol=..`。补算在该标的的同一把状态锁内原子完成：

- **as-if / 顺序无关**：留存事件用与实时接入完全相同的、满足交换律的窗口累加逻辑
  折回其事件时间所属窗口，结果等于这些事件从一开始就在允许范围内到达；先折哪条后折
  哪条不影响最终结果（有随机顺序对照用例）。
- **已对外确认的结果不可改写**：窗口第一次满足确认条件时被**永久封印（sealed）**，
  补算对封印窗口一律跳过（计入 `skippedWindowSealed`）；放宽延迟也不会把已确认窗口
  "翻回"未确认。
- **不重复计算**：折回的事件 id 进入去重索引（在其窗口封印前重放仍返回 DUPLICATE），
  留存记录被标记 `incorporated`；若该事件后来已通过实时接入进入窗口，记为
  `skippedDuplicate`。
- **重复触发稳定**：对同一批留存再次补算，全部落入 `skippedAlreadyIncorporated`，
  聚合结果逐字节不变。
- **期间服务不中断**：补算与接入、查询共用同一把锁，查询在锁内取不可变快照，
  看不到折到一半的中间态。

响应 `RecomputeOutcome`：`candidates / incorporated / skippedWindowSealed /
skippedAlreadyIncorporated / skippedDuplicate` 以及本次折回的事件明细。

> 说明：补算只填补"拒绝发生时其窗口尚开放"的事件。拒绝时窗口已确认（已发布）的事件，
> 即使放宽延迟也不会改写历史结果——这是"已确认结果不可变"的直接推论。这类事件保留在
> 留存区可查，补算时计入 `skippedWindowSealed`。

## 窗口与水位规则

- **滚动窗口**：按事件时间对齐到 `windowSizeMs` 的整数倍边界，输出 OHLC + 成交量 +
  tick 数。open/close 取窗口内事件时间最早/最晚的 tick（同时间戳按 sequence 决胜），
  与到达顺序无关。
- **水位线**：`watermark = 已接受事件的最大事件时间`（按 symbol 独立维护）。补算不
  回退水位（留存事件都老于拒绝时的水位）。
- **窗口确认 / 封印**：当 `watermark - allowedLatenessMs >= windowEnd` 时窗口确认；
  首次确认即封印，此后不可变。查询中封印窗口的 `confirmed=true` 不会因放宽延迟而翻转。

## 并发、内存与可观测性

- 每个 symbol 一个 `SymbolWindows` 状态机，其监视器锁串行化该 symbol 的接入、查询、
  留存查询、补算与统计；所有读操作在锁内生成不可变快照，**读不会看到中间态**。
- 内存长期有界：
  - 窗口数 ≤ `max-retained-windows-per-symbol`，且只淘汰已封印窗口；
  - 去重索引**不做 FIFO 淘汰**（否则被淘汰 id 的重放可能在其开放窗口内被计两次），
    而是随窗口封印整批回收。稳态下只保留水位视界内未封印窗口的事件 id，窗口随水位
    推进必然封印，因此占用长期封顶；瞬时峰值 = 当前未封印窗口内的事件数。
    `max-dedup-entries-per-symbol` 仅为配置兼容保留，不再触发淘汰；
  - 留存区 ≤ `max-retained-rejections-per-symbol`，独立 FIFO 淘汰。
  - 因此即便各类容量调到很小、投递量远超上限，**同一笔事件重复投递最多被计一次**，
    容量淘汰不会把它当成新事件，也不会改掉已确认结果（见
    `BoundedRedeliveryTest`：留存打满、窗口打满、并发重复风暴三类场景）。
- `GET /api/ticks/stats?symbol=..`（未知标的返回 404）与
  `GET /api/ticks/stats/all` 返回点时一致快照：`watermarkMs / sequenceFloor /
  allowedLatenessMs / retainedWindows / sealedWindows / dedupEntries /
  retainedRejections / acceptedCount / duplicateCount / rejectedLateCount /
  rejectedSequenceRegressionCount / retainedRejectionEvictedCount / incorporatedCount`。

## 可配置项（`application.properties`，前缀 `tick.aggregator`）

| 配置 | 默认 | 含义 |
|---|---|---|
| `window-size-ms` | 60000 | 滚动窗口大小（毫秒） |
| `allowed-lateness-ms` | 30000 | 初始允许的最大迟到；运行时只能通过管理接口放宽 |
| `max-retained-windows-per-symbol` | 1024 | 每 symbol 最多保留的窗口数（仅淘汰已封印窗口） |
| `max-retained-rejections-per-symbol` | 4096 | 每 symbol 留存区容量（FIFO 淘汰，0=不留存） |
| `max-dedup-entries-per-symbol` | 65536 | 已弃用，仅配置兼容；去重随窗口封印回收，不再 FIFO 淘汰 |

## 测试覆盖（31 个用例）

- `TickIngestionServiceTest`（7）：字段缺失、非法时间戳/数值、重复投递幂等、序列回退。
- `WindowAggregationTest`（6）：乱序归窗、任意顺序收敛、重复不改聚合、超时迟到拒绝
  不污染窗口、迟到边界、窗口确认边界。
- `RetainedRejectionTest`（3）：留存按标的+时间范围查询且携带判定现场、FIFO 容量淘汰
  只影响留存本身不碰窗口结果、同 id 重复被拒不产生重复记录。
- `RecomputeTest`（5）：放宽延迟后补算等于按时到达（as-if）、补算顺序无关、已封印
  窗口绝不被改写、重复补算幂等稳定且补算后实时接入/去重照常。
- `BoundedRedeliveryTest`（4）：留存容量打满后旧事件重放不复活、窗口容量打满后
  重放不计第二次、极小窗口容量下并发重复风暴每事件恰好计一次且读者不见中间态、
  stats 快照计数与占用正确。
- `ConcurrencyTest`（3）：并发读写一致、并发重复风暴、窗口保留有界。
- `TickApiIntegrationTest`（1）/ `RecomputeApiIntegrationTest`（1）：HTTP 端到端，
  含"拒绝→留存→放宽延迟→补算→再补算幂等→stats"全链路。

所有测试日志逐条打印事件输入与判定结果（`status`/`rejectionReason`/`detail`、
留存现场、补算 outcome、stats），失败时可直接对上判定现场。失败现场可用
`mvn test -Dtest=类名#方法名` 单独复现。
