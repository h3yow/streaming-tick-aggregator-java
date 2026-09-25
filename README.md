# Streaming Tick Aggregator

本地行情事件接入与时间序列聚合服务（Spring Boot 3 / Java 21，进程内存储）。
在事件**乱序、重复、延迟到达**的情况下，聚合与查询结果保持一致且可解释。

## 构建与验证

```bash
mvn test          # 运行全部测试（18 个用例，日志打印每条事件输入与判定依据）
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
   `REJECTED / LATE_BEYOND_ALLOWED_LATENESS`（不计入任何窗口，也不静默丢弃——拒绝原因显式返回）。
4. **接受**：归入 `floor(eventTs / windowSizeMs)` 对应的滚动窗口，并推进
   `watermark = max(watermark, eventTs)`。

字段缺失（`MISSING_FIELD`）、数值非法（`INVALID_VALUE`）、时间戳非法
（`INVALID_TIMESTAMP`）在进入聚合器之前由接入层校验拒绝。

## 窗口与延迟判定规则

- **滚动窗口**：按事件时间对齐到 `windowSizeMs` 的整数倍边界，输出 OHLC + 成交量 +
  tick 数。open/close 取窗口内事件时间最早/最晚的 tick（同时间戳按 sequence 决胜），
  因此**与到达顺序无关**：同一批在允许迟到范围内的事件，任意到达顺序收敛到同一结果。
- **水位线**：`watermark = 已接受事件的最大事件时间`（按 symbol 独立维护）。
- **允许乱序**：事件时间落后水位线不超过 `allowedLatenessMs` 的事件仍被接受并归入正确窗口；
  超出者被显式拒绝（规则 3），不会计入错误窗口。
- **窗口确认**：当 `watermark - allowedLatenessMs >= windowEnd` 时窗口**确认（confirmed）**，
  此后不可变；确认边界完全由 watermark 推导，可复现。查询默认只返回已确认窗口，
  `includeUnconfirmed=true` 时才返回进行中的窗口（`confirmed=false` 标记）。
- **序列语义**：`sequence` 是源端的流完整性序号。允许乱序的是**事件时间**；
  序列回退仅在相对**已确认**窗口判定时拒绝（规则 2），因此水位线 horizon 内的
  重排序不会被误判为回退，而重放/损坏的流会被确定性拒绝。

## 并发与内存

- 每个 symbol 一个 `SymbolWindows` 状态机，其监视器锁串行化该 symbol 的写入与查询；
  查询在锁内生成不可变快照（`OhlcBar`），**不会暴露窗口的中间态**。
- 去重集合（`LinkedHashSet`，FIFO 上限 `maxDedupEntriesPerSymbol`）与窗口数
  （`maxRetainedWindowsPerSymbol`，仅淘汰**已确认**的最老窗口）均有上界，
  高并发写入下内存不会无界增长。
- 拒绝路径不修改任何状态；只有聚合成功后才推进去重集合、序列与水位线。

## 可配置项（`application.properties`，前缀 `tick.aggregator`）

| 配置 | 默认 | 含义 |
|---|---|---|
| `window-size-ms` | 60000 | 滚动窗口大小（毫秒） |
| `allowed-lateness-ms` | 30000 | 允许的最大迟到（相对水位线） |
| `max-retained-windows-per-symbol` | 1024 | 每 symbol 最多保留的窗口数 |
| `max-dedup-entries-per-symbol` | 65536 | 每 symbol 去重集合上限（FIFO 淘汰） |

## 测试覆盖

- `TickIngestionServiceTest`：字段缺失、非法时间戳/数值、重复投递幂等、序列回退，
  每类拒绝原因可区分。
- `WindowAggregationTest`：乱序归入正确窗口、同批事件任意顺序收敛同一结果、
  重复投递不改变聚合、超时迟到显式拒绝且不污染窗口、迟到边界（恰好等于
  watermark-lateness 仍接受）、窗口确认边界。
- `ConcurrencyTest`：多线程写入 + 并发查询（读者永不见中间态）、并发重复风暴
  （每事件恰好计数一次）、窗口保留有界。
- `TickApiIntegrationTest`：HTTP 端到端（接入→去重→拒绝→查询）。

所有测试日志均打印事件输入与判定依据（`status`/`rejectionReason`/`detail`），
失败时可直接定位判定现场。
