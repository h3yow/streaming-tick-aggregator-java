# Streaming Tick Aggregator（本地行情事件接入与时间序列聚合）

进程内（in-memory）模拟行情源接入端与查询端：接收行情事件、幂等去重、按时间窗口聚合
OHLCV（开/高/低/收/量），并在**乱序、重复、延迟到达**与**高并发读写**下保证聚合结果
一致、可解释、可复现。

技术栈：Java 21 + Spring Boot 3.4（spring-boot-starter-web）+ JUnit 5，无外部中间件。

---

## 1. 接入协议

### 1.1 投递事件

`POST /api/ticks`，`Content-Type: application/json`

```json
{
  "eventId": "evt-1001",
  "symbol": "BTC",
  "sequence": 1001,
  "eventTime": 1727251200123,
  "price": 65120.5,
  "volume": 2
}
```

| 字段 | 类型 | 约束 |
|---|---|---|
| `eventId` | string | 必填、非空白；**幂等键**，同一事件重复投递必须复用同一 id |
| `symbol` | string | 必填、非空白；标的代码 |
| `sequence` | long | 必填、≥0；标的内的单调序列号（见 §3） |
| `eventTime` | long(epoch millis) | 必填、>0 且不晚于服务器当前时间 |
| `price` | double | 必填、有限值且 >0 |
| `volume` | long | 必填、≥0（允许 0 量报价） |

**成功响应 `200 OK`**

```json
{
  "accepted": true,
  "duplicate": false,
  "reason": null,
  "windowStart": 1727251200000,
  "windowStatus": "OPEN",
  "message": "accepted into window [..,..), watermark=.."
}
```

**拒绝响应 `422 Unprocessable Entity`**（请求被明确拒绝，不静默接受）

```json
{ "accepted": false, "duplicate": false, "reason": "WINDOW_CLOSED_LATE", "message": ".." }
```

非法 JSON / 类型不匹配返回 `400`，`reason = MALFORMED_REQUEST`。

### 1.2 可区分的拒绝原因（`reason` 枚举）

| reason | 含义 |
|---|---|
| `MISSING_FIELD` | 必填字段缺失或空白（eventId/symbol/sequence/eventTime/price/volume） |
| `INVALID_TIMESTAMP` | eventTime ≤0 或晚于当前墙钟时间 |
| `INVALID_PRICE` | price 非有限值（NaN/Infinity）或 ≤0 |
| `INVALID_VOLUME` | volume <0 |
| `INVALID_SEQUENCE` | sequence <0 |
| `SEQUENCE_REGRESSION` | 序列回退：与已接受事件按 `(eventTime, sequence)` 的严格次序矛盾（见 §3） |
| `WINDOW_CLOSED_LATE` | 超时迟到：事件所属窗口已因 watermark 关闭（见 §2） |

同一非法请求重复提交永远得到**相同**的拒绝原因（校验不依赖可变状态）。

### 1.3 幂等去重语义

- 以 `eventId` 为键记录**首次投递的完整判定**（接受或拒绝）。
- 重复投递：接受的事件返回 `200` 且 `duplicate=true`，窗口结果**不发生任何变化**；
  首次被拒绝的事件重放同样返回与首次一致的判定（`duplicate=true`，HTTP 422）。
- 去重表按"全局仍保留的最老已关闭窗口"为界周期回收：只要某事件的已确认结果仍可查询，
  其重复投递就一定命中首次判定；超出保留期的极旧事件重放按 `WINDOW_CLOSED_LATE` 归类。

---

## 2. 窗口、watermark 与延迟判定规则（按标的独立维护）

- **窗口划分**：`windowStart = floor(eventTime / windowSizeMs) * windowSizeMs`，
  窗口为左闭右开区间 `[windowStart, windowStart + windowSizeMs)`。
- **watermark**：`WM(symbol) = max(已接受事件 eventTime) - allowedLatenessMs`，
  无事件时为 `-∞`。watermark 只随观测推进、单调不减。
- **窗口关闭（结果确认）**：当 `windowEnd <= WM` 时窗口由 OPEN 变为 CLOSED。
  CLOSED 结果不可变，查询永远返回同一快照；关闭发生在事件处理临界区内，
  写入过程不会对外暴露中间态。
- **迟到判定**：事件所属窗口满足 `windowEnd <= WM` 即为**超时迟到**：
  拒绝（`WINDOW_CLOSED_LATE`）并归入该标的的迟到分类（`GET /api/late`），
  **绝不计入任何窗口、绝不静默丢弃**。
- **允许的乱序**：事件早于当前最大事件时间、但 `windowEnd > WM` 时，仍归入其正确窗口；
  因此同一批事件只要时间跨度不超过 `allowedLatenessMs`，**无论到达顺序**都收敛到同一结果。
- **边界示例**（window=60s, lateness=10s）：已观测到 75s 事件 → WM=65s →
  窗口 `[0,60s)` 关闭；20s 的迟到事件（end=60s ≤ 65s）被拒绝归类；
  而 62s 的事件属于窗口 `[60,120s)`，仍正常接受。

### OHLCV 取值（顺序无关）

为保证乱序收敛，open/close 不按到达顺序，而按确定性次序选取：
open = `(eventTime, sequence)` 最小事件的价，close = 最大事件的价；
high/low 为窗口内极值；volume 为成交量之和；tickCount 为事件数。

---

## 3. 序列回退判定（与到达顺序无关）

同标的内，已接受事件在按 `(eventTime, sequence)` 排序后，`sequence` 必须**严格递增**。
新事件通过双向邻接检查：

- 必须严格大于 `eventTime' <= eventTime` 中最大的 seq（含同一时间点）；
- 必须严格小于 `eventTime' > eventTime` 中最小的 seq。

因此：

- seq1@5s 先到、seq2@40s 后到，再补 seq 落在两者次序之间的更早/更晚事件，属于**允许乱序**；
- seq 与事件时间次序矛盾（如更早时间配更大 seq、同时间 seq 重复）判定 `SEQUENCE_REGRESSION`。

拒绝在写入任何窗口状态之前完成，不会污染聚合。

---

## 4. 查询协议

### 4.1 OHLCV

`GET /api/ohlcv?symbol=BTC&from=<epochMs>&to=<epochMs>&includeOpen=false`

- 时间范围按窗口起点过滤：`from <= windowStart < to`（均可省略）。
- 默认仅返回 **CLOSED**（已确认、不可变）窗口；`includeOpen=true` 时额外返回 OPEN
  窗口在某一时刻的**一致快照**（锁内拷贝，不含写入中间态）。
- 响应字段：`symbol, windowStart, windowEnd, open, high, low, close, volume, tickCount, status`。

### 4.2 超时迟到事件归类

`GET /api/late?symbol=BTC` —— 返回被 `WINDOW_CLOSED_LATE` 拒绝的事件记录（有界）。

---

## 5. 并发与内存边界

- 每个 `symbol` 一把内部锁，窗口状态、watermark、序列索引、迟到记录的读写全部在锁内串行；
  查询在锁内生成不可变快照后释放锁，因此对外永远不暴露中间态。
- 接入端按 `eventId` 哈希分 64 段锁：同一事件的首次/重复投递严格串行（结果一致），
  不同事件可并行；标的锁仅在状态更新的短临界区内持有。
- 内存有界：
  - 每个标的 CLOSED 窗口最多保留 `retained-closed-windows` 个（最老先裁）；
  - 迟到记录最多 `max-late-records-per-symbol` 条；
  - 幂等去重表按保留窗口边界周期回收；
  - 序列索引仅保留"仍可能接受迟到事件"的时间点（`eventTime > WM - windowSizeMs`）。
- 高并发下：窗口更新是锁内读改写（无丢失），幂等表保证唯一事件只计数一次（无重复计数）。

---

## 6. 可配置项（`src/main/resources/application.properties`，前缀 `aggregator.`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `aggregator.window-size-ms` | 60000 | 聚合窗口长度（毫秒） |
| `aggregator.allowed-lateness-ms` | 10000 | 允许乱序延迟；WM = maxEventTime - 该值 |
| `aggregator.retained-closed-windows` | 1000 | 每标的保留的已关闭窗口数上限 |
| `aggregator.max-late-records-per-symbol` | 1000 | 每标的迟到归类记录上限 |

`server.port=0` 表示本地随机端口；如需固定端口改为具体值。

---

## 7. 验证方法

```bash
mvn test
```

测试套件（12 个用例）：

| 测试类 | 覆盖场景 |
|---|---|
| `TickIngestionServiceTest` | 10 类字段/时间戳/价格/量/序列非法的**可区分拒绝原因**；正序/逆序/交错三种到达顺序的**收敛一致性**；允许乱序边界；**超时迟到拒绝与归类**、已确认窗口不被污染；**序列双向回退**；重复投递（接受事件与迟到事件重放）幂等；CLOSED 不可变 |
| `ConcurrencyTest` | 8 线程 ×100 事件 ×3 轮重复的并发写入，断言首次接受=100、重复=200、无丢失/重复计数、最终 OHLCV 正确；并发读线程持续校验快照自洽（无中间态撕裂）；合法/非法事件混发下状态不被污染 |
| `WebApiIntegrationTest` | HTTP 端到端：200/422/400 状态码、`duplicate` 标志、reason 区分、OHLCV 查询、`/api/late` 归类、includeOpen |

日志中打印每个事件的输入（eventId/seq/eventTime/price/volume）与判定依据
（`ACCEPT` / `DUPLICATE` / `REJECT-SEQ` / `REJECT-LATE` / `REJECT-INPUT`，
含窗口边界与 watermark 数值），便于复现与归因。

手动验证（启动后端口见日志，此处以 8080 为例）：

```bash
curl -X POST localhost:8080/api/ticks -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","symbol":"BTC","sequence":1,"eventTime":<过去的epochMs>,"price":100,"volume":1}'
curl 'localhost:8080/api/ohlcv?symbol=BTC&includeOpen=true'
curl 'localhost:8080/api/late?symbol=BTC'
```
