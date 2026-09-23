# SmartShip-Shore

岸端可靠数据链路：船端遥测经 MQTT 上岸，转 Kafka 事件总线，一路归档 MySQL 历史，
一路投影 Redis 最新状态。单体 Spring Boot，不拆微服务。

> 范围诚实声明：当前代码只有 `MQTT → Kafka → MySQL（含 Retry/DLT）` 与
> `Kafka → Redis 最新状态投影`。WebSocket、Elasticsearch、告警、微服务拆分均不存在。
> 语义是 at-least-once + 幂等，不是 exactly-once，不宣称零丢失。

## 1. 项目简介

船端（Smartship Edge）通过 MQTT 把遥测送上岸；岸端把 MQTT 当**设备接入层**
（弱网通信），把 Kafka 当**岸端事件总线**（持久化、可回放、多订阅），
把 MySQL 当**历史事件存档**（追加、可审计），把 Redis 当**最新状态视图**
（每船每类一键、可直接点查）。系统采用 at-least-once 消息语义，
通过 msg_id 唯一约束实现幂等消费；通过 Fault Injection 验证故障恢复行为。

## 2. 系统架构

```text
                Edge Device
                    |
                    | MQTT QoS1 (topic: zncb/{mmsi}/{topicSuffix})
                    v
              Shore Ingest (解析 + 校验 → Kafka，成功才 ACK)
                    |
                    | key = MMSI, acks=all + 幂等生产者
                    v
              Kafka Raw Topic (ship.telemetry.raw, 6 partitions)
                    |
          +---------+----------+
          |                    |
          v                    v
 History Consumer       LatestState Consumer
 (smartship-history)    (smartship-latest-state)
          |                    |
          v                    v
       MySQL                Redis
       History            Latest State
          +-------------------+
          |
          v
 Retry / Error Handler / DLT
          |
          v
 Metrics + Fault Injection Tests
```

完整组件映射、能力标注、失败路径与运行时拓扑见 [`docs/architecture.md`](docs/architecture.md)。
该文档只描述代码真实存在的东西：每个节点都能在 `src/main` 里找到对应类，
没有 Kafka Streams / Flink / ES / WebSocket / 微服务 / Kubernetes。

MQTT 后面为什么还需要 Kafka：MQTT 负责船岸弱网到达即走，无回放；
Kafka 负责岸端持久化、可回放、可多订阅——再加一个消费者只需加 consumer group，
不打扰船端。Key 用 MMSI：同船同 key 同 partition，partition 内保持顺序
（不声称跨 partition 全局有序）。

## 3. 核心设计

### 3.1 MQTT 可靠接入

- 船端按 `zncb/{mmsi}/{topicSuffix}` 以 QoS1 上报 Edge 扁平 JSON
 （`topicSuffix ∈ {nmea_gps, nmea_wind, nmea_depth, nmea_rudder, engine}`）；
  岸端订阅 `zncb/+/+`，经 `TelemetryMessageParser` 解析为 `TelemetryEnvelope`。
- Paho 启用 manual ACK：Kafka send future 成功后才调 `messageArrivedComplete`；
  失败或有界等待超时（默认 5s，`shore.mqtt.kafka-handoff-timeout-ms`，永不无限阻塞）
  则绝不 ACK，只记指标 + 日志，并主动断开当前连接，以同一 clientId +
  `cleanSession=false` 重连，让 broker 重投原 QoS1 消息。
- Paho 回调线程串行，有界等待使 ACK 顺序恒等于到达顺序；故意丢弃的毒消息照常
  ACK，避免单条坏消息在 broker 上无限循环。

### 3.2 Kafka 可靠消费

- 系统采用 at-least-once 消息语义：MQTT QoS1 与 Kafka 本身都会重复投递，
  重复是正常现象，不掩盖。
- 两个独立 consumer group（`smartship-history`、`smartship-latest-state`）
  消费同一 topic，各自推进 offset、各自重试/DLT，互不干扰。
- Offset 语义：`MANUAL_IMMEDIATE`，DB 落定（或 DLT 落定）后才提交；
  失败期间不提前提交。`concurrency=1`，保 partition 内顺序写库。
- 幂等消费：Edge `msg_id` 是确定性 SHA-256（业务自然键 + 事件时间，
  重传不改变）；重复消息撞 `UNIQUE(msg_id)` 即视为成功并 ACK，不重试、不进 DLT。
  崩溃窗口可解释：insert 成功 → commit 前 crash → 重投 → 唯一约束拦截 →
  offset 推进，最终永远只有一行。

### 3.3 Retry 与 DLT

- 只用 Spring Kafka 官方 `DefaultErrorHandler` +
  `DeadLetterPublishingRecoverer` + `FixedBackOff`，无自研重试线程。
- 瞬时 DB 异常按 1s 间隔有限重试 3 次（禁止无限重试），耗尽后进
  `ship.telemetry.raw.DLT`；JSON 无法解析 / 缺必填字段 / 确定性 SQL 错误
  （poison）直接进 DLT，不做无意义重试。
- Offset 提交安全：DLT publish 成功后原 offset 才推进（`commitRecovered`）；
  DLT 发送本身有界（失败/超时则不推进 offset、不计入 DLT 指标，重投继续恢复）。
- DLT 记录保留原 topic/partition/offset、key（MMSI 不变）、原始 payload、
  异常信息，以及 `shore-dlt-failed-at` 与 `shore-dlt-reason`
 （`transient`/`poison` 闭桶）。

### 3.4 Redis Latest State Projection

- Redis 保存船舶最新状态，MySQL 保存历史遥测数据：前者是可直接点查的现状物化视图，
  后者是追加、可回放、可审计的历史事实。两者职责分离，故障隔离。
- Key 为 `ship:{mmsi}:latest:{type}` 的 Redis Hash：`payload`（完整 Envelope JSON）
  + `ts_ms` / `sent_ms` / `msg_id` 比较字段 + TTL（默认 86400s，
  `shore.redis.latest-state-ttl-seconds`）。
- 不能直接 SET：at-least-once 下迟到事件会覆盖新状态。比较 + 更新在 Redis 内
  由 Lua（`redis/latest_state_cas.lua`）原子完成，返回 `UPDATED` / `STALE`。
- 新旧判断只用 Edge 原始 `timestamp`（不用岸端时间）；时间相等看 `sent_at`，
  再看 `msg_id` 字典序，全确定性。`UPDATED` 刷新 TTL，`STALE` 不刷新——
  旧数据不能延长状态生命周期；STALE 与同 `msg_id` 重复投递照常 ACK。

### 3.5 数据一致性

- `msg_id` 唯一约束是全链路唯一的幂等锚点：History 侧撞约束即成功，
  LatestState 侧同 `msg_id` 即 STALE。
- Duplicate absorption：重复投递永远只产生指标（`history_duplicate`），
  不产生第二行、不进 DLT、不报错升级。
- History 与 Latest 职责分离：同一毒消息会各自进一次 DLT
  （at-least-once 下各组独立处理，属正常现象，下游按原 offset/msg_id 去重）；
  DLT 自身无去重，下游按原 topic/offset 识别。

## 4. Design Trade-offs

### 为什么不用 Kafka exactly-once

端到端 exactly-once 要求事务横跨 Kafka + MySQL + Redis 三个异构系统，
成本高、脆弱，且单体岸端并不需要它。采用 at-least-once + `msg_id` 幂等：
重复可解释、吸收点唯一（DB 约束 + Lua 规则），简单、可测试、可讲清崩溃窗口。

### 为什么 Redis 和 MySQL 分离

读路径不同：最新状态要低延迟点查（Redis Hash 一次命中），历史要追加存储、
范围回放与审计（MySQL 索引 + SQL）。写路径也不同：覆盖语义 vs 追加语义。
分离后两组独立推进 offset，一侧故障不拖住另一侧（P2-4.2.1/2.2 实测）。

### 为什么使用 DLT

持续失败或毒消息若一直重试，会阻塞 partition 消费、拖住整组进度。
DLT 把“处理不了的消息”与“消费进度”解耦：原 offset 在 DLT 落定后继续推进，
系统继续消费；失败现场（原 payload + 异常 + 原因闭桶）完整保留，可审计、可重放。

## 5. Benchmark

详见 [`docs/BENCHMARK_BASELINE.md`](docs/BENCHMARK_BASELINE.md)。
单机 Docker（GitHub Actions Testcontainers）基线，不是生产容量证明，
不是 TPS 测试；`@Tag("benchmark")` + Maven `benchmark` profile 保证普通
`mvn test` 与普通 CI 永不执行大规模压测。

| Messages | Publish msg/s | History msg/s | P50 / P95 / P99 ms | Redis 收敛 ms | MySQL | Redis keys | DLT | Lost |
|---:|---:|---:|---|---|---:|---:|---:|---:|---:|
| 1000 | 1769.91 | 245.10 | 2119 / 3001 / 3064 | 4242 | 1000/1000 | 500/500 | 0 | 0 |
| 10000 | 2435.46 | 360.46 | 17431 / 22971 / 23347 | 27910 | 10000/10000 | 500/500 | 0 | 0 |
| 50000 | 3078.63 | 465.70 | 54784 / 87159 / 90397 | 107745 | 50000/50000 | 500/500 | 0 | 0 |

环境：Java 17 / Linux 4 CPU；P50 系 RECEIVE 延迟（不含单条 insert/commit，
不是 broker 延迟），完成吞吐才是持久化收敛标尺。三档一致性门全过。

### 多 Producer 并发（P2-4.1.1）

测试方法：每 producer 固定量（默认 50000 条），1 / 10 / 50 个独立 Edge 发布者
并发上报，总量分别为 50k / 500k / 2.5M。每个 producer 拥有独立 MQTT client、
独立连接、独立 MMSI，不共享发送状态；消息 `n` 恒归属 producer `n % producerCount`，
`msg_id` 确定可复现。链路与基线完全相同（MQTT → Mosquitto → 岸端服务 →
Kafka → MySQL + Redis），禁止 mock、禁止直调 Consumer、禁止绕过 MQTT。

```bash
BENCHMARK_MODE=concurrent PRODUCER_COUNTS=1,10,50 MESSAGES_PER_PRODUCER=50000 \
  mvn -Pbenchmark test
```

`BENCHMARK_MODE` 一次只选一个场景（`concurrent` 跑并发，`soak` 跑定速压测，
缺省跑基线档），现有 1k/10k/50k/2M baseline 不变。GitHub Actions `benchmark` workflow 也有同名
`mode` 输入（默认 `baseline`，现有 dispatch 行为逐字不变）。
结果进 `target/benchmark-results/` 下的
`concurrent-producer-results.{json,csv,md}`，与基线文件互不覆盖。

指标定义：每档输出 producer 数、publish/history 吞吐、History RECEIVE 延迟
P50/P95/P99（口径与基线一致）、max lag（最坏单条延迟）与 recovery time
（首发到 MySQL 收齐，即 backlog 排空时间；独立 offset-lag 采样是后续专项，
本阶段不做）、MySQL 行数、Redis 键数、DLT 数、lost 数、`history_duplicate` 数。
一致性门：行数 == 发送数、`DISTINCT(msg_id)` 相等、lost 为 0、DLT 为 0、
Redis 每键最终态正确。同样只是单机 Docker 基线，不是生产容量证明。

### 定速压测 + 断网补传（soak）

测试方法：N 艘船定速上报（如 50 船 × 50 点/秒），总量固定（定速只是上限，
总数精确、时长实测）。分三段：A 段岸端在线定速收流；随后停掉岸端 MQTT 订阅
（断网开始——发布者继续向 Mosquitto 发，broker 为 durable 会话排队 QoS1）；
B 段继续定速发入断网期（5 分钟时长由固定总量自然形成，非 sleep 常数），
期间断言 MySQL 行数冻结；重连后排空积压（补传），测 p99/端到端延迟与补传丢失。

```bash
BENCHMARK_MODE=soak SOAK_SHIPS=50 SOAK_POINTS_PER_SEC=50 \
  SOAK_STEADY_PER_PRODUCER=9000 SOAK_OUTAGE_PER_PRODUCER=15000 \
  mvn -Pbenchmark test
```

`BENCHMARK_MODE=soak` 与基线/并发互斥（assumption 门控，一次只跑一个场景）。
结果进 `target/benchmark-results/soak-results.{json,csv,md}`。
指标：实际发布速率、publish/history 吞吐、History RECEIVE 延迟 P50/P95/P99/max
（积压下含排队等待，口径与基线一致）、断网期积压数、补传排空耗时、
MySQL 行数、Redis 键数、DLT/lost/duplicates。
一致性门：行数/`DISTINCT` == 发送总数、lost 为 0、DLT 为 0、
Redis 每键最终态正确。同样只是单机 Docker 基线，不是生产容量证明。

## 6. Fault Injection

详见 [`docs/FAULT_INJECTION.md`](docs/FAULT_INJECTION.md)。
真实 Testcontainers 故障、bounded wait、验证故障中 + 恢复后状态；
禁止 mock 异常冒充故障、禁止改生产 retry 参数、禁止长 sleep 掩盖时序。

| 场景 | 故障 | 验证 |
|---|---|---|
| P2-4.2.1 | MySQL 短暂故障（pause → 恢复） | transient retry → 恢复后落库，Redis 不受影响，DLT 0 |
| P2-4.2.2 | Redis 短暂故障（pause → 恢复） | History 不受影响，latest retry → 恢复后投影，DLT 0 |
| P2-4.2.3 | MySQL 持续故障 → DLT | retry 耗尽 → A 进 DLT，恢复后 A 不自动落库，B 正常落库 |
| P2-4.2.4 | Redis 持续故障 → DLT | retry 耗尽 → A 进 DLT，恢复后 A 不自动投影，B 正常 |
| P2-4.2.5 | Kafka 短暂故障 → MQTT QoS1 重投 | 重投循环 → 恢复后单行落库，DLT 0 |
| P2-4.2.6 | Shore crash/restart → replay + 幂等 | 重投被去重吸收，B 正常，DLT 0 |

六场景 CI 全绿，run URL 落盘在文档中。核心结论：DLT 是最终失败去向，
不是自动延迟队列；重复投递由 `UNIQUE(msg_id)` 吸收，永不产生第二行。

## 7. 技术栈

- Java 17 / Spring Boot 3.3.5 / Spring Kafka（`acks=all` + 幂等生产者，
  `MANUAL_IMMEDIATE` 消费，`concurrency=1`）
- Eclipse Paho MQTT（QoS1，manual ACK，durable session）
- MySQL 8 + Flyway（`ship_telemetry_history`，`UNIQUE(msg_id)`）
- Redis 7 + Lua CAS（`ship:{mmsi}:latest:{type}` + TTL）
- Kafka 3.8 KRaft 单节点 / Mosquitto 2.0（本地 `docker compose`）
- Actuator + Prometheus（低基数指标），JUnit 5 + Testcontainers（无 Docker 自动 skip），
  GitHub Actions CI（JDK 17；benchmark 仅手动触发）

## 8. Future Work

以下均不在当前代码中，只是诚实列出的后续方向：

- 实时推送（WebSocket）与搜索（ES）：当前只有 MySQL 历史与 Redis 现状，无其他读路径。
- 告警：DLT 当前只保留现场，无告警通知。
- 消费并发：`concurrency=1` 保序但吞吐上限即单线程写库能力；放开并发需重做顺序保证。
- 精细退避：Kafka 长时间不可用时每条消息触发一次断线重连，当前是简单可解释策略。
- DLT 回放工具：当前 DLT 只存现场，无一键回放链路。
