# Smartship-Shore

岸端事件处理系统 **P2-1 → P2-2 → P2-3**：`MQTT → Kafka → MySQL 历史归档`
+ `Kafka → Redis 最新状态`（单体 Spring Boot，不拆微服务）。

> 本阶段只做 `MQTT → Kafka → MySQL（含 Retry/DLT）` 与 `Kafka → Redis 最新状态投影`。
> WebSocket、Elasticsearch、告警、微服务拆分均为后续阶段，当前代码不包含。

## 1. 项目定位

船端（Smartship Edge）通过 MQTT 把遥测送上岸；岸端把 MQTT 当**设备接入层**，
把 Kafka 当**岸端事件总线**，把 MySQL 当**历史事件存档**。P2-1 只保证一条链路可靠打通。

## 2. 总体架构

```text
Smartship Edge
      │
      │ MQTT QoS1  (topic: zncb/{mmsi}/{topicSuffix})
      ▼
  Mosquitto
      │
      ▼
MqttIngestService        解析 + 校验 → Kafka(有界同步交接) → 成功才 ACK MQTT
      │
      ▼
Kafka Producer           key = MMSI, acks=all + 幂等
      │ key=MMSI
      ▼
ship.telemetry.raw       (6 partitions, 单 topic)
      │                    Kafka = 多下游事件总线，两个独立 group 各自推进 offset
      ├─ HistoryConsumer          group = smartship-history, 手动提交 offset
      │       │
      │       ▼
      │      MySQL               ship_telemetry_history, UNIQUE(msg_id) = 历史事件
      │
      └─ LatestStateConsumer      group = smartship-latest-state, 手动提交 offset
              │
              ▼
             Redis               ship:{mmsi}:latest:{type} = 最新状态物化视图
```

### 为什么 MQTT 后面还需要 Kafka

| | MQTT (Mosquitto) | Kafka |
|---|---|---|
| 职责 | 船岸弱网通信 / 设备接入 | 岸端事件总线 / 削峰 / 解耦 / 多消费者扩展 |
| 语义 | QoS1 到达即走，无回放 | 日志持久化、可回放、可多订阅 |
| 扩展 | 再加一个消费者就要再订阅一次设备流 | 加 consumer group 即可，不打扰船端 |

船端只管把消息送到 Mosquitto；岸端内部谁消费、消费几次、以后加告警/实时状态，
都由 Kafka 解耦。P2-1 只有一个消费者（HistoryConsumer），P2-3 起有两个独立 group，
后续阶段加消费者不需要改船端。

### Kafka key 为什么用 MMSI

```java
kafkaTemplate.send("ship.telemetry.raw", envelope.getMmsi(), envelope);
```

相同 MMSI → 相同 key → 相同 partition → **partition 内保持顺序**。
同一条船的遥测在库里是按序到达的。不声称 Kafka 全局有序（跨 partition 无序）。

### at-least-once + msg_id 幂等（不声称 exactly-once）

- MQTT QoS1 与 Kafka 本身都是 at-least-once：重复投递是正常现象。
- Edge `msg_id` 是确定性 SHA-256（业务自然键 + 事件时间，见 Edge `MqttPublisher`），
  重传不改变 `msg_id`（`sent_at` 不参与指纹）。
- DB 对 `msg_id` 建 `UNIQUE` 约束；重复消息第二次 insert 直接撞约束，
  被视为“已处理成功”并正常提交 offset，不重试、不报错升级。
- 崩溃窗口可解释：`DB insert 成功 → commit offset 前 crash → Kafka 再投递 →
  UNIQUE(msg_id) 拦截 → offset 正常推进`，最终永远只有一行。

### P2-1.1 可靠交接（MQTT ACK 只跟随 Kafka 成功）

旧链路有个丢消息窗口：`messageArrived()` 里异步 `send()` 后立即返回，Paho 先 ACK，
而 Kafka 后续可能失败 —— 消息两边都没留下。P2-1.1 关闭这个窗口：

- Paho 启用 manual ACK（`setManualAcks(true)`，connect 之前设置）；
- Kafka send future **成功**后才调 `messageArrivedComplete(id, qos)`；
- send 失败（异步失败或同步拒绝）时绝不 ACK，只记指标 + 日志，等 QoS1 重投；
- 交接全异步，不阻塞 Paho 线程等 Kafka（拒绝“同步等待 + 吞异常”的伪可靠）；
- 故意丢弃的毒消息照常 ACK，避免单条坏消息在 broker 上无限循环。
- 因此完整崩溃窗口变成：`MQTT 未 ACK → shore crash → broker 重投 →
  Kafka → DB → UNIQUE 吸收 → 双双确认`，依然只有一行。

### P2-1.2 有序可重投交接（有界同步等待 + 主动断线）

P2-1.1 还有两个剩余问题：Kafka 失败后原消息未必在当前连接自动重投；多个异步
future 完成顺序可能打乱 MQTT ACK 顺序。P2-1.2 改成交接方式：

- `messageArrived` 内对 send future 做**有界等待**
  （`future.get(shore.mqtt.kafka-handoff-timeout-ms)`，默认 5s，永不无限阻塞）；
- Paho 回调线程本就串行，有界等待使 **ACK 顺序恒等于到达顺序**；
- 成功 → `messageArrivedComplete`；失败/超时 → 不 ACK + 指标 + 日志，
  并**主动断开当前 MQTT 连接**，以后台线程立即用同一 clientId +
  `cleanSession=false` 重连，durable session 让 broker 重投原 QoS1；
- 后续消息照常进入同样的有界交接，不因一次失败停摆；
- 毒消息仍明确丢弃并 ACK。
- 最终加固：`client` 引用在 connect **之前**发布（durable 会话恢复后旧 QoS1 可能
  在 CONNACK 后立即重投，早一刻可见才能正常 ACK），connect/subscribe 失败时清理
  引用并关闭半连接 client，不遗留 stale；`messageArrivedComplete` 异常不再只记
  日志，而是与 Kafka 失败同一出口——强制重连 + broker 重投。

### P2-1.1 Envelope 契约（消灭 data.data）

- MQTT 入站：Edge 扁平 JSON → `TelemetryMessageParser` → `TelemetryEnvelope`（不变）。
- Kafka 出站：Producer 直接序列化 `TelemetryEnvelope`（`data` 即业务 Map）。
- Kafka 入站：Consumer 用 `objectMapper.readValue(value, TelemetryEnvelope.class)`
  直接反序列化 + 必填校验，**不再经过 flat-JSON parser**（否则 `data` 会被当成
  普通业务字段再包一层，出现 `data.data.speed_knots`）。P2-3 起该契约收敛到
  共用的 `EnvelopeCodec`，历史归档与最新状态两个 Consumer 走同一份解析/校验/序列化。

### P2-3 Redis 最新状态（MySQL 存历史，Redis 存现状）

- **为什么两者同时存在**：`MySQL = 历史事件`（追加写、可回放、可审计），
  `Redis = 最新状态物化视图`（每船每类一键、可直接读），`Kafka = 多下游事件总线`
  （两个独立 group 各自消费、各自推进 offset，互不干扰）。
- **为什么独立 Consumer Group**：`smartship-latest-state` 与 `smartship-history`
  消费同一 topic 但 offset/重试/DLT 完全独立；投影挂掉不影响归档，反之亦然。
  配置路径统一在 `shore.kafka` 下：`group-id`（历史组）与
  `latest-state-group-id`（投影组），见 `application.yml`。
- **Key/Value**：`ship:{mmsi}:latest:{type}`（如 `ship:413999999:latest:nmea_gps`），
  Redis Hash 存 `payload`（完整 Envelope JSON，字段与 `TelemetryEnvelope` 一致，
  不另设 DTO）+ `ts_ms` / `sent_ms` / `msg_id` 比较字段 + TTL。
- **为什么不能直接 SET**：at-least-once 下迟到事件会覆盖新状态。比较 + 更新必须
  在 Redis 内原子完成，因此用 Lua（`redis/latest_state_cas.lua`）做 compare-and-set，
  返回 `UPDATED` / `STALE`。
- **Lua 规则**：无当前值 → 直接写；`msg_id` 相同即重复投递 → 直接 STALE
  （状态与 TTL 都不动，offset 照常推进）；有 `timestamp` 的 incoming 覆盖无
  timestamp 的当前值，反之 STALE；都有则 strictly newer 写、older 丢；时间相等
  且 `msg_id` 不同时看 `sent_at` 大者，最后看 `msg_id` 字典序——全确定性，
  无随机覆盖。STALE 照常 ACK。
- **timestamp/null 策略**：新旧判断只用 Edge 原始 `timestamp`，不用岸端 `receivedAt`；
  `timestamp == null` 时：无当前值可写；已有带 timestamp 的状态不允许 null 覆盖；
  双方都无 timestamp 则按 `sent_at` 比，再按 `msg_id` 比（见 Lua 与测试矩阵）。
- **offset 与失败语义**：`UPDATED` → ACK；`STALE` → 视为成功并 ACK；Redis 异常 →
  不 ACK 并抛给共用的有限重试/DLT handler（与历史组同一套）。仍是 at-least-once。
- **TTL**：`shore.redis.latest-state-ttl-seconds`（默认 86400），每次有效更新刷新；
  STALE 不刷新，旧数据不能延长状态生命周期。
- **Duplicate**：同 `msg_id` 重投直接 STALE（状态不变、TTL 不刷新）后正常 ACK，
  不维护去重 Set（`msg_id` 本身即幂等键）。

## 3. Edge MQTT 协议（以 Edge 源码为准，非自创）

来源：`smartship-edge-core`（`MqttPublisher` / `DatabaseUploadPoller` /
`MqttClientManager` / `application.yml`，QoS 1）：

- **Topic**：`zncb/{mmsi}/{topicSuffix}`，岸端订阅 `zncb/+/+`。
  `topicSuffix ∈ {nmea_gps, nmea_wind, nmea_depth, nmea_rudder, engine}`，
  对应 `type ∈ {nmea_gps, nmea_wind, nmea_depth, nmea_rudder, engine}`。
- **Payload**：扁平 JSON = 业务 row 原样字段 + `mmsi` + `type` + `msg_id` + 可选
  `timestamp`（row 自带才保留，无则不伪造）+ `sent_at`（上报时刻，`+08:00`）。
- **msg_id**：`SHA-256(mmsi|type|sourceIdentity|sourceTime)` 64 位 hex；
  无主键无时间时退化为 key 字典序规范 JSON 散列。岸端只信任它做幂等，
  不用 `mmsi + timestamp` 自行推导唯一性。
- **岸端 DTO**：`TelemetryEnvelope{msgId, mmsi, type, timestamp, sentAt, data}`，
  `data` 为各类型差异字段的 `Map`（不建巨型强类型 DTO）。字段名与 Edge 输出一致。

## 4. 关键设计

- **Producer 可靠性**：`acks=all`、`enable.idempotence=true`、`retries=5`、
  `max.in.flight.requests.per.connection=5`，序列化 JSON（无类型头，纯 JSON）。
  重试全部交给 Kafka 机制，无自研重试线程。
- **Consumer offset**：Spring Kafka `MANUAL_IMMEDIATE`，DB 成功后才
  `acknowledge()`；失败期间不提前提交 offset。
  `DuplicateKey` 视为成功并 ACK（不进 Retry、不进 DLT，幂等不变）。
- **P2-2 Retry / DLT**：只用 Spring Kafka 官方 `DefaultErrorHandler` +
  `DeadLetterPublishingRecoverer` + `FixedBackOff`，无自研 sleep/retry 线程。
  瞬时 DB 异常（`TransientDataAccessException` /
  `DataAccessResourceFailureException` / `CannotGetJdbcConnectionException`）
  按 1s 间隔有限重试 3 次（禁止 `UNLIMITED_ATTEMPTS`），耗尽后进
  `ship.telemetry.raw.DLT`；JSON 无法解析 / 缺必填字段 / 确定性 SQL 错误直接进
  DLT，不做无意义重试，也不再 log+ACK 永久丢弃。DLT publish 成功后原 offset
  才推进（`commitRecovered`）。DLT publish 本身有界：`failIfSendResultIsError` +
  5s 等待（生产者 `delivery.timeout.ms` 同步 5s 上限），失败/超时则恢复失败、
  不推进 offset、不计入 DLT 指标，后续重投继续恢复。P2-2.2 收口：DLT 生产者超时
  显式配置（`request=4000` / `delivery=5000` / `linger=0` / `max.block=5000`，
  满足 `delivery >= request + linger`），非法组合启动即失败，不等到运行时丢 DLT。DLT 保留：原 topic/partition/offset、key（MMSI
  不变）、原始 payload、异常类型/信息/堆栈，以及 Spring 默认头之外的
  `shore-dlt-failed-at`（失败时间）与 `shore-dlt-reason`（`transient`/`poison`）。
  语义仍只是 at-least-once + 幂等，不宣称 exactly-once，更不宣称零丢失。
- **DB schema**（Flyway `V1__create_ship_telemetry_history.sql`）：
  `id / msg_id UNIQUE / mmsi / type / event_time / sent_at / received_at /
  payload(JSON) / kafka_partition / kafka_offset / created_at`，
  另有 `mmsi / type / event_time` 索引。
  `event_time` 存 Edge 原始 `timestamp`，不用岸端时间覆盖；无 timestamp 的行存 NULL。
- **Metrics**（低基数、无 `mmsi`/`msg_id`/异常文本 tag）：
  `smartship_shore_mqtt_received_total`、`..._mqtt_invalid_total`、
  `..._kafka_produced_total`、`..._kafka_produce_failed_total`、
  `..._history_consumed_total`、`..._history_persisted_total`、
  `..._history_duplicate_total`、`..._history_failed_total`，
  以及 P2-2 新增 `..._history_retry_total{reason}` /
  `..._history_dlt_total{reason}`（`reason ∈ {transient, poison}`）。
  暴露 `/actuator/health`、`/actuator/prometheus`。

## 5. 本地启动

```bash
docker compose up -d   # Kafka(KRaft 单节点, 无 ZooKeeper) + Mosquitto + MySQL
```

端口（host 视角）：MQTT `localhost:1883`、Kafka `localhost:29092`、
MySQL `localhost:3306`（库 `smartship_shore`，用户 `shore/shore123`，见 `.env.example`）。

```bash
mvn clean package -DskipTests
java -jar target/smartship-shore-0.1.0.jar --spring.profiles.active=local
```

默认配置见 `src/main/resources/application.yml`，本地覆盖见 `application-local.yml`；
密码只出现在 yml / 环境变量，不在 Java 源码里。

## 6. 测试

```bash
mvn clean test      # 要求 0 failures / 0 errors
mvn clean package
```

| # | 测试 | 说明 |
|---|---|---|
| 1 | `TelemetryMessageParserTest` | Edge 真实 payload 样例解析：`msg_id/mmsi/type/timestamp/sent_at/data`，非法报文拒绝 |
| 2 | `TelemetryKafkaProducerTest` | `topic == ship.telemetry.raw` 且 `key == "413999999"`，失败回调计数 |
| 3 | `HistoryConsumerTest` 正常消费 | Kafka record → DB 一行 → ACK |
| 4 | `HistoryConsumerTest` 重复消费 | 同 `msg_id` 两次：`row=1`、`duplicate=1`，第二次正常 ACK |
| 5 | `HistoryConsumerTest` crash window | 先直接 insert（模拟 crash 前已提交），再投递：仍 `row=1` |
| 6 | `HistoryConsumerTest` 多 MMSI | A/B 两船各自一行 |
| — | `HistoryConsumerTest` 毒消息/瞬时异常（P2-2） | 非法 JSON / 缺字段抛给 DLT（不 ACK）；瞬时 DB 异常抛给有限重试 |
| — | `HistoryConsumerTest` Envelope 契约 | 直接反序列化、`data.speed_knots` 扁平、无 `data.data` |
| — | `MqttIngestAckTest` 交接契约（P2-1.2） | 成功同步 ACK / 失败不 ACK 并主动断线 / 超时有界不 ACK 并断线 / 到达顺序 ACK / 毒消息丢弃并 ACK |
| E2E | `HistoryConsumerTestcontainersTest` | 真实 Kafka(KRaft)+MySQL（含 Flyway V1）；无 Docker 时自动跳过 |
| E2E | `TrueE2EMqttKafkaMySqlTest` | 真实 Mosquitto→岸端服务→Kafka→Consumer→MySQL；断言 row=1、`data.speed_knots=12.5`、无 `data.data`；无 Docker 跳过 |
| E2E | `MqttRedeliveryE2ETest`（P2-1.2） | 只 publish 一次：首次 handoff 失败→不断线重投原 QoS1→恢复后恰一行，`msg_id` 不变；无 Docker 跳过 |
| — | `HistoryRetryDltTest`（P2-2 / P2-2.1） | 正常/重复不进 DLT；失败两次第三次成功落库；持续失败耗尽后恰 1 条 DLT；毒消息/缺字段直达 DLT 零重试；DLT 失败不恢复/不计数/不提交、后续可再恢复；DLT 超时约 5s 有界；DLT 保留原 topic/key/payload/异常头 |
| E2E | `HistoryDltTestcontainersTest`（P2-2） | 真实 Kafka+MySQL：有效 envelope 落库且 DLT 为空，毒消息进 DLT 且头完整；latest 组在该用例停掉以免同一毒消息进两次 DLT；无 Docker 跳过 |
| — | `LatestStateConsumerTest`（P2-3） | UPDATED/STALE 均 ACK 且计数；Redis 异常不 ACK 并传播；毒消息不碰 Redis；未知脚本结果不 ACK；null timestamp 透传空串；key/ARGV 契约精确断言 |
| E2E | `LatestStateLuaTestcontainersTest`（P2-3 / P2-3.1） | 真 Redis 上 Lua 矩阵：首次/覆盖/迟到/STALE 保值/TTL 不刷新、同 instant 与双 null 的 tiebreak、MMSI/type 隔离、同 msg_id 重复直接 STALE 且不刷新 TTL；无 Docker 跳过 |
| E2E | `LatestStateKafkaTestcontainersTest`（P2-3） | 真 Kafka→双组扇出：MySQL 3 行 + Redis 三键值/TTL/内容正确；无 Docker 跳过 |
| ⚙ | `ShoreBenchmarkTest`（P2-4.1，仅 `-Pbenchmark` + Docker） | 确定性 workload 走真实全链，输出 publish/History 吞吐、History 延迟 p50/p95/p99/max、Redis 收敛、MySQL/Redis/DLT 一致性到 `target/benchmark-results/`；普通 `mvn test` 永不执行 |

> P2-1.1 的 `MqttKafkaHandoffReliabilityTest`（脚本重发模拟重投）已被 P2-1.2 的
> 真实原消息重投 E2E 取代并删除：手动重发会与 broker 的真实重投竞态，断言不再可靠。

测试不依赖公网 Kafka / 公网 Broker / 真实船舶；不断言跨 partition 全局顺序。

## 7. 已知限制（诚实声明）

- at-least-once，不是 exactly-once；重复由 `UNIQUE(msg_id)` 吸收。
- MQTT 交接等待有界（默认 5s，可配 `shore.mqtt.kafka-handoff-timeout-ms`）；
  Kafka 长时间不可用时，每条消息触发一次断线重连（已合并连续失败为单次重连），
  属 P2-1 级别的简单可解释策略，精细退避留给后续阶段。
- Producer 异步发送失败（MQTT→Kafka 段）只记指标 + 日志，本阶段未加重试；
  Retry/DLT 覆盖的是 Kafka→DB 段（`HistoryConsumer`）。
- 单 consumer（`concurrency=1`，保 partition 内顺序写库）；吞吐上限即单线程写库能力，见第 8 节基线方法（基线数据只在 Docker 实跑后落盘，不在源码里断言任何 TPS）。
- Redis 投影与历史归档是两个独立 group：同一毒消息会各自进一次 DLT（at-least-once 下各组独立处理，属正常现象，DLT 下游按原 offset/msg_id 去重）。
- DLT 自身无去重：恢复重试可能产生多份 DLT 拷贝，下游按原 topic/offset 识别。
- 未验证：百万 TPS、零丢失、高并发生产流量。以上均需后续真实压测数据支撑。

## 8. 基线 Benchmark（P2-4.1 方法说明）

> 这是单机 Docker（GitHub/local Testcontainers）基线方法，不是生产容量证明，
> 不是百万 TPS 测试。不得把某台机器的一次结果写成“支持 xx TPS / 生产级并发”。

运行方式（默认 1000,10000,50000 三档 + 500 条 warm-up，warm-up 不计结果）：

```bash
BENCHMARK_SIZES=1000 mvn -Pbenchmark test        # smoke 单档
BENCHMARK_SIZES=1000,10000,50000 mvn -Pbenchmark test  # 全量
```

CI 手动实跑：GitHub Actions → benchmark → Run workflow（sizes 默认 1000，
结果进 Artifacts `benchmark-results` + Job Summary）；普通 push/PR CI 永不执行 benchmark。

`@Tag("benchmark")` + Maven `benchmark` profile 保证普通 `mvn test` / 普通 CI
永不执行大规模压测；无 Docker 时 benchmark 自动 skip。

- **架构**：benchmark publisher → Mosquitto → 岸端真实服务 → Kafka
  → MySQL + Redis。全真实基础设施，禁止 mock Kafka/Redis/MySQL、禁止直调
  `Consumer.listen()`、禁止跳过 MQTT。
- **Workload**（`BenchmarkWorkload`，零随机）：100 个固定 MMSI
  （`413900000`…）× 5 种真实 type 严格 round-robin；event 时间 = 固定基点 +
  序号秒数（全局与每键递增）；`msg_id` = `SHA-256(run_id|mmsi|type|seq)`，
  同 size 同配置业务序列相同，`run_id` 仅保证不同 run 不撞 `UNIQUE`；
  `sent_at` 在每条 publish 前即时生成；payload 为合法 Edge 扁平 JSON。
- **隔离**：每档独立 `run_id`；档前 `TRUNCATE` MySQL、`FLUSHDB` Redis；
  DLT 用 assign + seekToBeginning + captured endOffsets 读取完整 snapshot 后断言
  为空（禁止 subscribe 后空 poll 即判空，禁止固定 sleep 猜 drained，只查单分区）；
  档前 readiness 全过才计时。
- **Readiness**（全 bounded wait，无盲目长 sleep）：Mosquitto（岸端已订阅）、
  Kafka（AdminClient 取到 clusterId）、MySQL/Flyway（迁移表可查）、Redis（PING）、
  两组 partitions assigned、发布端 MQTT 建连（带限重试）。
- **指标定义**：
  - Publish 吞吐：第一条 publish 开始 → 最后一条 publish 返回；
  - History 吞吐（整批落库收敛）：第一条发送 → MySQL 收齐全部行；
  - History receive 延迟：每行 `received_at − sent_at`，即 publish 侧 `sent_at` →
    `HistoryConsumer` 构造持久化实体前的接收/处理时间戳，p50/p95/p99/max。
    明确声明：**不包含单条 MySQL insert/commit 完成时间，不是 Kafka broker
    延迟**（整批落库收敛由 History 吞吐衡量）；
  - Redis：只记收敛耗时（首发 → 500 个期望 key 全部就位）+ 期望/实际 key 数 +
    全量最终态校验 + stale 计数；**单消息 Redis 延迟本阶段不测（not measured）**，
    不为指标改生产 Redis schema。
- **百分位**：nearest-rank（`ceil(p/100·N)`），对真实样本排序计算；禁止均值冒充。
- **一致性门**：MySQL 行数 == 发送数、`COUNT(DISTINCT msg_id)` 相等、重复 0；
  Redis 键数 == 500 且每键 timestamp/msg_id 等于输入最新事件；DLT == 0；
  `lost == sent − rows` 必须为 0；stale 必须为 0（有序基线）。
- **输出**：`target/benchmark-results/benchmark-results.{json,csv,md}`（md 表列与
  任务一致）+ 环境信息（时间戳、Java/OS/CPU/内存、镜像版本、配置回显）；
  Micrometer 十个计数器按档取 after−before，避免 context 复用污染。
- **瓶颈观察**：以 Docker 实跑数据为准，当前无实跑结论，不预设瓶颈点。
