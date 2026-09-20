# Smartship-Shore

岸端事件处理系统 **P2-1**：`MQTT → Kafka → DB` 最小可靠链路（单体 Spring Boot，不拆微服务）。

> 本阶段只做 `MQTT → Kafka → MySQL`。Redis、WebSocket、Elasticsearch、告警、微服务拆分、
> Retry/DLT 均为后续阶段，当前代码不包含。

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
MqttIngestService        解析 + 校验 (msg_id/mmsi/type 必填)
      │
      ▼
Kafka Producer           key = MMSI, acks=all + 幂等
      │ key=MMSI
      ▼
ship.telemetry.raw       (6 partitions, 单 topic)
      │
      ▼
HistoryConsumer          group = smartship-history, 手动提交 offset
      │
      ▼
     MySQL               ship_telemetry_history, UNIQUE(msg_id)
```

### 为什么 MQTT 后面还需要 Kafka

| | MQTT (Mosquitto) | Kafka |
|---|---|---|
| 职责 | 船岸弱网通信 / 设备接入 | 岸端事件总线 / 削峰 / 解耦 / 多消费者扩展 |
| 语义 | QoS1 到达即走，无回放 | 日志持久化、可回放、可多订阅 |
| 扩展 | 再加一个消费者就要再订阅一次设备流 | 加 consumer group 即可，不打扰船端 |

船端只管把消息送到 Mosquitto；岸端内部谁消费、消费几次、以后加告警/实时状态，
都由 Kafka 解耦。P2-1 只有一个消费者（HistoryConsumer），后续阶段加消费者不需要改船端。

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
  `acknowledge()`；瞬时 DB 异常（连接/锁/超时）不 ACK，等 Kafka 重投；
  `DuplicateKey` 视为成功并 ACK；Kafka 里已存在的非法 payload 记录明确错误后跳过
  （ACK，避免单条毒消息卡住 partition；Retry/DLT 留给 P2-2）。
- **DB schema**（Flyway `V1__create_ship_telemetry_history.sql`）：
  `id / msg_id UNIQUE / mmsi / type / event_time / sent_at / received_at /
  payload(JSON) / kafka_partition / kafka_offset / created_at`，
  另有 `mmsi / type / event_time` 索引。
  `event_time` 存 Edge 原始 `timestamp`，不用岸端时间覆盖；无 timestamp 的行存 NULL。
- **Metrics**（低基数、无 `mmsi`/`msg_id` tag）：
  `smartship_shore_mqtt_received_total`、`..._mqtt_invalid_total`、
  `..._kafka_produced_total`、`..._kafka_produce_failed_total`、
  `..._history_consumed_total`、`..._history_persisted_total`、
  `..._history_duplicate_total`、`..._history_failed_total`。
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
| — | `HistoryConsumerTest` 毒消息/瞬时异常 | 非法 JSON 跳过并 ACK；瞬时 DB 异常不 ACK 并抛给重投 |
| E2E | `HistoryConsumerTestcontainersTest` | 真实 Kafka(KRaft)+MySQL（含 Flyway V1）；无 Docker 时自动跳过 |

测试不依赖公网 Kafka / 公网 Broker / 真实船舶；不断言跨 partition 全局顺序。

## 7. 已知限制（诚实声明）

- at-least-once，不是 exactly-once；重复由 `UNIQUE(msg_id)` 吸收。
- Producer 异步发送失败只记指标 + 日志（P2-2 才做 Retry/DLT）。
- Kafka 毒消息当前跳过并 ACK（有错误日志 + 指标），P2-2 转 DLT。
- 单 consumer（`concurrency=1`，保 partition 内顺序写库）；吞吐上限即单线程写库能力，未做 benchmark。
- 未验证：百万 TPS、零丢失、高并发生产流量。以上均需后续真实压测数据支撑。
