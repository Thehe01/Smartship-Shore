# SmartShip-Shore 系统架构

> 本文档只描述当前代码真实存在的东西。每一个方框都能在 `src/main` 里找到对应类，
> 每一条语义都有测试背书。没有 Kafka Streams / Flink / ES / WebSocket /
> 微服务集群 / Kubernetes——代码里没有，这里也不画。

## 架构总览

```text
                Edge Device
                    |
                    | MQTT QoS1 (topic: zncb/{mmsi}/{topicSuffix})
                    v

              Shore Ingest (MqttIngestService)
              解析 + 校验 → Kafka（有界同步交接，成功才 ACK）
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
 (ship_telemetry_history, (ship:{mmsi}:latest:{type},
  UNIQUE(msg_id))         Lua CAS + TTL)


          +-------------------+
          |
          v

 Retry / Error Handler / DLT
 (DefaultErrorHandler + FixedBackOff(1000ms, 3) + DLT topic)

          |
          v

 Metrics + Fault Injection Tests
 (Micrometer 低基数计数器 + docs/FAULT_INJECTION.md 六场景)
```

## 组件映射（图 → 代码，一一对应）

| 图中节点 | 代码位置 | 职责（一句话） |
|---|---|---|
| Edge Device | 仓库外（`smartship-edge-core`，只读其协议） | 按 `zncb/{mmsi}/{topicSuffix}` 以 QoS1 上报扁平 JSON，`msg_id` 为确定性 SHA-256 |
| Mosquitto | `docker-compose.yml`（`eclipse-mosquitto:2.0`）+ `mosquitto/config/mosquitto.conf` | 设备接入层：弱网通信，不管回放，不管多订阅 |
| Shore Ingest | `ingest/MqttIngestService.java` + `ingest/TelemetryMessageParser.java` | 订阅 `zncb/+/+`；解析校验后做有界同步 Kafka 交接（默认 5s，`shore.mqtt.kafka-handoff-timeout-ms`），成功才 manual ACK，失败/超时不 ACK 并强制重连触发 broker 重投 |
| Kafka Raw Topic | `config/KafkaConfig.java`（`ship.telemetry.raw`，6 partitions）+ `kafka/TelemetryKafkaProducer.java` | 岸端事件总线：持久化日志、可回放、多 group 独立订阅；Producer `acks=all` + 幂等 + `retries=5` |
| History Consumer | `kafka/HistoryConsumer.java`（group `smartship-history`） | 同一 topic 的归档分支：Envelope 反序列化 → MySQL 落库 → `MANUAL_IMMEDIATE` 提交 offset |
| LatestState Consumer | `kafka/LatestStateConsumer.java`（group `smartship-latest-state`） | 同一 topic 的投影分支：Lua CAS 写 Redis 最新状态，与归档组 offset/重试/DLT 完全独立 |
| MySQL History | `persistence/` + Flyway `V1__create_ship_telemetry_history.sql`（库 `smartship_shore`） | 历史事实存储：追加写、可审计；`UNIQUE(msg_id)` 是幂等吸收点 |
| Redis Latest State | `config/RedisConfig.java` + `redis/latest_state_cas.lua` | 现状物化视图：每船每类一键，原子 CAS + TTL，可直接点查 |
| Retry / DLT | `config/KafkaConfig.java`（`DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `FixedBackOff`） | 两组共用的有限重试/死信：transient 重试 3 次（1s 间隔）后进 `ship.telemetry.raw.DLT`，poison 直达；DLT 落定后原 offset 才推进 |
| Metrics | `observability/ShoreMetrics.java`（`/actuator/prometheus`） | 低基数计数器（无 mmsi/msg_id/异常文本 tag）+ `reason ∈ {transient, poison}` 闭桶 |

## 真实能力标注（面试时可逐条指着代码讲）

- **MQTT QoS1 + manual ACK**：Paho `setManualAcks(true)`，ACK 只跟随 Kafka 交接成功；毒消息明确丢弃并 ACK，避免单条坏消息在 broker 上无限循环。
- **Kafka at-least-once**：QoS1 与 Kafka 都是 at-least-once，重复投递是正常现象，不掩盖。
- **Consumer group isolation**：两组各自推进 offset、各自重试/DLT；投影挂掉不影响归档，反之亦然（P2-4.2.1/2.2 实测）。
- **msg_id idempotency**：Edge 确定性 SHA-256（重传不改变，`sent_at` 不参与指纹）；MySQL `UNIQUE(msg_id)` 撞约束即视为成功并 ACK；Redis 侧同 `msg_id` 直接 STALE。
- **Retry + DLT**：只用 Spring Kafka 官方原语，无自研重试线程；`concurrency=1` 保 partition 内顺序写库。
- **Redis Lua CAS**：比较 + 更新在 Redis 内原子完成，返回 `UPDATED` / `STALE`；迟到事件不能覆盖新状态。
- **Stale event protection**：时间相等看 `sent_at` 再看 `msg_id` 字典序，全确定性；STALE 照常 ACK 且不刷新 TTL。

## 失败路径（都有 Fault Injection 实测，不是纸面设计）

| 故障 | 机制 | 实测 |
|---|---|---|
| MySQL 短暂不可用 | History transient 重试，LatestState 独立投影 | P2-4.2.1 ✅ |
| Redis 短暂不可用 | Latest transient 重试，History 独立落库 | P2-4.2.2 ✅ |
| MySQL 持续不可用 | 重试耗尽 → DLT（`reason=transient`）；恢复后不自动回流 | P2-4.2.3 ✅ |
| Redis 持续不可用 | 同上，LatestState 分支进 DLT | P2-4.2.4 ✅ |
| Kafka 短暂不可用 | Handoff 超时 → 无 ACK → durable 会话重投；恢复后单行 | P2-4.2.5 ✅ |
| Shore 重启 | Kafka replay → `UNIQUE(msg_id)` 吸收，重投永不二次落库 | P2-4.2.6 ✅ |

详见 `docs/FAULT_INJECTION.md`（每场景 run URL 落盘）。

## 运行时拓扑（本地）

`docker compose up -d`：Kafka（KRaft 单节点，无 ZooKeeper）+ Mosquitto + MySQL + Redis。
Host 视角端口：MQTT `localhost:1883`、Kafka `localhost:29092`、
MySQL `localhost:3306`（库 `smartship_shore`，用户见 `.env.example`）、
Redis `localhost:6379`。默认配置 `src/main/resources/application.yml`，
本地覆盖 `application-local.yml`；密码只出现在 yml / 环境变量，不在 Java 源码里。

## 非目标（当前不做，故不画）

WebSocket 实时推送、Elasticsearch 搜索、告警、微服务拆分、百万 TPS 容量声明。
at-least-once，不是 exactly-once；不宣称零丢失，不宣称任意故障自动恢复。
