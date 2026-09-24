# Kafka Topic 副本扩容 Runbook（raw + DLT）

> 背景：`acks=all` 只有在多副本下才是真正的 quorum 确认。应用配置
> （`KAFKA_RAW_TOPIC_REPLICAS` / `KAFKA_DLT_TOPIC_REPLICAS` /
> `KAFKA_MIN_INSYNC_REPLICAS`，默认 `1/1/1`）只影响**新建** Topic：
> KafkaAdmin（`NewTopic` Bean）不会自动给**已存在**的 Topic 加副本。
> 存量单副本 Topic 必须按本手册做一次离线扩容，否则 durable ACK 仍是单机语义。

## 目标状态（生产）

| Topic | 分区 | RF | `min.insync.replicas` |
|---|---|---|---|
| `ship.telemetry.raw` | 6（保持不变） | 3 | 2 |
| `ship.telemetry.raw.DLT` | 6（保持不变） | 3 | 2 |

应用环境变量：`KAFKA_RAW_TOPIC_REPLICAS=3`，`KAFKA_DLT_TOPIC_REPLICAS=3`，
`KAFKA_MIN_INSYNC_REPLICAS=2`（三者不一致时应用启动即 fail-fast）。

## 前置条件

- Kafka 集群 ≥ 3 brokers，且 `default.replication.factor` / 目标 RF ≤ broker 数；
- 有一台能连集群的运维机（带 `kafka-reassign-partitions.sh`，版本与集群一致）；
- 扩容期间允许短暂的 ISR 抖动（不丢数据，`acks=all` 生产端会自动重试）。

## 步骤

### 1. 生成扩容方案（两个 Topic 一起）

`expand-topics.json`（按实际 broker id 调整 `replicas` 列表）：

```json
{
  "version": 1,
  "partitions": [
    {"topic": "ship.telemetry.raw", "partition": 0, "replicas": [1, 2, 3]},
    {"topic": "ship.telemetry.raw", "partition": 1, "replicas": [2, 3, 1]},
    {"topic": "ship.telemetry.raw", "partition": 2, "replicas": [3, 1, 2]},
    {"topic": "ship.telemetry.raw", "partition": 3, "replicas": [1, 2, 3]},
    {"topic": "ship.telemetry.raw", "partition": 4, "replicas": [2, 3, 1]},
    {"topic": "ship.telemetry.raw", "partition": 5, "replicas": [3, 1, 2]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 0, "replicas": [1, 2, 3]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 1, "replicas": [2, 3, 1]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 2, "replicas": [3, 1, 2]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 3, "replicas": [1, 2, 3]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 4, "replicas": [2, 3, 1]},
    {"topic": "ship.telemetry.raw.DLT", "partition": 5, "replicas": [3, 1, 2]}
  ]
}
```

> 分区首副本轮换（1/2/3 交错）是为了把 leader 均匀摊到三个 broker。

### 2. 执行扩容并验证

```bash
# 执行（先 --verify 看计划，再去掉 --verify 真正执行）
kafka-reassign-partitions.sh --bootstrap-server <broker>:9092 \
  --reassignment-json-file expand-topics.json --execute

# 跟踪进度直到 done
kafka-reassign-partitions.sh --bootstrap-server <broker>:9092 \
  --reassignment-json-file expand-topics.json --verify
```

### 3. 收紧 min.insync.replicas

```bash
kafka-configs.sh --bootstrap-server <broker>:9092 --entity-type topics \
  --entity-name ship.telemetry.raw --alter \
  --add-config min.insync.replicas=2

kafka-configs.sh --bootstrap-server <broker>:9092 --entity-type topics \
  --entity-name ship.telemetry.raw.DLT --alter \
  --add-config min.insync.replicas=2
```

### 4. 核对

```bash
kafka-topics.sh --bootstrap-server <broker>:9092 \
  --describe --topic ship.telemetry.raw --topic ship.telemetry.raw.DLT
# 期望：每个分区 Replicas: 3 个，Isr: 3 个（稳定后）
```

应用侧也有启动核对（`TopicDurabilityVerifier`）：实际 RF / `min.insync`
与期望不一致时打 WARN（绝不阻止启动，避免扩容窗口内反复CrashLoop）。
看到 WARN 按本手册处理；稳定后 WARN 消失。

### 5. 更新应用配置并滚动重启

把生产环境的三个环境变量设为 `3/3/2` 后滚动重启 Shore。
新 Topic（将来新增）会自动按期望创建，无需本手册。

## 回滚

`min.insync.replicas` 改回 1（命令同步骤 3）；副本收缩用同样的
reassign 流程把 `replicas` 改回单 broker。注意：回滚期间 durable ACK
退化为单机语义，应尽快完成。
