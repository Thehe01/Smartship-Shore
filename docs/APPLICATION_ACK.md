# Application ACK：Kafka durable 交接确认

> 三级语义，一字不可混用：
>
> - **PUBACK = Broker 接收确认**：Mosquitto 收到了，仅此而已。
> - **Application ACK = Kafka durable handoff 确认**：`ship/{mmsi}/ack` 上的
>   `{msg_id, seq, status=KAFKA_COMMITTED}`，表示该 envelope 已被 Kafka
>   `acks=all` 接收。**不等于 MySQL 已落库**——下游消费仍是异步的。
> - **MySQL/Redis = 后续异步消费与最终收敛**：History 与 LatestState 各自推进，
>   互不阻塞 ACK 链路。

## 契约

- Topic：`ship/{mmsi}/ack`，QoS 1。
- Payload：`{msg_id, seq, status}`，`status` 恒为 `KAFKA_COMMITTED`；
  `seq` 是 Edge 行 `id` 的透明回显（岸端不解读），缺失时 Edge 仅按 `msg_id` 匹配。
- ACK 可能丢失或重复（QoS1），绝不凭空产生：Edge 超时重发，重复靠稳定
  `msg_id` + `UNIQUE(msg_id)` 吸收。不追求 exactly-once。

## Shore 侧行为（`ingest/MqttAckPublisher.java`）

- Kafka send future 成功后发布 ACK，再确认原 MQTT 消息；Kafka 失败/超时路径
  **不发 ACK、不确认原消息**，沿用断连 + QoS1 重投。
- ACK 发布是 best-effort 有界等待（默认 3s，`shore.mqtt.ack-timeout-ms`）：
  失败只记指标（`kafka_ack_published/failed_total`）+ 日志，永不抛——
  Kafka 记录已 durable，丢 ACK 只花 Edge 一次重发。
- 独立 Paho 客户端（`{clientId}-ack`），不与订阅者共用；`shore.mqtt.ack-enabled=false`
  可整体关闭（未升级的 Edge 舰队）。
- `seq` 取自 payload `id` 字段（`TelemetryEnvelope.seq`，data 内同步保留，
  非搬移）。

## Edge 侧行为（`uploader/UploadAckTracker.java` + `DatabaseUploadPoller`）

- PUBACK 只记 `IN_FLIGHT`，游标不动；收到匹配 ACK 才记 `ACKED`。
- 游标 = 自当前值起连续 ACK 的 watermark；乱序 ACK 只记账，缺口前停住。
- 发送用有限滑动窗口（默认在途 200，`maxInFlight`）：满了停发等 ACK。
- 超时未 ACK（默认 30s，`ackTimeoutMs`）按 id 回查重发，`msg_id` 稳定故与之前
  相同；行已清理则放弃跟踪。
- 重启恢复天然成立：游标表持久化，未 ACK 行（id > 游标）下轮照常查出重发。
- `ack.enabled=false` 时退回 PUBACK 即推进的旧语义。

## 测试矩阵

| # | 场景 | 位置 | 断言 |
|---|---|---|---|
| 1 | Kafka 成功 → ACK → 游标推进 | Shore E2E `ApplicationAckE2ETest` + Edge `DatabaseUploadPollerAckTest` | ACK 内容（msg_id/seq/status）+ MySQL 落库 / watermark 推进 |
| 2 | Kafka 失败 → 无 ACK → 游标不动 | 同上 | 15s 静默 + 行数冻结 / 无 ACK 时游标冻结 |
| 3 | ACK 丢失 → 重发 → 成功 | Shore E2E（重发得第二 ACK）+ Edge 超时补发单测 | 最终成功 |
| 4 | ACK 乱序 → 连续水位 | Edge `UploadAckTrackerTest` | 1004 先到停 1002，补 1003 后到 1004 |
| 5 | 重启未 ACK 补传 | Edge `DatabaseUploadPollerAckTest` | 新 tracker + 持久游标只补传之后行 |
| 6 | 重复 msg_id → MySQL 一条 | Shore E2E | `UNIQUE` 吸收，`COUNT(*)==1` |

## 不宣称

- 不宣称 Kafka ACK 等于 MySQL 已落库（下游异步，lag 存在）。
- 不宣称 exactly-once（双向 at-least-once + 幂等吸收）。
- 不宣称任意故障自动恢复（覆盖的是：Kafka 瞬断、ACK 丢失/乱序、Edge 重启）。
