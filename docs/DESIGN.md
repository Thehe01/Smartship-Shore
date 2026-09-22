# SmartShip-Shore 系统设计（DESIGN）

> 面试导向：每一节都能在 10 分钟讲完，且每一句话都能指到代码或实测数据。
> 只描述已实现的东西；第 7 节是唯一例外，明确标为**未实现**。
> 总览图与组件映射见 [`docs/architecture.md`](architecture.md)，
> 实测数据见 [`docs/BENCHMARK_BASELINE.md`](BENCHMARK_BASELINE.md) 与
> [`docs/FAULT_INJECTION.md`](FAULT_INJECTION.md)，这里不重复贴表。

## 1. 背景与目标

船端（Edge）经 MQTT 把遥测送上岸；岸端要回答三个问题：消息进来了别丢（接入可靠）、
进了系统别乱（消费有序 + 幂等）、坏了别停（失败隔离 + 可审计）。
设计约束只有一条：**重复是常态，丢失不可接受，exactly-once 不追求**——
用 at-least-once + 确定性幂等覆盖全部崩溃窗口。

## 2. 总体链路（一句话）

```
Edge --MQTT QoS1--> Mosquitto --shore ingest--> Kafka --双group--> MySQL历史 + Redis现状
```

MQTT 是设备接入层（弱网、到达即走），Kafka 是岸端事件总线（持久、可回放、
多订阅），MySQL 是历史事实，Redis 是现状视图。加消费者只加 group，不碰船端。

## 3. 关键设计决策（每条：决策 → 理由 → 代码 → 实测）

### 3.1 MQTT 接入：manual ACK + 有界交接 + 断线重投

- Paho `setManualAcks(true)`，ACK 只跟随 Kafka 交接成功
 （`ingest/MqttIngestService.java`）。
- 交接是**有界同步等待**（默认 5s，`shore.mqtt.kafka-handoff-timeout-ms`），
  串行回调天然保证 ACK 顺序 = 到达顺序；失败/超时不 ACK 并主动断线，
  同一 clientId + `cleanSession=false` 重连，broker 重投原 QoS1。
- 毒消息明确丢弃并 ACK，不让单条坏消息卡死接入。
- 实测：P2-4.2.5（Kafka 冻结 → 重投循环 → 恢复后单行）。

### 3.2 Kafka 总线：MMSI 分区 + 双 group 隔离 + 手动 offset

- Key = MMSI：同船同 partition，partition 内有序；不声称全局有序
  （`kafka/TelemetryKafkaProducer.java`，`acks=all` + 幂等 + `retries=5`）。
- `smartship-history` 与 `smartship-latest-state` 各自推进 offset、
  各自重试/DLT，一侧故障不拖住另一侧（`config/KafkaConfig.java`，
  `concurrency=1` 保序）。
- `MANUAL_IMMEDIATE`：DB/DLT 落定后才提交，失败期间不提前提交。
- 实测：P2-4.2.1/2.2（单侧 freeze，另一侧照常）。

### 3.3 幂等：全链路唯一的 msg_id 锚点

- Edge `msg_id` 是确定性 SHA-256（业务自然键 + 事件时间，重传不变，
  `sent_at` 不参与指纹）；岸端不自创唯一性。
- MySQL `UNIQUE(msg_id)`：撞约束即成功并 ACK（`persistence/`）。
- Redis 同 `msg_id` 直接 STALE，状态与 TTL 都不动（`redis/latest_state_cas.lua`）。
- 崩溃窗口可解释：insert 成功 → commit 前 crash → 重投 → 约束拦截 →
  offset 推进，永远只有一行。
- 实测：P2-4.2.6（offset 倒回重放 → `history_duplicate` 吸收，无第二行）。

### 3.4 Retry 与 DLT：官方原语 + 闭桶分类 + 提交安全

- 只用 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `FixedBackOff`，
  无自研重试线程。
- transient（瞬时 DB 异常）1s 间隔重试 3 次后进 `ship.telemetry.raw.DLT`；
  poison（坏 JSON/缺字段/确定性 SQL）直达 DLT，不做无意义重试。
- DLT 落定后原 offset 才推进（`commitRecovered`）；DLT 发送本身有界
  （超时配置 fail-fast，`delivery >= request + linger` 启动即校验），
  失败则不推进、不计数、等重投继续恢复。
- DLT 存原 topic/partition/offset、key、原始 payload、异常信息与
  `shore-dlt-reason ∈ {transient, poison}`——是终局，不是延迟队列。
- 实测：P2-4.2.3/2.4（耗尽进 DLT；恢复后 A 不自动回流，B 探活证明组存活）。

### 3.5 Redis 投影：Lua CAS + 时间裁决 + TTL 语义

- Key `ship:{mmsi}:latest:{type}`，比较 + 更新在 Redis 内原子完成，
  返回 `UPDATED` / `STALE`（`config/RedisConfig.java`）。
- 裁决只用 Edge 原始 `timestamp`： strictly newer 才写；时间相等看 `sent_at`，
  再看 `msg_id` 字典序，全确定性。迟到事件永远覆盖不了新状态。
- `UPDATED` 刷新 TTL（默认 86400s），`STALE` 不刷新；两者都正常 ACK，
  Redis 异常才不 ACK 并进共用重试/DLT。

## 4. 一致性论证（三句话）

1. 任何两端之间的交接都是“下游落定才确认上游”：MQTT ACK 跟随 Kafka 成功，
   Kafka offset 跟随 MySQL/Redis（含 DLT）落定——**确认方向永远向后**。
2. 确认之前 crash，一切重投都是已验证可吸收的：MQTT 重投走同一解析，
   Kafka 重投撞同一约束。
3. 确认之后 crash，不需要任何恢复动作：offset 已过，数据已落。

## 5. 可观测性与实测背书

- Metrics 低基数（无 mmsi/msg_id/异常文本 tag），
  `reason ∈ {transient, poison}` 闭桶（`observability/ShoreMetrics.java`，
  `/actuator/prometheus`）。
- 六故障场景实测全绿（run URL 在 `docs/FAULT_INJECTION.md`）。
- 三档基线 + 三档并发 + 2M 基线落盘（`docs/BENCHMARK_BASELINE.md`）：
  单机基线，非容量证明；History 单线程是已知上限，数字里看得见。

## 6. Trade-offs（详见 README §4，这里只留结论）

- 不用 exactly-once：三系统无单事务，at-least-once + 幂等更便宜、更可解释。
- Redis/MySQL 分离：点查现状 vs 追加审计，读写路径与故障域都不同。
- 用 DLT：失败与进度解耦，毒消息不阻塞 partition，现场完整保留。

## 7. Future 演进（以下均未实现，只是方向）

- 读路径：WebSocket 实时推送、ES 搜索；写路径不动。
- 消费并发：`concurrency=1` 是当前吞吐上限，放开需重做顺序保证。
- DLT 回放工具：当前只有现场保留，无一键回放。
- 精细退避：Kafka 长不可用时逐条断线重连偏粗，可加退避。
- 独立 offset-lag 采样：benchmark 当前用派生值（最坏延迟/排空时间），专项另起。

## 8. 十分钟阅读顺序（给面试官的）

`README §1–§3`（是什么）→ 本文档 §3（为什么这样设计）→
`docs/FAULT_INJECTION.md`（坏了会怎样）→
`docs/BENCHMARK_BASELINE.md`（跑起来什么样）。
