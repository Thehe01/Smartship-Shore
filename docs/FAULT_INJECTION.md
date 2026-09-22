# Smartship-Shore fault injection（P2-4.2 实跑记录）

> 真实制造组件故障，验证现有可靠性机制，不加功能。
> 每个场景：真实 Testcontainers 故障、bounded wait、验证故障中 + 恢复后状态、
> 验证 metrics / MySQL / Redis / DLT。
> 禁止：mock DB 异常冒充故障注入、改生产 retry 参数、靠长 sleep 掩盖时序。

| 场景 | 故障 | 验证 | 状态 |
|---|---|---|---|
| P2-4.2.1 | MySQL 短暂故障（pause → 恢复） | transient retry → 恢复后落库，Redis 不受影响，DLT 0 | ✅ CI 已绿（[Run 35630161627](https://github.com/Thehe01/Smartship-Shore/actions/runs/35630161627)） |
| P2-4.2.2 | Redis 短暂故障（pause → 恢复） | History 不受影响，latest retry → 恢复后投影，DLT 0 | ✅ CI 已绿（[Run 35632810172](https://github.com/Thehe01/Smartship-Shore/actions/runs/35632810172)） |
| P2-4.2.3 | MySQL 持续故障 → DLT | retry 耗尽 → A 进 DLT，恢复后 A 不自动落库，B 正常落库 | ✅ CI 已绿（[Run 35677646998](https://github.com/Thehe01/Smartship-Shore/actions/runs/35677646998)） |
| P2-4.2.4 | Redis 持续故障 → DLT | retry 耗尽 → A 进 DLT，恢复后 A 不自动投影，B 正常 | ✅ CI 已绿（[Run 35679570625](https://github.com/Thehe01/Smartship-Shore/actions/runs/35679570625)） |
| P2-4.2.5 | Kafka 短暂故障 → MQTT QoS1 重投 | 重投循环 → 恢复后单行落库，DLT 0 | ✅ CI 已绿（[Run 35681578037](https://github.com/Thehe01/Smartship-Shore/actions/runs/35681578037)） |
| P2-4.2.6 | Shore crash/restart → replay + 幂等 | 重投被去重吸收，B 正常，DLT 0 | ✅ CI 已绿（[Run 35683110289](https://github.com/Thehe01/Smartship-Shore/actions/runs/35683110289)） |

## P2-4.2.1 MySQL transient outage

用例：`com.smartship.shore.fault.MySqlTransientOutageTest`
（Kafka + MySQL + Mosquitto + Redis 全真实容器，无 Docker 自动 skip）。

- **故障手法**：Docker `pause` 冻住 `mysqld`（连接 hang/fail），
  `unpause` 恢复。`stop/start` 重启要十几秒，会直接冲爆
  `FixedBackOff(1000ms, 3)` 的预算，不符合“重试耗尽前恢复”题设，故不用。
- **快速失败**：仅测试专属 Hikari/driver 超时
  （`connection-timeout=2000`、`validation-timeout=1000`、
  `connection-test-query=SELECT 1`、`socketTimeout=3000`），
  把冻结表现为可重试失败；**生产 retry 参数未动**。
- **时序**（全 bounded wait，无盲 sleep）：ready 门全过 → pause →
  发布 1 条合法消息 → 观测到 `history_retry{transient} > 0` 且 MySQL 仍为 0 行 →
  MySQL 仍暂停时确认 Redis 已投影（LatestState 组独立）→ unpause →
  120s 内收敛到 1 行。
- **验收**：`rows == 1`、`DISTINCT(msg_id) == 1`、DLT topic snapshot == 0、
  `history_retry{transient} > 0`、`history_persisted` 增量 == 1、
  DLT 两 bucket 均为 0、Redis payload 的 `msg_id`/`timestamp` 正确且 TTL 生效、
  `lost == 0`。
- **实跑**：CI 已绿（[Run 35630161627](https://github.com/Thehe01/Smartship-Shore/actions/runs/35630161627)）。
  途中修过一次测试自伤：故障期间曾用 `repository.countAll()` 做“零落库”
  断言，直查 paused MySQL 导致测试线程自己超时 ERROR
  （`b1a580d` 改为读 `history_persisted` 增量，故障期间不再用故障组件观测）。

## P2-4.2.2 Redis transient outage（CI 已绿）

用例：`com.smartship.shore.fault.RedisTransientOutageTest`
（与 2.1 同 infra：Kafka + MySQL + Mosquitto + Redis 全真实容器）。

- **故障手法**：Docker `pause` 冻住 `redis-server`，`unpause` 恢复
  （`finally` 保底，不脏环境）。
- **源码语义**（实现前已读码确认，生产未动）：`LatestStateConsumer` 与
  History 共用 `shoreKafkaListenerContainerFactory`，即同一个 bounded
  retry/DLT handler（`FixedBackOff(1000ms, 3)`）；冻住的 Redis 经 Lettuce
  超时由 Spring 转为 `TransientDataAccessException`，`classify()` 走 cause
  链判 `transient`（可重试），与 History 对称。若 CI 呈现 poison 路由，
  该失败本身即为发现，生产逻辑仍不动。
- **快速失败**：仅测试专属 `spring.data.redis.timeout=2s`（Lettuce 默认
  分钟级命令超时，会把第一次失败推到所有 bounded wait 之外）；
  生产 retry/backoff 未动。
- **时序**（全 bounded wait）：ready 门全过 → pause Redis → 发布 1 条合法
  消息 → Redis 仍暂停时：History 已落库 1 行
  （MySQL 健康，允许直查）且 `latest_state_failed`/`history_retry{transient}`
  增长（只读 metrics，**pause 期间不查 Redis**）→ unpause →
  120s 内 Redis 收敛到正确 payload。
- **验收**：`rows == 1`、`DISTINCT(msg_id) == 1`、`history_persisted` 增量
  == 1、`latest_state_failed` 增量 ≥ 1、transient retry > 0、
  DLT 两 bucket 为 0 且 DLT topic snapshot 为 0、Redis payload
  `msg_id`/`timestamp` 正确且 TTL 生效、`lost == 0`。

## P2-4.2.3 MySQL sustained outage → DLT（CI 已绿）

用例：`com.smartship.shore.fault.MySqlSustainedOutageTest`
（与 2.1 同 infra 与同套 test-only Hikari fast-fail 超时；生产 retry/backoff 未动）。

- **核心命题**：DLT 是最终失败去向，不是自动延迟队列。恢复后 A 不得自行
  落库；再发 B 并成功落库，证明 failed offset 已 recovery/commit 且 consumer
  继续推进。
- **时序**（全 bounded wait）：ready 门全过 → pause MySQL → 发布 A →
  观测到 `history_retry{transient} > 0` 且 `history_persisted` 增量为 0
  （只读 metrics，**pause 期间不查 MySQL**）→ MySQL 仍暂停时确认 Redis 已投影
  A → 继续保持 pause，直到 `history_dlt{transient}` 增量出现——这个 bounded
  wait 本身就是耗尽证明，不靠 sleep 猜 → unpause（`finally` 保底）。
- **恢复后三段式**：先等 MySQL 可查 → 断言 A 不在库且行数为 0；
  再读 DLT snapshot（assign + 固定 endOffsets，和 benchmark 同纪律）：
  恰 1 条、key 为 A 的 MMSI、value 含 A 的 `msg_id`、
  `shore-dlt-reason == transient`；最后发布健康消息 B（不同 MMSI，
  Redis 两键独立、无 CAS 干扰）→ 120s 内落库。
- **验收**：`history_retry{transient} > 0`、`history_dlt{transient}` 增量 == 1、
  poison DLT 增量 0、`history_persisted` 增量 == 1（仅 B）、`rows == 1`、
  `DISTINCT(msg_id) == 1`、B 在库而 A 永不在库、DLT 总数仍 1、
  Redis 两键 payload 与 TTL 正确。无 silent loss：A 在 DLT，B 在 MySQL。

## P2-4.2.4 Redis sustained outage → DLT（CI 已绿）

用例：`com.smartship.shore.fault.RedisSustainedOutageTest`
（与 2.2 同 infra 与同套 test-only `spring.data.redis.timeout=2s`；
生产 retry/backoff 未动）。

- **核心命题**（2.3 的镜像）：DLT 是终局，不是延迟队列。恢复后 A 不得自行
  投影；再发 B 并正常投影，证明 failed offset 已 recovery/commit 且 consumer
  继续推进。
- **组归属证明**（不耦合异常类名）：MySQL 最终持有 A **和** B
  （`history_persisted` 增量 == 2），证明 history 组全程零失败——唯一的 DLT
  记录只能来自 latest-state 组。transient 路由本身由已绿的 2.2 背书。
- **时序**（全 bounded wait）：ready 门全过 → pause Redis → 发布 A →
  History 落库 A（MySQL 健康，允许直查）且 `latest_state_failed`/
  `history_retry{transient}` 增长（只读 metrics，**pause 期间不查 Redis**）→
  继续保持 pause，直到 `history_dlt{transient}` 增量出现（耗尽证明，不靠
  sleep 猜）→ unpause（`finally` 保底）。
- **恢复后三段式**：先等 Redis PING 通 → 断言 A 键无 payload；
  再读 DLT snapshot：恰 1 条、key 为 A 的 MMSI、value 含 A 的 `msg_id`、
  `shore-dlt-reason == transient`；最后发布健康消息 B → 120s 内投影 + 落库。
- **验收**：`latest_state_failed` 增量 ≥ 1、transient retry > 0、
  DLT transient 增量 == 1、poison 增量 0、`history_persisted` 增量 == 2、
  `rows == 2`（A、B 均在库）、A 永不进 Redis、B payload/TTL 正确、
  DLT 总数仍 1。无 silent loss：A = MySQL + DLT，B = MySQL + Redis。

## P2-4.2.5 Kafka transient outage → MQTT QoS1 redelivery（CI 已绿）

用例：`com.smartship.shore.fault.KafkaTransientOutageTest`
（Kafka + MySQL + Mosquitto + Redis 全真实容器；**零** test-only 调参，
生产 5s handoff 超时即是故障转换器）。

- **故障手法**：Docker `pause` 冻住 KRaft broker，`unpause` 恢复
  （`finally` 保底）。`KafkaTemplate.send` 非阻塞，冻住后 future 悬挂，
  5s handoff 到时即触发无 ACK + 强制重连 → durable 会话重投原 QoS1。
- **重投证明**（非事后推断）：ACK 只在 handoff 成功后发出，所以 Kafka 仍
  暂停时 `mqtt_received` 增量 ≥ 2 本身就是重投循环转起来的确定性证据；
  同期 MySQL 0 行、Redis 无投影（两者健康，可直查），证明无泄漏。
- **时序**（全 bounded wait，单次 publish 永不手动重发）：ready 门全过 →
  pause Kafka → 发布 A → 120s 内观测到重投 → unpause → 180s 内 MySQL 收敛
  1 行（hung 住的 send 若在恢复后落地成复本，由 `UNIQUE(msg_id)` 吸收，
  故只断言单行 + `DISTINCT == 1`，不对复本数做脆弱断言）。
- **验收**：`mqtt_received` 增量 ≥ 2、`rows == 1`、`DISTINCT(msg_id) == 1`、
  `history_persisted` 增量 == 1、`kafka_produced` 增量 ≥ 1、
  DLT 两 bucket 为 0 且 DLT topic snapshot 为 0、Redis payload/TTL 正确、
  `lost == 0`。

## P2-4.2.6 Shore crash/restart → replay + 幂等（CI 已绿）

用例：`com.smartship.shore.fault.ShoreRestartReplayTest`
（Kafka + MySQL + Mosquitto + Redis 全真实容器；无基础设施被冻，
故障就是 shore 自己倒下又起来）。

- **等价性说明**（诚实项）：真 kill -9 落在 insert→commit 窗口时，history 组
  的可观测状态恰是“已落库记录因 offset 未提交而重投”。拿真 kill 赌纳秒窗口
  是 timing roulette，故用 `AdminClient.alterConsumerGroupOffsets` 把 history
  组倒回开头——重投输入完全相同，吸收路径（`UNIQUE(msg_id)` →
  `DuplicateKeyException` → 按成功 ack，永不 DLT）是 100% 生产代码。
  无 mock，无生产改动。
- **时序**（全 bounded wait）：ready 门全过 → 发布 A → 稳态（MySQL 1 行 +
  Redis 已投影）→ 停 history 容器（等到 `!isRunning`，无活成员才改 offset，
  防 race）→ 倒回开头并用 `listConsumerGroupOffsets` 回读确认（不盲信）→
  重启容器（等到 running + 分区重新 assigned）。
- **验收**：`history_duplicate` 增量 ≥ 1（重投被吸收的正证据）、重投后仍
  `rows == 1` 且 `DISTINCT == 1`、重启后发 B 正常落库 + 投影
  （`history_persisted` 增量 == 2：A 一次 + B 一次，重投永不二次 persist）、
  DLT 两 bucket 为 0 且 DLT topic snapshot 为 0、Redis B payload/TTL 正确、
  `lost == 0`。
