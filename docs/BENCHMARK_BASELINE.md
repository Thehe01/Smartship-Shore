# Smartship-Shore baseline benchmark（实跑记录）

> P2-4.1 基线数据落盘。单机 Docker/Testcontainers 基线，不是生产容量证明，
> 不是百万 TPS 测试。不得把某台机器的某次结果写成“支持 xx TPS / 生产级并发”。

## 环境（三次 run 一致）

- Java `17.0.20.1` / OS `Linux 6.17.0-1022-azure amd64` / CPUs `4`
- 镜像：`apache/kafka:3.8.0` / `mysql:8.0` / `redis:7-alpine` / `eclipse-mosquitto:2.0`
- Workload：100 固定 MMSI × 5 种真实 type，严格 round-robin，零随机；
  每档独立 `run_id`，档前 `TRUNCATE` + `FLUSHDB`；500 条 warm-up 不计结果
- 口径：Publish = 首发→末 publish 返回（含 PUBACK 排空的可持续吞吐）；
  History = 首发→MySQL 收齐；P50/P95/P99 为 RECEIVE 延迟
 （`sent_at → HistoryConsumer` 接收时间戳，**不含**单条 insert/commit，
  不是 broker 延迟）；Redis 只记收敛耗时

## 结果（2026-09-21，GitHub Actions `benchmark` workflow 手动 dispatch）

| Messages | Publish msg/s | History msg/s | P50 ms | P95 ms | P99 ms | Redis converge ms | MySQL rows | Redis keys | DLT | Lost | Harness commit | Run |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1000 | 1769.91 | 245.10 | 2119 | 3001 | 3064 | 4242 | 1000/1000 | 500/500 | 0 | 0 | `d9cb088` | [Run 35618393537](https://github.com/Thehe01/Smartship-Shore/actions/runs/35618393537) |
| 10000 | 2435.46 | 360.46 | 17431 | 22971 | 23347 | 27910 | 10000/10000 | 500/500 | 0 | 0 | `e5b5f1c` | [Run 35622631909](https://github.com/Thehe01/Smartship-Shore/actions/runs/35622631909) |
| 50000 | 3078.63 | 465.70 | 54784 | 87159 | 90397 | 107745 | 50000/50000 | 500/500 | 0 | 0 | `e5b5f1c` | [Run 35623305798](https://github.com/Thehe01/Smartship-Shore/actions/runs/35623305798) |
| 2000000 | 3118.22 | 523.96 | 1805660 | 3066056 | 3171124 | 3823368 | 2000000/2000000 | 500/500 | 0 | 0 | `079fa9f` | [Run 35693319147](https://github.com/Thehe01/Smartship-Shore/actions/runs/35693319147) |

- 2M 档（run [#35693319147](https://github.com/Thehe01/Smartship-Shore/actions/runs/35693319147)，
  约 65 分钟）：History 吞吐 523.96，延续 245 → 360 → 466 → 524 的随量反升；
  一致性门全过。P50/P99 量级为分钟（burst 排空形态，口径仍是 RECEIVE 延迟）。

## 并发场景（P2-4.1.1：1 / 10 / 50 producers × 50k）

run [#35707506141](https://github.com/Thehe01/Smartship-Shore/actions/runs/35707506141)
（`46d1d3b`，`mode=concurrent`，约 4h18m，success；harness commit `46d1d3b`）：

| Producers | Per producer | Messages | Publish msg/s | History msg/s | P50 ms | P95 ms | P99 ms | Max lag ms | Recovery ms | Redis converge ms | MySQL rows | Redis keys | DLT | Lost | Duplicates |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 50000 | 50000 | 4058.44 | 159.34 | 140425 | 284264 | 296277 | 301244 | 313798 | 314044 | 50000 | 5/5 | 0 | 0 | 0.00 |
| 10 | 50000 | 500000 | 17039.84 | 195.42 | 1296332 | 2428811 | 2513801 | 2529184 | 2558598 | 2559789 | 500000 | 50/50 | 0 | 0 | 0.00 |
| 50 | 50000 | 2500000 | 18797.84 | 201.05 | 6510108 | 11717702 | 12181916 | 12301192 | 12434891 | 12440747 | 2500000 | 250/250 | 0 | 0 | 0.00 |

- 一致性门三档全过：行数/`DISTINCT` 相等、lost/DLT/duplicates 全零、
  Redis 键数精确（5/5、50/50、250/250）且每键最终态正确。
- 方法：每 producer 独立 client/连接/MMSI，消息 `n` 恒归属 `n % P`，
  `msg_id` 确定可复现；结果进 `concurrent-producer-results.{json,csv,md}`；
  max lag = 最坏单条延迟，recovery = 首发到收齐（独立 offset-lag 采样是后续专项）。
- Publish 随 producer 数 4k → 17k → 18.8k：ingest 侧并行真实。
- History 160–201，低于同总量基线 50k 的 466——**冷热混杂，不归因于并发**：
  并发 P=1 跑在最前且无 warm-up，基线 50k 跑在第三；两 run 内部都递增
 （159 → 195 → 201；245 → 360 → 466）。分离变量需同暖度对照，未做，
  不下因果断言。
- 延迟随 backlog 线性涨（P50 ≈ 半程，P99/max lag 贴尾部，recovery ≈ 排空完成），
  与基线同一排队形状。
- 同样只是单机 Docker 基线，不是生产容量证明。

- 三档一致性门全过：行数 == 发送数、`DISTINCT msg_id` 相等、Redis 每键
  timestamp/msg_id 等于输入最新事件、stale == 0、DLT == 0、lost == 0

## 形状（读数，不做优化断言）

- 吞吐随量反升（History 245 → 360 → 466，Publish 1770 → 2435 → 3079）：
  JVM/MySQL/Kafka 预热进稳态后摊薄，burst 越大越明显
- 延迟随量线性涨（P50 2.1s → 17.4s → 54.8s）：串行 MQTT→Kafka handoff +
  单线程消费（`concurrency=1` 保序），排空时间 ≈ `size/History吞吐`
  （50k：50000/465.7 ≈ 107.4s ≈ Redis 收敛 107.7s），P50 ≈ 半程，P99 贴尾部
- 修复史：1k 档曾暴露 `DATETIME→LocalDateTime` 强转（`9e3b348`）、Paho
  `maxInflight=10` 限流（`d9cb088` bounded PUBACK drain）；10k 档曾因
  Mosquitto 默认有界 per-client 队列静默丢 burst（2205/10000 零错误 clean stop，
  诊断见 `c723c08` 超时进度行），`e5b5f1c` 将 **test** broker 置
  `max_queued_messages 0` 后三档全绿。生产 `mosquitto.conf` 未动，
  部署侧队列 sizing 仍是容量项
