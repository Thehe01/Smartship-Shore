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
