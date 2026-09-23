package com.smartship.shore.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Sustained-pressure soak with a shore-link outage and broker-side backfill.
 *
 * <p>Shape: N paced ship publishers (fixed per-producer totals, rate-capped —
 * totals exact, durations measured) drive the REAL full chain. Phase A runs
 * with shore connected; then shore's MQTT subscriber stops (the outage —
 * Mosquitto keeps accepting Edge publishes and queues QoS1 for the durable
 * shore session); phase B keeps publishing into the outage; then shore
 * reconnects and the backlog drains. No mocks, no direct listener calls.
 *
 * <p>The 5-minute outage is emergent, not slept: phase B's fixed totals at the
 * capped rate take ~300s wall time, during which the frozen history count is
 * asserted every 10s. Every wait is bounded.
 *
 * <p>Opt-in only: {@code @Tag("benchmark")} plus the {@code benchmark} Maven
 * profile, plus {@code BENCHMARK_MODE=soak}. Plain {@code mvn test}, normal CI
 * and the other benchmark scenarios never execute this class; without Docker
 * it skips gracefully.
 */
@Tag("benchmark")
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class SoakBenchmarkTest {

  private static final int DEFAULT_SHIPS = 50;
  private static final int DEFAULT_POINTS_PER_SEC = 50;
  private static final int DEFAULT_STEADY_PER_PRODUCER = 9000;
  private static final int DEFAULT_OUTAGE_PER_PRODUCER = 15000;
  private static final int PUBLISH_WINDOW = 200;

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

  @Container
  @SuppressWarnings("resource")
  static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("smartship_shore")
          .withUsername("shore")
          .withPassword("shore123");

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> MOSQUITTO =
      new GenericContainer<>(DockerImageName.parse("eclipse-mosquitto:2.0"))
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("mosquitto/mosquitto-test.conf"),
              "/mosquitto/config/mosquitto.conf")
          .withExposedPorts(1883)
          .waitingFor(Wait.forListeningPort());

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
          .withExposedPorts(6379)
          .waitingFor(Wait.forListeningPort());

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("shore.mqtt.broker-url",
        () -> "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883));
    registry.add("shore.mqtt.client-id", () -> "bench-soak-shore");
    registry.add("shore.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
  }

  @Autowired
  private MqttIngestService ingestService;

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Autowired
  private ShoreMetrics metrics;

  @Autowired
  private ShoreProperties properties;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private KafkaListenerEndpointRegistry listenerRegistry;

  @Test
  @DisplayName("Soak: paced ships, 5-minute shore outage, backfill drain, exact gates")
  void soakBenchmark() throws Exception {
    Assumptions.assumeTrue("soak".equalsIgnoreCase(System.getenv("BENCHMARK_MODE")),
        "BENCHMARK_MODE=soak selects the sustained-pressure scenario");
    int ships = parsePositive("SOAK_SHIPS", DEFAULT_SHIPS);
    int pointsPerSec = parsePositive("SOAK_POINTS_PER_SEC", DEFAULT_POINTS_PER_SEC);
    int steadyPerProducer = parsePositive("SOAK_STEADY_PER_PRODUCER", DEFAULT_STEADY_PER_PRODUCER);
    int outagePerProducer = parsePositive("SOAK_OUTAGE_PER_PRODUCER", DEFAULT_OUTAGE_PER_PRODUCER);

    Map<String, Object> config = new LinkedHashMap<>();
    config.put("mode", "soak");
    config.put("ships", ships);
    config.put("pointsPerSecPerShip", pointsPerSec);
    config.put("steadyPerProducer", steadyPerProducer);
    config.put("outagePerProducer", outagePerProducer);
    config.put("types", BenchmarkWorkload.TYPES);
    config.put("rawTopic", properties.getKafka().getRawTopic());
    config.put("dltTopic", properties.getKafka().getDltTopic());
    config.put("historyGroup", properties.getKafka().getGroupId());
    config.put("latestGroup", properties.getKafka().getLatestStateGroupId());
    config.put("latestStateTtlSeconds", properties.getRedis().getLatestStateTtlSeconds());

    SoakReport report = SoakReport.create(config);
    report.notes.add("Totals are exact (fixed per-producer counts); only durations vary."
        + " The outage length emerges from the fixed outage totals at the capped rate"
        + " (~300s at 50x50), it is never a slept constant.");
    report.notes.add("Outage mechanism: shore's MQTT subscriber stops while publishers keep"
        + " publishing into Mosquitto, which queues QoS1 for the durable shore session;"
        + " reconnect replays the backlog (store-and-forward backfill).");
    report.notes.add("Percentiles use nearest-rank over real per-row samples.");
    report.notes.add("History P50/P95/P99 are RECEIVE latencies: publish-side sent_at to"
        + " HistoryConsumer receive/processing timestamp; under backlog they include queue"
        + " wait by construction. They exclude per-row MySQL insert/commit time and are"
        + " not Kafka broker latencies.");
    report.notes.add("Redis per-message latency is not measured: latest-state"
        + " projection keeps no per-message consume time.");
    report.notes.add("Single-machine Docker/Testcontainers baseline only: not a production"
        + " capacity claim, not a millions-TPS test.");

    AdminClient admin = AdminClient.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    try {
      readinessGates(admin);

      String runId = "s" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      SoakResult result =
          runCase(runId, ships, pointsPerSec, steadyPerProducer, outagePerProducer);
      report.results.add(result);
      report.configuration.put("runIds", List.of(runId));
    } finally {
      try {
        // Always leave shore subscribed even if the outage phase threw.
        try {
          if (!ingestService.isReady()) {
            ingestService.start();
          }
        } catch (Exception ignored) {
          // Best effort; the test verdict already decided.
        }
        report.writeTo(Paths.get("target/benchmark-results"));
      } finally {
        admin.close();
      }
    }
  }

  // ------------------------------------------------------------------
  // One soak case: phase A connected, phase B into the outage, catch-up drain.
  // ------------------------------------------------------------------

  private SoakResult runCase(String runId, int ships, int pointsPerSec,
      int steadyPerProducer, int outagePerProducer) throws Exception {
    final int totalPerProducer = steadyPerProducer + outagePerProducer;
    final int messages = ships * totalPerProducer;
    // Isolation: no reliance on previous rounds or container leftovers.
    jdbcTemplate.execute("TRUNCATE TABLE ship_telemetry_history");
    redisTemplate.execute((RedisCallback<Object>) connection -> {
      connection.serverCommands().flushDb();
      return null;
    });
    Map<String, Double> countersBefore = snapshotCounters();

    String brokerUrl =
        "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883);
    AtomicLong sentTotal = new AtomicLong();

    // Phase A: paced publish with shore connected.
    long firstSendStart = System.nanoTime();
    runPublishPhase(runId, ships, 0, steadyPerProducer, pointsPerSec, brokerUrl, sentTotal);
    long phaseADone = System.nanoTime();

    // Outage: stop shore's subscriber, settle in-flight Kafka records, freeze proof.
    ingestService.stop();
    long frozenCount = settleHistoryCount();
    long outageStart = System.nanoTime();

    // Phase B: keep publishing into the outage; history must stay frozen.
    ExecutorService outagePubs = newFixedPublishers(ships);
    List<Future<?>> outageFutures = new ArrayList<>(ships);
    try {
      for (int p = 0; p < ships; p++) {
        final int producerIdx = p;
        outageFutures.add(outagePubs.submit((Callable<Void>) () -> {
          publishPaced(runId, ships, producerIdx, steadyPerProducer, outagePerProducer,
              pointsPerSec, brokerUrl, sentTotal);
          return null;
        }));
      }
      long phaseBDeadline = System.nanoTime() + Duration.ofMinutes(30).toNanos();
      while (!outageFutures.stream().allMatch(Future::isDone)) {
        Thread.sleep(10_000);
        long now = repository.countAll();
        if (now != frozenCount) {
          throw new AssertionError("history must freeze during the outage: frozen="
              + frozenCount + " now=" + now);
        }
        if (System.nanoTime() > phaseBDeadline) {
          throw new AssertionError("phase B publish did not finish in 30 minutes");
        }
      }
      for (Future<?> f : outageFutures) {
        f.get(5, TimeUnit.MINUTES);
      }
    } finally {
      outagePubs.shutdownNow();
    }
    long publishWallEnd = System.nanoTime();

    long sent = sentTotal.get();
    if (sent != messages) {
      throw new AssertionError("published " + sent + " != planned " + messages);
    }
    long outageBacklog = sent - frozenCount;

    // Catch-up: reconnect shore, drain the broker-side backlog.
    ingestService.start();
    MqttTestSupport.waitUntil("shore re-subscribed", Duration.ofSeconds(120),
        ingestService::isReady);
    long catchupStart = System.nanoTime();
    try {
      MqttTestSupport.waitUntil("history complete after backfill",
          Duration.ofHours(2), () -> repository.countAll() == messages);
    } catch (AssertionError e) {
      throw new AssertionError(
          "history incomplete after backfill: " + progressSnapshot(countersBefore), e);
    }
    long historyDone = System.nanoTime();

    long rows = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM ship_telemetry_history", Long.class);
    long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    long lost = messages - rows;

    // Per-row history latency samples (received_at - sent_at), nearest-rank.
    List<Map<String, Object>> samples = jdbcTemplate.queryForList(
        "SELECT sent_at, received_at FROM ship_telemetry_history");
    double[] latencies = samples.stream()
        .mapToDouble(r -> toEpochMilli(r.get("received_at")) - toEpochMilli(r.get("sent_at")))
        .sorted()
        .toArray();

    // Redis converge: every expected key holding its final (latest) event.
    Map<String, ExpectedLatest> expected = expectedLatest(runId, ships, messages);
    try {
      MqttTestSupport.waitUntil("redis converged after backfill", Duration.ofHours(1),
          () -> {
            for (Map.Entry<String, ExpectedLatest> e : expected.entrySet()) {
              Object payload = redisTemplate.opsForHash().entries(e.getKey()).get("payload");
              if (payload == null || !payload.toString().contains(e.getValue().msgId())) {
                return false;
              }
            }
            return true;
          });
    } catch (AssertionError e) {
      throw new AssertionError("redis unconverged after backfill: "
          + matchedKeys(expected) + "/" + expected.size() + " keys, "
          + progressSnapshot(countersBefore), e);
    }
    long redisDone = System.nanoTime();

    // Full per-key verification: timestamp AND msg_id equal the input latest.
    for (Map.Entry<String, ExpectedLatest> e : expected.entrySet()) {
      Object payload = redisTemplate.opsForHash().entries(e.getKey()).get("payload");
      JsonNode node = objectMapper.readTree(String.valueOf(payload));
      if (!e.getValue().msgId().equals(node.path("msg_id").asText())
          || !e.getValue().eventTime().equals(Instant.parse(node.path("timestamp").asText()))) {
        throw new AssertionError("latest-state mismatch for key " + e.getKey());
      }
    }
    long actualKeys = redisTemplate.keys("ship:*:latest:*").size();

    // DLT must stay empty across the whole run (fresh group, from earliest).
    long dltRecords = countDltSinceBeginning(runId);

    Map<String, Double> countersAfter = snapshotCounters();
    Map<String, Double> delta = new LinkedHashMap<>();
    countersAfter.forEach((k, v) -> delta.put(k, v - countersBefore.getOrDefault(k, 0.0)));

    SoakResult r = new SoakResult();
    r.runId = runId;
    r.ships = ships;
    r.pointsPerSecPerShip = pointsPerSec;
    r.steadyPerProducer = steadyPerProducer;
    r.outagePerProducer = outagePerProducer;
    r.messages = messages;
    r.achievedPublishRateMsgS =
        messages * 1_000_000_000.0 / Math.max(publishWallEnd - firstSendStart, 1);
    r.publishDurationMs = (publishWallEnd - firstSendStart) / 1_000_000;
    r.publishThroughputMsgS = messages * 1000.0 / Math.max(r.publishDurationMs, 1);
    r.historyCompletionMs = (historyDone - firstSendStart) / 1_000_000;
    r.historyThroughputMsgS = messages * 1000.0 / Math.max(r.historyCompletionMs, 1);
    r.historyReceiveLatencyP50Ms = BenchmarkResult.percentile(latencies, 50);
    r.historyReceiveLatencyP95Ms = BenchmarkResult.percentile(latencies, 95);
    r.historyReceiveLatencyP99Ms = BenchmarkResult.percentile(latencies, 99);
    r.historyReceiveLatencyMaxMs =
        latencies.length == 0 ? Double.NaN : latencies[latencies.length - 1];
    r.outageBacklogMessages = outageBacklog;
    r.catchupDrainMs = (historyDone - catchupStart) / 1_000_000;
    r.redisCompletionMs = (redisDone - firstSendStart) / 1_000_000;
    r.redisExpectedKeys = expected.size();
    r.redisActualKeys = (int) actualKeys;
    r.mysqlRows = rows;
    r.distinctMsgIds = distinct;
    r.dltRecords = dltRecords;
    r.lostMessages = lost;
    r.duplicateCount = delta.getOrDefault("history_duplicate_total", 0.0);
    r.metricsDelta = delta;
    // Phase timing note (not a column): phase A publish wall.
    r.metricsDelta.put("soak_phase_a_publish_ms",
        (double) ((phaseADone - firstSendStart) / 1_000_000));
    r.metricsDelta.put("soak_outage_wall_ms",
        (double) ((catchupStart - outageStart) / 1_000_000));

    // Final consistency gates for this case.
    if (rows != messages) {
      throw new AssertionError("row count " + rows + " != sent " + messages);
    }
    if (distinct != messages) {
      throw new AssertionError("distinct msg_id " + distinct + " != sent " + messages);
    }
    if (lost != 0) {
      throw new AssertionError("lost messages: " + lost);
    }
    if (dltRecords != 0) {
      throw new AssertionError("unexpected DLT records: " + dltRecords);
    }
    if (actualKeys != expected.size()) {
      throw new AssertionError("redis keys " + actualKeys + " != expected " + expected.size());
    }
    return r;
  }

  /** Phase publish helper: paced share per producer, joined with a timeout. */
  private void runPublishPhase(String runId, int ships, int kStart, int kCount,
      int pointsPerSec, String brokerUrl, AtomicLong sentTotal) throws Exception {
    ExecutorService pubs = newFixedPublishers(ships);
    List<Future<?>> futures = new ArrayList<>(ships);
    try {
      for (int p = 0; p < ships; p++) {
        final int producerIdx = p;
        futures.add(pubs.submit((Callable<Void>) () -> {
          publishPaced(runId, ships, producerIdx, kStart, kCount,
              pointsPerSec, brokerUrl, sentTotal);
          return null;
        }));
      }
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.MINUTES);
      }
    } finally {
      pubs.shutdownNow();
    }
  }

  private static ExecutorService newFixedPublishers(int ships) {
    return Executors.newFixedThreadPool(ships,
        r -> {
          Thread t = new Thread(r, "bench-soak-pub");
          t.setDaemon(true);
          return t;
        });
  }

  /**
   * One producer's rate-capped share: own indices {@code k} in
   * {@code [kStart, kStart + kCount)}, global sequence {@code n = k * P + p},
   * over an independent MQTT client and connection. Pacing caps the average at
   * {@code pointsPerSec}; totals stay exact, durations are measured.
   */
  private void publishPaced(String runId, int producerCount, int producerIdx,
      int kStart, int kCount, int pointsPerSec, String brokerUrl, AtomicLong sentTotal)
      throws Exception {
    MqttClient publisher = new MqttClient(
        brokerUrl, "bench-soak-pub-" + runId + "-" + producerIdx + "-" + kStart,
        new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    options.setAutomaticReconnect(false);
    options.setMaxInflight(500);
    connectWithRetry(publisher, options);
    long intervalNanos = 1_000_000_000L / pointsPerSec;
    try {
      long nextDeadline = System.nanoTime();
      int sinceDrain = 0;
      for (int j = 0; j < kCount; j++) {
        int k = kStart + j;
        int n = k * producerCount + producerIdx;
        BenchmarkWorkload.Spec spec =
            BenchmarkWorkload.concurrentSpec(runId, producerCount, n);
        Instant sentAt = Instant.now();
        String json = BenchmarkWorkload.payloadJson(spec, sentAt, n);
        MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        publisher.publish(spec.topic(), message);
        sentTotal.incrementAndGet();
        if (++sinceDrain % PUBLISH_WINDOW == 0) {
          for (IMqttDeliveryToken token : publisher.getPendingDeliveryTokens()) {
            token.waitForCompletion(30_000);
          }
        }
        nextDeadline += intervalNanos;
        long sleepNanos = nextDeadline - System.nanoTime();
        if (sleepNanos > 0) {
          Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
        } else {
          // Behind pace (publish itself slower than the cap): re-anchor so one
          // slow message does not compound into unbounded catch-up burst.
          nextDeadline = System.nanoTime();
        }
      }
      for (IMqttDeliveryToken token : publisher.getPendingDeliveryTokens()) {
        token.waitForCompletion(30_000);
      }
    } finally {
      try {
        publisher.disconnect();
      } catch (Exception ignored) {
        // Best effort; the measured assertions decide.
      }
      publisher.close();
    }
  }

  /**
   * After stopping the subscriber, in-flight Kafka records still land for a
   * while. Settle to a provably stable count (two polls 10s apart equal)
   * before the outage proof starts — otherwise the freeze assertion would
   * chase the drain tail.
   */
  private long settleHistoryCount() throws Exception {
    long previous = -1;
    long deadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
    while (true) {
      Thread.sleep(10_000);
      long now = repository.countAll();
      if (now == previous) {
        return now;
      }
      previous = now;
      if (System.nanoTime() > deadline) {
        throw new AssertionError(
            "history did not settle within 10 minutes after subscriber stop");
      }
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private record ExpectedLatest(String msgId, Instant eventTime) {
  }

  /** Every expected Redis key mapped to its final (latest) input event. */
  private static Map<String, ExpectedLatest> expectedLatest(String runId, int producerCount,
      int messages) {
    Map<String, ExpectedLatest> expected = new LinkedHashMap<>();
    for (int p = 0; p < producerCount; p++) {
      for (int ti = 0; ti < BenchmarkWorkload.TYPES.size(); ti++) {
        int last = BenchmarkWorkload.lastConcurrentSeqFor(producerCount, messages, p, ti);
        if (last >= 0) {
          String mmsi = BenchmarkWorkload.mmsi(p);
          String type = BenchmarkWorkload.TYPES.get(ti);
          expected.put("ship:" + mmsi + ":latest:" + type,
              new ExpectedLatest(BenchmarkWorkload.msgId(runId, mmsi, type, last),
                  BenchmarkWorkload.EVENT_BASE.plusSeconds(last)));
        }
      }
    }
    return expected;
  }

  private static int parsePositive(String name, int def) {
    String raw = System.getenv(name);
    if (raw == null || raw.isBlank()) {
      return def;
    }
    final int v;
    try {
      v = Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must hold a positive int, got: " + raw, e);
    }
    if (v <= 0) {
      throw new IllegalArgumentException(name + " must hold a positive int: " + raw);
    }
    return v;
  }

  private void readinessGates(AdminClient admin) throws Exception {
    // Mosquitto: shore holds the subscription (covers connect + subscribe).
    MqttTestSupport.waitUntil("mqtt subscribed", Duration.ofSeconds(60), ingestService::isReady);
    // Kafka: broker answers metadata.
    MqttTestSupport.waitUntil("kafka ready", Duration.ofSeconds(120), () -> {
      try {
        admin.describeCluster().clusterId().get(10, java.util.concurrent.TimeUnit.SECONDS);
        return true;
      } catch (Exception e) {
        return false;
      }
    });
    // MySQL/Flyway: migrated table answers.
    MqttTestSupport.waitUntil("mysql ready", Duration.ofSeconds(60), () -> {
      try {
        return Integer.valueOf(1).equals(jdbcTemplate.queryForObject("SELECT 1", Integer.class))
            && jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM ship_telemetry_history", Long.class) != null;
      } catch (Exception e) {
        return false;
      }
    });
    // Redis: answers PING.
    MqttTestSupport.waitUntil("redis ready", Duration.ofSeconds(60), () -> {
      try {
        return "PONG".equalsIgnoreCase(redisTemplate.execute(
            (RedisCallback<String>) connection -> connection.ping()));
      } catch (Exception e) {
        return false;
      }
    });
    // Both consumer groups own their partitions (not just "container running").
    MqttTestSupport.waitUntil("history partitions assigned", Duration.ofSeconds(120),
        () -> !assignedPartitions("smartship-history").isEmpty());
    MqttTestSupport.waitUntil("latest partitions assigned", Duration.ofSeconds(120),
        () -> !assignedPartitions("smartship-latest-state").isEmpty());
  }

  private List<TopicPartition> assignedPartitions(String containerId) {
    try {
      var container = (ConcurrentMessageListenerContainer<?, ?>) listenerRegistry
          .getListenerContainer(containerId);
      var assigned = container == null ? null : container.getAssignedPartitions();
      return assigned == null ? List.of() : List.copyOf(assigned);
    } catch (Exception e) {
      return List.of();
    }
  }

  private static void connectWithRetry(MqttClient publisher, MqttConnectOptions options)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (true) {
      try {
        publisher.connect(options);
        return;
      } catch (Exception e) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("benchmark publisher could not connect", e);
        }
        Thread.sleep(500);
      }
    }
  }

  /**
   * Exact DLT record count inside the snapshot visible right now. A fresh group per
   * case plus assign/seek-to-beginning/end-offset reading (see
   * {@link BenchmarkDltSnapshot}) makes this deterministic: an empty first poll can
   * no longer fake an empty DLT while assignment is still in flight.
   */
  private long countDltSinceBeginning(String runId) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-soak-dlt-" + runId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      return BenchmarkDltSnapshot.countSnapshot(
          consumer, properties.getKafka().getDltTopic(), Duration.ofSeconds(30));
    }
  }

  /**
   * JDBC drivers disagree on DATETIME mapping (MySQL: LocalDateTime, H2: Timestamp).
   * Both columns use the same conversion, so the receive-latency delta stays exact.
   */
  private static long toEpochMilli(Object value) {
    if (value instanceof java.sql.Timestamp ts) {
      return ts.getTime();
    }
    if (value instanceof LocalDateTime ldt) {
      return ldt.atZone(BenchmarkWorkload.EDGE_ZONE).toInstant().toEpochMilli();
    }
    if (value instanceof Instant instant) {
      return instant.toEpochMilli();
    }
    if (value instanceof java.util.Date date) {
      return date.getTime();
    }
    throw new IllegalArgumentException(
        "unsupported datetime value: " + (value == null ? "null" : value.getClass()));
  }

  /** One-line progress for timeout assertions: rows/distinct + consumer counters. */
  private String progressSnapshot(Map<String, Double> countersBefore) {
    long rows = -1;
    long distinct = -1;
    try {
      rows = jdbcTemplate.queryForObject(
          "SELECT COUNT(*) FROM ship_telemetry_history", Long.class);
      distinct = jdbcTemplate.queryForObject(
          "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    } catch (Exception ignored) {
      // Best effort; the counters below still discriminate stall vs slowdown.
    }
    Map<String, Double> after = snapshotCounters();
    Map<String, Double> delta = new LinkedHashMap<>();
    after.forEach((k, v) -> delta.put(k, v - countersBefore.getOrDefault(k, 0.0)));
    return "rows=" + rows + " distinct=" + distinct + " delta=" + delta;
  }

  private int matchedKeys(Map<String, ExpectedLatest> expected) {
    int matched = 0;
    for (Map.Entry<String, ExpectedLatest> e : expected.entrySet()) {
      try {
        Object payload = redisTemplate.opsForHash().entries(e.getKey()).get("payload");
        if (payload != null && payload.toString().contains(e.getValue().msgId())) {
          matched++;
        }
      } catch (Exception ignored) {
        // Best effort counting for the timeout message only.
      }
    }
    return matched;
  }

  private Map<String, Double> snapshotCounters() {
    Map<String, Double> snap = new LinkedHashMap<>();
    snap.put("mqtt_received_total", metrics.getMqttReceivedTotal().count());
    snap.put("mqtt_invalid_total", metrics.getMqttInvalidTotal().count());
    snap.put("kafka_produced_total", metrics.getKafkaProducedTotal().count());
    snap.put("kafka_produce_failed_total", metrics.getKafkaProduceFailedTotal().count());
    snap.put("history_consumed_total", metrics.getHistoryConsumedTotal().count());
    snap.put("history_persisted_total", metrics.getHistoryPersistedTotal().count());
    snap.put("history_duplicate_total", metrics.getHistoryDuplicateTotal().count());
    snap.put("history_failed_total", metrics.getHistoryFailedTotal().count());
    snap.put("history_retry_transient", metrics.retryCount("transient"));
    snap.put("history_retry_poison", metrics.retryCount("poison"));
    snap.put("history_dlt_transient", metrics.dltCount("transient"));
    snap.put("history_dlt_poison", metrics.dltCount("poison"));
    snap.put("latest_state_consumed_total", metrics.getLatestStateConsumedTotal().count());
    snap.put("latest_state_updated_total", metrics.getLatestStateUpdatedTotal().count());
    snap.put("latest_state_stale_ignored_total", metrics.getLatestStateStaleIgnoredTotal().count());
    snap.put("latest_state_failed_total", metrics.getLatestStateFailedTotal().count());
    return snap;
  }
}
