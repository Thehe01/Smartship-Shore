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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
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
 * P2-4.1 baseline benchmark: deterministic workload through the REAL full chain
 * (benchmark publisher → Mosquitto → shore → Kafka → MySQL + Redis), no mocks,
 * no direct listener calls, no skipped MQTT hop.
 *
 * <p>Opt-in only: {@code @Tag("benchmark")} plus the {@code benchmark} Maven profile.
 * Plain {@code mvn test} and normal CI never execute this class; without Docker it
 * skips gracefully.
 *
 * <p>One Spring context and one container set serve the whole run (warm-up + every
 * size). State is cleaned between cases (MySQL {@code TRUNCATE}, Redis
 * {@code FLUSHDB}); each case carries its own {@code run_id} inside every
 * {@code msg_id} so reruns never collide on {@code UNIQUE(msg_id)}.
 */
@Tag("benchmark")
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class ShoreBenchmarkTest {

  private static final int WARMUP_MESSAGES = 500;
  private static final int PUBLISH_WINDOW = 200;
  private static final String DEFAULT_SIZES = "1000,10000,50000";

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
    registry.add("shore.mqtt.client-id", () -> "bench-shore");
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
  @DisplayName("Baseline: deterministic workload through the real chain, per size")
  void baselineBenchmark() throws Exception {
    List<Integer> sizes = parseSizes();
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("sizes", sizes);
    config.put("mmsiCount", BenchmarkWorkload.MMSI_COUNT);
    config.put("types", BenchmarkWorkload.TYPES);
    config.put("warmupMessages", WARMUP_MESSAGES);
    config.put("rawTopic", properties.getKafka().getRawTopic());
    config.put("dltTopic", properties.getKafka().getDltTopic());
    config.put("historyGroup", properties.getKafka().getGroupId());
    config.put("latestGroup", properties.getKafka().getLatestStateGroupId());
    config.put("latestStateTtlSeconds", properties.getRedis().getLatestStateTtlSeconds());

    BenchmarkReport report = BenchmarkReport.create(config);
    report.notes.add("Warm-up (500 messages) is excluded from every result below.");
    report.notes.add("Percentiles use nearest-rank over real per-row samples.");
    report.notes.add("History P50/P95/P99 are RECEIVE latencies: publish-side sent_at to"
        + " HistoryConsumer receive/processing timestamp; they exclude per-row MySQL"
        + " insert/commit time and are not Kafka broker latencies. Batch persistence"
        + " convergence is measured by history completion/throughput instead.");
    report.notes.add("Redis per-message latency is not measured in P2-4.1: latest-state"
        + " projection keeps no per-message consume time.");
    report.notes.add("Single-machine Docker/Testcontainers baseline only: not a production"
        + " capacity claim, not a millions-TPS test.");

    AdminClient admin = AdminClient.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    try {
      readinessGates(admin);

      // Warm-up: validates the whole chain end to end, then discarded.
      runCase("warmup", WARMUP_MESSAGES);

      List<String> runIds = new ArrayList<>();
      for (int size : sizes) {
        String runId = "r" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        runIds.add(runId);
        BenchmarkResult result = runCase(runId, size);
        report.results.add(result);
      }
      report.configuration.put("runIds", runIds);
    } finally {
      try {
        report.writeTo(Paths.get("target/benchmark-results"));
      } finally {
        admin.close();
      }
    }
  }

  // ------------------------------------------------------------------
  // One measured case (null report = warm-up, still fully asserted).
  // ------------------------------------------------------------------

  private BenchmarkResult runCase(String runId, int size) throws Exception {
    // Isolation: no reliance on previous rounds or container leftovers.
    jdbcTemplate.execute("TRUNCATE TABLE ship_telemetry_history");
    redisTemplate.execute((RedisCallback<Object>) connection -> {
      connection.serverCommands().flushDb();
      return null;
    });
    Map<String, Double> countersBefore = snapshotCounters();

    List<BenchmarkWorkload.Spec> workload = BenchmarkWorkload.build(runId, size);

    MqttClient publisher = new MqttClient(
        "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883),
        "bench-pub-" + runId, new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    options.setAutomaticReconnect(false);
    // Paho defaults maxInflight to 10: a tight QoS1 loop over 1k+ messages would
    // hit "Too many publishes in progress" before the broker PUBACKs drain.
    // Bounded-inflight publishing below keeps at most PUBLISH_WINDOW unacked.
    options.setMaxInflight(500);
    connectWithRetry(publisher, options);

    // A. Publish throughput: first publish start -> last publish return (includes
    // bounded PUBACK drains, so this is sustainable publish throughput, not
    // fire-and-forget enqueue rate).
    long firstSendStart = System.nanoTime();
    int published = 0;
    try {
      for (BenchmarkWorkload.Spec spec : workload) {
        Instant sentAt = Instant.now();
        String json = BenchmarkWorkload.payloadJson(spec, sentAt, spec.seq());
        MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        publisher.publish(spec.topic(), message);
        if (++published % PUBLISH_WINDOW == 0) {
          for (IMqttDeliveryToken token : publisher.getPendingDeliveryTokens()) {
            token.waitForCompletion(30_000);
          }
        }
      }
      for (IMqttDeliveryToken token : publisher.getPendingDeliveryTokens()) {
        token.waitForCompletion(30_000);
      }
    } finally {
      try {
        publisher.disconnect();
      } catch (Exception ignored) {
        // Best effort; the measured assertions below decide.
      }
      publisher.close();
    }
    long publishDone = System.nanoTime();

    Duration budget = Duration.ofSeconds(90L + size / 100L);

    // B. End-to-end completion: first send -> MySQL holding every row.
    MqttTestSupport.waitUntil("history complete for " + size, budget,
        () -> repository.countAll() == size);
    long historyDone = System.nanoTime();

    long rows = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM ship_telemetry_history", Long.class);
    long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    long lost = size - rows;

    // C. Per-row history latency samples (received_at - sent_at), nearest-rank.
    // MySQL DATETIME(3) reads back as LocalDateTime while H2 reads back as
    // Timestamp, so coerce both (same zone both sides: the delta is zone-free).
    List<Map<String, Object>> samples = jdbcTemplate.queryForList(
        "SELECT sent_at, received_at FROM ship_telemetry_history");
    double[] latencies = samples.stream()
        .mapToDouble(r -> toEpochMilli(r.get("received_at")) - toEpochMilli(r.get("sent_at")))
        .sorted()
        .toArray();

    // Redis converge: every expected key holding its final (latest) event.
    Map<String, ExpectedLatest> expected = expectedLatest(runId, size);
    MqttTestSupport.waitUntil("redis converged for " + size, budget, () -> {
      for (Map.Entry<String, ExpectedLatest> e : expected.entrySet()) {
        Object payload = redisTemplate.opsForHash().entries(e.getKey()).get("payload");
        if (payload == null || !payload.toString().contains(e.getValue().msgId())) {
          return false;
        }
      }
      return true;
    });
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

    BenchmarkResult r = new BenchmarkResult();
    r.runId = runId;
    r.messages = size;
    r.mmsiCount = BenchmarkWorkload.MMSI_COUNT;
    r.typeCount = BenchmarkWorkload.TYPES.size();
    r.publishDurationMs = (publishDone - firstSendStart) / 1_000_000;
    r.publishThroughputMsgS = size * 1000.0 / Math.max(r.publishDurationMs, 1);
    r.historyCompletionMs = (historyDone - firstSendStart) / 1_000_000;
    r.historyThroughputMsgS = size * 1000.0 / Math.max(r.historyCompletionMs, 1);
    r.historyReceiveLatencyP50Ms = BenchmarkResult.percentile(latencies, 50);
    r.historyReceiveLatencyP95Ms = BenchmarkResult.percentile(latencies, 95);
    r.historyReceiveLatencyP99Ms = BenchmarkResult.percentile(latencies, 99);
    r.historyReceiveLatencyMaxMs =
        latencies.length == 0 ? Double.NaN : latencies[latencies.length - 1];
    r.redisCompletionMs = (redisDone - firstSendStart) / 1_000_000;
    r.redisExpectedKeys = expected.size();
    r.redisActualKeys = (int) actualKeys;
    r.mysqlRows = rows;
    r.distinctMsgIds = distinct;
    r.dltRecords = dltRecords;
    r.lostMessages = lost;
    r.metricsDelta = delta;

    // Final consistency gates for this case.
    if (rows != size) {
      throw new AssertionError("row count " + rows + " != sent " + size);
    }
    if (distinct != size) {
      throw new AssertionError("distinct msg_id " + distinct + " != sent " + size);
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
    if (delta.getOrDefault("latest_state_stale_ignored_total", 0.0) != 0.0) {
      throw new AssertionError("stale events observed in an ordered baseline");
    }
    if (delta.getOrDefault("latest_state_updated_total", 0.0) != size) {
      throw new AssertionError("expected every message to update latest-state");
    }
    return r;
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private record ExpectedLatest(String msgId, Instant eventTime) {
  }

  /** Every expected Redis key mapped to its final (latest) input event. */
  private static Map<String, ExpectedLatest> expectedLatest(String runId, int size) {
    Map<String, ExpectedLatest> expected = new LinkedHashMap<>();
    for (int mi = 0; mi < BenchmarkWorkload.MMSI_COUNT; mi++) {
      for (int ti = 0; ti < BenchmarkWorkload.TYPES.size(); ti++) {
        int last = BenchmarkWorkload.lastSeqFor(mi, ti, size);
        if (last < size) {
          String mmsi = BenchmarkWorkload.mmsi(mi);
          String type = BenchmarkWorkload.TYPES.get(ti);
          expected.put("ship:" + mmsi + ":latest:" + type,
              new ExpectedLatest(BenchmarkWorkload.msgId(runId, mmsi, type, last),
                  BenchmarkWorkload.EVENT_BASE.plusSeconds(last)));
        }
      }
    }
    return expected;
  }

  private static List<Integer> parseSizes() {
    String raw = System.getenv("BENCHMARK_SIZES");
    if (raw == null || raw.isBlank()) {
      raw = DEFAULT_SIZES;
    }
    List<Integer> sizes = new ArrayList<>();
    for (String part : raw.split(",")) {
      final int size;
      try {
        size = Integer.parseInt(part.trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "BENCHMARK_SIZES must hold positive ints, got: " + raw, e);
      }
      if (size <= 0) {
        throw new IllegalArgumentException("BENCHMARK_SIZES must hold positive ints: " + raw);
      }
      sizes.add(size);
    }
    return sizes;
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-dlt-" + runId);
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
    if (value instanceof Timestamp ts) {
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

  private Map<String, Double> snapshotCounters() {    Map<String, Double> snap = new LinkedHashMap<>();
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
