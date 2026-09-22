package com.smartship.shore.fault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.benchmark.BenchmarkDltSnapshot;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
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
 * P2-4.2.6: shore crash/restart → Kafka replay absorbed by idempotency.
 *
 * <p>No infrastructure is frozen here — the fault is shore itself going down.
 * A real {@code kill -9} between a successful MySQL insert and the offset
 * commit leaves the history group exactly one observable state: an
 * already-stored record is redelivered because its offset was never
 * committed. Hitting that nanosecond window with a real kill is timing
 * roulette, so this test reproduces <b>that exact consumer-side state</b>
 * deterministically instead: stop the history container, rewind the history
 * group's offsets to the beginning via {@code AdminClient}, restart the
 * container. The replay input (stored record, uncommitted offset) is
 * identical; the absorption path ({@code UNIQUE(msg_id)} →
 * {@code DuplicateKeyException} → acknowledge as success, never DLT) is 100%
 * production code. No mocks, no production changes, every wait bounded.
 *
 * <p>Expected shape, all asserted:
 * <ol>
 *   <li>message A flows normally to steady state (MySQL row + Redis
 *   projection);</li>
 *   <li>history container stopped, group rewound, container restarted: the
 *   replay of A is absorbed ({@code history_duplicate} grows) with still
 *   exactly one row — no second row, no DLT;</li>
 *   <li>liveness: a fresh message B flows normally after the restart
 *   (MySQL + Redis), DLT stays {@code 0}, nothing lost.</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class ShoreRestartReplayTest {

  private static final ZoneId EDGE_ZONE = ZoneId.of("Asia/Shanghai");

  private static final String TYPE = "nmea_gps";

  private static final String MMSI_A = "413900108";
  private static final String TOPIC_A = "zncb/" + MMSI_A + "/" + TYPE;
  private static final String REDIS_KEY_A = "ship:" + MMSI_A + ":latest:" + TYPE;
  private static final String EVENT_TS_A = "2026-09-21T17:20:00";
  private static final String MSG_ID_A =
      EdgeFixtures.edgeStyleMsgId(MMSI_A, TYPE, "fault-2426-A", EVENT_TS_A);

  private static final String MMSI_B = "413900109";
  private static final String TOPIC_B = "zncb/" + MMSI_B + "/" + TYPE;
  private static final String REDIS_KEY_B = "ship:" + MMSI_B + ":latest:" + TYPE;
  private static final String EVENT_TS_B = "2026-09-21T17:25:00";
  private static final Instant EXPECTED_EVENT_B =
      LocalDateTime.parse(EVENT_TS_B).atZone(EDGE_ZONE).toInstant();
  private static final String MSG_ID_B =
      EdgeFixtures.edgeStyleMsgId(MMSI_B, TYPE, "fault-2426-B", EVENT_TS_B);

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
    registry.add("shore.mqtt.client-id", () -> "shore-fault-restart-t1");
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
  @DisplayName("P2-4.2.6: restart replay of stored A absorbed, B flows, DLT 0")
  void restartReplayAbsorbedByIdempotency() throws Exception {
    double persistedBefore = metrics.getHistoryPersistedTotal().count();
    double duplicateBefore = metrics.getHistoryDuplicateTotal().count();
    double latestUpdatedBefore = metrics.getLatestStateUpdatedTotal().count();

    readinessGates();

    // ---- phase 1: steady state. A flows normally end to end.
    MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
        TOPIC_A, edgePayload(MMSI_A, MSG_ID_A, EVENT_TS_A, 9008, "S908"));
    MqttTestSupport.waitUntil("A persisted", Duration.ofSeconds(120),
        () -> repository.countAll() == 1L);
    MqttTestSupport.waitUntil("A projected", Duration.ofSeconds(120), () -> {
      Object payload = redisTemplate.opsForHash().entries(REDIS_KEY_A).get("payload");
      return payload != null && payload.toString().contains(MSG_ID_A);
    });
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count() - persistedBefore);

    // ---- phase 2: the crash state. Stop history, rewind its group to the
    // beginning (== offset never committed past the stored row), restart.
    // From the consumer's view this is indistinguishable from kill -9 in the
    // insert→commit window; only the timing roulette is removed.
    MessageListenerContainer history = listenerRegistry.getListenerContainer("smartship-history");
    history.stop();
    MqttTestSupport.waitUntil("history container stopped", Duration.ofSeconds(60),
        () -> !history.isRunning());
    rewindHistoryGroupToBeginning();
    history.start();
    MqttTestSupport.waitUntil("history container restarted", Duration.ofSeconds(120),
        () -> history.isRunning() && !assignedPartitions("smartship-history").isEmpty());

    // The replay of the stored A must be absorbed as a duplicate…
    MqttTestSupport.waitUntil("replay absorbed as duplicate", Duration.ofSeconds(120),
        () -> metrics.getHistoryDuplicateTotal().count() > duplicateBefore);
    // …with still exactly one row: no second row, no DLT, no loss.
    assertEquals(1L, repository.countAll(), "replay must not create a second row");
    Long distinctAfterReplay = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(1L, distinctAfterReplay, "DISTINCT msg_id still == 1 after replay");
    assertTrue(repository.findByMsgId(MSG_ID_A).isPresent(), "A still exactly once in MySQL");

    // ---- phase 3: liveness. A fresh message flows normally after the restart.
    MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
        TOPIC_B, edgePayload(MMSI_B, MSG_ID_B, EVENT_TS_B, 9009, "S909"));
    MqttTestSupport.waitUntil("B persisted after restart", Duration.ofSeconds(120),
        () -> repository.countAll() == 2L);
    MqttTestSupport.waitUntil("B projected after restart", Duration.ofSeconds(120), () -> {
      Object payload = redisTemplate.opsForHash().entries(REDIS_KEY_B).get("payload");
      return payload != null && payload.toString().contains(MSG_ID_B);
    });

    // ---- acceptance (P2-4.2.6) ----
    assertTrue(metrics.getHistoryDuplicateTotal().count() - duplicateBefore >= 1.0,
        "replay absorbed as duplicate");
    assertEquals(2.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
        "history_persisted delta == 2 (A once + B once, replay never re-persists)");
    assertEquals(2L, repository.countAll(), "MySQL rows == 2 (A and B)");
    Long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(2L, distinct, "DISTINCT msg_id == 2");
    assertTrue(repository.findByMsgId(MSG_ID_B).isPresent(), "B landed in MySQL");
    assertEquals(0.0, metrics.dltCount("transient"), "no transient DLT");
    assertEquals(0.0, metrics.dltCount("poison"), "no poison DLT");
    assertEquals(0L, countDltSinceBeginning(), "DLT topic stays empty");
    assertTrue(metrics.getLatestStateUpdatedTotal().count() - latestUpdatedBefore >= 2.0,
        "latest-state projected A and B");

    JsonNode nodeB = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries(REDIS_KEY_B).get("payload")));
    assertEquals(MSG_ID_B, nodeB.path("msg_id").asText(), "redis holds B");
    assertEquals(EXPECTED_EVENT_B, Instant.parse(nodeB.path("timestamp").asText()),
        "B event time untouched");
    Long ttl = redisTemplate.getExpire(REDIS_KEY_B, TimeUnit.SECONDS);
    assertTrue(ttl != null && ttl > 0 && ttl <= 86400L, "TTL set on the update");
  }

  /**
   * Rewinds the history group's offsets on the raw topic to the beginning.
   * Requires the group's containers stopped (no live members) so the alter
   * cannot race consumption. Bounded by explicit timeouts, never blind.
   */
  private void rewindHistoryGroupToBeginning() throws Exception {
    String bootstrap = KAFKA.getBootstrapServers();
    String group = properties.getKafka().getGroupId();
    String rawTopic = properties.getKafka().getRawTopic();
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-restart-rewind-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Map<TopicPartition, Long> beginnings;
    try (KafkaConsumer<String, String> probe = new KafkaConsumer<>(consumerProps)) {
      List<TopicPartition> partitions = new ArrayList<>();
      probe.partitionsFor(rawTopic).forEach(info ->
          partitions.add(new TopicPartition(rawTopic, info.partition())));
      if (partitions.isEmpty()) {
        throw new AssertionError("raw topic has no partitions: " + rawTopic);
      }
      probe.assign(partitions);
      beginnings = new LinkedHashMap<>(probe.beginningOffsets(partitions));
    }
    Map<TopicPartition, OffsetAndMetadata> rewind = new LinkedHashMap<>();
    beginnings.forEach((tp, offset) -> rewind.put(tp, new OffsetAndMetadata(offset)));
    Map<String, Object> adminProps = new LinkedHashMap<>();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin.alterConsumerGroupOffsets(group, rewind).all()
          .get(30, TimeUnit.SECONDS);
      // Confirm the rewind landed (bounded read, not trust).
      Map<TopicPartition, OffsetAndMetadata> listed = admin
          .listConsumerGroupOffsets(group,
              new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions()
                  .topicPartitions(new ArrayList<>(rewind.keySet())))
          .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
      for (Map.Entry<TopicPartition, OffsetAndMetadata> e : rewind.entrySet()) {
        OffsetAndMetadata actual = listed.get(e.getKey());
        if (actual == null || actual.offset() != e.getValue().offset()) {
          throw new AssertionError("rewind not confirmed for " + e.getKey()
              + ": expected=" + e.getValue().offset() + " actual=" + actual);
        }
      }
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private static String edgePayload(String mmsi, String msgId, String eventTs,
      int rowId, String shipId) {
    return "{"
        + "\"id\":" + rowId + ","
        + "\"ship_id\":\"" + shipId + "\","
        + "\"mmsi\":\"" + mmsi + "\","
        + "\"type\":\"" + TYPE + "\","
        + "\"msg_id\":\"" + msgId + "\","
        + "\"timestamp\":\"" + eventTs + "\","
        + "\"sent_at\":\"2026-09-21T17:20:05.123+08:00\","
        + "\"latitude\":31.2304,"
        + "\"longitude\":121.4737,"
        + "\"speed_knots\":12.5,"
        + "\"course_over_ground\":270.5,"
        + "\"source_database\":\"zncb_auth\"}";
  }

  private void readinessGates() throws Exception {
    MqttTestSupport.waitUntil("mqtt subscribed", Duration.ofSeconds(60),
        ingestService::isReady);
    // MySQL/Flyway answers (also proves the migrated table exists pre-fault).
    MqttTestSupport.waitUntil("mysql ready", Duration.ofSeconds(60), () -> {
      try {
        return repository.countAll() == 0L;
      } catch (Exception e) {
        return false;
      }
    });
    MqttTestSupport.waitUntil("redis ready", Duration.ofSeconds(60), () -> {
      try {
        return "PONG".equalsIgnoreCase(redisTemplate.execute(
            (RedisCallback<String>) connection -> connection.ping()));
      } catch (Exception e) {
        return false;
      }
    });
    // Both consumer groups own partitions before the fault starts.
    MqttTestSupport.waitUntil("history partitions assigned", Duration.ofSeconds(120),
        () -> !assignedPartitions("smartship-history").isEmpty());
    MqttTestSupport.waitUntil("latest partitions assigned", Duration.ofSeconds(120),
        () -> !assignedPartitions("smartship-latest-state").isEmpty());
  }

  private java.util.List<TopicPartition> assignedPartitions(String containerId) {
    try {
      var container = (ConcurrentMessageListenerContainer<?, ?>) listenerRegistry
          .getListenerContainer(containerId);
      var assigned = container == null ? null : container.getAssignedPartitions();
      return assigned == null ? java.util.List.of() : java.util.List.copyOf(assigned);
    } catch (Exception e) {
      return java.util.List.of();
    }
  }

  /**
   * Exact DLT record count in the snapshot visible right now (fresh assign /
   * seek-to-beginning read, same discipline as the benchmark DLT snapshot).
   */
  private long countDltSinceBeginning() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-restart-t1-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      return BenchmarkDltSnapshot.countSnapshot(
          consumer, properties.getKafka().getDltTopic(), Duration.ofSeconds(30));
    }
  }
}
