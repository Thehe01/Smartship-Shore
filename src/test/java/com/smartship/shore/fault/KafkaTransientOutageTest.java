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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * P2-4.2.5: real Kafka transient outage → MQTT QoS1 redelivery → exactly-once effect.
 *
 * <p>Fault: the KRaft broker frozen with Docker pause (a real infrastructure
 * fault — produce/fetch stall, no mocks), then unpaused. This is the only
 * fault scenario that needs <b>zero</b> test-only tuning: the production
 * 5s MQTT→Kafka handoff timeout ({@code shore.mqtt.kafka-handoff-timeout-ms})
 * is exactly what turns the stalled send into a redelivery cycle.
 *
 * <p>Mechanism under test (read from production code, unchanged):
 * {@code KafkaTemplate.send} is non-blocking, so while the broker is frozen
 * the send future simply never completes; {@code MqttIngestService} hits the
 * bounded handoff wait, holds the MQTT acknowledgment (ACK leaves only after
 * a successful handoff), and forces a reconnect — the durable session
 * ({@code cleanSession=false}, stable client id) makes Mosquitto redeliver
 * the original QoS1 message. Every cycle without an ACK is one more
 * redelivery, so {@code mqtt_received >= 2} <b>while Kafka is still
 * paused</b> is the deterministic proof that the redelivery loop engaged
 * (not a post-hoc inference).
 *
 * <p>Expected shape, all asserted:
 * <ol>
 *   <li>message A published exactly once, straight into the outage;</li>
 *   <li>while Kafka is still down: {@code mqtt_received} grows past the first
 *   delivery (redelivery proven), MySQL holds nothing and Redis holds nothing
 *   (both healthy, directly readable — nothing leaked through);</li>
 *   <li>after unpause: exactly one row lands ({@code DISTINCT(msg_id) == 1},
 *   a hung-then-landed duplicate send is absorbed by {@code UNIQUE(msg_id)}),
 *   Redis projects A, DLT stays {@code 0}, nothing lost.</li>
 * </ol>
 *
 * <p>Every wait is bounded; {@code finally} guarantees unpause.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class KafkaTransientOutageTest {

  private static final String MMSI = "413900107";
  private static final String TYPE = "nmea_gps";
  private static final String TOPIC = "zncb/" + MMSI + "/" + TYPE;
  private static final String REDIS_KEY = "ship:" + MMSI + ":latest:" + TYPE;
  private static final String EVENT_TS = "2026-09-21T17:10:00";
  private static final Instant EXPECTED_EVENT =
      LocalDateTime.parse(EVENT_TS).atZone(ZoneId.of("Asia/Shanghai")).toInstant();
  private static final String MSG_ID =
      EdgeFixtures.edgeStyleMsgId(MMSI, TYPE, "fault-2425", EVENT_TS);

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
    registry.add("shore.mqtt.client-id", () -> "shore-fault-kafka-t1");
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
  @DisplayName("P2-4.2.5: frozen Kafka → QoS1 redelivery loop → recover → exactly one row, DLT 0")
  void kafkaTransientOutageThenRecover() throws Exception {
    double mqttReceivedBefore = metrics.getMqttReceivedTotal().count();
    double persistedBefore = metrics.getHistoryPersistedTotal().count();
    double producedBefore = metrics.getKafkaProducedTotal().count();
    double latestUpdatedBefore = metrics.getLatestStateUpdatedTotal().count();

    readinessGates();

    pauseKafka();
    try {
      // THE single publish of this test: no manual re-publish follows, ever.
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC, edgePayload());

      // Redelivery proof while the outage is still on: no ACK ever leaves
      // without a successful handoff, so every delivery past the first IS a
      // broker redelivery of the original QoS1 message.
      MqttTestSupport.waitUntil("mqtt redelivery during kafka outage",
          Duration.ofSeconds(120),
          () -> metrics.getMqttReceivedTotal().count() - mqttReceivedBefore >= 2.0);

      // Nothing leaked through while Kafka was down (MySQL and Redis are both
      // healthy — directly readable): no row, no projection, no persistence.
      assertEquals(0L, repository.countAll(), "no history row while Kafka is down");
      assertEquals(0.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
          "history must not persist while Kafka is down");
      Object payloadDuringOutage =
          redisTemplate.opsForHash().entries(REDIS_KEY).get("payload");
      assertTrue(payloadDuringOutage == null,
          "redis must hold nothing while Kafka is down");
    } finally {
      unpauseKafka();
    }

    MqttTestSupport.waitUntil("history persisted after kafka recovery",
        Duration.ofSeconds(180), () -> repository.countAll() == 1L);
    MqttTestSupport.waitUntil("redis projected after kafka recovery",
        Duration.ofSeconds(120), () -> {
          Object payload =
              redisTemplate.opsForHash().entries(REDIS_KEY).get("payload");
          return payload != null && payload.toString().contains(MSG_ID);
        });

    // ---- acceptance (P2-4.2.5) ----
    assertTrue(metrics.getMqttReceivedTotal().count() - mqttReceivedBefore >= 2.0,
        "original + broker redelivery both arrived (single MQTT publish)");
    assertEquals(1L, repository.countAll(), "MySQL rows == 1");
    Long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(1L, distinct, "DISTINCT msg_id == 1");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
        "history_persisted == 1");
    assertTrue(metrics.getKafkaProducedTotal().count() - producedBefore >= 1.0,
        "handoff landed after recovery");
    assertEquals(0.0, metrics.dltCount("transient"), "no transient DLT");
    assertEquals(0.0, metrics.dltCount("poison"), "no poison DLT");
    assertEquals(0L, countDltSinceBeginning(), "DLT topic stays empty");
    assertTrue(metrics.getLatestStateUpdatedTotal().count() - latestUpdatedBefore >= 1.0,
        "latest-state projected (>= tolerates a rare broker redelivery)");

    JsonNode node = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries(REDIS_KEY).get("payload")));
    assertEquals(MSG_ID, node.path("msg_id").asText(), "redis holds this message");
    assertEquals(EXPECTED_EVENT, Instant.parse(node.path("timestamp").asText()),
        "event time untouched");
    Long ttl = redisTemplate.getExpire(REDIS_KEY, TimeUnit.SECONDS);
    assertTrue(ttl != null && ttl > 0 && ttl <= 86400L, "TTL set on the update");
  }

  // ------------------------------------------------------------------
  // Real fault: freeze / resume the KRaft broker via the Docker daemon.
  // ------------------------------------------------------------------

  private static void pauseKafka() {
    DockerClientFactory.instance().client()
        .pauseContainerCmd(KAFKA.getContainerId()).exec();
  }

  private static void unpauseKafka() {
    try {
      DockerClientFactory.instance().client()
          .unpauseContainerCmd(KAFKA.getContainerId()).exec();
    } catch (Exception e) {
      throw new IllegalStateException("Kafka unpause failed, test env is dirty", e);
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private static String edgePayload() {
    return "{"
        + "\"id\":9007,"
        + "\"ship_id\":\"S907\","
        + "\"mmsi\":\"" + MMSI + "\","
        + "\"type\":\"" + TYPE + "\","
        + "\"msg_id\":\"" + MSG_ID + "\","
        + "\"timestamp\":\"" + EVENT_TS + "\","
        + "\"sent_at\":\"2026-09-21T17:10:05.123+08:00\","
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
    // Both consumer groups own partitions before the fault starts (this also
    // proves the broker answers metadata pre-fault).
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-kafka-t1-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      return BenchmarkDltSnapshot.countSnapshot(
          consumer, properties.getKafka().getDltTopic(), Duration.ofSeconds(30));
    }
  }
}
