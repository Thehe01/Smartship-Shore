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
 * P2-4.2.2: real Redis transient outage.
 *
 * <p>Fault: {@code redis-server} frozen with Docker pause (a real infrastructure
 * fault — commands hang/fail, no mocks), then unpaused <b>before</b> the bounded
 * retry ({@code FixedBackOff(1000ms, 3)}) exhausts.
 *
 * <p>Source behavior this test relies on (read from production code, unchanged):
 * {@code LatestStateConsumer} shares the {@code shoreKafkaListenerContainerFactory}
 * and therefore the same bounded retry/DLT handler as history; a frozen Redis
 * surfaces through Lettuce as a command timeout, which Spring translates to a
 * {@code TransientDataAccessException}, so {@code classify()} buckets it
 * {@code transient} (retryable) rather than {@code poison}. If CI ever shows
 * poison routing here, that failure is the finding — production logic stays
 * untouched either way.
 *
 * <p>Expected shape, all asserted:
 * <ol>
 *   <li>one legal message published straight into the outage;</li>
 *   <li>history archives into MySQL normally <b>while Redis is still down</b>
 *   (group isolation, proven via healthy MySQL + metrics — Redis itself is
 *   never queried during the outage);</li>
 *   <li>{@code LatestStateConsumer} fails and enters transient retry
 *   ({@code latest_state_failed} and {@code history_retry{transient}} grow,
 *   shared retry counter: history itself never fails in this scenario);</li>
 *   <li>after unpause, latest-state converges on the correct payload with TTL,
 *   DLT stays {@code 0}, nothing lost.</li>
 * </ol>
 *
 * <p>Only test-scoped Lettuce timeout makes the frozen outage fail fast inside
 * the retry budget; production retry/backoff parameters are untouched.
 * Every wait is bounded; nothing sleeps blindly.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true",
    // Test-only: Lettuce defaults to a minute-scale command timeout, which
    // would push the first failure past every bounded wait. 2s keeps the
    // frozen outage inside the retry budget. Production Redis tuning untouched.
    "spring.data.redis.timeout=2s"
})
class RedisTransientOutageTest {

  private static final String MMSI = "413900102";
  private static final String TYPE = "nmea_gps";
  private static final String TOPIC = "zncb/" + MMSI + "/" + TYPE;
  private static final String REDIS_KEY = "ship:" + MMSI + ":latest:" + TYPE;
  private static final String EVENT_TS = "2026-09-21T16:40:00";
  private static final Instant EXPECTED_EVENT =
      LocalDateTime.parse(EVENT_TS).atZone(ZoneId.of("Asia/Shanghai")).toInstant();
  private static final String MSG_ID =
      EdgeFixtures.edgeStyleMsgId(MMSI, TYPE, "fault-2422", EVENT_TS);

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
    registry.add("shore.mqtt.client-id", () -> "shore-fault-redis-t1");
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
  @DisplayName("P2-4.2.2: frozen Redis → history unaffected, latest retry → recover, DLT 0")
  void redisTransientOutageThenRecover() throws Exception {
    double retryBefore = metrics.retryCount("transient");
    double persistedBefore = metrics.getHistoryPersistedTotal().count();
    double latestFailedBefore = metrics.getLatestStateFailedTotal().count();
    double latestUpdatedBefore = metrics.getLatestStateUpdatedTotal().count();

    readinessGates();

    pauseRedis();
    try {
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC, edgePayload());

      // Group isolation while the outage is still on, proven WITHOUT touching
      // the paused Redis: history (healthy MySQL) archives the same message…
      MqttTestSupport.waitUntil("history archived during redis outage",
          Duration.ofSeconds(90), () -> repository.countAll() == 1L);
      // …while latest-state fails and enters transient retry (metrics only).
      MqttTestSupport.waitUntil("latest transient retry during outage",
          Duration.ofSeconds(90),
          () -> metrics.retryCount("transient") > retryBefore
              && metrics.getLatestStateFailedTotal().count() > latestFailedBefore);
      // Nothing projected yet is implied by the retry loop still holding the
      // offset; the positive proof comes after recovery below. Redis stays
      // unread until unpause.
    } finally {
      // Recover before FixedBackOff(1000ms, 3) exhausts: remaining retries land.
      unpauseRedis();
    }

    MqttTestSupport.waitUntil("latest projected after recovery", Duration.ofSeconds(120),
        () -> {
          Object payload = redisTemplate.opsForHash().entries(REDIS_KEY).get("payload");
          return payload != null && payload.toString().contains(MSG_ID);
        });

    // ---- acceptance (P2-4.2.2) ----
    assertEquals(1L, repository.countAll(), "MySQL rows == 1");
    Long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(1L, distinct, "DISTINCT msg_id == 1");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
        "history_persisted delta == 1");
    assertTrue(metrics.getLatestStateFailedTotal().count() - latestFailedBefore >= 1.0,
        "latest_state_failed > 0 during the outage");
    assertTrue(metrics.retryCount("transient") > retryBefore,
        "transient retry observed during the outage");
    assertEquals(0.0, metrics.dltCount("transient"), "no transient DLT");
    assertEquals(0.0, metrics.dltCount("poison"), "no poison DLT");
    assertEquals(0L, countDltSinceBeginning(), "DLT topic stays empty");
    assertTrue(metrics.getLatestStateUpdatedTotal().count() - latestUpdatedBefore >= 1.0,
        "latest-state projected after recovery (>= tolerates a rare redelivery)");

    JsonNode node = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries(REDIS_KEY).get("payload")));
    assertEquals(MSG_ID, node.path("msg_id").asText(), "redis holds this message");
    assertEquals(EXPECTED_EVENT, Instant.parse(node.path("timestamp").asText()),
        "event time untouched");
    Long ttl = redisTemplate.getExpire(REDIS_KEY, TimeUnit.SECONDS);
    assertTrue(ttl != null && ttl > 0 && ttl <= 86400L, "TTL set on the update");
  }

  // ------------------------------------------------------------------
  // Real fault: freeze / resume redis-server via the Docker daemon.
  // ------------------------------------------------------------------

  private static void pauseRedis() {
    DockerClientFactory.instance().client()
        .pauseContainerCmd(REDIS.getContainerId()).exec();
  }

  private static void unpauseRedis() {
    try {
      DockerClientFactory.instance().client()
          .unpauseContainerCmd(REDIS.getContainerId()).exec();
    } catch (Exception e) {
      throw new IllegalStateException("Redis unpause failed, test env is dirty", e);
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private static String edgePayload() {
    return "{"
        + "\"id\":9002,"
        + "\"ship_id\":\"S902\","
        + "\"mmsi\":\"" + MMSI + "\","
        + "\"type\":\"" + TYPE + "\","
        + "\"msg_id\":\"" + MSG_ID + "\","
        + "\"timestamp\":\"" + EVENT_TS + "\","
        + "\"sent_at\":\"2026-09-21T16:40:05.123+08:00\","
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-redis-t1-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      return BenchmarkDltSnapshot.countSnapshot(
          consumer, properties.getKafka().getDltTopic(), Duration.ofSeconds(30));
    }
  }
}
