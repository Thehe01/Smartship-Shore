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
 * P2-4.2.1: real MySQL transient outage.
 *
 * <p>Fault: {@code mysqld} frozen with Docker pause (a real infrastructure fault —
 * connections hang/fail, no mocks), then unpaused <b>before</b> the bounded
 * history retry ({@code FixedBackOff(1000ms, 3)}) exhausts.
 *
 * <p>Expected shape, all asserted:
 * <ol>
 *   <li>one legal message published straight into the outage;</li>
 *   <li>{@code HistoryConsumer} fails and enters transient retry
 *   ({@code history_retry{transient} > 0}, still zero history rows);</li>
 *   <li>the independent {@code LatestState} group keeps projecting into Redis
 *   <b>while MySQL is still down</b>;</li>
 *   <li>after unpause, history converges: {@code rows == 1},
 *   {@code DISTINCT(msg_id) == 1}, DLT stays {@code 0}, nothing lost.</li>
 * </ol>
 *
 * <p>Production retry parameters are untouched; only test-scoped Hikari/driver
 * timeouts make the frozen outage fail fast inside the retry budget.
 * Every wait is bounded; nothing sleeps blindly.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true",
    // Test-only fast failure while mysqld is frozen: borrow validation plus a
    // driver socket cap turn the hang into a retryable failure within ~seconds.
    "spring.datasource.hikari.connection-timeout=2000",
    "spring.datasource.hikari.validation-timeout=1000",
    "spring.datasource.hikari.connection-test-query=SELECT 1",
    "spring.datasource.hikari.data-source-properties.socketTimeout=3000"
})
class MySqlTransientOutageTest {

  private static final String MMSI = "413900101";
  private static final String TYPE = "nmea_gps";
  private static final String TOPIC = "zncb/" + MMSI + "/" + TYPE;
  private static final String REDIS_KEY = "ship:" + MMSI + ":latest:" + TYPE;
  private static final String EVENT_TS = "2026-09-21T16:30:00";
  private static final Instant EXPECTED_EVENT =
      LocalDateTime.parse(EVENT_TS).atZone(ZoneId.of("Asia/Shanghai")).toInstant();
  private static final String MSG_ID =
      EdgeFixtures.edgeStyleMsgId(MMSI, TYPE, "fault-2421", EVENT_TS);

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
    registry.add("shore.mqtt.client-id", () -> "shore-fault-mysql-t1");
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
  @DisplayName("P2-4.2.1: frozen MySQL → transient retry, Redis unaffected, recover → 1 row, DLT 0")
  void mysqlTransientOutageThenRecover() throws Exception {
    double retryBefore = metrics.retryCount("transient");
    double persistedBefore = metrics.getHistoryPersistedTotal().count();
    double latestUpdatedBefore = metrics.getLatestStateUpdatedTotal().count();

    readinessGates();

    pauseMysql();
    try {
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC, edgePayload());

      // Failure proof while the outage is still on: the record entered
      // transient retry, and MySQL holds nothing yet.
      MqttTestSupport.waitUntil("transient retry during outage", Duration.ofSeconds(90),
          () -> metrics.retryCount("transient") > retryBefore);
      assertEquals(0L, repository.countAll(), "no history row while MySQL is down");

      // The LatestState group shares nothing with MySQL: it must project
      // the same message while the outage is still on.
      MqttTestSupport.waitUntil("redis projected during mysql outage", Duration.ofSeconds(90),
          () -> {
            Object payload =
                redisTemplate.opsForHash().entries(REDIS_KEY).get("payload");
            return payload != null && payload.toString().contains(MSG_ID);
          });
    } finally {
      // Recover before FixedBackOff(1000ms, 3) exhausts: remaining retries land.
      unpauseMysql();
    }

    MqttTestSupport.waitUntil("history persisted after recovery", Duration.ofSeconds(120),
        () -> repository.countAll() == 1L);

    // ---- acceptance (P2-4.2.1) ----
    assertEquals(1L, repository.countAll(), "MySQL rows == 1");
    Long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(1L, distinct, "DISTINCT msg_id == 1");
    assertTrue(metrics.retryCount("transient") > retryBefore,
        "history_retry{transient} > 0");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
        "history_persisted == 1");
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
  // Real fault: freeze / resume mysqld via the Docker daemon.
  // ------------------------------------------------------------------

  private static void pauseMysql() {
    DockerClientFactory.instance().client()
        .pauseContainerCmd(MYSQL.getContainerId()).exec();
  }

  private static void unpauseMysql() {
    try {
      DockerClientFactory.instance().client()
          .unpauseContainerCmd(MYSQL.getContainerId()).exec();
    } catch (Exception e) {
      throw new IllegalStateException("MySQL unpause failed, test env is dirty", e);
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private static String edgePayload() {
    return "{"
        + "\"id\":9001,"
        + "\"ship_id\":\"S901\","
        + "\"mmsi\":\"" + MMSI + "\","
        + "\"type\":\"" + TYPE + "\","
        + "\"msg_id\":\"" + MSG_ID + "\","
        + "\"timestamp\":\"" + EVENT_TS + "\","
        + "\"sent_at\":\"2026-09-21T16:30:05.123+08:00\","
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-mysql-t1-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      return BenchmarkDltSnapshot.countSnapshot(
          consumer, properties.getKafka().getDltTopic(), Duration.ofSeconds(30));
    }
  }
}
