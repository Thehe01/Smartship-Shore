package com.smartship.shore.fault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
 * P2-4.2.3: real MySQL sustained outage → retry exhaustion → DLT.
 *
 * <p>Fault: {@code mysqld} frozen with Docker pause and held past the bounded
 * history retry ({@code FixedBackOff(1000ms, 3)}), then unpaused. A real
 * infrastructure fault — connections hang/fail, no mocks.
 *
 * <p>Core assertion of this scenario: <b>the DLT is a terminal failure
 * destination, not a delayed queue.</b> After recovery, message A must NOT
 * land in MySQL on its own; a fresh message B must flow normally, proving the
 * failed offset was recovery-committed and the consumer kept working.
 *
 * <p>Expected shape, all asserted:
 * <ol>
 *   <li>message A published straight into the outage: history enters transient
 *   retry while the independent LatestState group projects A into Redis
 *   <b>while MySQL is still down</b>;</li>
 *   <li>MySQL stays paused until the retry exhausts: A lands in the DLT
 *   ({@code history_dlt{transient} + 1}, DLT topic holds exactly one record
 *   with A's payload and {@code shore-dlt-reason == transient});</li>
 *   <li>after unpause: A never auto-persists (offset was recovery-committed),
 *   a fresh message B lands in MySQL normally; final state is A in DLT,
 *   B in MySQL, lost == 0.</li>
 * </ol>
 *
 * <p>Production retry/backoff parameters are untouched; only test-scoped
 * Hikari/driver timeouts (same as P2-4.2.1) make the frozen outage fail fast.
 * The paused MySQL is never queried — outage observation uses metrics, the
 * healthy Redis, and the DLT topic only. Every wait is bounded.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true",
    // Test-only fast failure while mysqld is frozen (same as P2-4.2.1):
    // borrow validation plus a driver socket cap turn the hang into a
    // retryable failure within ~seconds.
    "spring.datasource.hikari.connection-timeout=2000",
    "spring.datasource.hikari.validation-timeout=1000",
    "spring.datasource.hikari.connection-test-query=SELECT 1",
    "spring.datasource.hikari.data-source-properties.socketTimeout=3000"
})
class MySqlSustainedOutageTest {

  private static final ZoneId EDGE_ZONE = ZoneId.of("Asia/Shanghai");

  private static final String TYPE = "nmea_gps";

  // Message A: the outage victim. Ends in the DLT, never in MySQL.
  private static final String MMSI_A = "413900103";
  private static final String TOPIC_A = "zncb/" + MMSI_A + "/" + TYPE;
  private static final String REDIS_KEY_A = "ship:" + MMSI_A + ":latest:" + TYPE;
  private static final String EVENT_TS_A = "2026-09-21T16:50:00";
  private static final Instant EXPECTED_EVENT_A =
      LocalDateTime.parse(EVENT_TS_A).atZone(EDGE_ZONE).toInstant();
  private static final String MSG_ID_A =
      EdgeFixtures.edgeStyleMsgId(MMSI_A, TYPE, "fault-2423-A", EVENT_TS_A);

  // Message B: the post-recovery health probe. Lands in MySQL normally.
  // A distinct MMSI keeps Redis keys independent (no CAS interaction).
  private static final String MMSI_B = "413900104";
  private static final String TOPIC_B = "zncb/" + MMSI_B + "/" + TYPE;
  private static final String REDIS_KEY_B = "ship:" + MMSI_B + ":latest:" + TYPE;
  private static final String EVENT_TS_B = "2026-09-21T16:55:00";
  private static final Instant EXPECTED_EVENT_B =
      LocalDateTime.parse(EVENT_TS_B).atZone(EDGE_ZONE).toInstant();
  private static final String MSG_ID_B =
      EdgeFixtures.edgeStyleMsgId(MMSI_B, TYPE, "fault-2423-B", EVENT_TS_B);

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
    registry.add("shore.mqtt.client-id", () -> "shore-fault-mysql-s1");
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
  @DisplayName("P2-4.2.3: sustained MySQL outage → A to DLT, recovery → A stays out, B lands")
  void mysqlSustainedOutageRoutesToDlt() throws Exception {
    double retryBefore = metrics.retryCount("transient");
    double dltTransientBefore = metrics.dltCount("transient");
    double dltPoisonBefore = metrics.dltCount("poison");
    double persistedBefore = metrics.getHistoryPersistedTotal().count();
    double latestUpdatedBefore = metrics.getLatestStateUpdatedTotal().count();

    readinessGates();

    // ---- phase 1: outage. A retries, Redis projects A, retry exhausts → DLT.
    pauseMysql();
    try {
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC_A, edgePayload(MMSI_A, MSG_ID_A, EVENT_TS_A, 9003, "S903"));

      // Transient retry entered (metrics only — the paused MySQL is never read).
      MqttTestSupport.waitUntil("transient retry for A during outage",
          Duration.ofSeconds(90), () -> metrics.retryCount("transient") > retryBefore);
      assertEquals(0.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
          "history must not persist while MySQL is paused");

      // Group isolation while MySQL is still down: LatestState projects A.
      // Redis is healthy, so reading it is safe; MySQL stays untouched.
      MqttTestSupport.waitUntil("redis projected A during mysql outage",
          Duration.ofSeconds(90), () -> {
            Object payload =
                redisTemplate.opsForHash().entries(REDIS_KEY_A).get("payload");
            return payload != null && payload.toString().contains(MSG_ID_A);
          });

      // Hold the outage past FixedBackOff(1000ms, 3): this bounded wait IS the
      // exhaustion proof — no sleep guessing. A must reach the DLT.
      MqttTestSupport.waitUntil("A exhausted to DLT", Duration.ofSeconds(120),
          () -> metrics.dltCount("transient") > dltTransientBefore);
    } finally {
      unpauseMysql();
    }

    // ---- phase 2: recovery. MySQL healthy again, but A must stay out.
    MqttTestSupport.waitUntil("mysql healthy after unpause", Duration.ofSeconds(120),
        () -> {
          try {
            repository.countAll();
            return true;
          } catch (Exception e) {
            return false;
          }
        });
    assertTrue(repository.findByMsgId(MSG_ID_A).isEmpty(),
        "A must not auto-persist after recovery: DLT is terminal, not a delayed queue");
    assertEquals(0L, repository.countAll(), "MySQL holds nothing before B");

    // The DLT record itself: exactly one, A's payload, transient reason.
    // Kafka (hence the DLT topic) was never faulty, so it is readable now.
    List<ConsumerRecord<String, String>> dlt = readDltSnapshot();
    assertEquals(1, dlt.size(), "DLT topic holds exactly A");
    ConsumerRecord<String, String> rec = dlt.get(0);
    assertEquals(MMSI_A, rec.key(), "DLT key=MMSI preserved");
    assertTrue(rec.value() != null && rec.value().contains(MSG_ID_A),
        "DLT carries A's original payload");
    assertEquals("transient", dltHeader(rec, "shore-dlt-reason"));

    // ---- phase 3: liveness. A fresh message flows normally post-recovery.
    MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
        TOPIC_B, edgePayload(MMSI_B, MSG_ID_B, EVENT_TS_B, 9004, "S904"));
    MqttTestSupport.waitUntil("B persisted after recovery", Duration.ofSeconds(120),
        () -> repository.countAll() == 1L);

    // ---- acceptance (P2-4.2.3) ----
    assertTrue(metrics.retryCount("transient") > retryBefore,
        "history retry transient > 0 for A");
    assertEquals(1.0, metrics.dltCount("transient") - dltTransientBefore,
        "history DLT transient delta == 1 (exactly A)");
    assertEquals(0.0, metrics.dltCount("poison") - dltPoisonBefore, "no poison DLT");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count() - persistedBefore,
        "history_persisted delta == 1 (exactly B)");
    assertEquals(1L, repository.countAll(), "MySQL rows == 1");
    Long distinct = jdbcTemplate.queryForObject(
        "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
    assertEquals(1L, distinct, "DISTINCT msg_id == 1");
    assertTrue(repository.findByMsgId(MSG_ID_B).isPresent(), "B landed in MySQL");
    assertTrue(repository.findByMsgId(MSG_ID_A).isEmpty(), "A never re-entered MySQL");
    assertEquals(1, readDltSnapshot().size(), "DLT total still == 1");
    assertTrue(metrics.getLatestStateUpdatedTotal().count() - latestUpdatedBefore >= 2.0,
        "latest-state projected A and B (>= tolerates a rare broker redelivery)");

    // Redis final state: A projected during the outage, B after recovery.
    JsonNode nodeA = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries(REDIS_KEY_A).get("payload")));
    assertEquals(MSG_ID_A, nodeA.path("msg_id").asText(), "redis holds A");
    assertEquals(EXPECTED_EVENT_A, Instant.parse(nodeA.path("timestamp").asText()),
        "A event time untouched");
    JsonNode nodeB = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries(REDIS_KEY_B).get("payload")));
    assertEquals(MSG_ID_B, nodeB.path("msg_id").asText(), "redis holds B");
    assertEquals(EXPECTED_EVENT_B, Instant.parse(nodeB.path("timestamp").asText()),
        "B event time untouched");
    Long ttl = redisTemplate.getExpire(REDIS_KEY_B, TimeUnit.SECONDS);
    assertTrue(ttl != null && ttl > 0 && ttl <= 86400L, "TTL set on the update");
    // No silent loss: A accounted for in the DLT, B accounted for in MySQL.
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

  private static String edgePayload(String mmsi, String msgId, String eventTs,
      int rowId, String shipId) {
    return "{"
        + "\"id\":" + rowId + ","
        + "\"ship_id\":\"" + shipId + "\","
        + "\"mmsi\":\"" + mmsi + "\","
        + "\"type\":\"" + TYPE + "\","
        + "\"msg_id\":\"" + msgId + "\","
        + "\"timestamp\":\"" + eventTs + "\","
        + "\"sent_at\":\"2026-09-21T16:50:05.123+08:00\","
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

  private static String dltHeader(ConsumerRecord<String, String> rec, String name) {
    var h = rec.headers().lastHeader(name);
    assertTrue(h != null, "DLT header missing: " + name);
    return new String(h.value(), StandardCharsets.UTF_8);
  }

  /**
   * Deterministic DLT snapshot read: assign every partition explicitly (no group
   * rebalance), pin {@code endOffsets} once, then collect exactly the records
   * below the captured ends. Same discipline as {@code BenchmarkDltSnapshot},
   * but returning the records (headers included) instead of a bare count — an
   * empty first poll can never fake an empty DLT while assignment is in flight.
   */
  private List<ConsumerRecord<String, String>> readDltSnapshot() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "fault-mysql-s1-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    String topic = properties.getKafka().getDltTopic();
    List<ConsumerRecord<String, String>> out = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      List<TopicPartition> partitions = new ArrayList<>();
      consumer.partitionsFor(topic).forEach(info ->
          partitions.add(new TopicPartition(topic, info.partition())));
      if (partitions.isEmpty()) {
        throw new AssertionError("DLT topic has no partitions (missing topic?): " + topic);
      }
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> ends = new LinkedHashMap<>(consumer.endOffsets(partitions));
      Map<TopicPartition, Long> positions = new LinkedHashMap<>();
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (TopicPartition tp : partitions) {
          long end = ends.getOrDefault(tp, 0L);
          for (ConsumerRecord<String, String> r : records.records(tp)) {
            if (r.offset() < end) {
              out.add(r);
            }
          }
          positions.put(tp, consumer.position(tp));
        }
        boolean done = true;
        for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
          Long pos = positions.get(e.getKey());
          if (pos == null || pos < e.getValue()) {
            done = false;
            break;
          }
        }
        if (done) {
          return out;
        }
        if (System.nanoTime() > deadline) {
          throw new AssertionError("timed out waiting for DLT snapshot end of topic "
              + topic + " (ends=" + ends + ", positions=" + positions + ")");
        }
      }
    }
  }
}
