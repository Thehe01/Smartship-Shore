package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * P2-3 wiring: plain producer → real Kafka → both shore groups fan out —
 * {@code HistoryConsumer} archives rows in MySQL while {@code LatestStateConsumer}
 * projects per-ship/per-stream latest state into Redis with TTL.
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=false",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class LatestStateKafkaTestcontainersTest {

  private static final String RAW = "ship.telemetry.raw";

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
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
          .withExposedPorts(6379)
          .waitingFor(Wait.forListeningPort());

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
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
  private TelemetryHistoryRepository repository;

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private ShoreMetrics metrics;

  private String envelopeJson(String mmsi, String type, String rowId, Instant ts, Instant sent,
      Map<String, Object> data) throws Exception {
    return objectMapper.writeValueAsString(TelemetryEnvelope.builder()
        .msgId(EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, ts.toString()))
        .mmsi(mmsi)
        .type(type)
        .timestamp(ts)
        .sentAt(sent)
        .data(new LinkedHashMap<>(data))
        .build());
  }

  private static void waitUntil(String what, Duration timeout,
      java.util.function.BooleanSupplier cond) throws Exception {
    MqttTestSupport.waitUntil(what, timeout, cond);
  }

  @Test
  @DisplayName("Kafka fans out to MySQL history and Redis latest-state with TTL")
  void kafkaToMysqlAndRedis() throws Exception {
    Instant t1 = Instant.parse("2026-09-19T02:03:00Z");
    Instant t2 = Instant.parse("2026-09-19T02:05:00Z");
    String gps1 = envelopeJson("413910001", "nmea_gps", "g1", t1, t1.plusSeconds(5),
        Map.of("speed_knots", 9.0));
    String gps2 = envelopeJson("413910001", "nmea_gps", "g2", t2, t2.plusSeconds(5),
        Map.of("speed_knots", 12.5));
    String eng = envelopeJson("413910002", "engine", "e1", t2, t2.plusSeconds(5),
        Map.of("rpm", 1200));

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(RAW, "413910001", gps1)).get();
      producer.send(new ProducerRecord<>(RAW, "413910001", gps2)).get();
      producer.send(new ProducerRecord<>(RAW, "413910002", eng)).get();
      producer.flush();

      // Newer event overwrites the older one in Redis…
      waitUntil("latest gps projected", Duration.ofSeconds(90), () -> {
        var hash = redisTemplate.opsForHash().entries("ship:413910001:latest:nmea_gps");
        Object payload = hash.get("payload");
        return payload != null && payload.toString().contains("2026-09-19T02:05:00Z");
      });
      // …the other ship/stream projects in isolation…
      waitUntil("engine projected", Duration.ofSeconds(90), () ->
          redisTemplate.opsForHash().entries("ship:413910002:latest:engine").get("payload")
              != null);
      // …and history archives every event.
      waitUntil("history archived", Duration.ofSeconds(90), () -> repository.countAll() == 3L);
    }

    JsonNode gps = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries("ship:413910001:latest:nmea_gps").get("payload")));
    assertEquals("413910001", gps.path("mmsi").asText());
    assertEquals("nmea_gps", gps.path("type").asText());
    assertEquals(12.5, gps.path("data").path("speed_knots").asDouble(), 1e-9);
    assertTrue(!gps.path("data").has("data"));

    JsonNode engNode = objectMapper.readTree(String.valueOf(
        redisTemplate.opsForHash().entries("ship:413910002:latest:engine").get("payload")));
    assertEquals(1200, engNode.path("data").path("rpm").asInt());

    Long ttl = redisTemplate.getExpire("ship:413910001:latest:nmea_gps", TimeUnit.SECONDS);
    assertTrue(ttl != null && ttl > 0 && ttl <= 86400L, "TTL set on effective updates");

    // Timestamps travelled through the real chain untouched.
    assertEquals(t2, Instant.parse(gps.path("timestamp").asText()));
    // Three effective updates (first-write × 2, newer-wins × 1); >= tolerates a rare
    // broker redelivery, which can only rewrite the same state and never a second row.
    assertTrue(metrics.getLatestStateUpdatedTotal().count() >= 3.0);
  }
}
