package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Full chain against real containers: Kafka (KRaft) -&gt; HistoryConsumer -&gt; MySQL + Flyway V1.
 *
 * <p>Runs in CI with Docker; skips gracefully without it
 * ({@code disabledWithoutDocker = true}), so {@code mvn test} stays green on machines
 * without a Docker daemon. No public brokers, no physical ships involved.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class HistoryConsumerTestcontainersTest {

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

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("shore.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
  }

  @Autowired
  private KafkaTemplate<String, TelemetryEnvelope> kafkaTemplate;

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private ShoreMetrics metrics;

  @Test
  @DisplayName("E2E: Kafka record keyed by MMSI is persisted once, replay absorbed")
  void fullChainIsReliable() throws Exception {
    TelemetryEnvelope envelope = TelemetryEnvelope.builder()
        .msgId("e2e-413999999-nmea-gps-0001")
        .mmsi("413999999")
        .type("nmea_gps")
        .timestamp(Instant.parse("2026-09-19T02:00:00Z"))
        .sentAt(Instant.parse("2026-09-19T02:00:05.123Z"))
        .data(Map.of("latitude", 31.2304, "longitude", 121.4737, "speed_knots", 12.5))
        .build();

    kafkaTemplate.send("ship.telemetry.raw", envelope.getMmsi(), envelope).get(30, TimeUnit.SECONDS);

    waitUntil("history row persisted", Duration.ofSeconds(60), () -> repository.countAll() == 1L);
    var stored = repository.findByMsgId(envelope.getMsgId());
    assertTrue(stored.isPresent());
    assertEquals("413999999", stored.get().getMmsi());
    assertEquals("nmea_gps", stored.get().getType());
    assertEquals(Instant.parse("2026-09-19T02:00:00Z"), stored.get().getEventTime());
    assertTrue(stored.get().getPayloadJson().contains("speed_knots"));

    // Redeliver the same msg_id (at-least-once replay): still one row, duplicate counted.
    kafkaTemplate.send("ship.telemetry.raw", envelope.getMmsi(), envelope).get(30, TimeUnit.SECONDS);
    waitUntil("duplicate absorbed", Duration.ofSeconds(60),
        () -> metrics.getHistoryDuplicateTotal().count() >= 1.0);
    assertEquals(1L, repository.countAll());
  }

  private static void waitUntil(String what, Duration timeout, java.util.function.BooleanSupplier cond)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for: " + what);
      }
      Thread.sleep(500);
    }
  }
}
