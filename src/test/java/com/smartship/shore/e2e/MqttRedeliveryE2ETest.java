package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.kafka.TelemetryKafkaProducer;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
 * True redelivery E2E (P2-1.2): real Mosquitto + real shore service + real Kafka + real
 * consumer + real MySQL.
 *
 * <p>The test publishes the Edge payload <b>exactly once</b> — re-publishing it to fake a
 * redelivery is forbidden here. Instead the first Kafka handoff is scripted to fail: shore
 * holds the MQTT acknowledgment and drops its connection, Mosquitto redelivers the
 * <b>original</b> QoS1 message after the durable-session reconnect, Kafka has recovered by
 * then, and exactly one row lands with the original {@code msg_id}.
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class MqttRedeliveryE2ETest {

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

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("shore.mqtt.broker-url",
        () -> "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883));
    registry.add("shore.mqtt.client-id", () -> "shore-redelivery-e2e");
    registry.add("shore.mqtt.kafka-handoff-timeout-ms", () -> "10000");
    registry.add("shore.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
  }

  /**
   * Kafka boundary that fails the first handoff on purpose, then delegates to the real
   * producer (real Kafka) for everything after — modelling "Kafka briefly unavailable,
   * then recovered" without touching the broker.
   */
  static final class FlakyProducer extends TelemetryKafkaProducer {
    final AtomicBoolean failNext = new AtomicBoolean(true);

    FlakyProducer(KafkaTemplate<String, TelemetryEnvelope> template, ShoreMetrics metrics) {
      super(template, metrics);
    }

    @Override
    public CompletableFuture<SendResult<String, TelemetryEnvelope>> send(
        String rawTopic, TelemetryEnvelope envelope) {
      if (failNext.compareAndSet(true, false)) {
        CompletableFuture<SendResult<String, TelemetryEnvelope>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("scripted kafka outage"));
        return failed;
      }
      return super.send(rawTopic, envelope);
    }
  }

  @TestConfiguration
  static class ScriptedKafkaConfig {
    @Bean
    @Primary
    FlakyProducer flakyProducer(KafkaTemplate<String, TelemetryEnvelope> template,
        ShoreMetrics metrics) {
      return new FlakyProducer(template, metrics);
    }
  }

  @Autowired
  private MqttIngestService ingestService;

  @Autowired
  private FlakyProducer flakyProducer;

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private ShoreMetrics shoreMetrics;

  @Autowired
  private KafkaListenerEndpointRegistry listenerRegistry;

  @Test
  @DisplayName("Single publish → failed handoff → broker redelivers original → exactly one row")
  void originalMessageRedeliveredOnce() throws Exception {
    // P2-3: the latest-state group has no Redis in this suite — park it so the suite
    // stays focused on the MQTT redelivery → history chain it was built for.
    listenerRegistry.getListenerContainer("smartship-latest-state").stop();

    List<Integer> ackedIds = new CopyOnWriteArrayList<>();
    ingestService.setAckListener((id, qos) -> ackedIds.add(id));

    MqttTestSupport.waitUntil("shore subscribed", Duration.ofSeconds(60), ingestService::isReady);

    // THE single publish of this test: no manual re-publish follows, ever.
    MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
        "zncb/413999999/nmea_gps", EdgeFixtures.gpsPayload());

    // First handoff fails (scripted) → no ACK → forced reconnect → Mosquitto redelivers
    // the ORIGINAL QoS1 → recovered Kafka → HistoryConsumer → MySQL.
    MqttTestSupport.waitUntil("history row persisted", Duration.ofSeconds(120),
        () -> repository.countAll() == 1L);

    String msgId =
        EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
    var row = repository.findByMsgId(msgId).orElseThrow(() -> new AssertionError("row missing"));
    assertEquals(msgId, row.getMsgId(), "msg_id unchanged end to end");
    assertEquals("413999999", row.getMmsi());
    assertEquals("nmea_gps", row.getType());
    assertEquals(Instant.parse("2026-09-19T02:00:00Z"), row.getEventTime());

    JsonNode payload = new ObjectMapper().readTree(row.getPayloadJson());
    assertEquals(12.5, payload.path("data").path("speed_knots").asDouble(), 1e-9);
    assertTrue(!payload.path("data").has("data"), "nested data.data must never appear");

    // The scripted failure really fired exactly once, and both the original and the broker
    // redelivery reached shore — yet only one row exists: at-least-once delivery plus
    // deterministic msg_id, duplicate safety intact. (Counts stay tolerant where the broker
    // may legally repeat a redelivery during session takeover; every repeat is absorbed
    // by UNIQUE(msg_id), never as a second row.)
    assertTrue(!flakyProducer.failNext.get(), "scripted first-handoff failure was exercised");
    MqttTestSupport.waitUntil("redelivery acknowledged", Duration.ofSeconds(60),
        () -> !ackedIds.isEmpty());
    assertTrue(shoreMetrics.getMqttReceivedTotal().count() >= 2.0,
        "original + broker redelivery both arrived");
    assertEquals(1L, repository.countAll(), "exactly one row");
    assertEquals(1.0, shoreMetrics.getHistoryPersistedTotal().count());
  }
}
