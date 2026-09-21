package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
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
 * True E2E (P2-1.1): a real Edge-format payload travels
 * Mosquitto -&gt; {@code MqttIngestService} -&gt; Kafka -&gt; {@code HistoryConsumer} -&gt; MySQL.
 *
 * <p>Runs in CI with Docker; skips gracefully without it
 * ({@code disabledWithoutDocker = true}), so {@code mvn test} stays green on machines
 * without a Docker daemon.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class TrueE2EMqttKafkaMySqlTest {

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
    registry.add("shore.mqtt.client-id", () -> "shore-true-e2e");
    registry.add("shore.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
  }

  @Autowired
  private MqttIngestService ingestService;

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private KafkaListenerEndpointRegistry listenerRegistry;

  @Test
  @DisplayName("E2E: zncb/413999999/nmea_gps → Kafka → MySQL, one row, flat data, no data.data")
  void mqttToKafkaToMysql() throws Exception {
    // P2-3: the latest-state group has no Redis in this suite — park it so the suite
    // stays focused on the MQTT→Kafka→MySQL chain it was built for.
    listenerRegistry.getListenerContainer("smartship-latest-state").stop();

    // Gating: publish only after shore holds the subscription, otherwise the broker
    // (clean first session) has nowhere to queue the message yet.
    MqttTestSupport.waitUntil("shore subscribed", Duration.ofSeconds(60), ingestService::isReady);

    MqttTestPublisher.publish(EdgeFixtures.gpsPayload());

    MqttTestSupport.waitUntil("history row persisted", Duration.ofSeconds(90),
        () -> repository.countAll() == 1L);

    String msgId = EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
    var row = repository.findByMsgId(msgId).orElseThrow(() -> new AssertionError("row missing"));
    assertEquals("413999999", row.getMmsi());
    assertEquals("nmea_gps", row.getType());
    // Edge event time survived the whole chain untouched.
    assertEquals(Instant.parse("2026-09-19T02:00:00Z"), row.getEventTime());

    JsonNode payload = new ObjectMapper().readTree(row.getPayloadJson());
    assertEquals(12.5, payload.path("data").path("speed_knots").asDouble(), 1e-9);
    assertTrue(!payload.path("data").has("data"), "nested data.data must never appear");
  }

  /** Tiny publisher bound to this test's Mosquitto container. */
  static final class MqttTestPublisher {
    private MqttTestPublisher() {
    }

    static void publish(String json) throws Exception {
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          "zncb/413999999/nmea_gps", json);
    }
  }
}
