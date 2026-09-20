package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.ingest.TelemetryMessageParser;
import com.smartship.shore.kafka.HistoryConsumer;
import com.smartship.shore.kafka.TelemetryKafkaProducer;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Handoff reliability (P2-1.1): real Mosquitto + real {@code MqttIngestService} + real
 * {@code HistoryConsumer} + real MySQL, with a scripted Kafka boundary in between.
 *
 * <p>Phase 1 (Kafka down): the send future fails, so the MQTT delivery must stay
 * unacknowledged. Phase 2 (Kafka recovered): the redelivered message lands exactly one row.
 * Phase 3 (duplicate redelivery): still one row, absorbed by {@code UNIQUE(msg_id)}.
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=false",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=false"
})
class MqttKafkaHandoffReliabilityTest {

  private static final String TOPIC = "zncb/413999999/nmea_gps";

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
  static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("smartship_shore")
          .withUsername("shore")
          .withPassword("shore123");

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
  }

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private HistoryConsumer historyConsumer;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private ShoreMetrics shoreMetrics;

  /** Kafka boundary with a script: fail while "down", deliver straight to the consumer when up. */
  static final class ScriptedProducer extends TelemetryKafkaProducer {
    volatile boolean failing = true;
    private final ObjectMapper mapper;
    private final HistoryConsumer consumer;
    private long seq;

    @SuppressWarnings({"unchecked", "rawtypes"})
    ScriptedProducer(ObjectMapper mapper, HistoryConsumer consumer) {
      super((KafkaTemplate) mock(KafkaTemplate.class),
          new ShoreMetrics(new SimpleMeterRegistry()));
      this.mapper = mapper;
      this.consumer = consumer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<SendResult<String, TelemetryEnvelope>> send(
        String rawTopic, TelemetryEnvelope envelope) {
      if (failing) {
        CompletableFuture<SendResult<String, TelemetryEnvelope>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka unavailable (scripted outage)"));
        return failed;
      }
      try {
        // Same bytes the real producer would put on the wire, delivered to the real consumer.
        String json = mapper.writeValueAsString(envelope);
        consumer.listen(
            new ConsumerRecord<>(rawTopic, 0, seq++, envelope.getMmsi(), json),
            (Acknowledgment) () -> {
            });
        return CompletableFuture.completedFuture(mock(SendResult.class));
      } catch (RuntimeException e) {
        CompletableFuture<SendResult<String, TelemetryEnvelope>> failed = new CompletableFuture<>();
        failed.completeExceptionally(e);
        return failed;
      } catch (Exception e) {
        CompletableFuture<SendResult<String, TelemetryEnvelope>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException(e));
        return failed;
      }
    }
  }

  @Test
  @DisplayName("Outage: no MQTT ACK; recovery: redelivery lands exactly one row")
  void outageHoldsAck_recoveryDeliversOnce() throws Exception {
    String host = MOSQUITTO.getHost();
    int port = MOSQUITTO.getMappedPort(1883);

    ShoreProperties props = new ShoreProperties();
    props.getMqtt().setBrokerUrl("tcp://" + host + ":" + port);
    props.getMqtt().setClientId("shore-handoff-test");
    ScriptedProducer scripted = new ScriptedProducer(objectMapper, historyConsumer);
    ShoreMetrics ingressMetrics = new ShoreMetrics(new SimpleMeterRegistry());
    MqttIngestService ingress = new MqttIngestService(
        props, new TelemetryMessageParser(new ObjectMapper()), scripted, ingressMetrics);

    // Acknowledgment probe: observes real broker ACKs without interfering with them.
    List<Integer> ackedIds = new CopyOnWriteArrayList<>();
    ingress.setAckListener((id, qos) -> ackedIds.add(id));
    ingress.start();
    try {
      MqttTestSupport.waitUntil("shore subscribed", Duration.ofSeconds(60), ingress::isReady);

      // Phase 1 — Kafka down: message arrives, handoff fails, MQTT stays unacknowledged.
      MqttTestSupport.publishOnce(host, port, TOPIC, EdgeFixtures.gpsPayload());
      MqttTestSupport.waitUntil("delivery attempted", Duration.ofSeconds(30),
          () -> ingressMetrics.getMqttReceivedTotal().count() == 1.0);
      Thread.sleep(2000); // grace: let any wrongful acknowledgment land
      assertTrue(ackedIds.isEmpty(), "failed handoff must not acknowledge MQTT");
      assertEquals(0L, repository.countAll(), "nothing may reach MySQL while Kafka is down");

      // Phase 2 — Kafka recovered: the redelivered message lands exactly one row, then ACK.
      scripted.failing = false;
      MqttTestSupport.publishOnce(host, port, TOPIC, EdgeFixtures.gpsPayload());
      MqttTestSupport.waitUntil("one row persisted", Duration.ofSeconds(60),
          () -> repository.countAll() == 1L);
      MqttTestSupport.waitUntil("recovery acknowledged", Duration.ofSeconds(30),
          () -> ackedIds.size() == 1);

      // Phase 3 — duplicate redelivery: still one row, absorbed by UNIQUE(msg_id).
      MqttTestSupport.publishOnce(host, port, TOPIC, EdgeFixtures.gpsPayload());
      MqttTestSupport.waitUntil("all deliveries consumed", Duration.ofSeconds(60),
          () -> shoreMetrics.getHistoryConsumedTotal().count() == 3.0);
      assertEquals(1L, repository.countAll());
      assertEquals(1.0, shoreMetrics.getHistoryDuplicateTotal().count());
      assertEquals(2, ackedIds.size(), "recovery + duplicate both acknowledged");
    } finally {
      ingress.stop();
    }
  }
}
