package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.ingest.MqttAckPublisher;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
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
 * Application ACK E2E: a real Edge-format payload travels Mosquitto → shore →
 * Kafka (acks=all), and shore answers {@code ship/{mmsi}/ack} with
 * {@code {msg_id, seq, status=KAFKA_COMMITTED}} — observed here by a stub Edge
 * standing in for the real {@code UploadAckTracker}.
 *
 * <p>Covers the Shore side of the contract: ACK on Kafka success (T1), no ACK
 * while Kafka is down with the cursor unmoved (T2), and resend-after-ACK-loss
 * converging to a second ACK but still exactly one MySQL row (T3+T6).
 * Edge-side watermark/ordering/restart semantics live in the Edge repo.
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class ApplicationAckE2ETest {

  private static final String MMSI = "413999999";
  private static final String TOPIC = "zncb/413999999/nmea_gps";
  private static final String MSG_ID =
      EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");

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
    registry.add("shore.mqtt.client-id", () -> "shore-app-ack-e2e");
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
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private ShoreMetrics metrics;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private KafkaListenerEndpointRegistry listenerRegistry;

  /** Stub Edge: subscribes ship/+/ack and records every Application ACK. */
  static final class StubEdge implements AutoCloseable {
    final Queue<JsonNode> acks = new ConcurrentLinkedQueue<>();
    private final MqttClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    StubEdge() throws Exception {
      client = new MqttClient(
          "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883),
          "stub-edge-" + UUID.randomUUID().toString().substring(0, 8),
          new MemoryPersistence());
      client.setCallback(new MqttCallbackExtended() {
        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
        }

        @Override
        public void connectionLost(Throwable cause) {
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
          try {
            acks.add(mapper.readTree(message.getPayload()));
          } catch (Exception ignored) {
            // Malformed ACKs fail the assertions below, not here.
          }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
      });
      MqttConnectOptions options = new MqttConnectOptions();
      options.setCleanSession(true);
      options.setConnectionTimeout(10);
      options.setAutomaticReconnect(true);
      long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (true) {
        try {
          client.connect(options);
          break;
        } catch (Exception e) {
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("stub edge could not connect", e);
          }
          Thread.sleep(500);
        }
      }
      client.subscribe("ship/+/ack", 1);
    }

    JsonNode awaitAckFor(String msgId, Duration timeout) throws Exception {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() <= deadline) {
        for (JsonNode ack : acks) {
          if (msgId.equals(ack.path("msg_id").asText())) {
            return ack;
          }
        }
        Thread.sleep(200);
      }
      throw new AssertionError("timed out waiting for Application ACK of " + msgId);
    }

    int ackCountFor(String msgId) {
      int n = 0;
      for (JsonNode ack : acks) {
        if (msgId.equals(ack.path("msg_id").asText())) {
          n++;
        }
      }
      return n;
    }

    @Override
    public void close() {
      try {
        client.disconnect();
      } catch (Exception ignored) {
        // Best effort.
      }
      try {
        client.close();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }

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

  @Test
  @DisplayName("T1+T2+T3/T6: ACK on success, silence on outage, resend converges to one row")
  void applicationAckContract() throws Exception {
    // No Redis in this suite: park the latest-state group, history owns the assertions.
    listenerRegistry.getListenerContainer("smartship-latest-state").stop();
    MqttTestSupport.waitUntil("shore subscribed", Duration.ofSeconds(60), ingestService::isReady);
    double ackPublishedBefore = metrics.getKafkaAckPublishedTotal().count();

    try (StubEdge edge = new StubEdge()) {
      // ---- T1: Kafka success → Application ACK, cursor (row) advances. ----
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC, EdgeFixtures.gpsPayload());
      JsonNode ack = edge.awaitAckFor(MSG_ID, Duration.ofSeconds(90));
      assertEquals(MSG_ID, ack.path("msg_id").asText());
      assertEquals("1001", ack.path("seq").asText(), "row id echoes for Edge matching");
      assertEquals(MqttAckPublisher.STATUS_KAFKA_COMMITTED, ack.path("status").asText());
      MqttTestSupport.waitUntil("history row persisted", Duration.ofSeconds(90),
          () -> repository.countAll() == 1L);
      assertTrue(metrics.getKafkaAckPublishedTotal().count() > ackPublishedBefore);

      // ---- T2: Kafka down → no Application ACK, cursor unmoved. ----
      pauseKafka();
      try {
        String outageMsgId =
            EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "9001", "2026-09-21T16:00:00");
        String outageJson = "{"
            + "\"id\":9001,\"mmsi\":\"413999999\",\"type\":\"nmea_gps\","
            + "\"msg_id\":\"" + outageMsgId + "\","
            + "\"timestamp\":\"2026-09-21T16:00:00\","
            + "\"sent_at\":\"2026-09-21T16:00:05.123+08:00\","
            + "\"speed_knots\":9.9}";
        MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
            TOPIC, outageJson);
        // Bounded silence: longer than one handoff timeout + one redelivery cycle,
        // so a stray ACK would have shown up.
        long silenceEnd = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < silenceEnd) {
          assertEquals(0, edge.ackCountFor(outageMsgId),
              "no Application ACK while Kafka is down");
          Thread.sleep(500);
        }
        assertEquals(1L, repository.countAll(), "cursor unmoved while Kafka is down");
      } finally {
        unpauseKafka();
      }
      // Recovery: the redelivered message lands and its ACK follows.
      String outageMsgId =
          EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "9001", "2026-09-21T16:00:00");
      JsonNode outageAck = edge.awaitAckFor(outageMsgId, Duration.ofSeconds(120));
      assertEquals(MqttAckPublisher.STATUS_KAFKA_COMMITTED, outageAck.path("status").asText());

      // ---- T3+T6: simulated resend after ACK loss → second ACK, still one row. ----
      MqttTestSupport.publishOnce(MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883),
          TOPIC, EdgeFixtures.gpsPayload());
      MqttTestSupport.waitUntil("second ACK after resend", Duration.ofSeconds(90),
          () -> edge.ackCountFor(MSG_ID) == 2);
      assertEquals(2L, repository.countAll(), "outage message + original message");
      Long distinct = jdbcTemplate.queryForObject(
          "SELECT COUNT(DISTINCT msg_id) FROM ship_telemetry_history", Long.class);
      assertEquals(2L, distinct);
      assertTrue(repository.findByMsgId(MSG_ID).isPresent());
      long gpsRows = jdbcTemplate.queryForObject(
          "SELECT COUNT(*) FROM ship_telemetry_history WHERE msg_id = ?", Long.class, MSG_ID);
      assertEquals(1L, gpsRows, "resend absorbed by UNIQUE(msg_id): still exactly one row");
    }
  }
}
