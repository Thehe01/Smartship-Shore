package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.uploader.DatabaseUploadPoller;
import com.smartship.edge.uploader.UploadAckTracker;
import com.smartship.edge.uploader.mqtt.MqttClientManager;
import com.smartship.edge.uploader.mqtt.MqttPublisher;
import com.smartship.shore.MqttTestSupport;
import com.smartship.shore.ingest.MqttIngestService;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
 * True cross-repo E2E: REAL Edge classes drive the full protocol loop.
 *
 * <p>Chain under test (no stubs on either side except the in-memory Edge H2):
 * real Edge {@code DatabaseUploadPoller} + {@code MqttPublisher} +
 * {@code MqttClientManager} (H2 row → stable msg_id → {@code zncb/{mmsi}/…}) →
 * real Mosquitto → real Shore ingest → real Kafka ({@code acks=all}) → real
 * MySQL history → real Application ACK → real Edge {@code MqttClientManager}
 * subscription → real {@code UploadAckTracker} watermark → Edge cursor table.
 *
 * <p>The test fails if EITHER side drifts: payload/contract drift breaks the
 * Shore parse or the Edge ACK match, and cursor advancement proves the round
 * trip instead of asserting each half in isolation.
 *
 * <p>The row mirrors contract v1 ({@code id=1001,
 * update_time=2026-09-19T10:00:00}), so the Shore history {@code msg_id} must
 * equal the frozen vector ({@code 7b20b18a…c960bc}, see
 * {@code EdgeContractPinTest}).
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=true",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class EdgeProtocolE2ETest {

  private static final String MMSI = "413999999";
  private static final String SHIP_ID = "S001";
  private static final String TABLE = "zncb_gps_data";
  /** Frozen contract-v1 msg_id for (413999999, nmea_gps, 1001, 2026-09-19T10:00:00). */
  private static final String FROZEN_MSG_ID_V1 =
      "7b20b18ae89960f8f6cd04a198af268de85b67ca72e3adc4b7cfd224c960bc69";

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
    registry.add("shore.mqtt.client-id", () -> "shore-edge-proto-e2e");
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

  /** Real Edge stack (plain constructors, no Spring): H2 → poller → MQTT. */
  static final class RealEdge implements AutoCloseable {
    final JdbcTemplate edgeJt;
    final MqttClientManager manager;
    final DatabaseUploadPoller poller;
    final UploadAckTracker tracker;
    final DatabaseUploadPoller.LocalShip ship;
    final DatabaseUploadPoller.IncrementalStream stream;

    RealEdge(String brokerUrl) {
      DriverManagerDataSource ds = new DriverManagerDataSource(
          "jdbc:h2:mem:edge_e2e_" + UUID.randomUUID().toString().substring(0, 8)
              + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
          "sa", "");
      edgeJt = new JdbcTemplate(ds);
      edgeJt.execute("CREATE TABLE zncb_gps_data ("
          + " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
          + " update_time VARCHAR(32) NOT NULL,"
          + " speed DOUBLE NOT NULL)");
      edgeJt.execute("CREATE TABLE zncb_upload_cursor ("
          + " stream_name VARCHAR(64) NOT NULL,"
          + " partition_key VARCHAR(64) NOT NULL DEFAULT '',"
          + " last_uploaded_id BIGINT NOT NULL DEFAULT 0,"
          + " last_uploaded_time TIMESTAMP NULL,"
          + " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
          + " PRIMARY KEY (stream_name, partition_key))");

      EdgeProperties props = new EdgeProperties();
      props.setMmsi(MMSI);
      props.setShipId(SHIP_ID);
      props.setSchemaReady(true);
      props.getUploader().setEnabled(true);
      props.getUploader().getPoll().setBatchSize(10);
      props.getUploader().getAck().setEnabled(true);
      props.getUploader().getMqtt().setEnabled(true);
      props.getUploader().getMqtt().setBrokerUrl(brokerUrl);
      props.getUploader().getMqtt().setClientId(
          "real-edge-e2e-" + UUID.randomUUID().toString().substring(0, 8));
      props.getUploader().getMqtt().setQos(1);

      com.smartship.edge.observability.SmartShipMetrics edgeMetrics =
          new com.smartship.edge.observability.SmartShipMetrics(new SimpleMeterRegistry());
      manager = new MqttClientManager(props, edgeMetrics);
      MqttPublisher publisher = new MqttPublisher(manager);
      tracker = new UploadAckTracker();
      poller = new DatabaseUploadPoller(props, edgeJt, publisher, null, null, tracker);
      ship = new DatabaseUploadPoller.LocalShip(SHIP_ID, MMSI);
      stream = new DatabaseUploadPoller.IncrementalStream("gps", TABLE, "nmea_gps", "nmea_gps");
    }

    long cursor() {
      Long v = edgeJt.queryForObject(
          "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ?",
          Long.class, TABLE);
      return v == null ? 0L : v;
    }

    @Override
    public void close() {
      manager.disconnect();
    }
  }

  @Test
  @DisplayName("E2E: real Edge row → Shore → Kafka → MySQL → ACK → Edge cursor")
  void edgeToShoreToEdgeCursor() throws Exception {
    listenerRegistry.getListenerContainer("smartship-latest-state").stop();
    MqttTestSupport.waitUntil("shore subscribed", Duration.ofSeconds(60),
        ingestService::isReady);

    String brokerUrl =
        "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883);
    try (RealEdge edge = new RealEdge(brokerUrl)) {
      edge.edgeJt.update(
          "INSERT INTO zncb_gps_data (id, update_time, speed) VALUES (?, ?, ?)",
          1001, "2026-09-19T10:00:00", 12.5);

      // Round 1: real Edge poller publishes; broker may still settle → retry.
      MqttTestSupport.waitUntil("edge published (in-flight tracked)", Duration.ofSeconds(60),
          () -> {
            edge.poller.uploadIncrementalStream(edge.edgeJt, edge.ship, edge.stream);
            return edge.tracker.inFlightCount(MMSI, TABLE) > 0;
          });

      // The ACK round-trips through the real Edge subscription into the real tracker.
      MqttTestSupport.waitUntil("edge watermark advanced by real ACK", Duration.ofSeconds(90),
          () -> edge.tracker.watermark(MMSI, TABLE, 0) == 1001L);

      // Round 2: watermark persists into the Edge cursor table.
      edge.poller.uploadIncrementalStream(edge.edgeJt, edge.ship, edge.stream);
      assertEquals(1001L, edge.cursor(), "Edge cursor must advance on real KAFKA_COMMITTED ACK");

      // Shore side: exactly one history row with the contract-v1 msg_id.
      MqttTestSupport.waitUntil("shore history persisted", Duration.ofSeconds(90),
          () -> repository.countAll() == 1L);
      assertTrue(repository.findByMsgId(FROZEN_MSG_ID_V1).isPresent(),
          "history msg_id must equal the frozen contract vector");
    }
  }
}
