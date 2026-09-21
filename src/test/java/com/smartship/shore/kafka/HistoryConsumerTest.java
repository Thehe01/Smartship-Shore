package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryEntity;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Tests 3–6 — Kafka -&gt; HistoryConsumer -&gt; DB against in-memory H2.
 *
 * <p>Covers: normal persist, duplicate {@code msg_id} redelivery, the at-least-once crash
 * window (insert committed, offset not), and multi-MMSI handling. Partition-global ordering
 * is neither promised nor tested.
 *
 * <p>P2-1.1 contract: Kafka values are {@code TelemetryEnvelope} JSON (as written by
 * {@code TelemetryKafkaProducer}), deserialized directly — the Edge flat-JSON parser must
 * never run here, otherwise business fields would nest under {@code data.data}.
 */
class HistoryConsumerTest {

  private static final String TOPIC = "ship.telemetry.raw";

  private EmbeddedDatabase db;
  private TelemetryHistoryRepository repository;
  private HistoryConsumer consumer;
  private ShoreMetrics metrics;
  private ObjectMapper objectMapper;
  private long offsetSeq;

  /** Manual acknowledgment stub standing in for the Kafka container's Acknowledgment. */
  static final class TestAck implements Acknowledgment {
    boolean acknowledged;

    @Override
    public void acknowledge() {
      acknowledged = true;
    }
  }

  @BeforeEach
  void setUp() {
    db = new EmbeddedDatabaseBuilder()
        .setType(EmbeddedDatabaseType.H2)
        .build();
    JdbcTemplate jdbc = new JdbcTemplate(db);
    jdbc.execute("CREATE TABLE ship_telemetry_history ("
        + " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        + " msg_id VARCHAR(64) NOT NULL,"
        + " mmsi VARCHAR(32) NOT NULL,"
        + " type VARCHAR(64) NOT NULL,"
        + " event_time TIMESTAMP NULL,"
        + " sent_at TIMESTAMP NULL,"
        + " received_at TIMESTAMP NOT NULL,"
        + " payload CLOB NOT NULL,"
        + " kafka_partition INT NULL,"
        + " kafka_offset BIGINT NULL,"
        + " CONSTRAINT uk_history_msg_id UNIQUE (msg_id))");
    jdbc.execute("CREATE INDEX idx_history_mmsi ON ship_telemetry_history (mmsi)");

    objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    repository = new TelemetryHistoryRepository(jdbc);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    consumer = new HistoryConsumer(repository, objectMapper, metrics);
    offsetSeq = 0L;
  }

  @AfterEach
  void tearDown() {
    db.shutdown();
  }

  private ConsumerRecord<String, String> record(String key, String json) {
    return new ConsumerRecord<>(TOPIC, 2, offsetSeq++, key, json);
  }

  /**
   * Kafka-value fixture in exactly the shape {@code TelemetryKafkaProducer} writes:
   * envelope fields plus a flat business {@code data} map. Timestamps are zone-qualified
   * ISO instants (producer-serialized {@code Instant}).
   */
  private static String envelope(String mmsi, String type, String rowId, String eventInstant) {
    String msgId = EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, eventInstant);
    return "{\"msg_id\":\"" + msgId + "\","
        + "\"mmsi\":\"" + mmsi + "\","
        + "\"type\":\"" + type + "\","
        + "\"timestamp\":\"" + eventInstant + "\","
        + "\"sent_at\":\"2026-09-19T02:00:05.123Z\","
        + "\"data\":{\"speed_knots\":12.5,\"latitude\":31.2304}}";
  }

  private static String msgIdOf(String mmsi, String type, String rowId, String eventInstant) {
    return EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, eventInstant);
  }

  private JsonNode storedPayload(String msgId) throws Exception {
    Optional<TelemetryHistoryEntity> stored = repository.findByMsgId(msgId);
    assertTrue(stored.isPresent(), "expected row for " + msgId);
    return new ObjectMapper().readTree(stored.get().getPayloadJson());
  }

  @Test
  @DisplayName("Test 3 — normal consume: Kafka record becomes exactly one DB row, then ACK")
  void normalConsumePersistsOneRow() throws Exception {
    String json = envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", json), ack);

    assertTrue(ack.acknowledged, "offset must be acknowledged after DB success");
    assertEquals(1L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryConsumedTotal().count());
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count());

    String msgId = msgIdOf("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    Optional<TelemetryHistoryEntity> stored = repository.findByMsgId(msgId);
    assertTrue(stored.isPresent());
    assertEquals("413999999", stored.get().getMmsi());
    assertEquals("nmea_gps", stored.get().getType());
    assertEquals(2, stored.get().getKafkaPartition());
    assertEquals(0L, stored.get().getKafkaOffset());
    // Shore time must not overwrite the Edge event time.
    assertEquals(
        java.time.Instant.parse("2026-09-19T02:00:00Z"), stored.get().getEventTime());

    // Envelope contract: business fields stay flat under data — never data.data.
    JsonNode payload = storedPayload(msgId);
    assertEquals(12.5, payload.path("data").path("speed_knots").asDouble(), 1e-9);
    assertTrue(!payload.path("data").has("data"), "nested data.data must never appear");
  }

  @Test
  @DisplayName("Envelope contract: direct deserialization, no data.data nesting")
  void envelopeDeserializedDirectly() throws Exception {
    String json = envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", json), ack);

    assertTrue(ack.acknowledged);
    JsonNode payload = storedPayload(
        msgIdOf("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z"));
    assertEquals("413999999", payload.path("mmsi").asText());
    assertEquals("nmea_gps", payload.path("type").asText());
    assertEquals(12.5, payload.path("data").path("speed_knots").asDouble(), 1e-9);
    assertEquals(31.2304, payload.path("data").path("latitude").asDouble(), 1e-9);
    assertTrue(!payload.path("data").has("data"), "nested data.data must never appear");
    assertTrue(!payload.path("data").has("msg_id"), "envelope keys must not leak into data");
  }

  @Test
  @DisplayName("Test 4 — same msg_id twice: one row, duplicate counted, second ACK still advances")
  void duplicateConsumeIsAbsorbed() {
    String json = envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    TestAck first = new TestAck();
    TestAck second = new TestAck();

    consumer.listen(record("413999999", json), first);
    consumer.listen(record("413999999", json), second);

    assertTrue(first.acknowledged);
    assertTrue(second.acknowledged, "duplicate must also ACK so the offset advances");
    assertEquals(1L, repository.countAll(), "DB row count stays 1");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count());
    assertEquals(1.0, metrics.getHistoryDuplicateTotal().count());
  }

  @Test
  @DisplayName("Test 5 — crash window: insert committed but offset lost, replay stays at one row")
  void crashWindowReplayIsIdempotent() {
    String msgId = msgIdOf("413999999", "nmea_depth", "5001", "2026-09-19T03:00:00Z");
    // Simulate the insert that succeeded just before the crash (offset never committed).
    repository.insert(TelemetryHistoryEntity.builder()
        .msgId(msgId)
        .mmsi("413999999")
        .type("nmea_depth")
        .eventTime(java.time.Instant.parse("2026-09-19T03:00:00Z"))
        .sentAt(java.time.Instant.parse("2026-09-19T03:00:05Z"))
        .receivedAt(java.time.Instant.now())
        .payloadJson("{\"msg_id\":\"" + msgId + "\"}")
        .kafkaPartition(0)
        .kafkaOffset(41L)
        .build());
    assertEquals(1L, repository.countAll());

    // Kafka redelivers after the restart: absorbed by UNIQUE(msg_id), offset advances.
    TestAck replayAck = new TestAck();
    consumer.listen(
        record("413999999", envelope("413999999", "nmea_depth", "5001", "2026-09-19T03:00:00Z")),
        replayAck);

    assertTrue(replayAck.acknowledged);
    assertEquals(1L, repository.countAll(), "at-least-once + msg_id keeps exactly one row");
    assertEquals(1.0, metrics.getHistoryDuplicateTotal().count());
    assertEquals(0.0, metrics.getHistoryPersistedTotal().count());
  }

  @Test
  @DisplayName("Test 6 — two MMSIs are both processed independently")
  void multiMmsi() {
    TestAck ackA = new TestAck();
    TestAck ackB = new TestAck();

    consumer.listen(
        record("413999999", envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z")), ackA);
    consumer.listen(
        record("412888888", envelope("412888888", "nmea_wind", "2002", "2026-09-19T02:01:00Z")), ackB);

    assertTrue(ackA.acknowledged);
    assertTrue(ackB.acknowledged);
    assertEquals(2L, repository.countAll());
    assertEquals(1L, repository.countByMmsi("413999999"));
    assertEquals(1L, repository.countByMmsi("412888888"));
  }

  @Test
  @DisplayName("Poison record in Kafka: thrown for DLT routing, never acknowledged, never stored")
  void poisonRecordGoesToDlt() {
    TestAck ack = new TestAck();

    assertThrows(InvalidTelemetryException.class,
        () -> consumer.listen(record("413999999", "{not-json-at-all"), ack));

    assertTrue(!ack.acknowledged, "poison must NOT acknowledge (DLT path owns the offset)");
    assertEquals(0L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryFailedTotal().count());
  }

  @Test
  @DisplayName("Envelope missing msg_id: thrown for DLT routing, never persisted")
  void envelopeMissingRequiredFieldGoesToDlt() {
    TestAck ack = new TestAck();
    String json = "{\"mmsi\":\"413999999\",\"type\":\"nmea_gps\","
        + "\"timestamp\":\"2026-09-19T02:00:00Z\","
        + "\"sent_at\":\"2026-09-19T02:00:05Z\",\"data\":{\"speed_knots\":12.5}}";

    assertThrows(InvalidTelemetryException.class,
        () -> consumer.listen(record("413999999", json), ack));

    assertTrue(!ack.acknowledged);
    assertEquals(0L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryFailedTotal().count());
  }

  @Test
  @DisplayName("Transient DB failure: not acknowledged, exception propagates for redelivery")
  void transientFailureRedelivers() {
    TelemetryHistoryRepository failing = org.mockito.Mockito.mock(TelemetryHistoryRepository.class);
    org.mockito.Mockito.doThrow(new TransientDataAccessResourceException("lock wait timeout"))
        .when(failing).insert(org.mockito.ArgumentMatchers.any());
    HistoryConsumer failingConsumer = new HistoryConsumer(failing, objectMapper, metrics);
    TestAck ack = new TestAck();

    assertThrows(RuntimeException.class, () -> failingConsumer.listen(
        record("413999999", envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z")), ack));

    assertTrue(!ack.acknowledged, "transient failure must NOT acknowledge");
    assertEquals(1.0, metrics.getHistoryFailedTotal().count());
  }

  @Test
  @DisplayName("Connection failure propagates like any DB failure (bounded retry, then DLT)")
  void connectionFailureRedelivers() {
    TelemetryHistoryRepository failing = org.mockito.Mockito.mock(TelemetryHistoryRepository.class);
    // NOTE: Spring classifies this as non-transient, but the error handler still routes
    // every unknown DB error through bounded retry into the DLT — never a silent skip.
    org.mockito.Mockito.doThrow(new DataAccessResourceFailureException("connection reset"))
        .when(failing).insert(org.mockito.ArgumentMatchers.any());
    HistoryConsumer failingConsumer = new HistoryConsumer(failing, objectMapper, metrics);
    TestAck ack = new TestAck();

    assertThrows(RuntimeException.class, () -> failingConsumer.listen(
        record("413999999", envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z")), ack));

    assertTrue(!ack.acknowledged, "connection failure must NOT acknowledge");
  }
}
