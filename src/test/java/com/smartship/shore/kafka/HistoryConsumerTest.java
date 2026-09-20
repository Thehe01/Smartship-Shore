package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.ingest.TelemetryMessageParser;
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
 */
class HistoryConsumerTest {

  private static final String TOPIC = "ship.telemetry.raw";

  private EmbeddedDatabase db;
  private TelemetryHistoryRepository repository;
  private HistoryConsumer consumer;
  private ShoreMetrics metrics;
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

    ObjectMapper objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    repository = new TelemetryHistoryRepository(jdbc);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    consumer = new HistoryConsumer(
        repository, new TelemetryMessageParser(new ObjectMapper()), objectMapper, metrics);
    offsetSeq = 0L;
  }

  @AfterEach
  void tearDown() {
    db.shutdown();
  }

  private ConsumerRecord<String, String> record(String key, String json) {
    return new ConsumerRecord<>(TOPIC, 2, offsetSeq++, key, json);
  }

  private static String payload(String mmsi, String type, String rowId, String businessTime) {
    String msgId = EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, businessTime);
    String ts = "\"timestamp\":\"" + businessTime + "\",";
    return "{\"id\":" + rowId + ",\"mmsi\":\"" + mmsi + "\",\"type\":\"" + type + "\","
        + "\"msg_id\":\"" + msgId + "\"," + ts
        + "\"sent_at\":\"2026-09-19T10:00:05.123+08:00\","
        + "\"speed_knots\":12.5}";
  }

  @Test
  @DisplayName("Test 3 — normal consume: Kafka record becomes exactly one DB row, then ACK")
  void normalConsumePersistsOneRow() {
    String json = payload("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", json), ack);

    assertTrue(ack.acknowledged, "offset must be acknowledged after DB success");
    assertEquals(1L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryConsumedTotal().count());
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count());

    Optional<TelemetryHistoryEntity> stored =
        repository.findByMsgId(EdgeFixtures.edgeStyleMsgId(
            "413999999", "nmea_gps", "1001", "2026-09-19T10:00:00"));
    assertTrue(stored.isPresent());
    assertEquals("413999999", stored.get().getMmsi());
    assertEquals("nmea_gps", stored.get().getType());
    assertEquals(2, stored.get().getKafkaPartition());
    assertEquals(0L, stored.get().getKafkaOffset());
    // Shore time must not overwrite the Edge event time.
    assertEquals(
        java.time.Instant.parse("2026-09-19T02:00:00Z"), stored.get().getEventTime());
    assertTrue(stored.get().getPayloadJson().contains("speed_knots"));
  }

  @Test
  @DisplayName("Test 4 — same msg_id twice: one row, duplicate counted, second ACK still advances")
  void duplicateConsumeIsAbsorbed() {
    String json = payload("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
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
    // Simulate the insert that succeeded just before the crash (offset never committed).
    String json = payload("413999999", "nmea_depth", "5001", "2026-09-19T11:00:00");
    TelemetryMessageParser standaloneParser =
        new TelemetryMessageParser(new ObjectMapper());
    var envelope = standaloneParser.parse(json);
    repository.insert(TelemetryHistoryEntity.builder()
        .msgId(envelope.getMsgId())
        .mmsi(envelope.getMmsi())
        .type(envelope.getType())
        .eventTime(envelope.getTimestamp())
        .sentAt(envelope.getSentAt())
        .receivedAt(java.time.Instant.now())
        .payloadJson("{\"msg_id\":\"" + envelope.getMsgId() + "\"}")
        .kafkaPartition(0)
        .kafkaOffset(41L)
        .build());
    assertEquals(1L, repository.countAll());

    // Kafka redelivers after the restart: absorbed by UNIQUE(msg_id), offset advances.
    TestAck replayAck = new TestAck();
    consumer.listen(record("413999999", json), replayAck);

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
        record("413999999", payload("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00")), ackA);
    consumer.listen(
        record("412888888", payload("412888888", "nmea_wind", "2002", "2026-09-19T10:01:00")), ackB);

    assertTrue(ackA.acknowledged);
    assertTrue(ackB.acknowledged);
    assertEquals(2L, repository.countAll());
    assertEquals(1L, repository.countByMmsi("413999999"));
    assertEquals(1L, repository.countByMmsi("412888888"));
  }

  @Test
  @DisplayName("Poison record in Kafka: logged, counted, skipped — partition keeps moving")
  void poisonRecordSkipped() {
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", "{not-json-at-all"), ack);

    assertTrue(ack.acknowledged);
    assertEquals(0L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryFailedTotal().count());
  }

  @Test
  @DisplayName("Transient DB failure: not acknowledged, exception propagates for redelivery")
  void transientFailureRedelivers() {
    TelemetryHistoryRepository failing = org.mockito.Mockito.mock(TelemetryHistoryRepository.class);
    org.mockito.Mockito.doThrow(new TransientDataAccessResourceException("lock wait timeout"))
        .when(failing).insert(org.mockito.ArgumentMatchers.any());
    ObjectMapper objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    HistoryConsumer failingConsumer = new HistoryConsumer(
        failing, new TelemetryMessageParser(new ObjectMapper()), objectMapper, metrics);
    TestAck ack = new TestAck();

    assertThrows(RuntimeException.class, () -> failingConsumer.listen(
        record("413999999", payload("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00")), ack));

    assertTrue(!ack.acknowledged, "transient failure must NOT acknowledge");
    assertEquals(1.0, metrics.getHistoryFailedTotal().count());
  }

  @Test
  @DisplayName("Connection failure is NOT a deterministic skip: it redelivers too")
  void connectionFailureRedelivers() {
    TelemetryHistoryRepository failing = org.mockito.Mockito.mock(TelemetryHistoryRepository.class);
    // NOTE: Spring classifies this as non-transient, but shore policy redelivers every
    // unknown DB error and only skips proven-deterministic ones.
    org.mockito.Mockito.doThrow(new DataAccessResourceFailureException("connection reset"))
        .when(failing).insert(org.mockito.ArgumentMatchers.any());
    ObjectMapper objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    HistoryConsumer failingConsumer = new HistoryConsumer(
        failing, new TelemetryMessageParser(new ObjectMapper()), objectMapper, metrics);
    TestAck ack = new TestAck();

    assertThrows(RuntimeException.class, () -> failingConsumer.listen(
        record("413999999", payload("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00")), ack));

    assertTrue(!ack.acknowledged, "connection failure must NOT acknowledge");
  }
}
