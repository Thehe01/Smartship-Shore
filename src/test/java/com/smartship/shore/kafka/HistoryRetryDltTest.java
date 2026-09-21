package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.config.KafkaConfig;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryEntity;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

/**
 * P2-2 unit contract: the production {@code DefaultErrorHandler} wiring
 * (bounded retry + DLT) driven directly, with an H2 repository and a stubbed DLT send.
 *
 * <p>Covers: normal (no DLT), duplicate (no retry/DLT), fail-fail-success (row, no DLT),
 * exhausted (DLT after bounded retries), poison and missing-field (immediate DLT, no
 * retries), and DLT content (original topic/payload/key plus failure metadata).
 *
 * <p>Note on timing: the container mock is never "running", so Spring's backoff sleep is
 * skipped here by design ({@code ListenerUtils.stoppableSleep} returns immediately for a
 * stopped container). What this suite verifies is the attempt counting and recovery
 * routing (3 handleOne calls → still retrying, 4th → DLT; poison → DLT on the 1st).
 * In production the container runs, so the configured 1s intervals really separate
 * the attempts — that interval lives in one place ({@code KafkaConfig}).
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class HistoryRetryDltTest {

  private static final String RAW = "ship.telemetry.raw";
  private static final String DLT = "ship.telemetry.raw.DLT";

  private EmbeddedDatabase db;
  private JdbcTemplate jdbc;
  private TelemetryHistoryRepository repository;
  private ObjectMapper objectMapper;
  private ShoreMetrics metrics;
  private HistoryConsumer consumer;

  private org.springframework.kafka.core.KafkaTemplate<String, String> dltTemplate;
  private DefaultErrorHandler handler;
  private Consumer<String, String> kafkaConsumer;
  private MessageListenerContainer container;
  private long offsetSeq;

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
    jdbc = new JdbcTemplate(db);
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

    objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    repository = new TelemetryHistoryRepository(jdbc);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    consumer = new HistoryConsumer(repository, objectMapper, metrics);

    dltTemplate = mock(org.springframework.kafka.core.KafkaTemplate.class);
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(dltTemplate).send(any(ProducerRecord.class));
    // The exact production wiring (interval 1s × 3 attempts).
    handler = KafkaConfig.historyErrorHandler(dltTemplate, metrics);

    kafkaConsumer = mock(Consumer.class);
    container = mock(MessageListenerContainer.class);
    offsetSeq = 0L;
  }

  @AfterEach
  void tearDown() {
    db.shutdown();
  }

  private ConsumerRecord<String, String> record(String key, String json) {
    return new ConsumerRecord<>(RAW, 0, offsetSeq++, key, json);
  }

  private static String envelope(String mmsi, String type, String rowId, String eventInstant) {
    String msgId = EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, eventInstant);
    return "{\"msg_id\":\"" + msgId + "\","
        + "\"mmsi\":\"" + mmsi + "\","
        + "\"type\":\"" + type + "\","
        + "\"timestamp\":\"" + eventInstant + "\","
        + "\"sent_at\":\"2026-09-19T02:00:05.123Z\","
        + "\"data\":{\"speed_knots\":12.5}}";
  }

  /** One container delivery: listener first, production error handler on failure. */
  private void deliver(HistoryConsumer target, ConsumerRecord<String, String> rec, TestAck ack) {
    try {
      target.listen(rec, ack);
    } catch (Exception e) {
      handler.handleOne(e, rec, kafkaConsumer, container);
    }
  }

  private ProducerRecord<String, String> singleDltSend() {
    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(dltTemplate, times(1)).send(captor.capture());
    return captor.getValue();
  }

  private static String header(ProducerRecord<String, String> rec, String name) {
    var h = rec.headers().lastHeader(name);
    assertNotNull(h, "DLT header missing: " + name);
    return new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("Normal message → one DB row, never touches the DLT")
  void normalNeverTouchesDlt() {
    TestAck ack = new TestAck();
    deliver(consumer, record("413999999",
        envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z")), ack);

    assertTrue(ack.acknowledged);
    assertEquals(1L, repository.countAll());
    verify(dltTemplate, never()).send(any(ProducerRecord.class));
    assertEquals(0.0, metrics.dltCount("transient"));
    assertEquals(0.0, metrics.dltCount("poison"));
  }

  @Test
  @DisplayName("DuplicateKey → ACK, no retry, no DLT")
  void duplicateSkipsRetryAndDlt() {
    String json = envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    TestAck first = new TestAck();
    TestAck second = new TestAck();

    deliver(consumer, record("413999999", json), first);
    deliver(consumer, record("413999999", json), second);

    assertTrue(first.acknowledged);
    assertTrue(second.acknowledged);
    assertEquals(1L, repository.countAll());
    assertEquals(1.0, metrics.getHistoryDuplicateTotal().count());
    verify(dltTemplate, never()).send(any(ProducerRecord.class));
    assertEquals(0.0, metrics.retryCount("transient"));
    assertEquals(0.0, metrics.dltCount("transient"));
    assertEquals(0.0, metrics.dltCount("poison"));
  }

  @Test
  @DisplayName("DB fails twice then succeeds → one row, retried, never DLT'd")
  void retryThenSuccess() {
    TelemetryHistoryRepository flaky = spy(new TelemetryHistoryRepository(jdbc));
    doThrow(new TransientDataAccessResourceException("lock timeout"))
        .doThrow(new TransientDataAccessResourceException("lock timeout"))
        .doCallRealMethod()
        .when(flaky).insert(any(TelemetryHistoryEntity.class));
    HistoryConsumer flakyConsumer = new HistoryConsumer(flaky, objectMapper, metrics);
    ConsumerRecord<String, String> rec = record("413999999",
        envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z"));

    for (int i = 0; i < 3; i++) {
      TestAck ack = new TestAck();
      try {
        flakyConsumer.listen(rec, ack);
        assertTrue(ack.acknowledged, "third attempt must ACK");
        break;
      } catch (Exception e) {
        handler.handleOne(e, rec, kafkaConsumer, container);
      }
    }

    assertEquals(1L, repository.countAll(), "eventual success stores exactly one row");
    assertEquals(1.0, metrics.getHistoryPersistedTotal().count());
    verify(dltTemplate, never()).send(any(ProducerRecord.class));
    assertEquals(2.0, metrics.retryCount("transient"), "two bounded retries happened");
    assertEquals(0.0, metrics.dltCount("transient"));
  }

  @Test
  @DisplayName("DB keeps failing → bounded retries exhausted → exactly one DLT record")
  void exhaustedRetriesGoToDlt() {
    TelemetryHistoryRepository failing = mock(TelemetryHistoryRepository.class);
    doThrow(new TransientDataAccessResourceException("connection reset"))
        .when(failing).insert(any(TelemetryHistoryEntity.class));
    HistoryConsumer failingConsumer = new HistoryConsumer(failing, objectMapper, metrics);
    String json = envelope("413999999", "nmea_gps", "1001", "2026-09-19T02:00:00Z");
    ConsumerRecord<String, String> rec = record("413999999", json);

    // Attempts 1–3: still retrying, nothing in the DLT yet (bounded, not infinite).
    for (int i = 0; i < 3; i++) {
      try {
        failingConsumer.listen(rec, new TestAck());
      } catch (Exception e) {
        handler.handleOne(e, rec, kafkaConsumer, container);
      }
    }
    verify(dltTemplate, never()).send(any(ProducerRecord.class));

    // Attempt 4: budget spent → exactly one DLT record.
    try {
      failingConsumer.listen(rec, new TestAck());
    } catch (Exception e) {
      handler.handleOne(e, rec, kafkaConsumer, container);
    }

    assertEquals(0L, repository.countAll(), "nothing persisted");
    ProducerRecord<String, String> dlt = singleDltSend();
    assertEquals(DLT, dlt.topic());
    assertEquals("413999999", dlt.key(), "key=MMSI preserved");
    assertEquals(json, dlt.value(), "original payload preserved verbatim");
    assertEquals(RAW, header(dlt, KafkaHeaders.DLT_ORIGINAL_TOPIC));
    assertNotNull(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION));
    assertNotNull(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET));
    assertTrue(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN)
        .contains("TransientDataAccessResourceException"));
    assertTrue(header(dlt, KafkaHeaders.DLT_EXCEPTION_MESSAGE).contains("connection reset"));
    assertNotNull(Instant.parse(header(dlt, "shore-dlt-failed-at")), "failure time present");
    assertEquals("transient", header(dlt, "shore-dlt-reason"));
    assertEquals(1.0, metrics.dltCount("transient"));
    assertEquals(0.0, metrics.dltCount("poison"));
  }

  @Test
  @DisplayName("Illegal JSON → no pointless retries → exactly one DLT record")
  void poisonGoesStraightToDlt() {
    ConsumerRecord<String, String> rec = record("413999999", "{not-json-at-all");

    // A single handler pass is enough: no backoff sleeps, straight to recovery.
    long started = System.nanoTime();
    try {
      consumer.listen(rec, new TestAck());
    } catch (Exception e) {
      handler.handleOne(e, rec, kafkaConsumer, container);
    }
    long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - started);

    assertTrue(elapsedMs < 5000, "poison must not sit through retry backoffs");
    assertEquals(0L, repository.countAll());
    ProducerRecord<String, String> dlt = singleDltSend();
    assertEquals(DLT, dlt.topic());
    assertEquals("413999999", dlt.key());
    assertEquals("{not-json-at-all", dlt.value());
    assertTrue(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN).contains("InvalidTelemetryException"));
    assertEquals("poison", header(dlt, "shore-dlt-reason"));
    // One failed-delivery observation, zero backoff waits (see elapsed): never retried.
    assertEquals(1.0, metrics.retryCount("poison"));
    assertEquals(0.0, metrics.retryCount("transient"));
    assertEquals(1.0, metrics.dltCount("poison"));
  }

  @Test
  @DisplayName("Envelope missing required fields → straight to DLT, never retried")
  void missingFieldGoesStraightToDlt() {
    String json = "{\"mmsi\":\"413999999\",\"type\":\"nmea_gps\","
        + "\"timestamp\":\"2026-09-19T02:00:00Z\","
        + "\"sent_at\":\"2026-09-19T02:00:05Z\",\"data\":{\"speed_knots\":12.5}}";
    ConsumerRecord<String, String> rec = record("413999999", json);

    try {
      consumer.listen(rec, new TestAck());
    } catch (Exception e) {
      handler.handleOne(e, rec, kafkaConsumer, container);
    }

    assertEquals(0L, repository.countAll());
    ProducerRecord<String, String> dlt = singleDltSend();
    assertEquals(json, dlt.value(), "original payload preserved verbatim");
    assertEquals(0.0, metrics.retryCount("transient"));
    assertEquals(1.0, metrics.dltCount("poison"));
  }
}
