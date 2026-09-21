package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.support.Acknowledgment;

/**
 * P2-3 unit contract for {@code LatestStateConsumer} with a stubbed Lua outcome.
 *
 * <p>Covers: UPDATED → ACK, STALE → ACK, Redis failure → NO ACK + propagate, poison →
 * propagate without touching Redis, unexpected script outcome → NO ACK, null-timestamp
 * pass-through, and the exact key/ARGV contract handed to the script. Real Lua
 * compare-and-set semantics live in the Redis Testcontainers suite.
 */
class LatestStateConsumerTest {

  private static final String TOPIC = "ship.telemetry.raw";

  private StringRedisTemplate redisTemplate;
  private DefaultRedisScript<String> casScript;
  private ObjectMapper objectMapper;
  private ShoreProperties properties;
  private ShoreMetrics metrics;
  private LatestStateConsumer consumer;
  private long offsetSeq;

  static final class TestAck implements Acknowledgment {
    boolean acknowledged;

    @Override
    public void acknowledge() {
      acknowledged = true;
    }
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    redisTemplate = mock(StringRedisTemplate.class);
    casScript = new DefaultRedisScript<>();
    casScript.setResultType(String.class);
    objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    properties = new ShoreProperties();
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    consumer = new LatestStateConsumer(redisTemplate, casScript, objectMapper, properties, metrics);
    offsetSeq = 0L;
  }

  private ConsumerRecord<String, String> record(String key, String json) {
    return new ConsumerRecord<>(TOPIC, 0, offsetSeq++, key, json);
  }

  private String envelopeJson(String mmsi, String type, String msgId,
      Instant timestamp, Instant sentAt, Map<String, Object> data) throws Exception {
    return objectMapper.writeValueAsString(TelemetryEnvelope.builder()
        .msgId(msgId)
        .mmsi(mmsi)
        .type(type)
        .timestamp(timestamp)
        .sentAt(sentAt)
        .data(new LinkedHashMap<>(data))
        .build());
  }

  @Test
  @DisplayName("UPDATED → ACK with the exact key/ARGV contract")
  @SuppressWarnings("unchecked")
  void updatedAcksWithContract() throws Exception {
    doReturn("UPDATED").when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(), any());
    Instant ts = Instant.parse("2026-09-19T02:05:00Z");
    Instant sent = Instant.parse("2026-09-19T02:05:05Z");
    String json = envelopeJson("413999999", "nmea_gps", "mid-1", ts, sent,
        Map.of("speed_knots", 12.5));
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", json), ack);

    assertTrue(ack.acknowledged);
    assertEquals(1.0, metrics.getLatestStateConsumedTotal().count());
    assertEquals(1.0, metrics.getLatestStateUpdatedTotal().count());
    assertEquals(0.0, metrics.getLatestStateStaleIgnoredTotal().count());

    ArgumentCaptor<List<String>> keys = ArgumentCaptor.forClass(List.class);
    verify(redisTemplate, times(1)).execute(eq(casScript), keys.capture(),
        any(), any(), any(), any(), any());
    assertEquals(List.of("ship:413999999:latest:nmea_gps"), keys.getValue());
  }

  @Test
  @DisplayName("Script ARGV carries payload, millis timestamps, msg_id and TTL")
  @SuppressWarnings("unchecked")
  void scriptArgsCarryContract() throws Exception {
    ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
    doReturn("UPDATED").when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(),
        any());
    Instant ts = Instant.parse("2026-09-19T02:05:00Z");
    Instant sent = Instant.parse("2026-09-19T02:05:05.123Z");
    String json = envelopeJson("413999999", "engine", "mid-9", ts, sent, Map.of("rpm", 1200));

    consumer.listen(record("413999999", json), new TestAck());

    verify(redisTemplate, times(1)).execute(eq(casScript), eq(List.of("ship:413999999:latest:engine")),
        args.capture(), args.capture(), args.capture(), args.capture(), args.capture());
    List<Object> values = args.getAllValues();
    assertEquals(5, values.size());
    // Payload round-trips the envelope JSON (business fields intact).
    assertEquals(objectMapper.readTree(json), objectMapper.readTree(String.valueOf(values.get(0))));
    assertEquals(String.valueOf(ts.toEpochMilli()), values.get(1));
    assertEquals(String.valueOf(sent.toEpochMilli()), values.get(2));
    assertEquals("mid-9", values.get(3));
    assertEquals("86400", values.get(4));
  }

  @Test
  @DisplayName("STALE → still ACK, counted as stale")
  @SuppressWarnings("unchecked")
  void staleStillAcks() throws Exception {
    doReturn("STALE").when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(), any());
    String json = envelopeJson("413999999", "nmea_gps", "mid-old",
        Instant.parse("2026-09-19T02:03:00Z"), Instant.parse("2026-09-19T02:03:05Z"),
        Map.of("speed_knots", 9.0));
    TestAck ack = new TestAck();

    consumer.listen(record("413999999", json), ack);

    assertTrue(ack.acknowledged, "stale is success: the offset must advance");
    assertEquals(1.0, metrics.getLatestStateStaleIgnoredTotal().count());
    assertEquals(0.0, metrics.getLatestStateUpdatedTotal().count());
  }

  @Test
  @DisplayName("Redis failure → NO ACK, exception propagates for retry/DLT")
  @SuppressWarnings("unchecked")
  void redisFailureDoesNotAck() throws Exception {
    doThrow(new RedisConnectionFailureException("redis down"))
        .when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(), any());
    String json = envelopeJson("413999999", "nmea_gps", "mid-1",
        Instant.parse("2026-09-19T02:05:00Z"), Instant.parse("2026-09-19T02:05:05Z"),
        Map.of("speed_knots", 12.5));
    TestAck ack = new TestAck();

    assertThrows(RedisConnectionFailureException.class,
        () -> consumer.listen(record("413999999", json), ack));

    assertTrue(!ack.acknowledged, "Redis failure must NOT acknowledge");
    assertEquals(1.0, metrics.getLatestStateFailedTotal().count());
  }

  @Test
  @DisplayName("Poison value → propagates without touching Redis")
  @SuppressWarnings("unchecked")
  void poisonNeverTouchesRedis() {
    TestAck ack = new TestAck();

    assertThrows(InvalidTelemetryException.class,
        () -> consumer.listen(record("413999999", "{not-json"), ack));

    assertTrue(!ack.acknowledged);
    verify(redisTemplate, never()).execute(any(), anyList(), any(), any(), any(), any(), any());
    assertEquals(1.0, metrics.getLatestStateFailedTotal().count());
  }

  @Test
  @DisplayName("Unexpected script outcome → NO ACK, treated as failure")
  @SuppressWarnings("unchecked")
  void unexpectedOutcomeDoesNotAck() throws Exception {
    doReturn("WEIRD").when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(), any());
    String json = envelopeJson("413999999", "nmea_gps", "mid-1",
        Instant.parse("2026-09-19T02:05:00Z"), null, Map.of("speed_knots", 12.5));
    TestAck ack = new TestAck();

    assertThrows(IllegalStateException.class,
        () -> consumer.listen(record("413999999", json), ack));

    assertTrue(!ack.acknowledged);
  }

  @Test
  @DisplayName("Null-timestamp envelope passes through with empty ts arg (Lua decides)")
  @SuppressWarnings("unchecked")
  void nullTimestampPassesThrough() throws Exception {
    ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
    doReturn("UPDATED").when(redisTemplate).execute(any(), anyList(), any(), any(), any(), any(),
        any());
    String json = envelopeJson("412888888", "nmea_wind", "mid-n",
        null, Instant.parse("2026-09-19T02:05:05Z"), Map.of("true_wind_speed", 8.2));
    TestAck ack = new TestAck();

    consumer.listen(record("412888888", json), ack);

    assertTrue(ack.acknowledged);
    verify(redisTemplate, times(1)).execute(eq(casScript),
        eq(List.of("ship:412888888:latest:nmea_wind")),
        args.capture(), args.capture(), args.capture(), args.capture(), args.capture());
    assertEquals("", args.getAllValues().get(1), "null event time travels as empty string");
  }

  @Test
  @DisplayName("Key pattern isolates ships and streams")
  void keyPattern() {
    assertEquals("ship:413999999:latest:nmea_gps",
        LatestStateConsumer.stateKey("413999999", "nmea_gps"));
    assertEquals("ship:412888888:latest:engine",
        LatestStateConsumer.stateKey("412888888", "engine"));
  }
}
