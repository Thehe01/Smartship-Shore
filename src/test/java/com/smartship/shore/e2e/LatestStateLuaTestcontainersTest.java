package com.smartship.shore.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.kafka.LatestStateConsumer;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.model.TelemetryEnvelope;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * P2-3 Lua matrix against real Redis: first-write, newer-wins, stale-ignored, equal-time
 * tiebreaks, null-timestamp policy, duplicate idempotency, MMSI/type isolation and TTL.
 *
 * <p>Drives the real {@code LatestStateConsumer} (same script, same serialization) without
 * Kafka in the loop; the Kafka→Redis wiring has its own suite. Skips without Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=false",
    "spring.flyway.enabled=false",
    "spring.kafka.listener.auto-startup=false"
})
class LatestStateLuaTestcontainersTest {

  private static final String TOPIC = "ship.telemetry.raw";
  private static final long TTL = 86400L;

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
          .withExposedPorts(6379)
          .waitingFor(Wait.forListeningPort());

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
  }

  @Autowired
  private LatestStateConsumer consumer;

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private ShoreMetrics metrics;

  static final class TestAck implements Acknowledgment {
    boolean acknowledged;

    @Override
    public void acknowledge() {
      acknowledged = true;
    }
  }

  private long seq;

  private String key(String mmsi, String type) {
    return "ship:" + mmsi + ":latest:" + type;
  }

  private String envelopeJson(String mmsi, String type, String rowId, Instant ts, Instant sent,
      Map<String, Object> data) throws Exception {
    return objectMapper.writeValueAsString(TelemetryEnvelope.builder()
        .msgId(EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId,
            ts == null ? "null-ts" : ts.toString()))
        .mmsi(mmsi)
        .type(type)
        .timestamp(ts)
        .sentAt(sent)
        .data(new LinkedHashMap<>(data))
        .build());
  }

  private TestAck listen(String mmsi, String json) {
    TestAck ack = new TestAck();
    consumer.listen(new ConsumerRecord<>(TOPIC, 0, seq++, mmsi, json), ack);
    assertTrue(ack.acknowledged, "every Lua outcome must ACK the offset");
    return ack;
  }

  private Map<Object, Object> hash(String key) {
    return redisTemplate.opsForHash().entries(key);
  }

  private String payloadTs(String key) throws Exception {
    JsonNode payload = objectMapper.readTree(String.valueOf(hash(key).get("payload")));
    return payload.path("timestamp").asText();
  }

  private Long ttl(String key) {
    return redisTemplate.getExpire(key, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("First write wins a slot, newer overwrites, older stays stale")
  void firstNewerStale() throws Exception {
    String mmsi = "413900001";
    String type = "nmea_gps";
    String k = key(mmsi, type);
    Instant t3 = Instant.parse("2026-09-19T02:03:00Z");
    Instant t5 = Instant.parse("2026-09-19T02:05:00Z");
    double updatedBefore = metrics.getLatestStateUpdatedTotal().count();
    double staleBefore = metrics.getLatestStateStaleIgnoredTotal().count();

    listen(mmsi, envelopeJson(mmsi, type, "r1", t3, t3.plusSeconds(5), Map.of("speed_knots", 9.0)));
    assertEquals("2026-09-19T02:03:00Z", payloadTs(k));

    listen(mmsi, envelopeJson(mmsi, type, "r2", t5, t5.plusSeconds(5), Map.of("speed_knots", 12.5)));
    assertEquals("2026-09-19T02:05:00Z", payloadTs(k));

    Long ttlAfterUpdate = ttl(k);
    assertTrue(ttlAfterUpdate != null && ttlAfterUpdate > 0 && ttlAfterUpdate <= TTL);

    Thread.sleep(2200); // let the clock move so a TTL refresh would be visible
    listen(mmsi, envelopeJson(mmsi, type, "r1b", t3, t3.plusSeconds(6), Map.of("speed_knots", 9.5)));
    assertEquals("2026-09-19T02:05:00Z", payloadTs(k), "late event must not overwrite");
    Long ttlAfterStale = ttl(k);
    assertTrue(ttlAfterStale != null && ttlAfterStale < ttlAfterUpdate,
        "stale event must not refresh TTL");

    assertEquals(2.0, metrics.getLatestStateUpdatedTotal().count() - updatedBefore);
    assertEquals(1.0, metrics.getLatestStateStaleIgnoredTotal().count() - staleBefore);
  }

  @Test
  @DisplayName("Equal timestamps: duplicate is stale, newer sent_at wins, older loses")
  void equalTimestampTiebreaks() throws Exception {
    String mmsi = "413900002";
    String type = "nmea_gps";
    String k = key(mmsi, type);
    Instant t = Instant.parse("2026-09-19T02:05:00Z");
    double updatedBefore = metrics.getLatestStateUpdatedTotal().count();
    double staleBefore = metrics.getLatestStateStaleIgnoredTotal().count();

    String same = envelopeJson(mmsi, type, "same", t, t.plusSeconds(5), Map.of("speed_knots", 10.0));
    listen(mmsi, same);
    listen(mmsi, same); // same msg_id redelivery: stale, state and TTL untouched
    assertEquals(t.toString(), payloadTs(k));

    // Different msg_id, same instant, newer sent_at → wins.
    listen(mmsi, envelopeJson(mmsi, type, "newer", t, t.plusSeconds(9), Map.of("speed_knots", 11.0)));
    assertEquals(11.0,
        objectMapper.readTree(String.valueOf(hash(k).get("payload")))
            .path("data").path("speed_knots").asDouble(), 1e-9);

    // Different msg_id, same instant, older sent_at → stale.
    listen(mmsi, envelopeJson(mmsi, type, "older", t, t.plusSeconds(1), Map.of("speed_knots", 1.0)));
    assertEquals(11.0,
        objectMapper.readTree(String.valueOf(hash(k).get("payload")))
            .path("data").path("speed_knots").asDouble(), 1e-9);

    assertEquals(2.0, metrics.getLatestStateUpdatedTotal().count() - updatedBefore);
    assertEquals(2.0, metrics.getLatestStateStaleIgnoredTotal().count() - staleBefore);
  }

  @Test
  @DisplayName("Null-timestamp policy: real beats unknown, unknown never clobbers real")
  void nullTimestampPolicy() throws Exception {
    String mmsi = "413900003";
    String type = "engine";
    String k = key(mmsi, type);
    Instant t = Instant.parse("2026-09-19T02:05:00Z");

    // No current state: null-timestamp observation wins a slot.
    listen(mmsi, envelopeJson(mmsi, type, "n1", null, t.plusSeconds(5), Map.of("rpm", 1000)));
    assertTrue(hash(k).get("payload").toString().contains("\"rpm\":1000"));

    // Real event time overwrites the unknown one.
    listen(mmsi, envelopeJson(mmsi, type, "n2", t, t.plusSeconds(6), Map.of("rpm", 1200)));
    assertEquals(t.toString(), payloadTs(k));

    // Null event time must never clobber a real one, even with a newer sent_at.
    listen(mmsi, envelopeJson(mmsi, type, "n3", null, t.plusSeconds(99), Map.of("rpm", 1)));
    assertEquals(t.toString(), payloadTs(k));

    // Both null: newer sent_at wins, older loses — deterministic, no randomness.
    String mmsi2 = "413900004";
    String k2 = key(mmsi2, type);
    listen(mmsi2, envelopeJson(mmsi2, type, "m1", null, t.plusSeconds(5), Map.of("rpm", 1000)));
    listen(mmsi2, envelopeJson(mmsi2, type, "m2", null, t.plusSeconds(9), Map.of("rpm", 1100)));
    assertTrue(hash(k2).get("payload").toString().contains("\"rpm\":1100"));
    listen(mmsi2, envelopeJson(mmsi2, type, "m3", null, t.plusSeconds(1), Map.of("rpm", 100)));
    assertTrue(hash(k2).get("payload").toString().contains("\"rpm\":1100"));
  }

  @Test
  @DisplayName("Ships and streams project into isolated keys with correct content")
  void isolationAndContent() throws Exception {
    Instant t = Instant.parse("2026-09-19T02:05:00Z");
    listen("413900005", envelopeJson("413900005", "nmea_gps", "a",
        t, t.plusSeconds(5), Map.of("speed_knots", 12.5)));
    listen("413900006", envelopeJson("413900006", "engine", "b",
        t, t.plusSeconds(5), Map.of("rpm", 1200)));
    listen("413900005", envelopeJson("413900005", "engine", "c",
        t, t.plusSeconds(5), Map.of("rpm", 800)));

    JsonNode gps = objectMapper.readTree(
        String.valueOf(hash(key("413900005", "nmea_gps")).get("payload")));
    assertEquals("413900005", gps.path("mmsi").asText());
    assertEquals("nmea_gps", gps.path("type").asText());
    assertEquals(12.5, gps.path("data").path("speed_knots").asDouble(), 1e-9);
    assertTrue(!gps.path("data").has("data"));

    JsonNode eng = objectMapper.readTree(
        String.valueOf(hash(key("413900005", "engine")).get("payload")));
    assertEquals(800, eng.path("data").path("rpm").asInt());

    assertTrue(hash(key("413900006", "engine")).get("payload").toString()
        .contains("413900006"));
    assertTrue(ttl(key("413900005", "nmea_gps")) > 0);
  }

  @Test
  @DisplayName("Duplicate redelivery keeps state and TTL untouched, still ACKs")
  void duplicateRedelivery() throws Exception {
    String mmsi = "413900007";
    String k = key(mmsi, "nmea_gps");
    Instant t = Instant.parse("2026-09-19T02:05:00Z");
    String json = envelopeJson(mmsi, "nmea_gps", "dup", t, t.plusSeconds(5),
        Map.of("speed_knots", 12.5));
    double staleBefore = metrics.getLatestStateStaleIgnoredTotal().count();

    listen(mmsi, json);
    String first = String.valueOf(hash(k).get("payload"));
    Long ttlAfterFirst = ttl(k);
    assertTrue(ttlAfterFirst != null && ttlAfterFirst > 0);

    Thread.sleep(2200); // let the clock move so a TTL refresh would be visible
    listen(mmsi, json); // same msg_id: stale, must not refresh anything

    assertEquals(objectMapper.readTree(first),
        objectMapper.readTree(String.valueOf(hash(k).get("payload"))),
        "duplicate must leave the stored state untouched");
    Long ttlAfterDuplicate = ttl(k);
    assertTrue(ttlAfterDuplicate != null && ttlAfterDuplicate < ttlAfterFirst,
        "duplicate must not refresh TTL");
    assertEquals(1.0, metrics.getLatestStateStaleIgnoredTotal().count() - staleBefore);
  }
}
