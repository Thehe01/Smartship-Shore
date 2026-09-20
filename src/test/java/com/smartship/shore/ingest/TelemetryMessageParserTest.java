package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.model.TelemetryEnvelope;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 1 — MQTT payload compatibility with the real Edge {@code MqttPublisher} output.
 *
 * <p>Fixtures below mirror Edge field-for-field: flat JSON with {@code msg_id} (deterministic
 * SHA-256 over {@code mmsi|type|identity|time}), {@code mmsi}, {@code type}, optional original
 * {@code timestamp}, {@code sent_at} with +08:00 offset, and schemaless business columns.
 * Topic convention under test: {@code zncb/{mmsi}/{topicSuffix}} with suffixes
 * {@code nmea_gps/nmea_wind/nmea_depth/nmea_rudder/engine} and QoS 1 (see Edge
 * {@code DatabaseUploadPoller} + {@code MqttClientManager}).
 */
class TelemetryMessageParserTest {

  private TelemetryMessageParser parser;

  @BeforeEach
  void setUp() {
    parser = new TelemetryMessageParser(new ObjectMapper());
  }

  @Test
  @DisplayName("GPS payload: msg_id/mmsi/type/timestamp/sent_at/data all parse, timestamp untouched")
  void parsesRealGpsPayload() {
    TelemetryEnvelope env = parser.parse(EdgeFixtures.gpsPayload());

    assertEquals(EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00"),
        env.getMsgId());
    assertEquals("413999999", env.getMmsi());
    assertEquals("nmea_gps", env.getType());
    // Original Edge event time preserved exactly (zone-less Edge time read as Asia/Shanghai).
    assertEquals(Instant.parse("2026-09-19T02:00:00Z"), env.getTimestamp());
    assertEquals(Instant.parse("2026-09-19T02:00:05.123Z"), env.getSentAt());

    // Business columns ride along verbatim; reserved keys stay out of data.
    assertEquals(31.2304, (Double) env.getData().get("latitude"), 1e-9);
    assertEquals(121.4737, (Double) env.getData().get("longitude"), 1e-9);
    assertEquals("S001", env.getData().get("ship_id"));
    assertTrue(env.getData().containsKey("speed_knots"));
    for (String reserved : new String[] {"msg_id", "mmsi", "type", "timestamp", "sent_at"}) {
      assertTrue(!env.getData().containsKey(reserved), "data must not contain " + reserved);
    }
  }

  @Test
  @DisplayName("Engine payload without timestamp: no event time fabricated, sent_at kept")
  void parsesEnginePayloadWithoutTimestamp() {
    String msgId = EdgeFixtures.edgeStyleMsgId("413999999", "engine", "3003", "unknown");
    String json = "{\"id\":3003,\"mmsi\":\"413999999\",\"type\":\"engine\","
        + "\"msg_id\":\"" + msgId + "\","
        + "\"sent_at\":\"2026-09-19T10:05:00.000+08:00\",\"rpm\":1200.0}";

    TelemetryEnvelope env = parser.parse(json);

    assertEquals("engine", env.getType());
    assertNull(env.getTimestamp(), "missing timestamp must stay null, never fabricated");
    assertNotNull(env.getSentAt());
    assertEquals(1200.0, ((Number) env.getData().get("rpm")).doubleValue(), 1e-9);
  }

  @Test
  @DisplayName("JDBC-style 'yyyy-MM-dd HH:mm:ss' timestamps (Edge DB rows) parse in Edge zone")
  void parsesSpaceSeparatedTimestamp() {
    String msgId = EdgeFixtures.edgeStyleMsgId("413999999", "nmea_wind", "77", "2026-09-19 13:00:00");
    String json = "{\"id\":77,\"mmsi\":\"413999999\",\"type\":\"nmea_wind\","
        + "\"msg_id\":\"" + msgId + "\","
        + "\"timestamp\":\"2026-09-19 13:00:00\","
        + "\"sent_at\":\"2026-09-19T13:00:01+08:00\",\"true_wind_speed\":8.2}";

    TelemetryEnvelope env = parser.parse(json);

    assertEquals(Instant.parse("2026-09-19T05:00:00Z"), env.getTimestamp());
    assertEquals(8.2, ((Number) env.getData().get("true_wind_speed")).doubleValue(), 1e-9);
  }

  @Test
  @DisplayName("Invalid payloads are rejected: bad JSON, blank or missing msg_id/mmsi/type")
  void rejectsInvalidPayloads() {
    assertThrows(InvalidTelemetryException.class, () -> parser.parse("{not-json"));
    assertThrows(InvalidTelemetryException.class, () -> parser.parse(""));
    assertThrows(InvalidTelemetryException.class, () -> parser.parse((byte[]) null));

    String base = "{\"mmsi\":\"413999999\",\"type\":\"nmea_gps\","
        + "\"msg_id\":\"abc\",\"sent_at\":\"2026-09-19T10:00:05+08:00\"}";
    // Sanity: complete payload passes.
    assertNotNull(parser.parse(base));

    assertThrows(InvalidTelemetryException.class,
        () -> parser.parse("{\"mmsi\":\"413999999\",\"type\":\"nmea_gps\"}"));
    assertThrows(InvalidTelemetryException.class,
        () -> parser.parse("{\"msg_id\":\"abc\",\"type\":\"nmea_gps\"}"));
    assertThrows(InvalidTelemetryException.class,
        () -> parser.parse("{\"msg_id\":\"abc\",\"mmsi\":\"413999999\"}"));
    assertThrows(InvalidTelemetryException.class,
        () -> parser.parse(
            "{\"msg_id\":\"  \",\"mmsi\":\"413999999\",\"type\":\"nmea_gps\"}"));
  }
}
