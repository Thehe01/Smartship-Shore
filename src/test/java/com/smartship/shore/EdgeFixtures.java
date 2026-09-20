package com.smartship.shore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Shared test fixtures reproducing the real Edge {@code MqttPublisher} output
 * (see {@code smartship-edge-core}: topic {@code zncb/{mmsi}/{topicSuffix}}, QoS 1,
 * deterministic {@code msg_id}, optional {@code timestamp}, always {@code sent_at}).
 */
public final class EdgeFixtures {

  private EdgeFixtures() {
  }

  /** Reproduces Edge {@code stableMessageId(mmsi, type, row)} for the natural-key case. */
  public static String edgeStyleMsgId(String mmsi, String type, String identity, String time) {
    try {
      String canonical = mmsi + "|" + type + "|" + identity + "|" + time;
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Realistic Edge GPS uplink payload (flat JSON, all envelope fields present). */
  public static String gpsPayload() {
    String msgId = edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
    return "{"
        + "\"id\":1001,"
        + "\"ship_id\":\"S001\","
        + "\"mmsi\":\"413999999\","
        + "\"type\":\"nmea_gps\","
        + "\"msg_id\":\"" + msgId + "\","
        + "\"timestamp\":\"2026-09-19T10:00:00\","
        + "\"sent_at\":\"2026-09-19T10:00:05.123+08:00\","
        + "\"latitude\":31.2304,"
        + "\"longitude\":121.4737,"
        + "\"speed_knots\":12.5,"
        + "\"course_over_ground\":270.5,"
        + "\"source_database\":\"zncb_auth\"}";
  }
}
