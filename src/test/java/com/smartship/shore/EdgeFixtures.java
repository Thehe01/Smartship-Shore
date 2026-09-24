package com.smartship.shore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Shared test fixtures reproducing the real Edge {@code MqttPublisher} output
 * (see {@code smartship-edge-core}: topic {@code zncb/{mmsi}/{topicSuffix}}, QoS 1,
 * deterministic {@code msg_id}, optional {@code timestamp}, always {@code sent_at}).
 *
 * <p>线路协议契约 <b>edge-mqtt-contract-v1</b> 的岸仓冻结端：{@code edgeStyleMsgId}
 * 的输出被 {@code EdgeContractPinTest} pin 在冻结向量上，改算法（任一仓）必须同步
 * bump 两边——单边绿不代表兼容。日常用例仍可用本 helper 构造载荷，但向量本身不可 drift。
 */
public final class EdgeFixtures {

  private EdgeFixtures() {
  }

  /** Mirrors Edge {@code MqttPublisher.topicFor} (contract v1, pinned by test). */
  public static String topicFor(String mmsi, String topicSuffix) {
    return "zncb/" + mmsi + "/" + topicSuffix;
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
