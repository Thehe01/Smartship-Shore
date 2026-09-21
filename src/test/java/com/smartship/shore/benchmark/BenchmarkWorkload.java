package com.smartship.shore.benchmark;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic benchmark workload (benchmark-only, never production code).
 *
 * <p>100 fixed MMSIs × 5 fixed telemetry types in strict round-robin. Message {@code n}
 * always carries the same business content for a given size:
 * <ul>
 *   <li>{@code mmsi} = {@code 41390} + zero-padded index ({@code n % 100}),</li>
 *   <li>{@code type} = the five real stream types, cycling every 100 messages,</li>
 *   <li>event time = fixed base + {@code n} seconds (strictly increasing, also per
 *   (mmsi, type) subsequence),</li>
 *   <li>{@code msg_id} = SHA-256 over {@code runId|mmsi|type|seq} — unique per run
 *   (idempotent re-runs never collide) and fully reproducible,</li>
 *   <li>business fields = pure functions of {@code n} (no randomness anywhere).</li>
 * </ul>
 * {@code sent_at} is intentionally NOT precomputed: the harness stamps it immediately
 * before each MQTT publish, so history latency measures the real pipeline.
 */
public final class BenchmarkWorkload {

  /** Fixed Edge zone: zone-less row timestamps are read back in this zone (see parser). */
  public static final ZoneId EDGE_ZONE = ZoneId.of("Asia/Shanghai");

  /** Deterministic event-time base; every message adds its sequence in seconds. */
  public static final Instant EVENT_BASE = Instant.parse("2026-06-01T00:00:00Z");

  private static final DateTimeFormatter ROW_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
  private static final DateTimeFormatter SENT_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

  /** The five real telemetry stream types, in fixed order. */
  public static final List<String> TYPES =
      List.of("nmea_gps", "nmea_wind", "nmea_depth", "nmea_rudder", "engine");

  /** Fixed ship count per workload. */
  public static final int MMSI_COUNT = 100;

  /** MQTT topic suffix per type (matches the Edge publisher convention). */
  public static String topicSuffix(String type) {
    return type;
  }

  /** Fixed-format MMSI for ship index {@code 0..99}: {@code 413900000} + index. */
  public static String mmsi(int index) {
    return "4139" + String.format("%05d", index);
  }

  /** One precomputed message spec; {@code sent_at} is stamped at publish time. */
  public record Spec(
      int seq,
      String mmsi,
      String type,
      String topic,
      String msgId,
      Instant eventTime,
      String rowTimestamp,
      Map<String, Object> data) {
  }

  /** Builds the full deterministic sequence for {@code size} messages. */
  public static List<Spec> build(String runId, int size) {
    List<Spec> out = new ArrayList<>(size);
    for (int n = 0; n < size; n++) {
      int mmsiIdx = n % MMSI_COUNT;
      int typeIdx = (n / MMSI_COUNT) % TYPES.size();
      String mmsi = mmsi(mmsiIdx);
      String type = TYPES.get(typeIdx);
      Instant eventTime = EVENT_BASE.plusSeconds(n);
      String rowTs = LocalDateTime.ofInstant(eventTime, EDGE_ZONE).format(ROW_FMT);
      out.add(new Spec(n, mmsi, type, "zncb/" + mmsi + "/" + topicSuffix(type),
          msgId(runId, mmsi, type, n), eventTime, rowTs, businessData(type, n)));
    }
    return out;
  }

  /** Deterministic unique fingerprint; rerunning the same runId reproduces it exactly. */
  public static String msgId(String runId, String mmsi, String type, int seq) {
    try {
      String canonical = runId + "|" + mmsi + "|" + type + "|" + seq;
      byte[] digest = MessageDigest.getInstance("SHA-256")
          .digest(canonical.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Flat Edge-style row JSON. {@code sentAt} must be stamped immediately before publish. */
  public static String payloadJson(Spec spec, Instant sentAt, long rowId) {
    StringBuilder sb = new StringBuilder(256);
    sb.append('{');
    sb.append("\"id\":").append(rowId).append(',');
    sb.append("\"mmsi\":\"").append(spec.mmsi()).append("\",");
    sb.append("\"type\":\"").append(spec.type()).append("\",");
    sb.append("\"msg_id\":\"").append(spec.msgId()).append("\",");
    sb.append("\"timestamp\":\"").append(spec.rowTimestamp()).append("\",");
    sb.append("\"sent_at\":\"")
        .append(ZonedDateTime.ofInstant(sentAt, EDGE_ZONE).format(SENT_FMT)).append("\",");
    boolean first = true;
    for (Map.Entry<String, Object> e : spec.data().entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
    }
    sb.append('}');
    return sb.toString();
  }

  /** Last sequence number below {@code size} carrying the given (mmsi, type) combo. */
  public static int lastSeqFor(int mmsiIdx, int typeIdx, int size) {
    int combo = mmsiIdx + MMSI_COUNT * typeIdx;
    int last = combo;
    while (last + MMSI_COUNT * TYPES.size() < size) {
      last += MMSI_COUNT * TYPES.size();
    }
    return last;
  }

  private static Map<String, Object> businessData(String type, int n) {
    Map<String, Object> data = new LinkedHashMap<>();
    switch (type) {
      case "nmea_gps" -> {
        data.put("latitude", 31.0 + (n % 100) * 0.001);
        data.put("longitude", 121.0 + (n % 100) * 0.001);
        data.put("speed_knots", 10.0 + (n % 20) * 0.5);
        data.put("course_over_ground", (double) (n % 360));
      }
      case "nmea_wind" -> {
        data.put("true_wind_speed", 5.0 + (n % 30) * 0.3);
        data.put("true_wind_direction", (double) (n % 360));
      }
      case "nmea_depth" -> data.put("depth_m", 20.0 + (n % 50) * 0.2);
      case "nmea_rudder" -> data.put("rudder_angle", (double) ((n % 70) - 35));
      case "engine" -> {
        data.put("rpm", 800 + (n % 40) * 10);
        data.put("coolant_temp", 70.0 + (n % 20) * 0.5);
      }
      default -> throw new IllegalArgumentException("unknown type: " + type);
    }
    return data;
  }

  private BenchmarkWorkload() {
  }
}
