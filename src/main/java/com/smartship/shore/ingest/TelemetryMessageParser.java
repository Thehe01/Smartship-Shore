package com.smartship.shore.ingest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.model.TelemetryEnvelope;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Parses raw Edge MQTT payloads into {@link TelemetryEnvelope}.
 *
 * <p>Compatible with the real Edge {@code MqttPublisher} output:
 * a flat JSON object carrying {@code msg_id}, {@code mmsi}, {@code type}, an optional
 * {@code timestamp} (original business event time, never overwritten here), {@code sent_at},
 * plus arbitrary per-type business columns.
 *
 * <p>Validation is intentionally minimal: {@code msg_id}, {@code mmsi} and {@code type} must be
 * non-blank. Bad JSON or missing required fields raise {@link InvalidTelemetryException} and the
 * caller must drop the message (count it, never forward it to Kafka).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryMessageParser {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /** Reserved envelope keys; everything else lands in {@code data} verbatim. */
  private static final Set<String> RESERVED_KEYS =
      Set.of("msg_id", "mmsi", "type", "timestamp", "sent_at");

  /**
   * Edge devices report wall-clock time in Asia/Shanghai ({@code MqttPublisher} formats
   * {@code sent_at} with {@code ZoneOffset.ofHours(8)}), so zone-less timestamps are
   * interpreted in that zone instead of the shore host zone.
   */
  private static final ZoneId EDGE_ZONE = ZoneId.of("Asia/Shanghai");

  private static final DateTimeFormatter SPACE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");

  private final ObjectMapper objectMapper;

  public TelemetryEnvelope parse(byte[] payload) {
    if (payload == null || payload.length == 0) {
      throw new InvalidTelemetryException("empty MQTT payload");
    }
    return parse(new String(payload, StandardCharsets.UTF_8));
  }

  public TelemetryEnvelope parse(String json) {
    final Map<String, Object> flat;
    try {
      flat = objectMapper.readValue(json, MAP_TYPE);
    } catch (Exception e) {
      throw new InvalidTelemetryException("payload is not valid JSON: " + truncate(json), e);
    }
    if (flat == null) {
      throw new InvalidTelemetryException("payload decoded to null JSON");
    }

    String msgId = text(flat.get("msg_id"));
    String mmsi = text(flat.get("mmsi"));
    String type = text(flat.get("type"));
    if (msgId == null || mmsi == null || type == null) {
      throw new InvalidTelemetryException(
          "missing required field (need non-blank msg_id/mmsi/type): " + truncate(json));
    }

    Instant timestamp = coerceInstant(flat.get("timestamp"));
    Instant sentAt = coerceInstant(flat.get("sent_at"));

    Map<String, Object> data = new LinkedHashMap<>();
    flat.forEach((k, v) -> {
      if (!RESERVED_KEYS.contains(k)) {
        data.put(k, v);
      }
    });

    return TelemetryEnvelope.builder()
        .msgId(msgId)
        .mmsi(mmsi)
        .type(type)
        .timestamp(timestamp)
        .sentAt(sentAt)
        .data(data)
        .build();
  }

  private static String text(Object value) {
    if (value == null) {
      return null;
    }
    String s = String.valueOf(value).trim();
    return s.isEmpty() ? null : s;
  }

  /**
   * Lenient instant coercion. Zone-less values are read in the Edge zone ({@code Asia/Shanghai}).
   * Returns {@code null} when absent/blank/unparseable — {@code timestamp} is optional per the
   * P2-1 contract, so a bad event time degrades to null instead of dropping the message.
   */
  static Instant coerceInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      long v = number.longValue();
      // Heuristic: millis vs seconds.
      return v > 1_000_000_000_000L ? Instant.ofEpochMilli(v) : Instant.ofEpochSecond(v);
    }
    String s = String.valueOf(value).trim();
    if (s.isEmpty() || "null".equalsIgnoreCase(s)) {
      return null;
    }
    try {
      return Instant.parse(s);
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return OffsetDateTime.parse(s).toInstant();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return ZonedDateTime.parse(s).toInstant();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(s).atZone(EDGE_ZONE).toInstant();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(s, SPACE_FORMATTER).atZone(EDGE_ZONE).toInstant();
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      // JDBC-style "2026-09-19 13:00:00.0" sometimes arrives with a trailing ".0".
      String cleaned = s.replaceAll("\\.0+$", "");
      return LocalDateTime.parse(cleaned, SPACE_FORMATTER).atZone(EDGE_ZONE).toInstant();
    } catch (DateTimeParseException e) {
      log.debug("Unparseable event time, degrading to null: {}", s);
      return null;
    }
  }

  private static String truncate(String s) {
    if (s == null) {
      return "null";
    }
    return s.length() <= 300 ? s : s.substring(0, 300) + "...";
  }
}
