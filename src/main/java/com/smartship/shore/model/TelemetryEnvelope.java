package com.smartship.shore.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Shore-side view of one Edge telemetry message.
 *
 * <p>Field names mirror the Edge {@code MqttPublisher} payload ({@code msg_id}, {@code mmsi},
 * {@code type}, {@code timestamp}, {@code sent_at}); per-type business columns stay schemaless in
 * {@link #data} so new Edge streams never force a DTO change.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TelemetryEnvelope {

  /** Deterministic Edge message id (SHA-256 hex); the idempotency key. */
  @JsonProperty("msg_id")
  private String msgId;

  /** Ship identity; also the Kafka record key. */
  @JsonProperty("mmsi")
  private String mmsi;

  /**
   * Telemetry stream type, e.g. {@code nmea_gps}, {@code nmea_wind}, {@code nmea_depth},
   * {@code nmea_rudder}, {@code engine}.
   */
  @JsonProperty("type")
  private String type;

  /** Original Edge business event time; may be absent when the source row has none. */
  @JsonProperty("timestamp")
  private Instant timestamp;

  /** Edge network send time; always present in real Edge payloads. */
  @JsonProperty("sent_at")
  private Instant sentAt;

  /**
   * Edge source row id (the {@code id} column Edge cursors advance on), echoed
   * transparently in the Application ACK so Edge can match ACKs to rows. Never
   * interpreted by shore; may be absent for schemaless rows. Kept in
   * {@link #data} as well — this field is a view, not a move.
   */
  @JsonProperty("seq")
  private String seq;

  /** Remaining business columns, verbatim from the Edge payload. Never null (may be empty). */
  @JsonProperty("data")
  @Builder.Default
  private Map<String, Object> data = new LinkedHashMap<>();
}
