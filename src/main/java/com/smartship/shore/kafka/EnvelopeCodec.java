package com.smartship.shore.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import java.util.LinkedHashMap;

/**
 * Single place that owns the Kafka-side envelope contract, shared by every downstream
 * consumer (history archive, latest-state projection, ...).
 *
 * <p>The Kafka value is always a {@link TelemetryEnvelope} as written by
 * {@code TelemetryKafkaProducer}. It is deserialized directly — the Edge flat-JSON parser
 * must never run here, otherwise the business map would nest as {@code data.data.*}.
 */
public final class EnvelopeCodec {

  private EnvelopeCodec() {
  }

  /**
   * Reads one Kafka value straight into the envelope contract, validating the required
   * {@code msg_id} / {@code mmsi} / {@code type} fields.
   *
   * @throws InvalidTelemetryException when the value is not an envelope or a required
   *     field is missing (poison: must go to the DLT path, never silently dropped)
   */
  public static TelemetryEnvelope read(ObjectMapper objectMapper, String json) {
    final TelemetryEnvelope envelope;
    try {
      envelope = objectMapper.readValue(json, TelemetryEnvelope.class);
    } catch (JsonProcessingException e) {
      throw new InvalidTelemetryException("Kafka value is not a TelemetryEnvelope: "
          + truncate(json), e);
    }
    if (envelope == null
        || isBlank(envelope.getMsgId())
        || isBlank(envelope.getMmsi())
        || isBlank(envelope.getType())) {
      throw new InvalidTelemetryException(
          "Kafka envelope misses required msg_id/mmsi/type: " + truncate(json));
    }
    if (envelope.getData() == null) {
      envelope.setData(new LinkedHashMap<>());
    }
    return envelope;
  }

  /** Serializes an envelope back to the exact JSON shape carried on Kafka / in Redis. */
  public static String write(ObjectMapper objectMapper, TelemetryEnvelope envelope) {
    try {
      return objectMapper.writeValueAsString(envelope);
    } catch (Exception e) {
      throw new IllegalStateException("failed to serialize envelope for msg_id="
          + envelope.getMsgId(), e);
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  private static String truncate(String s) {
    if (s == null) {
      return "null";
    }
    return s.length() <= 300 ? s : s.substring(0, 300) + "...";
  }
}
