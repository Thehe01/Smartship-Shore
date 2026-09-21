package com.smartship.shore.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryEntity;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Persists {@code ship.telemetry.raw} into MySQL ({@code smartship-history} group).
 *
 * <p>Ordering contract per record:
 * <ol>
 *   <li>Kafka value -&gt; direct {@link TelemetryEnvelope} deserialization + required-field
 *   validation. The value is already an envelope (written by {@code TelemetryKafkaProducer});
 *   it must <b>not</b> go through the Edge flat-JSON parser again, otherwise the business map
 *   would nest as {@code data.data.*}.</li>
 *   <li>{@code UNIQUE(msg_id)} idempotency check + MySQL insert.</li>
 *   <li>Offset acknowledged <b>only after</b> the insert succeeded. Never the reverse.</li>
 * </ol>
 *
 * <p>P2-2 failure routing (executed by Spring Kafka's {@code DefaultErrorHandler}, not here):
 * <ul>
 *   <li>success / {@code DuplicateKeyException} — acknowledged in-listener, never retried,
 *   never DLT'd (idempotency unchanged);</li>
 *   <li>transient DB failures — bounded retry (1s × 3), then DLT;</li>
 *   <li>poison (bad JSON, missing fields) and deterministic DB failures — straight to
 *   the DLT with no pointless retries. Nothing is silently dropped anymore.</li>
 * </ul>
 * Overall semantics: at-least-once delivery + deterministic {@code msg_id} idempotency.
 * Exactly-once is explicitly <b>not</b> claimed: a crash between a successful insert and the
 * offset commit replays the record, and the unique constraint absorbs the replay.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HistoryConsumer {

  private final TelemetryHistoryRepository repository;
  private final ObjectMapper objectMapper;
  private final ShoreMetrics metrics;

  @KafkaListener(
      topics = "${shore.kafka.raw-topic:ship.telemetry.raw}",
      groupId = "${shore.kafka.group-id:smartship-history}",
      containerFactory = "shoreKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
    metrics.historyConsumed();

    final TelemetryEnvelope envelope;
    try {
      envelope = readEnvelope(record.value());
    } catch (InvalidTelemetryException e) {
      // Poison already in Kafka: count it and let the error handler route it straight to
      // the DLT (non-retryable — retrying garbage cannot heal it, and it is never dropped).
      metrics.historyFailed();
      log.error(
          "[Shore-History] unparseable record, routing to DLT: topic={} partition={} offset={}"
              + " key={} err={}",
          record.topic(), record.partition(), record.offset(), record.key(), e.getMessage());
      throw e;
    }

    TelemetryHistoryEntity entity =
        TelemetryHistoryEntity.builder()
            .msgId(envelope.getMsgId())
            .mmsi(envelope.getMmsi())
            .type(envelope.getType())
            .eventTime(envelope.getTimestamp())
            .sentAt(envelope.getSentAt())
            .receivedAt(Instant.now())
            .payloadJson(toPayloadJson(envelope))
            .kafkaPartition(record.partition())
            .kafkaOffset(record.offset())
            .build();

    try {
      repository.insert(entity);
      metrics.historyPersisted();
      acknowledgment.acknowledge();
    } catch (DuplicateKeyException e) {
      // Redelivery of an already-stored msg_id (normal under at-least-once): success, advance.
      // Never retried, never DLT'd.
      metrics.historyDuplicate();
      log.info(
          "[Shore-History] duplicate absorbed: msg_id={} mmsi={} partition={} offset={}",
          envelope.getMsgId(), envelope.getMmsi(), record.partition(), record.offset());
      acknowledgment.acknowledge();
    } catch (DataAccessException e) {
      // Every DB failure — transient (bounded retry, then DLT) or deterministic
      // (straight to DLT) — propagates to the error handler. The offset is never
      // committed ahead of the outcome, and nothing is silently skipped.
      metrics.historyFailed();
      log.warn(
          "[Shore-History] DB failure, entering retry/DLT handling: msg_id={} err={}",
          envelope.getMsgId(), e.getMessage());
      throw e;
    } catch (RuntimeException e) {
      // Non-DB bug (mapping, serialization, ...): bounded retry, then DLT for inspection.
      metrics.historyFailed();
      log.error(
          "[Shore-History] unexpected failure, entering retry/DLT handling: msg_id={} err={}",
          envelope.getMsgId(), e.toString());
      throw e;
    }
  }

  /**
   * Reads one Kafka value straight into the envelope contract. The value was written by
   * {@code TelemetryKafkaProducer} from a validated envelope, so no Edge flat-JSON parsing
   * applies here — re-parsing would nest the business map under {@code data.data}.
   */
  private TelemetryEnvelope readEnvelope(String json) {
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
      envelope.setData(new java.util.LinkedHashMap<>());
    }
    return envelope;
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

  private String toPayloadJson(TelemetryEnvelope envelope) {
    try {
      return objectMapper.writeValueAsString(envelope);
    } catch (Exception e) {
      // Envelope came from parsed JSON, so re-serializing cannot realistically fail.
      throw new IllegalStateException("failed to serialize envelope for msg_id="
          + envelope.getMsgId(), e);
    }
  }
}
