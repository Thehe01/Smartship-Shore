package com.smartship.shore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.ingest.TelemetryMessageParser;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryEntity;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Persists {@code ship.telemetry.raw} into MySQL ({@code smartship-history} group).
 *
 * <p>Ordering contract per record:
 * <ol>
 *   <li>Kafka record -&gt; envelope parse/validation (poison is logged, counted and skipped —
 *   retrying it forever would stall the partition; Retry/DLT topics arrive in P2-2).</li>
 *   <li>{@code UNIQUE(msg_id)} idempotency check + MySQL insert.</li>
 *   <li>Offset acknowledged <b>only after</b> the insert succeeded. Never the reverse.</li>
 * </ol>
 *
 * <p>Duplicates ({@code DuplicateKeyException}) are treated as already-processed success and
 * acknowledged — never as system errors. Transient DB failures propagate so Kafka redelivers.
 * Overall semantics: at-least-once delivery + deterministic {@code msg_id} idempotency.
 * Exactly-once is explicitly <b>not</b> claimed: a crash between a successful insert and the
 * offset commit replays the record, and the unique constraint absorbs the replay.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HistoryConsumer {

  private final TelemetryHistoryRepository repository;
  private final TelemetryMessageParser parser;
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
      envelope = parser.parse(record.value());
    } catch (InvalidTelemetryException e) {
      // Poison record already in Kafka (the MQTT layer filters these, but defense in depth):
      // log it with partition/offset and skip. P2-2 will route these to a DLT.
      metrics.historyFailed();
      log.error(
          "[Shore-History] skipped unparseable record: topic={} partition={} offset={} key={} err={}",
          record.topic(), record.partition(), record.offset(), record.key(), e.getMessage());
      acknowledgment.acknowledge();
      return;
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
      metrics.historyDuplicate();
      log.info(
          "[Shore-History] duplicate absorbed: msg_id={} mmsi={} partition={} offset={}",
          envelope.getMsgId(), envelope.getMmsi(), record.partition(), record.offset());
      acknowledgment.acknowledge();
    } catch (DataIntegrityViolationException | BadSqlGrammarException
        | InvalidDataAccessApiUsageException e) {
      // Deterministic failure (constraint/SQL/programming bug — retrying the same record can
      // never heal it): log loudly and skip instead of stalling the partition.
      // Visible, explainable, P2-1. P2-2 will route these to a DLT instead of skipping.
      metrics.historyFailed();
      log.error(
          "[Shore-History] skipped record on deterministic DB failure: msg_id={} err={}",
          envelope.getMsgId(), e.toString());
      acknowledgment.acknowledge();
    } catch (DataAccessException e) {
      // Any other DB failure (connectivity, lock wait, timeout, ...): do NOT acknowledge;
      // Kafka will redeliver. Safe default: unknown DB errors redeliver, never silently skip.
      metrics.historyFailed();
      log.warn(
          "[Shore-History] transient DB failure, awaiting redelivery: msg_id={} err={}",
          envelope.getMsgId(), e.getMessage());
      throw e;
    } catch (RuntimeException e) {
      // Deterministic failure (bad SQL, mapping bug, ...): retrying forever cannot heal it,
      // so log loudly and skip instead of stalling the partition. Visible, explainable, P2-1.
      metrics.historyFailed();
      log.error(
          "[Shore-History] skipped record on non-transient failure: msg_id={} err={}",
          envelope.getMsgId(), e.toString());
      acknowledgment.acknowledge();
    }
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
