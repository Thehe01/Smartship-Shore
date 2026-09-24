package com.smartship.shore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import com.smartship.shore.persistence.TelemetryHistoryEntity;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Persists {@code ship.telemetry.raw} into MySQL ({@code smartship-history} group).
 *
 * <p>Batch shape, single-record policy: each poll batch is parsed record by record,
 * already-stored {@code msg_id}s are filtered by one dedup pre-check, the rest go
 * in one JDBC batch, and the whole batch is acknowledged once. Throughput comes
 * from the batch; correctness still comes from the per-record contract:
 *
 * <ol>
 *   <li>Kafka value -&gt; direct {@link TelemetryEnvelope} deserialization + required-field
 *   validation. The value is already an envelope (written by {@code TelemetryKafkaProducer});
 *   it must <b>not</b> go through the Edge flat-JSON parser again, otherwise the business map
 *   would nest as {@code data.data.*}.</li>
 *   <li>{@code UNIQUE(msg_id)} idempotency check + batched MySQL insert.</li>
 *   <li>Offset acknowledged <b>only after</b> the batch landed. Never the reverse.</li>
 * </ol>
 *
 * <p>Every failure below delegates to the shared bounded retry/DLT handler with the
 * exact batch index ({@link BatchListenerFailedException}), so retry/DLT stay
 * record-precise and every P2-2 behavior is unchanged:
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
      id = "smartship-history",
      topics = "${shore.kafka.raw-topic:ship.telemetry.raw}",
      groupId = "${shore.kafka.group-id:smartship-history}",
      containerFactory = "shoreBatchKafkaListenerContainerFactory")
  public void listenBatch(List<ConsumerRecord<String, String>> records,
      Acknowledgment acknowledgment) {
    List<PendingInsert> pending = new ArrayList<>(records.size());
    for (int i = 0; i < records.size(); i++) {
      ConsumerRecord<String, String> record = records.get(i);
      metrics.historyConsumed();

      final TelemetryEnvelope envelope;
      try {
        envelope = EnvelopeCodec.read(objectMapper, record.value());
      } catch (InvalidTelemetryException e) {
        // Poison already in Kafka: count it and let the error handler route it straight to
        // the DLT (non-retryable — retrying garbage cannot heal it, and it is never dropped).
        metrics.historyFailed();
        log.error(
            "[Shore-History] unparseable record, routing to DLT: topic={} partition={} offset={}"
                + " key={} err={}",
            record.topic(), record.partition(), record.offset(), record.key(), e.getMessage());
        throw new BatchListenerFailedException(
            "unparseable history record at batch index " + i, e, i);
      }
      pending.add(new PendingInsert(i, record, toEntity(envelope, record)));
    }

    // Batch dedup pre-check: redeliveries never reach the insert path. The unique
    // constraint — not this pre-check — remains the correctness mechanism under races.
    // Only msg_ids confirmed stored are filtered; an intra-batch repeat stays in the
    // batch so the fallback converges it with exact duplicate accounting.
    if (!pending.isEmpty()) {
      List<String> ids = new ArrayList<>(pending.size());
      for (PendingInsert p : pending) {
        ids.add(p.entity().getMsgId());
      }
      Set<String> stored = repository.existingMsgIds(ids);
      if (!stored.isEmpty()) {
        pending.removeIf(p -> {
          if (stored.contains(p.entity().getMsgId())) {
            absorbDuplicate(p);
            return true;
          }
          return false;
        });
      }
    }

    if (!pending.isEmpty()) {
      try {
        repository.insertBatch(toEntities(pending));
        for (PendingInsert p : pending) {
          metrics.historyPersisted();
        }
      } catch (DataAccessException e) {
        // The batch did not land as a whole (partial outcomes are driver-specific
        // and untrusted): converge record by record through the single-record
        // policy so the exact failed record is identified for retry/DLT.
        insertOneByOne(pending);
      }
    }
    acknowledgment.acknowledge();
  }

  /**
   * Per-record convergence after a batch failure. Already-stored rows are absorbed,
   * and the first real failure is reported with its batch index so the error
   * handler retries/recovers exactly that record; later records in this batch are
   * redelivered by the next poll (their offsets were never committed).
   */
  private void insertOneByOne(List<PendingInsert> pending) {
    for (PendingInsert p : pending) {
      try {
        repository.insert(p.entity());
        metrics.historyPersisted();
      } catch (DuplicateKeyException e) {
        // Redelivery of an already-stored msg_id (normal under at-least-once):
        // success, advance. Never retried, never DLT'd.
        absorbDuplicate(p);
      } catch (DataAccessException e) {
        // Every DB failure — transient (bounded retry, then DLT) or deterministic
        // (straight to DLT) — propagates to the error handler. The offset is never
        // committed ahead of the outcome, and nothing is silently skipped.
        metrics.historyFailed();
        log.warn(
            "[Shore-History] DB failure, entering retry/DLT handling: msg_id={} err={}",
            p.entity().getMsgId(), e.getMessage());
        throw new BatchListenerFailedException(
            "history insert failed at batch index " + p.index(), e, p.index());
      } catch (RuntimeException e) {
        // Non-DB bug (mapping, serialization, ...): bounded retry, then DLT for inspection.
        metrics.historyFailed();
        log.error(
            "[Shore-History] unexpected failure, entering retry/DLT handling: msg_id={} err={}",
            p.entity().getMsgId(), e.toString());
        throw new BatchListenerFailedException(
            "history insert failed at batch index " + p.index(), e, p.index());
      }
    }
  }

  private void absorbDuplicate(PendingInsert dup) {
    metrics.historyDuplicate();
    log.info(
        "[Shore-History] duplicate absorbed: msg_id={} mmsi={} partition={} offset={}",
        dup.entity().getMsgId(), dup.entity().getMmsi(),
        dup.record().partition(), dup.record().offset());
  }

  private TelemetryHistoryEntity toEntity(TelemetryEnvelope envelope,
      ConsumerRecord<String, String> record) {
    return TelemetryHistoryEntity.builder()
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
  }

  private static List<TelemetryHistoryEntity> toEntities(List<PendingInsert> pending) {
    List<TelemetryHistoryEntity> entities = new ArrayList<>(pending.size());
    for (PendingInsert p : pending) {
      entities.add(p.entity());
    }
    return entities;
  }

  private record PendingInsert(int index, ConsumerRecord<String, String> record,
      TelemetryHistoryEntity entity) {
  }

  private String toPayloadJson(TelemetryEnvelope envelope) {
    return EnvelopeCodec.write(objectMapper, envelope);
  }
}
