package com.smartship.shore.kafka;

import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes validated envelopes to {@code ship.telemetry.raw}.
 *
 * <p>The record key is always the MMSI, so all messages of one ship land on the same partition
 * and stay ordered there. No claim is made about ordering across partitions or ships.
 *
 * <p>Reliability comes from the broker-side producer configuration
 * ({@code acks=all}, idempotence, retries); this class adds no hand-rolled retry thread.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryKafkaProducer {

  private final KafkaTemplate<String, TelemetryEnvelope> kafkaTemplate;
  private final ShoreMetrics metrics;

  /**
   * Sends one envelope asynchronously.
   *
   * @param rawTopic target topic, normally {@code ship.telemetry.raw}
   * @param envelope validated envelope; its {@code mmsi} becomes the record key
   */
  public CompletableFuture<SendResult<String, TelemetryEnvelope>> send(
      String rawTopic, TelemetryEnvelope envelope) {
    ProducerRecord<String, TelemetryEnvelope> record =
        new ProducerRecord<>(rawTopic, envelope.getMmsi(), envelope);
    CompletableFuture<SendResult<String, TelemetryEnvelope>> future = kafkaTemplate.send(record);
    future.whenComplete((result, ex) -> {
      if (ex != null) {
        metrics.kafkaProduceFailed();
        log.warn(
            "[Shore-Produce] Kafka send failed: topic={}, mmsi={}, msg_id={}, err={}",
            rawTopic, envelope.getMmsi(), envelope.getMsgId(), ex.getMessage());
      } else {
        metrics.kafkaProduced();
        if (log.isDebugEnabled()) {
          log.debug(
              "[Shore-Produce] ok: topic={} partition={} offset={} mmsi={} msg_id={}",
              rawTopic,
              result.getRecordMetadata().partition(),
              result.getRecordMetadata().offset(),
              envelope.getMmsi(),
              envelope.getMsgId());
        }
      }
    });
    return future;
  }
}
