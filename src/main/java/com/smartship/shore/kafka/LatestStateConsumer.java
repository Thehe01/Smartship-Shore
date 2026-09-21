package com.smartship.shore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * P2-3 latest-state projection: an independent consumer group
 * ({@code smartship-latest-state}) that materializes, per ship and stream, the newest
 * known telemetry into Redis hashes ({@code ship:{mmsi}:latest:{type}}).
 *
 * <p>Ordering contract per record:
 * <ol>
 *   <li>Kafka value -&gt; shared {@link EnvelopeCodec} contract (same as history).</li>
 *   <li>One atomic Lua compare-and-set keyed on the Edge event time:
 *   {@code UPDATED} (real change, TTL refreshed) or {@code STALE} (late event ignored,
 *   TTL untouched) — both acknowledge the offset.</li>
 *   <li>Redis failure → no acknowledgment, exception propagates to the shared bounded
 *   retry/DLT handler. Failures are never swallowed.</li>
 * </ol>
 *
 * <p>Same-msg_id redelivery rewrites an equivalent state (idempotent result) and
 * acknowledges normally; no dedup set is maintained. Semantics stay at-least-once;
 * exactly-once is explicitly <b>not</b> claimed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LatestStateConsumer {

  /** Lua outcomes — kept as literals matching {@code latest_state_cas.lua}. */
  static final String UPDATED = "UPDATED";
  static final String STALE = "STALE";

  private final StringRedisTemplate redisTemplate;
  private final DefaultRedisScript<String> latestStateCasScript;
  private final ObjectMapper objectMapper;
  private final ShoreProperties properties;
  private final ShoreMetrics metrics;

  @KafkaListener(
      id = "smartship-latest-state",
      topics = "${shore.kafka.raw-topic:ship.telemetry.raw}",
      groupId = "${shore.kafka.latest-state-group-id:smartship-latest-state}",
      containerFactory = "shoreKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
    metrics.latestStateConsumed();

    final TelemetryEnvelope envelope;
    try {
      envelope = EnvelopeCodec.read(objectMapper, record.value());
    } catch (InvalidTelemetryException e) {
      // Same poison policy as history: counted, then straight to the DLT (non-retryable).
      metrics.latestStateFailed();
      log.error(
          "[Shore-Latest] unparseable record, routing to DLT: topic={} partition={} offset={}"
              + " key={} err={}",
          record.topic(), record.partition(), record.offset(), record.key(), e.getMessage());
      throw e;
    }

    final String key = stateKey(envelope.getMmsi(), envelope.getType());
    final String result;
    try {
      result = redisTemplate.execute(
          latestStateCasScript,
          List.of(key),
          EnvelopeCodec.write(objectMapper, envelope),
          epochMillisOrEmpty(envelope.getTimestamp()),
          epochMillisOrEmpty(envelope.getSentAt()),
          envelope.getMsgId(),
          String.valueOf(properties.getRedis().getLatestStateTtlSeconds()));
    } catch (RuntimeException e) {
      // Redis failure is never swallowed: no ACK, bounded retry then DLT via the handler.
      metrics.latestStateFailed();
      log.warn(
          "[Shore-Latest] Redis update failed, entering retry/DLT handling: key={} msg_id={}"
              + " err={}",
          key, envelope.getMsgId(), e.getMessage());
      throw e;
    }

    if (UPDATED.equals(result)) {
      metrics.latestStateUpdated();
      log.debug("[Shore-Latest] updated: key={} msg_id={}", key, envelope.getMsgId());
    } else if (STALE.equals(result)) {
      metrics.latestStateStaleIgnored();
      log.debug("[Shore-Latest] stale ignored: key={} msg_id={}", key, envelope.getMsgId());
    } else {
      // The script contract only ever returns UPDATED/STALE; anything else is a bug
      // worth retrying rather than acknowledging blindly.
      metrics.latestStateFailed();
      throw new IllegalStateException(
          "unexpected Lua outcome for key=" + key + ": " + result);
    }
    acknowledgment.acknowledge();
  }

  /** Redis key pattern: {@code ship:{mmsi}:latest:{type}}. Package-visible for tests. */
  static String stateKey(String mmsi, String type) {
    return "ship:" + mmsi + ":latest:" + type;
  }

  private static String epochMillisOrEmpty(Instant instant) {
    return instant == null ? "" : String.valueOf(instant.toEpochMilli());
  }
}
