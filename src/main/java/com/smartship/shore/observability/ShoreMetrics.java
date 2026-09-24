package com.smartship.shore.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Component;

/**
 * P2-1 / P2-2 low-cardinality counters.
 *
 * <p>Deliberately tagless except the P2-2 {@code reason} bucket ({@code transient} /
 * {@code poison}): {@code mmsi} / {@code msg_id} / exception messages must never become
 * metric tags (high-cardinality risk). Stream {@code type} has only a handful of values
 * today, but even it is left out to keep the surface minimal and predictable.
 */
@Component
@Getter
public class ShoreMetrics {

  private final Counter mqttReceivedTotal;
  private final Counter mqttInvalidTotal;

  private final Counter kafkaProducedTotal;
  private final Counter kafkaProduceFailedTotal;
  private final Counter kafkaAckPublishedTotal;
  private final Counter kafkaAckFailedTotal;
  private final Counter ackClientRebuildTotal;

  private final Counter historyConsumedTotal;
  private final Counter historyPersistedTotal;
  private final Counter historyDuplicateTotal;
  private final Counter historyFailedTotal;

  private final Counter latestStateConsumedTotal;
  private final Counter latestStateUpdatedTotal;
  private final Counter latestStateStaleIgnoredTotal;
  private final Counter latestStateFailedTotal;

  /** P2-2: reason bucket is closed ({@code transient} / {@code poison}) — never raw values. */
  private final Map<String, Counter> historyRetryTotals = new ConcurrentHashMap<>();
  private final Map<String, Counter> historyDltTotals = new ConcurrentHashMap<>();
  private final MeterRegistry registry;

  public ShoreMetrics(MeterRegistry registry) {
    this.registry = registry;
    this.mqttReceivedTotal =
        Counter.builder("smartship_shore_mqtt_received_total")
            .description("MQTT messages received from the Edge broker")
            .register(registry);
    this.mqttInvalidTotal =
        Counter.builder("smartship_shore_mqtt_invalid_total")
            .description("MQTT messages dropped: unparseable JSON or missing msg_id/mmsi/type")
            .register(registry);
    this.kafkaProducedTotal =
        Counter.builder("smartship_shore_kafka_produced_total")
            .description("Envelopes successfully acknowledged by Kafka")
            .register(registry);
    this.kafkaProduceFailedTotal =
        Counter.builder("smartship_shore_kafka_produce_failed_total")
            .description("Envelopes rejected by Kafka (async send failure)")
            .register(registry);
    this.kafkaAckPublishedTotal =
        Counter.builder("smartship_shore_kafka_ack_published_total")
            .description("Application ACKs published to ship/{mmsi}/ack after Kafka acks=all")
            .register(registry);
    this.kafkaAckFailedTotal =
        Counter.builder("smartship_shore_kafka_ack_failed_total")
            .description("Application ACK publishes that failed or timed out (Edge resends;"
                + " the Kafka record itself is already durable)")
            .register(registry);
    this.ackClientRebuildTotal =
        Counter.builder("smartship_shore_ack_client_rebuild_total")
            .description("ACK client rebuilds after consecutive PUBACK timeouts"
                + " (wedged-connection self-heal)")
            .register(registry);
    this.historyConsumedTotal =
        Counter.builder("smartship_shore_history_consumed_total")
            .description("Records polled from ship.telemetry.raw")
            .register(registry);
    this.historyPersistedTotal =
        Counter.builder("smartship_shore_history_persisted_total")
            .description("Records inserted into ship_telemetry_history")
            .register(registry);
    this.historyDuplicateTotal =
        Counter.builder("smartship_shore_history_duplicate_total")
            .description("Redeliveries absorbed by UNIQUE(msg_id)")
            .register(registry);
    this.historyFailedTotal =
        Counter.builder("smartship_shore_history_failed_total")
            .description("Failed history delivery attempts (before retry / DLT routing)")
            .register(registry);
    this.latestStateConsumedTotal =
        Counter.builder("smartship_shore_latest_state_consumed_total")
            .description("Records polled by the latest-state projection group")
            .register(registry);
    this.latestStateUpdatedTotal =
        Counter.builder("smartship_shore_latest_state_updated_total")
            .description("Latest-state hashes effectively updated in Redis")
            .register(registry);
    this.latestStateStaleIgnoredTotal =
        Counter.builder("smartship_shore_latest_state_stale_ignored_total")
            .description("Late events and duplicate redeliveries ignored by the Redis"
                + " compare-and-set (state and TTL untouched, offset still advances)")
            .register(registry);
    this.latestStateFailedTotal =
        Counter.builder("smartship_shore_latest_state_failed_total")
            .description("Latest-state deliveries that failed (retried, then DLT)")
            .register(registry);
  }

  public void mqttReceived() {
    mqttReceivedTotal.increment();
  }

  public void mqttInvalid() {
    mqttInvalidTotal.increment();
  }

  public void kafkaProduced() {
    kafkaProducedTotal.increment();
  }

  public void kafkaProduceFailed() {
    kafkaProduceFailedTotal.increment();
  }

  public void kafkaAckPublished() {
    kafkaAckPublishedTotal.increment();
  }

  public void kafkaAckFailed() {
    kafkaAckFailedTotal.increment();
  }

  public void recordAckClientRebuild() {
    ackClientRebuildTotal.increment();
  }

  public void historyConsumed() {
    historyConsumedTotal.increment();
  }

  public void historyPersisted() {
    historyPersistedTotal.increment();
  }

  public void historyDuplicate() {
    historyDuplicateTotal.increment();
  }

  public void historyFailed() {
    historyFailedTotal.increment();
  }

  /**
   * P2-2: one failed history delivery observed by the error handler. Retryable failures
   * produce one observation per attempt (each followed by a bounded backoff, except the
   * last one which triggers recovery); poison produces exactly one with immediate recovery.
   *
   * @param reason closed bucket: {@code transient} (retryable DB failure) or
   *     {@code poison} (anything else reaching the error handler)
   */
  public void historyRetry(String reason) {
    historyRetryTotals
        .computeIfAbsent(reason, r -> Counter.builder("smartship_shore_history_retry_total")
            .description("Bounded retry attempts for failed history records")
            .tags(Tags.of("reason", r))
            .register(registry))
        .increment();
  }

  /**
   * P2-2: one record was published to the DLT after immediate routing (poison) or
   * retry exhaustion (transient).
   *
   * @param reason closed bucket: {@code transient} or {@code poison}
   */
  public void historyDlt(String reason) {
    historyDltTotals
        .computeIfAbsent(reason, r -> Counter.builder("smartship_shore_history_dlt_total")
            .description("History records routed to the dead-letter topic")
            .tags(Tags.of("reason", r))
            .register(registry))
        .increment();
  }

  /** Test/observation helper: current value of a reason-bucketed counter (0 when absent). */
  public double retryCount(String reason) {
    Counter c = historyRetryTotals.get(reason);
    return c == null ? 0.0 : c.count();
  }

  /** Test/observation helper: current value of a reason-bucketed counter (0 when absent). */
  public double dltCount(String reason) {
    Counter c = historyDltTotals.get(reason);
    return c == null ? 0.0 : c.count();
  }

  public void latestStateConsumed() {
    latestStateConsumedTotal.increment();
  }

  public void latestStateUpdated() {
    latestStateUpdatedTotal.increment();
  }

  public void latestStateStaleIgnored() {
    latestStateStaleIgnoredTotal.increment();
  }

  public void latestStateFailed() {
    latestStateFailedTotal.increment();
  }

  /**
   * Closed-bucket classification shared by the retry listener and the DLT recoverer:
   * known-retryable DB failures are {@code transient}, everything else {@code poison}.
   * Never uses mmsi / msg_id / exception text as a tag.
   *
   * <p>Walks the whole cause chain because a real {@code @KafkaListener} failure arrives
   * wrapped (e.g. {@code ListenerExecutionFailedException} around the DB exception);
   * checking only the outer layer would mislabel transient outages as poison.
   * Cycle-safe via identity tracking.
   */
  public static String classify(Throwable ex) {
    java.util.Set<Throwable> seen =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    for (Throwable t = ex; t != null && seen.add(t); t = t.getCause()) {
      if (t instanceof TransientDataAccessException
          || t instanceof DataAccessResourceFailureException
          || t instanceof CannotGetJdbcConnectionException) {
        return "transient";
      }
    }
    return "poison";
  }
}
