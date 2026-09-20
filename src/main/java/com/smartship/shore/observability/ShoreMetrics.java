package com.smartship.shore.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import org.springframework.stereotype.Component;

/**
 * P2-1 low-cardinality counters.
 *
 * <p>Deliberately tagless: {@code mmsi} / {@code msg_id} must never become metric tags
 * (high-cardinality risk). Stream {@code type} has only a handful of values today, but even it
 * is left out to keep the P2-1 surface minimal and predictable.
 */
@Component
@Getter
public class ShoreMetrics {

  private final Counter mqttReceivedTotal;
  private final Counter mqttInvalidTotal;

  private final Counter kafkaProducedTotal;
  private final Counter kafkaProduceFailedTotal;

  private final Counter historyConsumedTotal;
  private final Counter historyPersistedTotal;
  private final Counter historyDuplicateTotal;
  private final Counter historyFailedTotal;

  public ShoreMetrics(MeterRegistry registry) {
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
            .description("Records that failed: transient DB errors (redelivered) or poison skips")
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
}
