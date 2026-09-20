package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Test 2 — Kafka record key contract.
 *
 * <p>Every send to {@code ship.telemetry.raw} must carry {@code key = MMSI} so one ship always
 * lands on one partition (partition-local order). No cross-partition global order is claimed
 * or tested.
 */
class TelemetryKafkaProducerTest {

  private KafkaTemplate<String, TelemetryEnvelope> kafkaTemplate;
  private ShoreMetrics metrics;
  private TelemetryKafkaProducer producer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    kafkaTemplate = mock(KafkaTemplate.class);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    producer = new TelemetryKafkaProducer(kafkaTemplate, metrics);
  }

  private static TelemetryEnvelope envelope(String mmsi) {
    return TelemetryEnvelope.builder()
        .msgId("msg-" + mmsi + "-1")
        .mmsi(mmsi)
        .type("nmea_gps")
        .timestamp(Instant.parse("2026-09-19T02:00:00Z"))
        .sentAt(Instant.parse("2026-09-19T02:00:05Z"))
        .data(Map.of("speed_knots", 12.5))
        .build();
  }

  @Test
  @DisplayName("ProducerRecord carries topic ship.telemetry.raw and key = MMSI")
  @SuppressWarnings("unchecked")
  void sendUsesMmsiAsKey() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(kafkaTemplate).send(any(ProducerRecord.class));

    producer.send("ship.telemetry.raw", envelope("413999999")).get();

    ArgumentCaptor<ProducerRecord<String, TelemetryEnvelope>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
    ProducerRecord<String, TelemetryEnvelope> record = captor.getValue();

    assertEquals("ship.telemetry.raw", record.topic());
    assertEquals("413999999", record.key());
    assertEquals("413999999", record.value().getMmsi());
    assertEquals(1.0, metrics.getKafkaProducedTotal().count());
    assertEquals(0.0, metrics.getKafkaProduceFailedTotal().count());
  }

  @Test
  @DisplayName("Async send failure surfaces via the failure callback + metric, not an exception")
  @SuppressWarnings("unchecked")
  void sendFailureCounted() throws Exception {
    doReturn(CompletableFuture.<SendResult<String, TelemetryEnvelope>>failedFuture(
            new RuntimeException("broker down")))
        .when(kafkaTemplate).send(any(ProducerRecord.class));

    // No .get() here: the failure surfaces through the callback + metric, and the
    // future's own exceptional completion is covered by sendFuturePropagatesFailure.
    producer.send("ship.telemetry.raw", envelope("413999999"));

    assertEquals(0.0, metrics.getKafkaProducedTotal().count());
    assertEquals(1.0, metrics.getKafkaProduceFailedTotal().count());
  }

  @Test
  @DisplayName("The send future itself still completes exceptionally for the caller")
  @SuppressWarnings("unchecked")
  void sendFuturePropagatesFailure() {
    doReturn(CompletableFuture.<SendResult<String, TelemetryEnvelope>>failedFuture(
            new RuntimeException("broker down")))
        .when(kafkaTemplate).send(any(ProducerRecord.class));

    var future = producer.send("ship.telemetry.raw", envelope("413999999"));

    org.junit.jupiter.api.Assertions.assertThrows(
        java.util.concurrent.ExecutionException.class, future::get);
  }
}
