package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.kafka.TelemetryKafkaProducer;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

/**
 * P2-1.1 handoff contract: with Paho manual acknowledgments, the MQTT delivery is completed
 * only after the Kafka send future succeeds. A failed handoff stays unacknowledged so QoS1
 * redelivers it; deliberately dropped poison is still acknowledged.
 *
 * <p>No broker involved: the producer is stubbed with pre-completed futures (which invoke
 * {@code whenComplete} synchronously, exactly as a settled future would) and the Paho client
 * is a mock observed for {@code messageArrivedComplete}.
 */
class MqttIngestAckTest {

  private ShoreProperties properties;
  private TelemetryKafkaProducer producer;
  private ShoreMetrics metrics;
  private MqttIngestService service;
  private MqttClient mqttClient;

  @BeforeEach
  void setUp() {
    properties = new ShoreProperties();
    producer = mock(TelemetryKafkaProducer.class);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    service = new MqttIngestService(
        properties, new TelemetryMessageParser(new ObjectMapper()), producer, metrics);
    mqttClient = mock(MqttClient.class);
    service.setClientForTests(mqttClient);
  }

  private static MqttMessage message(String json, int id) {
    MqttMessage msg = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
    msg.setId(id);
    msg.setQos(1);
    return msg;
  }

  @Test
  @DisplayName("Kafka success → exactly one MQTT acknowledgment with the same id/qos")
  @SuppressWarnings("unchecked")
  void ackOnlyAfterKafkaSuccess() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 42));

    verify(producer, times(1)).send(eq("ship.telemetry.raw"), any(TelemetryEnvelope.class));
    verify(mqttClient, times(1)).messageArrivedComplete(42, 1);
    assertEquals(1.0, metrics.getMqttReceivedTotal().count());
    // kafkaProduced counting lives inside TelemetryKafkaProducer (covered there);
    // this contract only asserts the MQTT acknowledgment followed the success.
  }

  @Test
  @DisplayName("Kafka failure → NO MQTT acknowledgment, failure counted for redelivery")
  @SuppressWarnings("unchecked")
  void noAckOnKafkaFailure() throws Exception {
    doReturn(CompletableFuture.<SendResult<String, TelemetryEnvelope>>failedFuture(
            new RuntimeException("broker down")))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 43));

    verify(mqttClient, never()).messageArrivedComplete(anyInt(), anyInt());
    assertEquals(1.0, metrics.getMqttReceivedTotal().count());
    // kafkaProduceFailed counting lives inside TelemetryKafkaProducer (covered there);
    // this contract only asserts the MQTT delivery stayed unacknowledged.
  }

  @Test
  @DisplayName("Synchronous send rejection → NO MQTT acknowledgment")
  void noAckOnSynchronousRejection() throws Exception {
    doThrow(new RuntimeException("buffer exhausted"))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 44));

    verify(mqttClient, never()).messageArrivedComplete(anyInt(), anyInt());
    assertEquals(1.0, metrics.getKafkaProduceFailedTotal().count());
  }

  @Test
  @DisplayName("Invalid payload → dropped, counted, but still acknowledged (no poison loop)")
  void invalidPayloadAckedAsDrop() throws Exception {
    service.messageArrived("zncb/413999999/nmea_gps", message("{not-json", 45));

    verify(producer, never()).send(anyString(), any(TelemetryEnvelope.class));
    verify(mqttClient, times(1)).messageArrivedComplete(45, 1);
    assertEquals(1.0, metrics.getMqttInvalidTotal().count());
  }
}
