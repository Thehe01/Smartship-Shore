package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

/**
 * Application ACK wiring: Kafka success emits one {@code ship/{mmsi}/ack},
 * Kafka failure emits none, and an ACK-publish failure never blocks the
 * original MQTT acknowledgment (the Kafka record is already durable).
 */
class ApplicationAckTest {

  private ShoreProperties properties;
  private TelemetryKafkaProducer producer;
  private MqttAckPublisher ackPublisher;
  private ShoreMetrics metrics;
  private MqttIngestService service;
  private MqttClient mqttClient;

  @BeforeEach
  void setUp() {
    properties = new ShoreProperties();
    properties.getMqtt().setKafkaHandoffTimeoutMs(2000L);
    producer = mock(TelemetryKafkaProducer.class);
    ackPublisher = mock(MqttAckPublisher.class);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    service = new MqttIngestService(
        properties, new TelemetryMessageParser(new ObjectMapper()), producer, metrics,
        ackPublisher);
    mqttClient = mock(MqttClient.class);
    service.setClientForTests(mqttClient);
  }

  @AfterEach
  void tearDown() {
    service.stop();
  }

  private static String payload() {
    return EdgeFixtures.gpsPayload();
  }

  private static MqttMessage message(String json, int id) {
    MqttMessage msg = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
    msg.setId(id);
    msg.setQos(1);
    return msg;
  }

  private static String expectedMsgId() {
    return EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00");
  }

  @Test
  @DisplayName("Parser exposes Edge row id as seq and keeps it in data")
  void parserExposesSeq() {
    TelemetryEnvelope env = new TelemetryMessageParser(new ObjectMapper()).parse(payload());
    assertEquals("1001", env.getSeq(), "row id echoes transparently for ACK matching");
  }

  @Test
  @DisplayName("Parser leaves seq null when the row carries no id")
  void parserSeqAbsentWithoutId() throws Exception {
    String json = "{\"mmsi\":\"413999999\",\"type\":\"nmea_gps\",\"msg_id\":\"abc\","
        + "\"sent_at\":\"2026-09-19T10:00:05+08:00\"}";
    TelemetryEnvelope env = new TelemetryMessageParser(new ObjectMapper()).parse(json);
    assertNull(env.getSeq(), "schemaless rows still flow; ACK matches on msg_id alone");
  }

  @Test
  @DisplayName("Kafka success → exactly one Application ACK with mmsi/msg_id/seq")
  @SuppressWarnings("unchecked")
  void ackPublishedOnKafkaSuccess() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));
    doReturn(true).when(ackPublisher).publishAck(anyString(), anyString(), anyString());

    service.messageArrived("zncb/413999999/nmea_gps", message(payload(), 7));

    verify(ackPublisher).publishAck(eq("413999999"), eq(expectedMsgId()), eq("1001"));
    verify(mqttClient).messageArrivedComplete(7, 1);
  }

  @Test
  @DisplayName("Kafka failure → no Application ACK and no MQTT acknowledgment")
  @SuppressWarnings("unchecked")
  void noAckOnKafkaFailure() throws Exception {
    CompletableFuture<SendResult<String, TelemetryEnvelope>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("kafka down"));
    doReturn(failed).when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(payload(), 8));

    verify(ackPublisher, never()).publishAck(anyString(), anyString(), anyString());
  }

  @Test
  @DisplayName("ACK-publish failure does not block the original MQTT acknowledgment")
  @SuppressWarnings("unchecked")
  void ackFailureNeverBlocksMqttAck() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));
    // The Kafka record is already durable: a lost ACK only costs Edge one resend.
    doReturn(false).when(ackPublisher).publishAck(anyString(), anyString(), anyString());

    service.messageArrived("zncb/413999999/nmea_gps", message(payload(), 9));

    verify(ackPublisher).publishAck(eq("413999999"), eq(expectedMsgId()), eq("1001"));
    verify(mqttClient).messageArrivedComplete(9, 1);
  }
}
