package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
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
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.springframework.kafka.support.SendResult;

/**
 * P2-1.2 handoff contract: bounded synchronous handoff inside the (serial) Paho callback.
 *
 * <ul>
 *   <li>Kafka success → MQTT acknowledged exactly once, synchronously within the call.</li>
 *   <li>Kafka failure/timeout → NO acknowledgment and the MQTT connection is actively
 *   dropped so the durable session redelivers the original QoS1 on reconnect.</li>
 *   <li>Sequential deliveries acknowledge in arrival order — no future interleaving.</li>
 *   <li>Deliberately dropped poison is still acknowledged (no poison loop, no reconnect).</li>
 * </ul>
 *
 * <p>No broker involved and the service is never started: the redelivery executor is
 * created eagerly, so failure paths run deterministically while no background thread can
 * steal the mock client. The Paho client is a mock observed for
 * {@code messageArrivedComplete} / {@code disconnectForcibly}.
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
    properties.getMqtt().setKafkaHandoffTimeoutMs(2000L);
    producer = mock(TelemetryKafkaProducer.class);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    service = new MqttIngestService(
        properties, new TelemetryMessageParser(new ObjectMapper()), producer, metrics);
    mqttClient = mock(MqttClient.class);
    service.setClientForTests(mqttClient);
  }

  @AfterEach
  void tearDown() {
    service.stop();
  }

  private static MqttMessage message(String json, int id) {
    MqttMessage msg = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
    msg.setId(id);
    msg.setQos(1);
    return msg;
  }

  @Test
  @DisplayName("Kafka success → ACK exactly once, already done when the call returns")
  @SuppressWarnings("unchecked")
  void ackOnlyAfterKafkaSuccess() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 42));

    verify(producer, times(1)).send(eq("ship.telemetry.raw"), any(TelemetryEnvelope.class));
    // Synchronous handoff: the ACK is complete before messageArrived returns.
    verify(mqttClient, times(1)).messageArrivedComplete(42, 1);
    verify(mqttClient, never()).disconnectForcibly(anyLong());
    assertEquals(1.0, metrics.getMqttReceivedTotal().count());
  }

  @Test
  @DisplayName("Kafka failure → NO ACK and the MQTT connection is actively dropped")
  @SuppressWarnings("unchecked")
  void noAckAndDisconnectOnKafkaFailure() throws Exception {
    doReturn(CompletableFuture.<SendResult<String, TelemetryEnvelope>>failedFuture(
            new RuntimeException("broker down")))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 43));

    verify(mqttClient, never()).messageArrivedComplete(anyInt(), anyInt());
    // The forced reconnect runs on the background starter: observe it with a deadline.
    verify(mqttClient, timeout(5000).times(1)).disconnectForcibly(anyLong());
    assertEquals(1.0, metrics.getMqttReceivedTotal().count());
  }

  @Test
  @DisplayName("Kafka timeout → NO ACK and the MQTT connection is actively dropped")
  @SuppressWarnings("unchecked")
  void noAckAndDisconnectOnKafkaTimeout() throws Exception {
    // A future that never settles: the bounded wait must give up on its own.
    doReturn(new CompletableFuture<SendResult<String, TelemetryEnvelope>>())
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    long started = System.nanoTime();
    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 44));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertTrue(elapsedMs < 10000, "handoff must be bounded, took " + elapsedMs + " ms");
    verify(mqttClient, never()).messageArrivedComplete(anyInt(), anyInt());
    verify(mqttClient, timeout(5000).times(1)).disconnectForcibly(anyLong());
  }

  @Test
  @DisplayName("Synchronous send rejection → NO ACK, counted, reconnect forced")
  void noAckOnSynchronousRejection() throws Exception {
    doThrow(new RuntimeException("buffer exhausted"))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 45));

    verify(mqttClient, never()).messageArrivedComplete(anyInt(), anyInt());
    verify(mqttClient, timeout(5000).times(1)).disconnectForcibly(anyLong());
    assertEquals(1.0, metrics.getKafkaProduceFailedTotal().count());
  }

  @Test
  @DisplayName("Sequential deliveries ACK in arrival order: ACK1 before ACK2")
  @SuppressWarnings("unchecked")
  void ackOrderingFollowsArrival() throws Exception {
    // First handoff settles slowly on purpose; ordering must still follow arrival,
    // because the callback waits boundedly instead of racing two futures.
    CompletableFuture<SendResult<String, TelemetryEnvelope>> slow = new CompletableFuture<>();
    new Thread(() -> {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      slow.complete(mock(SendResult.class));
    }).start();
    doReturn(slow)
        .doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 51));
    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 52));

    InOrder order = inOrder(mqttClient);
    order.verify(mqttClient, times(1)).messageArrivedComplete(51, 1);
    order.verify(mqttClient, times(1)).messageArrivedComplete(52, 1);
    verify(mqttClient, never()).disconnectForcibly(anyLong());
  }

  @Test
  @DisplayName("Invalid payload → dropped, counted, still acknowledged, no reconnect")
  void invalidPayloadAckedAsDrop() throws Exception {
    service.messageArrived("zncb/413999999/nmea_gps", message("{not-json", 53));

    verify(producer, never()).send(anyString(), any(TelemetryEnvelope.class));
    verify(mqttClient, times(1)).messageArrivedComplete(53, 1);
    verify(mqttClient, never()).disconnectForcibly(anyLong());
    assertEquals(1.0, metrics.getMqttInvalidTotal().count());
  }

  @Test
  @DisplayName("Reconnect race: client reference is already visible while connect runs")
  void clientVisibleBeforeConnectCompletes() throws Exception {
    try (MockedConstruction<MqttClient> mocked = mockConstruction(MqttClient.class,
        (mock, context) -> doAnswer(invocation -> {
          // Inside connect(): the durable session may already redeliver, so the
          // reference must be published before connect, not after subscribe.
          assertSame(mock, service.getClientForTests());
          return null;
        }).when(mock).connect(any(MqttConnectOptions.class)))) {
      service.connectAndSubscribe();

      assertEquals(1, mocked.constructed().size());
      assertSame(mocked.constructed().get(0), service.getClientForTests());
    }
  }

  @Test
  @DisplayName("Connect failure leaves no stale client behind")
  void noStaleClientAfterConnectFailure() {
    try (MockedConstruction<MqttClient> mocked = mockConstruction(MqttClient.class,
        (mock, context) -> doThrow(new MqttException(32103))
            .when(mock).connect(any(MqttConnectOptions.class)))) {
      assertThrows(MqttException.class, () -> service.connectAndSubscribe());

      assertEquals(1, mocked.constructed().size());
      assertNull(service.getClientForTests(),
          "a half-connected client must never stay published");
    }
  }

  @Test
  @DisplayName("MQTT ACK failure is not success: forces redelivery reconnect, no ack observed")
  @SuppressWarnings("unchecked")
  void ackFailureTriggersRedelivery() throws Exception {
    doReturn(CompletableFuture.completedFuture(mock(SendResult.class)))
        .when(producer).send(anyString(), any(TelemetryEnvelope.class));
    doThrow(new MqttException(32102))
        .when(mqttClient).messageArrivedComplete(anyInt(), anyInt());
    java.util.List<Integer> acked = new java.util.concurrent.CopyOnWriteArrayList<>();
    service.setAckListener(acked::add);

    service.messageArrived("zncb/413999999/nmea_gps", message(EdgeFixtures.gpsPayload(), 61));

    verify(mqttClient, times(1)).messageArrivedComplete(61, 1);
    assertTrue(acked.isEmpty(), "a failed ACK must not count as acknowledged");
    // Same recovery as every other failure exit: forced reconnect on a background thread.
    verify(mqttClient, timeout(5000).times(1)).disconnectForcibly(anyLong());
  }
}
